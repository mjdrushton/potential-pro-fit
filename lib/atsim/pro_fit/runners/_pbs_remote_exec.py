import collections
import logging
import traceback
import uuid
import os
import subprocess
import re


torque_jobid_regex = re.compile("^([0-9]+?)(-([0-9]+?))?(\.(.*?))?(@.*?)?$")


def torque_split(pbs_id):
    m = torque_jobid_regex.match(pbs_id)
    groups = m.groups()
    jobnum = groups[0]
    subjob = groups[2]
    host = groups[4]
    return jobnum, subjob, host


def submission_script(pbsConfig, jobs, header_lines):
    try:
        # For python 3
        import shlex

        quote = shlex.quote
    except (ImportError, AttributeError):
        # For python 2.7
        import pipes

        quote = pipes.quote

    if jobs:
        batchdir = os.path.abspath(jobs[0])
        batchdir = os.path.dirname(batchdir)
        batchdir = os.path.dirname(batchdir)
    else:
        # This shouldn't happen
        batchdir = ""

    std_headerlines = [
        "#PBS -N pprofit",
        "#PBS -j oe",
        '#PBS -o "%s"' % batchdir,
        "",
    ]

    jobs = [quote(j) for j in jobs]
    singleJob = not len(jobs) > 1

    if singleJob:
        lines = []
    else:
        arrayjobline = "#PBS %s 1-%d" % (pbsConfig.arrayFlag, len(jobs))
        lines = [arrayjobline]

    lines.extend(std_headerlines)
    lines.extend(header_lines)

    if not singleJob:
        for i, j in enumerate(jobs):
            jobnum = i + 1
            line = 'JOB_ARRAY[%d]="%s"' % (jobnum, j)
            lines.append(line)
        lines.append('JOB_PATH="${JOB_ARRAY[$%s]}"' % pbsConfig.arrayIDVariable)
    else:
        lines.append('JOB_PATH="%s"' % jobs[0])

    bodylines = [
        'if [ -z "$TMPDIR" ];then',
        ' export TMPDIR="$(mktemp -d)"',
        " CLEANTMP=YES",
        "fi",
        'JOB_DIR="$(dirname "$JOB_PATH")"',
        'cp -r "$JOB_DIR"/* "$TMPDIR"',
        "function finish {",
        '        mkdir "$JOB_DIR/output"',
        '        cp -r *  "$JOB_DIR/output/"',
        '        if [ -n "$CLEANTMP" ];then',
        '          rm -rf "$TMPDIR"',
        "        fi",
        "}",
        "trap finish EXIT",
        'cd "$TMPDIR"',
        'RUNSCRIPT="$(basename "$JOB_PATH")"',
        '"$SHELL" "$RUNSCRIPT" > STDOUT 2> STDERR',
        "echo $? > STATUS",
    ]

    lines.extend(bodylines)
    lines = os.linesep.join(lines)
    return lines


def compressTORQUEArrayJobs(pbs_ids):
    # qselect on torque lists all the members of an array job
    # this function compresses the list so that only the parent
    # array jobs are listed
    outids = []
    for i in pbs_ids:
        jobid, subid, host = torque_split(i)
        newid = "".join([jobid, ".", host])
        outids.append(newid)

    outids = list(set(outids))
    return sorted(outids)


def uncompressTORQUEArrayJobs(pbs_id):
    allids = qselect()
    outids = []

    jobnum = torque_split(pbs_id)[0]

    for i in allids:
        g = torque_split(i)
        subjobnum = g[0]
        if subjobnum == jobnum:
            outids.append(i)
    return outids


class QSelectException(Exception):
    pass


def qselect():
    p = subprocess.Popen(
        ["qselect"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    output, err = p.communicate()

    if p.returncode != 0:
        raise QSelectException(err.strip())

    output = output.strip()
    pbs_ids = [i for i in output.split(os.linesep) if i]
    return pbs_ids


class QRlsException(Exception):
    pass


def qrls(pbs_ids):
    args = ["qrls"]
    args.extend(pbs_ids)

    p = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    output, err = p.communicate()

    if p.returncode != 0:
        raise QRlsException(err.strip())


class QDelException(Exception):
    pass


def qdel(pbs_ids, force, pbsConfig):
    args = ["qdel"]

    if force:
        args.extend(pbsConfig.qdelForceFlags)

    args.extend(pbs_ids)

    p = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    output, err = p.communicate()

    if p.returncode != 0:
        raise QDelException(err.strip())


def qdel_handler(channel, pbsConfig, channel_id, msg):
    try:
        pbs_ids = msg["job_ids"]
    except KeyError:
        error(
            channel,
            "required field 'job_ids' missing from QDEL request",
            channel_id=channel_id,
        )
        return

    force = msg.get("force", False)

    try:
        expanded = []
        if pbsConfig.flavour == "TORQUE":
            for pbs_id in pbs_ids:
                ids = uncompressTORQUEArrayJobs(pbs_id)
                expanded.extend(ids)
        else:
            expanded.extend(pbs_ids)
        qdel(pbs_ids, force, pbsConfig)
    except QDelException as e:
        error(channel, str(e), channel_id=channel_id)
        return

    transid_send(channel, msg, "QDEL", channel_id=channel_id, job_ids=pbs_ids)


def qrls_handler(channel, pbsConfig, channel_id, msg):
    try:
        pbs_id = msg["job_id"]
    except KeyError:
        error(
            channel,
            "required field 'job_id' missing from QRLS request",
            channel_id=channel_id,
        )
        return

    try:
        pbs_ids = [pbs_id]
        if pbsConfig.flavour == "TORQUE":
            pbs_ids = uncompressTORQUEArrayJobs(pbs_id)
        qrls(pbs_ids)
    except QRlsException as e:
        error(channel, str(e), channel_id=channel_id)
        return

    transid_send(channel, msg, "QRLS", channel_id=channel_id, job_id=pbs_id)


def qsub_handler(channel, pbsConfig, channel_id, msg):
    try:
        jobs = msg["jobs"]
    except KeyError:
        error(
            channel,
            "required field 'jobs' missing from QSUB request",
            channel_id=channel_id,
        )
        return

    # Check that the job files exist.
    jobs = [os.path.abspath(p) for p in jobs]
    for j in jobs:
        if not os.path.isfile(j):
            error(
                channel,
                'no job found at path for QSUB request: "%s"' % j,
                channel_id=channel_id,
            )
            return

    header_lines = msg.get("header_lines", [])
    script = submission_script(pbsConfig, jobs, header_lines)

    p = subprocess.Popen(
        ["qsub", "-h"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    output, err = p.communicate(script)

    if p.returncode != 0:
        error(channel, err.strip(), channel_id=channel_id)
        return

    transid_send(
        channel, msg, "QSUB", channel_id=channel_id, job_id=output.strip()
    )


def qselect_handler(channel, pbsConfig, channel_id, msg):
    try:
        pbs_ids = qselect()
    except QSelectException as e:
        error(channel, str(e), channel_id=channel_id)

    if pbsConfig.flavour == "TORQUE":
        pbs_ids = compressTORQUEArrayJobs(pbs_ids)

    transid_send(
        channel, msg, "QSELECT", channel_id=channel_id, job_ids=pbs_ids
    )


PBSIdentifyRecord = collections.namedtuple(
    "PBSIdentifyRecord",
    ["arrayFlag", "arrayIDVariable", "qdelForceFlags", "flavour"],
)


def pbsIdentify(versionString):
    """Given output of qstat --version, return a record containing fields used to configure
  PBSRunner for the version of PBS being used.

  Record has following fields:
    arrayFlag - The qsub flag used to specify array job rangs.
    arrayIDVariable - Environment variable name provided to submission script to identify ID of current array sub-job.

  @param versionString String as returned by qstat --versionString
  @return Field of the form described above"""
    logger = logging.getLogger("atsim.pro_fit.runners.PBSRunner.pbsIdentify")
    import re

    if re.search("PBSPro", versionString):
        # PBS Pro
        logger.info("Identified PBS as: PBSPro")
        record = PBSIdentifyRecord(
            arrayFlag="-J",
            arrayIDVariable="PBS_ARRAY_INDEX",
            qdelForceFlags=["-Wforce"],
            flavour="PBSPro",
        )
    else:
        # TORQUE
        logger.info("Identified PBS as: TORQUE")
        record = PBSIdentifyRecord(
            arrayFlag="-t",
            arrayIDVariable="PBS_ARRAYID",
            qdelForceFlags=["-W", "0"],
            flavour="TORQUE",
        )

    logger.debug("pbsIdentify record: %s" % str(record))
    return record


class NoPBSException(Exception):
    pass


def checkPBS():
    # Attempt to run qstat
    try:
        p = subprocess.Popen(
            ["qselect"], stdout=subprocess.PIPE, close_fds=True
        )
        output, err = p.communicate()
    except OSError:
        raise NoPBSException("Could not run 'qselect'")

    try:
        p = subprocess.Popen(
            ["qsub", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        output, err = p.communicate()
        output = output.strip()
        err = err.strip()
        if err:
            sstring = err
        else:
            sstring = output
    except OSError:
        raise NoPBSException("Could not run 'qsub'")

    return sstring


def send(channel, mtype, **kwargs):
    msgdict = dict(msg=mtype)
    msgdict.update(kwargs)
    channel.send(msgdict)


def transid_send(channel, origmsg, mtype, **kwargs):
    kwargs = dict(kwargs)
    transid = origmsg.get("transaction_id", None)

    if transid:
        kwargs["transaction_id"] = transid

    send(channel, mtype, **kwargs)


def error(channel, reason, **kwargs):
    send(channel, "ERROR", reason=reason, **kwargs)


def remote_exec(channel):
    channel_id = None
    msghandlers = dict(
        QSUB=qsub_handler,
        QSELECT=qselect_handler,
        QRLS=qrls_handler,
        QDEL=qdel_handler,
    )

    msg = channel.receive()

    try:
        mtype = msg["msg"]
    except:
        error(
            channel,
            "malformed message. Was expecting msg='START_CHANNEL', received %s "
            % msg,
        )
        return

    if mtype != "START_CHANNEL":
        error(channel, "Was expecting msg='START_CHANNEL', received %s " % msg)
        return

    channel_id = msg.get("channel_id", str(uuid.uuid4()))

    # Configure PBS
    try:
        versionstring = checkPBS()
    except NoPBSException as e:
        msg = "PBS not found: %s" % str(e)
        error(channel, msg, channel_id=channel_id)
        return

    pbsConfig = pbsIdentify(versionstring)
    send(
        channel,
        "READY",
        channel_id=channel_id,
        pbs_identify=dict(pbsConfig._asdict()),
    )

    for msg in channel:

        if msg is None:
            return

        try:
            mtype = msg["msg"]
        except:
            error(
                channel,
                "malformed message, could not find 'msg' field in %s" % msg,
                channel_id=channel_id,
            )
            continue
        try:
            handler = msghandlers[mtype]
        except KeyError:
            error(
                channel,
                "unknown message type '%s' for message: %s" % (mtype, msg),
                channel_id=channel_id,
            )
            continue

        handler(channel, pbsConfig, channel_id, msg)


if __name__ == "__channelexec__":
    remote_exec(channel)
