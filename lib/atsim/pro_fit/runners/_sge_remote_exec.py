import uuid
import os
import subprocess


def submission_script(jobs, header_lines):
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

    stdout_stream = os.path.join(batchdir, "batch.o")
    stderr_stream = os.path.join(batchdir, "batch.e")

    std_headerlines = [
        "#$ -N pprofit",
        '#$ -o "%s"' % stdout_stream,
        '#$ -e "%s"' % stderr_stream,
        "#$ -S /bin/bash",
        "#$ -notify",
        "",
    ]

    jobs = [quote(j) for j in jobs]
    arrayjobline = "#$ %s 1-%d" % ("-t", len(jobs))
    lines = ["#! /bin/bash", arrayjobline]

    lines.extend(std_headerlines)
    lines.extend(header_lines)

    for i, j in enumerate(jobs):
        jobnum = i + 1
        line = 'JOB_ARRAY[%d]="%s"' % (jobnum, j)
        lines.append(line)
    lines.append('JOB_PATH="${JOB_ARRAY[$%s]}"' % "SGE_TASK_ID")

    bodylines = [
        "CLEANTMP=YES",
        'export RUNDIR="$(mktemp -d)"',
        'JOB_DIR="$(dirname "$JOB_PATH")"',
        'cp -r "$JOB_DIR"/* "$RUNDIR"',
        "function finish {",
        '        mkdir "$JOB_DIR/output"',
        '        cp -r *  "$JOB_DIR/output/"',
        '        if [ -n "$CLEANTMP" ];then',
        '          rm -rf "$RUNDIR"',
        "        fi",
        "}",
        "trap finish EXIT SIGUSR1 SIGUSR2",
        'cd "$RUNDIR"',
        'RUNSCRIPT="$(basename "$JOB_PATH")"',
        '"$SHELL" "$RUNSCRIPT" > STDOUT 2> STDERR',
        "echo $? > STATUS",
    ]

    lines.extend(bodylines)
    lines = os.linesep.join(lines)

    return lines


class QSelectException(Exception):
    pass


def qselect():
    p = subprocess.Popen(
        ["qstat"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    output, err = p.communicate()

    if p.returncode != 0:
        raise QSelectException(err.strip())

    output = output.strip()
    lines = output.split(os.linesep)

    if not lines:
        return []

    # Strip the header
    lines = lines[2:]
    job_ids = set([l.split()[0] for l in lines if l])

    return list(job_ids)


class QRlsException(Exception):
    pass


def qrls(job_ids):
    args = ["qrls"]
    args.extend(job_ids)

    p = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    output, err = p.communicate()

    if p.returncode != 0:
        raise QRlsException(err.strip())


class QDelException(Exception):
    pass


def qdel(job_ids, force):
    args = ["qdel"]

    # SGE qdel -f is only for admin user
    # if force:
    #   args.extend(["-f"])

    args.extend(job_ids)

    p = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    output, err = p.communicate()

    if p.returncode != 0:
        raise QDelException(err.strip())


def qdel_handler(channel, channel_id, msg):
    try:
        job_ids = msg["job_ids"]
    except KeyError:
        error(
            channel,
            "required field 'job_ids' missing from QDEL request",
            channel_id=channel_id,
        )
        return

    force = msg.get("force", False)

    qdel(job_ids, force)

    transid_send(channel, msg, "QDEL", channel_id=channel_id, job_ids=job_ids)


def qrls_handler(channel, channel_id, msg):
    try:
        job_id = msg["job_id"]
    except KeyError:
        error(
            channel,
            "required field 'job_id' missing from QRLS request",
            channel_id=channel_id,
        )
        return

    try:
        job_ids = [job_id]
        qrls(job_ids)
    except QRlsException as e:
        error(channel, str(e), channel_id=channel_id)
        return

    transid_send(channel, msg, "QRLS", channel_id=channel_id, job_id=job_id)


def qsub_handler(channel, channel_id, msg):
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
        if not os.path.isfile(p):
            error(
                channel,
                'no job found at path for QSUB request: "%s"' % p,
                channel_id=channel_id,
            )
            return

    header_lines = msg.get("header_lines", [])
    script = submission_script(jobs, header_lines)

    p = subprocess.Popen(
        ["qsub", "-h", "-terse"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    output, err = p.communicate(script)

    if p.returncode != 0:
        error(channel, err.strip(), channel_id=channel_id)
        return

    job_id = output.strip()
    job_id = job_id.split(".", 1)[0]

    transid_send(channel, msg, "QSUB", channel_id=channel_id, job_id=job_id)


def qselect_handler(channel, channel_id, msg):
    try:
        job_ids = qselect()
    except QSelectException as e:
        error(channel, str(e), channel_id=channel_id)

    transid_send(
        channel, msg, "QSELECT", channel_id=channel_id, job_ids=job_ids
    )


class NoSGEException(Exception):
    pass


def checkCommand(cmd):
    try:
        p = subprocess.Popen(
            [cmd, "-help"], stdout=subprocess.PIPE, close_fds=True
        )
        output, err = p.communicate()
    except OSError:
        raise NoSGEException("Could not run '%s'" % cmd)


def checkSGE():
    for cmd in ["qstat", "qrls", "qsub", "qdel"]:
        checkCommand(cmd)


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

    # Configure SGE
    try:
        checkSGE()
    except NoSGEException as e:
        msg = "SGE not found: " + str(e)
        error(channel, msg, channel_id=channel_id)
        return

    send(channel, "READY", channel_id=channel_id)

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

        handler(channel, channel_id, msg)


if __name__ == "__channelexec__":
    remote_exec(channel)
