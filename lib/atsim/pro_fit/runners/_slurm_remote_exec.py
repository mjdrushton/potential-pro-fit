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
        "#SBATCH -J pprofit",
        '#SBATCH -o "%s"' % stdout_stream,
        '#SBATCH -e "%s"' % stderr_stream,
        "",
    ]

    jobs = [quote(j) for j in jobs]
    arrayjobline = "#SBATCH %s1-%d" % ("--array=", len(jobs))
    lines = ["#! /bin/bash", arrayjobline]

    lines.extend(std_headerlines)
    lines.extend(header_lines)

    for i, j in enumerate(jobs):
        jobnum = i + 1
        line = 'JOB_ARRAY[%d]="%s"' % (jobnum, j)
        lines.append(line)
    lines.append('JOB_PATH="${JOB_ARRAY[$%s]}"' % "SLURM_ARRAY_TASK_ID")

    bodylines = [
        "CLEANTMP=YES",
        'export TMPDIR="$(mktemp -d)"',
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


class QSelectException(Exception):
    pass


def qselect():
    p = subprocess.Popen(
        ["squeue", "-h", "-o", "%F"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    output, err = p.communicate()

    if p.returncode != 0:
        raise QSelectException(err.strip())

    output = output.strip()
    job_ids = [i for i in output.split(os.linesep) if i]
    return job_ids


class QRlsException(Exception):
    pass


def qrls(job_ids):
    args = ["scontrol", "release"]
    args.extend(job_ids)

    p = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    _output, err = p.communicate()

    if p.returncode != 0:
        raise QRlsException(err.strip())


class QDelException(Exception):
    pass


def qdel(job_ids, force):
    args = ["scancel"]

    if force:
        args.extend(["--signal=KILL"])

    args.extend(job_ids)

    p = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    _output, err = p.communicate()

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
        if not os.path.isfile(j):
            error(
                channel,
                'no job found at path for QSUB request: "%s"' % j,
                channel_id=channel_id,
            )
            return

    header_lines = msg.get("header_lines", [])
    script = submission_script(jobs, header_lines)

    p = subprocess.Popen(
        ["sbatch", "-H"],
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
    job_id = job_id.split()[-1]

    transid_send(channel, msg, "QSUB", channel_id=channel_id, job_id=job_id)


def qselect_handler(channel, channel_id, msg):
    try:
        job_ids = qselect()
    except QSelectException as e:
        error(channel, str(e), channel_id=channel_id)

    transid_send(
        channel, msg, "QSELECT", channel_id=channel_id, job_ids=job_ids
    )


class NoSlurmException(Exception):
    pass


def checkSlurm():
    # Attempt to run qstat
    try:
        p = subprocess.Popen(
            ["squeue"], stdout=subprocess.PIPE, close_fds=True
        )
        output, err = p.communicate()
    except OSError:
        raise NoSlurmException("Could not run 'squeue'")

    try:
        p = subprocess.Popen(
            ["sbatch", "--version"],
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
        raise NoSlurmException("Could not run 'qsub'")

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

    # Configure Slurm
    try:
        checkSlurm()
    except NoSlurmException as e:
        msg = "Slurm not found: " + str(e)
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
    remote_exec(channel) # pylint: disable=undefined-variable
