from ..testutil import _make_vagrant_box
from atsim.pro_fit.runners._sge_channel import SGEChannel
from atsim.pro_fit.runners import SGERunner

name = "SGE"

Channel_Class = SGEChannel
Runner_Class = SGERunner


def clearqueue(channel):
    import subprocess
    import os

    output = subprocess.check_output(["qstat"])
    if not output.strip():
        return
    lines = output.strip().split(os.linesep)
    lines = lines[2:]
    job_ids = []
    for l in lines:
        job_ids.append(l.split()[0])

    for job_id in job_ids:
        cmd = "qdel %s" % job_id
        print(cmd)
        subprocess.call(cmd, shell=True)
    import time

    cleared = False
    while not cleared:
        time.sleep(0.5)
        output = subprocess.check_output(["qstat"])
        output = output.strip()
        cleared = not output


def vagrant_box():
    return _make_vagrant_box("SGE")
