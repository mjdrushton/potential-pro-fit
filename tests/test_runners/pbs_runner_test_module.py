from atsim.pro_fit.runners._pbs_channel import PBSChannel
from atsim.pro_fit.runners import PBSRunner

name = "PBS"

Channel_Class = PBSChannel
Runner_Class = PBSRunner


def clearqueue(channel):
    import subprocess

    subprocess.call("qdel -W 0 all", shell=True)
    import time

    cleared = False
    while not cleared:
        time.sleep(0.5)
        output = subprocess.check_output(["qselect"])
        output = output.strip()
        cleared = not output


from ..testutil import _make_vagrant_box


def vagrant_box():
    return _make_vagrant_box("torque")
