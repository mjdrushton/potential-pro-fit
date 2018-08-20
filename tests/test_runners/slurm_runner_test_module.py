from atsim.pro_fit.runners._slurm_channel import SlurmChannel

name = "Slurm"

Channel_Class = SlurmChannel

def clearqueue(channel):
    import subprocess
    subprocess.call("scancel --user=vagrant --signal=KILL", shell = True)
    import time
    cleared = False
    while not cleared:
      time.sleep(0.5)
      output = subprocess.check_output("squeue -h", shell = True)
      output = output.strip()
      cleared = not output

from ..testutil import _make_vagrant_box

def vagrant_box():
  return _make_vagrant_box("slurm")


