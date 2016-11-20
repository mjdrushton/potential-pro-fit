
from assertpy import assert_that

from ..testutil import vagrant_torque
from _runnercommon import runfixture, DIR, FILE, runnertestjob

from atsim import pro_fit

def _createRunner(runfixture, vagrantbox):
  username = vagrantbox.user()
  hostname = vagrantbox.hostname()
  port = vagrantbox.port()
  keyfilename = vagrantbox.keyfile()

  pbsIdentify = PBSIdentifyRecord(arrayFlag="-t", arrayIDVariable = "PBS_ARRAYID", flavour = "TORQUE")

  extraoptions = [("StrictHostKeyChecking","no")]
  runner = pro_fit.runners.PBSRunner('PBSRunner', "ssh://%s@%s:%s" % (username, hostname, port),
    "", #pbsinclude
    pbsIdentify,
    identityfile = keyfilename,
    extra_ssh_options = extraoptions)

  return runner

# Useful for testing againts HPC
# def _createRunner(runfixture, vagrantbox):
#   username = vagrantbox.user()
#   hostname = vagrantbox.hostname()
#   port = vagrantbox.port()
#   keyfilename = vagrantbox.keyfile()

#   pbsIdentify = PBSIdentifyRecord(arrayFlag="-J", arrayIDVariable = "PBS_ARRAY_INDEX", flavour = "PBSPro")

#   runner = pro_fit.runners.PBSRunner('PBSRunner', "ssh://mjdr@login.cx1.hpc.ic.ac.uk:/home/mjdr/pprofit_jobs",
#     "", #pbsinclude
#     pbsIdentify)

#   return runner

def _runBatch(runner, jobs):
  return runner.runBatch(jobs)

def testTerminate():
  """Test runner's .terminate() method."""
  assert(False)

def testClose():
  """Test runner's .close() method."""
  assert(False)

def testAllInSingleBatch(runfixture, vagrant_torque):
  runner = _createRunner(runfixture, vagrant_torque)
  _runBatch(runner, runfixture.jobs).join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id)

def testAllInMultipleBatch(runfixture, vagrant_torque):
  runner = _createRunner(runfixture, vagrant_torque)
  # import pdb;pdb.set_trace()
  f1 = _runBatch(runner, runfixture.jobs[:6])
  f2 = _runBatch(runner, runfixture.jobs[6:])
  f2.join()
  f1.join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id)

