
import logging
import sys

from ..testutil import vagrant_torque
from _runnercommon import runfixture, DIR, FILE, runnertestjob

from atsim import pro_fit

from atsim.pro_fit.runners._pbsrunner_batch import PBSRunnerJobRecord

def _createRunner(runfixture, vagrantbox, sub_batch_size):
  username = vagrantbox.user()
  hostname = vagrantbox.hostname()
  port = vagrantbox.port()
  keyfilename = vagrantbox.keyfile()

  extraoptions = [("StrictHostKeyChecking","no")]
  runner = pro_fit.runners.PBSRunner('PBSRunner', "ssh://%s@%s:%s" % (username, hostname, port),
    "", #pbsinclude
    qselect_poll_interval = 1.0,
    pbsbatch_size = sub_batch_size,
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

def tstSingleBatch(runfixture, vagrant_torque, sub_batch_size):
  # root = logging.getLogger()
  # root.setLevel(logging.DEBUG)

  # ch = logging.StreamHandler(sys.stdout)
  # ch.setLevel(logging.DEBUG)
  # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  # ch.setFormatter(formatter)
  # root.addHandler(ch)
  runner = _createRunner(runfixture, vagrant_torque, sub_batch_size)
  _runBatch(runner, runfixture.jobs).join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id, True)

def testAllInSingleBatch(runfixture, vagrant_torque):
  tstSingleBatch(runfixture, vagrant_torque, None)

def testAllInSingleBatch_sub_batch_size_1(runfixture, vagrant_torque):
  tstSingleBatch(runfixture, vagrant_torque, 1)

def testAllInSingleBatch_sub_batch_size_5(runfixture, vagrant_torque):
  tstSingleBatch(runfixture, vagrant_torque, 3)


def testAllInMultipleBatch(runfixture, vagrant_torque):
  runner = _createRunner(runfixture, vagrant_torque, None)
  f1 = _runBatch(runner, runfixture.jobs[:6])
  f2 = _runBatch(runner, runfixture.jobs[6:])
  f2.join()
  f1.join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id, True)

def testAllInMultipleBatch_sub_batch_size_5(runfixture, vagrant_torque):
  runner = _createRunner(runfixture, vagrant_torque, 5)
  f1 = _runBatch(runner, runfixture.jobs[:6])
  f2 = _runBatch(runner, runfixture.jobs[6:])
  f2.join()
  f1.join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id, True)

def testAllInMultipleBatch_sub_batch_size_1(runfixture, vagrant_torque):
  runner = _createRunner(runfixture, vagrant_torque, 1)
  f1 = _runBatch(runner, runfixture.jobs[:6])
  f2 = _runBatch(runner, runfixture.jobs[6:])
  f2.join()
  f1.join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id, True)


def testPBSRunnerJobRecordIsFull():
  class Client:
    pass

  jr = PBSRunnerJobRecord("name", 2, Client())
  assert not jr.isFull

  jr.append(None)
  assert not jr.isFull

  jr.append(None)
  assert jr.isFull

  try:
    jr.append(None)
    assert False, "IndexError not raised"
  except IndexError:
    pass
