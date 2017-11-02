
import logging
import sys
import os

from ..testutil import vagrant_torque
from _runnercommon import runfixture, DIR, FILE, runnertestjob

from atsim import pro_fit

from atsim.pro_fit.runners._pbsrunner_batch import PBSRunnerJobRecord
from atsim.pro_fit.runners._pbs_client import PBSChannel

from test_pbs_remote_exec import _mkexecnetgw, clearqueue
from test_pbs_client import chIsDir

import gevent

import pytest
# pytestmark = pytest.mark.skip()

def _createRunner(runfixture, vagrantbox, sub_batch_size, pbsinclude = ""):
  username = vagrantbox.user()
  hostname = vagrantbox.hostname()
  port = vagrantbox.port()
  keyfilename = vagrantbox.keyfile()

  extraoptions = [("StrictHostKeyChecking","no")]
  runner = pro_fit.runners.PBSRunner('PBSRunner', "ssh://%s@%s:%s" % (username, hostname, port),
    pbsinclude,
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

def _remoteIsDir(gw, path):
  ch = gw.remote_exec(chIsDir)
  ch.send(path)
  return ch.receive()

def waitcb(f):
    while not f._submittedPBSRecords:
      gevent.sleep(1)

def makesleepy(jobs):
  for j in jobs:
    with open(os.path.join(j.path, 'job_files', 'runjob'), 'a') as runjob:
      runjob.write("sleep 1200\n")


def testBatchTerminate(clearqueue, runfixture):
  """Test batch .terminate() method."""

  # Make some sleepy jobs
  makesleepy(runfixture.jobs)
  try:
    runner = _createRunner(runfixture, clearqueue, None)
    indyrunner = _createRunner(runfixture, clearqueue, None)

    f1 = runner.runBatch(runfixture.jobs[:6])
    f2 = runner.runBatch(runfixture.jobs[6:8])

    # Create a second runner to make sure that closing one runner doesn't affect the other.
    if3 = indyrunner.runBatch(runfixture.jobs[8:])

    assert gevent.wait([
      gevent.spawn(waitcb, f1),
      gevent.spawn(waitcb, f2),
      gevent.spawn(waitcb, if3)],60)

    jr1 = f1._submittedPBSRecords[0]
    jr2 = f2._submittedPBSRecords[0]
    ij3 = if3._submittedPBSRecords[0]

    assert jr1.pbs_submit_event.wait(60)
    assert jr1.pbsId

    assert jr2.pbs_submit_event.wait(60)
    assert jr2.pbsId

    assert ij3.pbs_submit_event.wait(60)
    assert ij3.pbsId

    gevent.sleep(0)

    jr1_id = jr1.pbsId
    jr2_id = jr2.pbsId

    # Spin up a pbs_channel and check we can see the two jobs
    gw = _mkexecnetgw(clearqueue)
    ch = PBSChannel(gw, 'check_channel', nocb = True)
    try:
      def qsel():
        ch.send({'msg': 'QSELECT'})
        msg = ch.next()
        assert 'QSELECT' == msg.get('msg', None)
        running_pbsids = set(msg['pbs_ids'])
        return running_pbsids

      pbsids = set([jr1.pbsId, jr2.pbsId])
      running_pbsids = qsel()
      assert pbsids.issubset(running_pbsids)
      assert pbsids != running_pbsids

      # Check the job directories exist
      for j in f1.jobs:
        assert _remoteIsDir(gw, j.remotePath)

      for j in f2.jobs:
        assert _remoteIsDir(gw, j.remotePath)

      for j in if3.jobs:
        assert _remoteIsDir(gw, j.remotePath)


      # Now close the second batch
      closevent = f2.terminate()
      assert closevent.wait(60)
      attempts = 5
      delay = 1
      for i in xrange(5):
        try:
          assert qsel() == set([jr1.pbsId, ij3.pbsId])
        except AssertionError:
          if i == attempts - 1:
            raise
          else:
            gevent.sleep(delay)
            delay *= 2.0

      # Check the job directories exist
      for j in f1.jobs:
        assert _remoteIsDir(gw, j.remotePath)

      for j in f2.jobs:
        assert not _remoteIsDir(gw, j.remotePath)

      for j in if3.jobs:
        assert _remoteIsDir(gw, j.remotePath)

      # Now close the first batch
      closevent = f1.terminate()
      assert closevent.wait(60)
      attempts = 5
      delay = 1
      for i in xrange(5):
        try:
          assert qsel() == set([ij3.pbsId])
        except AssertionError:
          if i == attempts - 1:
            raise
          else:
            gevent.sleep(delay)
            delay *= 2.0

      # Check the job directories exist
      for j in f1.jobs:
        assert not _remoteIsDir(gw, j.remotePath)

      for j in f2.jobs:
        assert not _remoteIsDir(gw, j.remotePath)

      for j in if3.jobs:
        assert _remoteIsDir(gw, j.remotePath)

      # Now close the second runner's batch
      closevent = if3.terminate()
      assert closevent.wait(60)

      closevent = f1.terminate()
      assert closevent.wait(60)
      attempts = 5
      delay = 1
      for i in xrange(5):
        try:
          assert qsel() == set()
        except AssertionError:
          if i == attempts - 1:
            raise
          else:
            gevent.sleep(delay)
            delay *= 2.0

      # Check the job directories exist
      for j in f1.jobs:
        assert not _remoteIsDir(gw, j.remotePath)

      for j in f2.jobs:
        assert not _remoteIsDir(gw, j.remotePath)

      for j in if3.jobs:
        assert not _remoteIsDir(gw, j.remotePath)

    finally:
      ch.send(None)
  finally:
    runner.close()
    indyrunner.close()

def testRunnerClose(clearqueue, runfixture):
  """Test batch .terminate() method."""

  # Make some sleepy jobs

  # root = logging.getLogger()
  # root.setLevel(logging.DEBUG)

  # ch = logging.StreamHandler(sys.stdout)
  # ch.setLevel(logging.DEBUG)
  # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  # ch.setFormatter(formatter)
  # root.addHandler(ch)

  for j in runfixture.jobs:
    with open(os.path.join(j.path, 'job_files', 'runjob'), 'a') as runjob:
      runjob.write("sleep 1200\n")

  runner = _createRunner(runfixture, clearqueue, None)
  indyrunner = _createRunner(runfixture, clearqueue, None)

  f1 = runner.runBatch(runfixture.jobs[:6])
  f2 = runner.runBatch(runfixture.jobs[6:8])

  # Create a second runner to make sure that closing one runner doesn't affect the other.
  if3 = indyrunner.runBatch(runfixture.jobs[8:])

  assert gevent.wait([
    gevent.spawn(waitcb, f1),
    gevent.spawn(waitcb, f2),
    gevent.spawn(waitcb, if3)],60)

  jr1 = f1._submittedPBSRecords[0]
  jr2 = f2._submittedPBSRecords[0]
  ij3 = if3._submittedPBSRecords[0]

  assert jr1.pbs_submit_event.wait(60)
  assert jr1.pbsId

  assert jr2.pbs_submit_event.wait(60)
  assert jr2.pbsId

  assert ij3.pbs_submit_event.wait(60)
  assert ij3.pbsId

  gevent.sleep(0)

  jr1_id = jr1.pbsId
  jr2_id = jr2.pbsId

  # Spin up a pbs_channel and check we can see the two jobs
  gw = _mkexecnetgw(clearqueue)
  ch = PBSChannel(gw, 'check_channel', nocb = True)
  try:
    def qsel():
      ch.send({'msg': 'QSELECT'})
      msg = ch.next()
      assert 'QSELECT' == msg.get('msg', None)
      running_pbsids = set(msg['pbs_ids'])
      return running_pbsids

    pbsids = set([jr1.pbsId, jr2.pbsId])
    running_pbsids = qsel()
    assert pbsids.issubset(running_pbsids)
    assert pbsids != running_pbsids

    # Check the job directories exist
    for j in f1.jobs:
      assert _remoteIsDir(gw, j.remotePath)

    for j in f2.jobs:
      assert _remoteIsDir(gw, j.remotePath)

    for j in if3.jobs:
      assert _remoteIsDir(gw, j.remotePath)


    # Now close the runner
    closevent = runner.close()
    assert closevent.wait(60)
    attempts = 5
    delay = 5
    for i in xrange(5):
      try:
        assert qsel() == set([ij3.pbsId])
      except AssertionError:
        if i == attempts - 1:
          raise
        else:
          gevent.sleep(delay)
          delay *= 2.0

    # Check the job directories exist
    for j in f1.jobs:
      assert not _remoteIsDir(gw, j.remotePath)

    for j in f2.jobs:
      assert not _remoteIsDir(gw, j.remotePath)

    for j in if3.jobs:
      assert _remoteIsDir(gw, j.remotePath)

    # Now close the first batch
    closevent = f1.terminate()
    assert closevent.wait(60)
    attempts = 5
    delay = 1
    for i in xrange(5):
      try:
        assert qsel() == set([ij3.pbsId])
      except AssertionError:
        if i == attempts - 1:
          raise
        else:
          gevent.sleep(delay)
          delay *= 2.0

    # Check the job directories exist
    for j in f1.jobs:
      assert not _remoteIsDir(gw, j.remotePath)

    for j in f2.jobs:
      assert not _remoteIsDir(gw, j.remotePath)

    for j in if3.jobs:
      assert _remoteIsDir(gw, j.remotePath)


    try:
      runner._inner._pbschannel.send({'msg' : 'QSELECT'})
      assert False, "IOError not raised"
    except IOError:
      pass

  finally:
    ch.send(None)

def _tstSingleBatch(runfixture, vagrant_torque, sub_batch_size):
  # root = logging.getLogger()
  # root.setLevel(logging.DEBUG)

  # ch = logging.StreamHandler(sys.stdout)
  # ch.setLevel(logging.DEBUG)
  # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  # ch.setFormatter(formatter)
  # root.addHandler(ch)
  runner = _createRunner(runfixture, vagrant_torque, sub_batch_size)
  try:
    _runBatch(runner, runfixture.jobs).join()
    for job in runfixture.jobs:
      runnertestjob(runfixture, job.variables.id, True)
  finally:
    runner.close()

def testAllInSingleBatch(runfixture, clearqueue):
  _tstSingleBatch(runfixture, clearqueue, None)

def testAllInSingleBatch_sub_batch_size_1(runfixture, clearqueue):
  _tstSingleBatch(runfixture, clearqueue, 1)

def testAllInSingleBatch_sub_batch_size_5(runfixture, clearqueue):
  _tstSingleBatch(runfixture, clearqueue, 5)

def testAllInMultipleBatch(runfixture, clearqueue):
  runner = _createRunner(runfixture, clearqueue, None)
  try:
    f1 = _runBatch(runner, runfixture.jobs[:6])
    f2 = _runBatch(runner, runfixture.jobs[6:])
    f2.join()
    f1.join()
    for job in runfixture.jobs:
      runnertestjob(runfixture, job.variables.id, True)
  finally:
    runner.close()

def testAllInMultipleBatch_sub_batch_size_5(runfixture, clearqueue):
  runner = _createRunner(runfixture, clearqueue, 5)
  try:
    f1 = _runBatch(runner, runfixture.jobs[:6])
    f2 = _runBatch(runner, runfixture.jobs[6:])
    f2.join()
    f1.join()
    for job in runfixture.jobs:
      runnertestjob(runfixture, job.variables.id, True)
  finally:
    runner.close()

def testAllInMultipleBatch_sub_batch_size_1(runfixture, clearqueue):
  runner = _createRunner(runfixture, clearqueue, 1)
  try:
    f1 = _runBatch(runner, runfixture.jobs[:6])
    f2 = _runBatch(runner, runfixture.jobs[6:])
    f2.join()
    f1.join()
    for job in runfixture.jobs:
      runnertestjob(runfixture, job.variables.id, True)
  finally:
    runner.close()

def testPBSRunnerJobRecordIsFull():
  class Client:
    pass

  jr = PBSRunnerJobRecord("name", 2, Client(), None)
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

def qstatRemoteExec(channel):
  pbsId = channel.receive()
  import subprocess
  output = subprocess.check_output(['qstat', '-f', pbsId])
  channel.send(output)

def testPBSInclude(runfixture, clearqueue):
  makesleepy(runfixture.jobs)
  runner = _createRunner(runfixture, clearqueue, 1, "#PBS -l mem=10Mb\n#PBS -l walltime=5:00")
  try:
    batch = _runBatch(runner, [runfixture.jobs[0]])
    gevent.wait([gevent.spawn(waitcb, batch)], 60)

    j = batch._submittedPBSRecords[0]
    assert j.pbs_submit_event.wait(60)
    assert j.pbsId

    pbsid = j.pbsId

    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(qstatRemoteExec)
    ch.send(pbsid)
    qstat = ch.receive()
    qstat = qstat.split("\n")

    # for i in xrange(5):
    #   ch = gw.remote_exec(qstatRemoteExec)
    #   ch.send(pbsid)
    #   qstat = ch.receive()
    #   qstat = qstat.split("\n")
    #   if

    memline = [ line for line in qstat if line.strip().startswith("Resource_List.mem")]
    walltimeline = [ line for line in qstat if line.strip().startswith("Resource_List.walltime")]

    assert len(walltimeline) == 1
    walltimeline= walltimeline[0]
    assert walltimeline.endswith("00:05:00")

    assert len(memline) == 1
    memline = memline[0]
    assert memline.endswith("10mb")
  finally:
    runner.close()
