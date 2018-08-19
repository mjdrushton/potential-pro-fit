from atsim.pro_fit._channel import ChannelException

from ..testutil import vagrant_basic
from _runnercommon import channel_id, mkrunjobs

from test_pbs_remote_exec import _mkexecnetgw #, clearqueue
# from atsim.pro_fit.runners._pbs_channel import PBSChannel

import atsim.pro_fit.runners._queueing_system_client as generic_client
from gevent.event import Event
import gevent

from contextlib import closing

import pytest

@pytest.fixture(scope = "session", params = [
                                              "tests.test_runners.slurm_runner_test_module", 
                                              "tests.test_runners.pbs_runner_test_module"])
def runner_test_module(request):
  return pytest.importorskip(request.param)

@pytest.fixture(scope = "session")
def vagrant_box(runner_test_module):
  box = runner_test_module.vagrant_box()
  yield box
  box.suspend()

@pytest.fixture(scope="function")
def gw(vagrant_box):
  gw = _mkexecnetgw(vagrant_box)
  return gw

@pytest.fixture(scope = "function")
def channel(channel_id, runner_test_module, gw):
  ch = runner_test_module.Channel_Class(gw, channel_id)
  return ch

@pytest.fixture(scope = "function")
def client(channel):
  client = generic_client.QueueingSystemClient(channel, pollEvery = 1.0)
  return client

@pytest.fixture(scope = "function")
def clearqueue(gw, runner_test_module):
  ch = gw.remote_exec(runner_test_module.clearqueue)
  ch.waitclose(20)

def _mkclient(channel_id, channel_class, vagrant_box):
  gw = _mkexecnetgw(vagrant_box)
  ch = channel_class(gw, channel_id)
  client = generic_client.QueueingSystemClient(ch, pollEvery = 0.5)
  return client

def testStartChannel(client):
  client.close()

def testHostHasNoPbs(vagrant_basic, runner_test_module, channel_id):
    with pytest.raises(ChannelException):
      _mkclient('testHostHasNoPbs', runner_test_module.Channel_Class, vagrant_basic)

class JobsChangedListener(generic_client.QueueingSystemStateListenerAdapter):

  def __init__(self):
    self.event = Event()
    self.reset()

  def jobsChanged(self, oldJobs, newJobs):
    if self.event.isSet():
      return
    self.oldJobs = oldJobs
    self.newJobs = newJobs
    self.event.set()

  def reset(self):
    self.oldJobs = None
    self.newJobs = None
    self.event.clear()

class QSUBCallback(object):

  def __init__(self):
    self.event = Event()
    self.reset()

  def __call__(self, msg):
    mtype = msg.get("msg", None)

    if mtype == "QSUB":
      self.lastPbsID = msg.get("job_id", None)
      self.event.set()

  def reset(self):
    self.event.clear()
    self.lastPbsID = None

@pytest.mark.usefixtures("clearqueue")
def testQueuingSystemState(gw, channel, channel_id):
  qsubcallback = QSUBCallback()
  channel.callback.append(qsubcallback)

  # Instantiate QueueingSystemState
  state = generic_client.QueueingSystemState(channel, pollEvery = 1.0)
  listener = JobsChangedListener()
  state.listeners.append(listener)

  # Create some jobs
  clch, runjobs = mkrunjobs(gw, 5)
  try:
    assert not state._jobIds
    channel.send({'msg' : 'QSUB', 'jobs' : runjobs[:3]})
    assert qsubcallback.event.wait(10)
    pbs_id_1 = qsubcallback.lastPbsID
    qsubcallback.reset()
    assert not pbs_id_1 is None

    assert listener.event.wait(10)
    assert list(listener.oldJobs) == []
    assert sorted(listener.newJobs) == sorted([pbs_id_1])
    listener.reset()

    channel.send({'msg' : 'QSUB', 'jobs' : runjobs[3:]})
    assert qsubcallback.event.wait(10)
    pbs_id_2 = qsubcallback.lastPbsID
    qsubcallback.reset()
    assert not pbs_id_2 is None

    assert listener.event.wait(10)
    assert sorted(listener.oldJobs) == sorted([pbs_id_1])
    assert sorted(listener.newJobs) == sorted([pbs_id_1, pbs_id_2])
    listener.reset()

    channel.send({'msg' : 'QRLS', 'job_id' : pbs_id_2 })
    assert listener.event.wait(30)
    assert sorted(listener.oldJobs) == sorted([pbs_id_1, pbs_id_2])
    assert sorted(listener.newJobs) == sorted([pbs_id_1])
    listener.reset()

    channel.send({'msg' : 'QDEL', 'job_ids' : [pbs_id_1] })
    assert listener.event.wait(30)
    assert sorted(listener.oldJobs) == sorted([pbs_id_1])
    assert sorted(listener.newJobs) == sorted([])
    listener.reset()

  finally:
    clch.send(None)

def testQueueStateAddRemovePrivateListener():
  class Listener(generic_client.QueueingSystemStateListenerAdapter):

    def __init__(self):
      self.reset()

    def jobsAdded(self, pbsIds):
      self.jobsAddedCalled = True
      self.addedIds = pbsIds

    def jobsRemoved(self, pbsIds):
      self.jobsRemovedCalled = True
      self.removedIds = pbsIds

    def reset(self):
      self.jobsAddedCalled = False
      self.jobsRemovedCalled = False
      self.addedIds = None
      self.removedIds = None

  listener = Listener()
  listenerList = [listener]
  addRemoveListener = generic_client._AddRemoveListener(listenerList)

  addRemoveListener.jobsChanged(set(), set(["a", "b", "c"]))

  assert not listener.jobsRemovedCalled
  assert listener.jobsAddedCalled
  assert listener.addedIds == set(["a", "b", "c"])
  listener.reset()

  addRemoveListener.jobsChanged(set(["a", "b", "c"]), set(["a", "b", "c", "d", "e"]))

  assert not listener.jobsRemovedCalled
  assert listener.jobsAddedCalled
  assert listener.addedIds == set(["d", "e"])
  listener.reset()

  addRemoveListener.jobsChanged(set(["a", "b", "c", "d", "e"]), set(["a", "c", "f"]))

  assert listener.jobsRemovedCalled
  assert listener.jobsAddedCalled
  assert listener.addedIds == set(["f"])
  assert listener.removedIds == set(["b", "d", "e"])
  listener.reset()

def chIsDir(channel):
  pth = channel.receive()
  import os
  channel.send(os.path.isdir(pth))

def chIsFile(channel):
  pth = channel.receive()
  import os
  channel.send(os.path.isfile(pth))

def chFileContents(channel):
  pth = channel.receive()

  with open(pth,'r') as infile:
    channel.send(infile.read())

class TstCallback(object):

  def __init__(self):
    self.called = False

  def __call__(self, exception):
    self.called = True
    self.exception = exception

@pytest.mark.usefixtures("clearqueue")
def testQueueingSystemClientSingleJob(gw, client):
    # gw = _mkexecnetgw(clearqueue)
    clch, runjobs = mkrunjobs(gw, 1, numSuffix = True)
    try:
      # client = _mkclient('testPBSClientSingleJob', clearqueue)
      # with closing(_mkclient('testPBSClientSingleJob', clearqueue)) as client:
      j1cb = TstCallback()
      jr1 = client.runJobs([runjobs[0]], j1cb)

      # Wait for job submission
      assert jr1.qsubEvent.wait(120)
      assert len(client._qsState._jobIds) == 1
      assert list(client._qsState._jobIds)[0] == jr1.jobId
      # Now wait for the job to complete
      assert jr1.completion_event.wait(120)
      assert j1cb.called
      assert j1cb.exception is None

      # Now check the contents of the output directory
      ch = gw.remote_exec(chIsDir)

      import posixpath
      outputdir = posixpath.join(posixpath.dirname(runjobs[0]), "output")
      ch.send(outputdir)
      actual = ch.receive()
      assert actual

      outputpath = posixpath.join(outputdir, "outfile")
      ch = gw.remote_exec(chIsFile)
      ch.send(outputpath)
      actual = ch.receive()
      assert actual

      ch = gw.remote_exec(chFileContents)
      ch.send(outputpath)
      actual = ch.receive()
      assert actual == "Hello0\n"
    finally:
      clch.send(None)

@pytest.mark.usefixtures("clearqueue")
def testQueueingSystemMultipleJobsInSingleBatch(gw, client):
    # gw = _mkexecnetgw(clearqueue)
    clch, runjobs = mkrunjobs(gw, 3, numSuffix = True)
    try:
      # client = _mkclient('testPBSClientMultipleJobsInSingleBatch', clearqueue)
      j1cb = TstCallback()
      jr1 = client.runJobs(runjobs, j1cb)

      # Wait for job submission
      assert jr1.qsubEvent.wait(120)
      assert len(client._qsState._jobIds) == 1
      assert list(client._qsState._jobIds)[0] == jr1.jobId
      # Now wait for the job to complete
      assert jr1.completion_event.wait(120)
      assert j1cb.called
      assert j1cb.exception is None

      for i, job in enumerate(runjobs):
          # Now check the contents of the output directory
          ch = gw.remote_exec(chIsDir)
          try:
            import posixpath
            outputdir = posixpath.join(posixpath.dirname(job), "output")
            ch.send(outputdir)
            actual = ch.receive()
            assert actual
          finally:
            ch.close()
            ch.waitclose(2)

          outputpath = posixpath.join(outputdir, "outfile")
          ch = gw.remote_exec(chIsFile)
          try:
            ch.send(outputpath)
            actual = ch.receive()
            assert actual
          finally:
            ch.close()
            ch.waitclose(2)


          ch = gw.remote_exec(chFileContents)
          try:
            ch.send(outputpath)
            actual = ch.receive()
            assert actual == "Hello%d\n" % i
          finally:
            ch.close()
            ch.waitclose(2)
    finally:
      clch.send(None)

@pytest.mark.usefixtures("clearqueue")
def testQueueingSystemClientMultipleJobsInMultipleBatches(gw, client):
    # gw = _mkexecnetgw(clearqueue)
    clch, runjobs = mkrunjobs(gw, 3, numSuffix = True)
    try:
      # client = _mkclient('testPBSClientMultipleJobsInMultipleBatches', clearqueue)
      rj1 = [runjobs[0]]
      rj2 = runjobs[1:]

      j1cb = TstCallback()
      jr1 = client.runJobs(rj1, j1cb)

      j2cb = TstCallback()
      jr2 = client.runJobs(rj2, j2cb)

      # Now wait for the job to complete
      # assert jr1.completion_event.wait(120)
      # assert jr2.completion_event.wait(120)
      gevent.wait([jr1.completion_event, jr2.completion_event], 120)
      assert j1cb.called
      assert j1cb.exception is None
      assert j2cb.called
      assert j2cb.exception is None

      for i, job in enumerate(runjobs):
        # Now check the contents of the output directory
        ch = gw.remote_exec(chIsDir)

        import posixpath
        try:
          outputdir = posixpath.join(posixpath.dirname(job), "output")
          ch.send(outputdir)
          actual = ch.receive()
          assert actual
        finally:
          ch.close()
          ch.waitclose(5)

        outputpath = posixpath.join(outputdir, "outfile")
        ch = gw.remote_exec(chIsFile)
        try:
          ch.send(outputpath)
          actual = ch.receive()
          assert actual
        finally:
          ch.close()
          ch.waitclose(5)

        ch = gw.remote_exec(chFileContents)
        try:
          ch.send(outputpath)
          actual = ch.receive()
          assert actual == "Hello%d\n" % i
        finally:
          ch.close()
          ch.waitclose(5)
    finally:
      clch.send(None)

@pytest.mark.usefixtures("clearqueue")
def testQueueingSystemClientKillJob(gw, client):
    # gw = _mkexecnetgw(clearqueue)
    clch1, rj1 = mkrunjobs(gw, 1, numSuffix = True, sleep = None)
    clch2, rj2 = mkrunjobs(gw, 3, numSuffix = True, sleep = 4)

    try:
      # client = _mkclient('testPBSClientKillJob', clearqueue)
      #TODO: Test after qsub but before pbsId received.
      #TODO: Test after qsub but before qrls
      #TODO: Test after qrls
      #TODO: Test after job finished.

      j1cb = TstCallback()
      jr1 = client.runJobs(rj1, j1cb)

      j2cb = TstCallback()
      jr2 = client.runJobs(rj2, j2cb)

      killEvent = jr2.kill()

      assert killEvent.wait(60)
      assert jr1.completion_event.wait(60)
      assert j1cb.called
      assert j1cb.exception is None
      assert j2cb.called

      try:
        raise j2cb.exception
      except generic_client.QueueingSystemJobKilledException:
        pass

      client.close()

      for i, job in enumerate(rj1):
        # Now check the contents of the output directory
        ch = gw.remote_exec(chIsDir)

        import posixpath
        try:
          outputdir = posixpath.join(posixpath.dirname(job), "output")
          ch.send(outputdir)
          actual = ch.receive()
          assert actual
        finally:
          ch.close()
          ch.waitclose(5)

        outputpath = posixpath.join(outputdir, "outfile")
        ch = gw.remote_exec(chIsFile)
        try:
          ch.send(outputpath)
          actual = ch.receive()
          assert actual
        finally:
          ch.close()
          ch.waitclose(5)

        ch = gw.remote_exec(chFileContents)
        try:
          ch.send(outputpath)
          actual = ch.receive()
          assert actual == "Hello%d\n" % i
        finally:
          ch.close()
          ch.waitclose(5)

      for i, job in enumerate(rj2):
        # Now check the contents of the output directory
        ch = gw.remote_exec(chIsDir)

        import posixpath
        try:
          outputdir = posixpath.join(posixpath.dirname(job), "output")
          ch.send(outputdir)
          actual = ch.receive()
          assert actual
        finally:
          ch.close()
          ch.waitclose(5)

        outputpath = posixpath.join(outputdir, "outfile")
        ch = gw.remote_exec(chIsFile)
        try:
          ch.send(outputpath)
          actual = ch.receive()
          assert not actual
        finally:
          ch.close()
          ch.waitclose(5)

    finally:
      clch1.send(None)
      clch2.send(None)

