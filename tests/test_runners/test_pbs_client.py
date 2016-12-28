from atsim.pro_fit._channel import ChannelException


from ..testutil import vagrant_torque, vagrant_basic
from _runnercommon import channel_id

from test_pbs_remote_exec import _mkexecnetgw, clearqueue, _mkrunjobs

import atsim.pro_fit.runners._pbs_client

from gevent.event import Event

pbs_client = atsim.pro_fit.runners._pbs_client

def _mkclient(channel_id, vagrant_box):
  gw = _mkexecnetgw(vagrant_box)
  ch = pbs_client.PBSChannel(gw, channel_id)
  client = pbs_client.PBSClient(ch, pollEvery = 0.5)
  return client

def testStartChannel(vagrant_torque, channel_id):
  client = _mkclient(channel_id, vagrant_torque)

def testHostHasNoPbs(vagrant_basic, channel_id):
  try:
    client = _mkclient(channel_id, vagrant_basic)
    assert False, "PBSChannelException was not raised"
  except ChannelException as e:
    assert e.message.endswith("PBS not found: Could not run 'qselect'")

class JobsChangedListener(pbs_client.PBSStateListenerAdapter):

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
      self.lastPbsID = msg.get("pbs_id", None)
      self.event.set()

  def reset(self):
    self.event.clear()
    self.lastPbsID = None

def testPBSState(clearqueue, channel_id):
  client = _mkclient(channel_id, clearqueue)

  qsubcallback = QSUBCallback()
  client.channel.callback.append(qsubcallback)

  # Instantiate PBSState
  state = pbs_client.PBSState(client.channel, pollEvery = 1.0)
  listener = JobsChangedListener()
  state.listeners.append(listener)

  gw = _mkexecnetgw(clearqueue)

  # Create some jobs
  clch, runjobs = _mkrunjobs(gw, 5)
  try:
    assert not state._jobIds
    client.channel.send({'msg' : 'QSUB', 'jobs' : runjobs[:3]})
    assert qsubcallback.event.wait(10)
    pbs_id_1 = qsubcallback.lastPbsID
    qsubcallback.reset()
    assert not pbs_id_1 is None

    assert listener.event.wait(10)
    assert list(listener.oldJobs) == []
    assert sorted(listener.newJobs) == sorted([pbs_id_1])
    listener.reset()

    client.channel.send({'msg' : 'QSUB', 'jobs' : runjobs[3:]})
    assert qsubcallback.event.wait(10)
    pbs_id_2 = qsubcallback.lastPbsID
    qsubcallback.reset()
    assert not pbs_id_2 is None

    assert listener.event.wait(10)
    assert sorted(listener.oldJobs) == sorted([pbs_id_1])
    assert sorted(listener.newJobs) == sorted([pbs_id_1, pbs_id_2])
    listener.reset()

    client.channel.send({'msg' : 'QRLS', 'pbs_id' : pbs_id_2 })
    assert listener.event.wait(30)
    assert sorted(listener.oldJobs) == sorted([pbs_id_1, pbs_id_2])
    assert sorted(listener.newJobs) == sorted([pbs_id_1])
    listener.reset()

    client.channel.send({'msg' : 'QDEL', 'pbs_ids' : [pbs_id_1] })
    assert listener.event.wait(30)
    assert sorted(listener.oldJobs) == sorted([pbs_id_1])
    assert sorted(listener.newJobs) == sorted([])
    listener.reset()

  finally:
    clch.send(None)

def testPBSQStateAddRemovePrivateListener():
  class Listener(pbs_client.PBSStateListenerAdapter):

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
  addRemoveListener = pbs_client._AddRemoveListener(listenerList)

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


def testPBSClientSingleJob(clearqueue, channel_id):

  import logging
  logging.basicConfig(level = logging.DEBUG)

  class TstCallback(object):

    def __init__(self):
      self.called = False

    def __call__(self, exception):
      self.called = True
      self.exception = exception

  client = _mkclient(channel_id, clearqueue)
  gw = _mkexecnetgw(clearqueue)
  clch, runjobs = _mkrunjobs(gw, 1)
  try:
    j1cb = TstCallback()
    jr1 = client.runJobs([runjobs[0]], j1cb)

    # Wait for job submission
    assert jr1.qsubEvent.wait(120)
    assert len(client._pbsState._jobIds) == 1
    assert list(client._pbsState._jobIds)[0] == jr1.pbsId
    # Now wait for the job to complete
    assert jr1.finishEvent.wait(120)
    assert j1cb.called
    assert j1cb.exception is None
    client.close()

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
    assert actual == "Hello\n"
  finally:
    clch.send(None)


