from atsim.pro_fit._channel import AbstractChannel
from atsim.pro_fit._util import MultiCallback, CallbackRegister

import _pbs_remote_exec

import logging

from gevent import Greenlet
import gevent
from gevent.event import Event

import itertools
import uuid


class PBSStateListenerAdapter(object):

  def jobsChanged(self, oldJobs, newJobs):
    pass

  def jobsAdded(self, pbsId):
    pass

  def jobsRemoved(self, pbsId):
    pass


class PBSChannel(AbstractChannel):

  _logger = logging.getLogger(__name__).getChild("PBSChannel")

  def __init__(self, execnet_gw, channel_id = None):
    super(PBSChannel, self).__init__(
      execnet_gw,
      _pbs_remote_exec,
      channel_id)

    self.callback = MultiCallback()

  def make_start_message(self):
    return {'msg' : 'START_CHANNEL', 'channel_id' : self.channel_id}

class PBSJobRecord(object):

  def __init__(self, joblist, callback):
    self._joblist = joblist
    self._callback = callback
    self._qsubEvent = Event()
    self._finishEvent = Event()
    self.pbsId = None

  @property
  def qsubEvent(self):
    return self._qsubEvent

  @property
  def finishEvent(self):
    return self._finishEvent

  @property
  def callback(self):
    return self._callback


class _QSubCallback(PBSStateListenerAdapter):

  def __init__(self, pbsClient, transaction_id, jobRecord):
    self.pbsClient = pbsClient
    self.transaction_id = transaction_id
    self.jobRecord = jobRecord
    self.active = True
    self.jobReleased = False

  def __call__(self, msg):
    try:
      mtype = msg["msg"]
    except:
      return

    transaction_id = msg['transaction_id']

    if not (transaction_id == self.transaction_id and mtype == 'QSUB'):
      return

    pbsId = msg['pbs_id']
    self.jobRecord.pbsId = pbsId

    # Register event handlers with the pbsState object, the first to trigger QRLS, then second to indicate job completion.
    self.pbsClient._pbsState.listeners.append(self)
    if self.pbsClient._pbsState.hasJobId(pbsId):
      self._jobSeen()

    self.active = False
    return True

  def jobsAdded(self, pbsIds):
    if not self.jobRecord.pbsId in pbsIds:
      return
    self._jobSeen()

  def jobsRemoved(self, pbsIds):
    if not self.jobRecord.pbsId in pbsIds:
      return
    self._jobRemoved()
    # Remove this object as a listener from the _pbsState
    del self.pbsClient._pbsState.listeners[self.pbsClient._pbsState.listeners.index(self)]

  def _jobSeen(self):
    if self.jobReleased:
      return
    self.jobRecord.qsubEvent.set()
    # Issue QRLS
    self._qrls()

  def _qrls(self):
    self.jobReleased = True
    transId = str(uuid.uuid4())
    self.pbsClient._qrls(transId, self.jobRecord.pbsId)

  def _jobRemoved(self):
    # Indicate that the job is finished
    self.jobRecord.callback(None)
    self.jobRecord.finishEvent.set()

class PBSClient(object):

  def __init__(self, pbsChannel, pollEvery = 10.0):
    self._channel = pbsChannel
    self._pbsState = PBSState(pbsChannel, pollEvery)
    self._cbregister = CallbackRegister()
    self._channel.callback.append(self._cbregister)

  def runJobs(self, jobList, callback):
    jr = PBSJobRecord(jobList, callback)
    transid = str(uuid.uuid4())
    qsubcallback = _QSubCallback(self, transid, jr)
    self._cbregister.append(qsubcallback)
    self._qsub(transid, jobList)
    return jr

  def _qsub(self, transId, jobList):
    msg = {'msg' : 'QSUB', 'transaction_id' : transId, 'channel_id' : self.channel.channel_id, 'jobs' : jobList}
    self.channel.send(msg)

  def _qrls(self, transId, pbsId):
    msg = {'msg' : 'QRLS', 'transaction_id' : transId, 'channel_id' : self.channel.channel_id, 'pbs_id' : pbsId}
    self.channel.send(msg)

  @property
  def channel(self):
    return self._channel

  def close(self):
    self._pbsState.close()


class _QSelectLoop(Greenlet):

  def __init__(self, pbsState):
    Greenlet.__init__(self)
    self.pbsState = pbsState

  def _run(self):
    while True:
      self.pbsState._qselect()
      gevent.sleep(self.pbsState._pollEvery)


class _PBSStateQSelectCallback(object):

  def __init__(self, pbsState):
    self._pbsState = pbsState


  def __call__(self, msg):
    print msg
    try:
      mtype = msg.get('msg', None)
    except:
      return

    if mtype != 'QSELECT':
      return

    pbs_ids = msg.get('pbs_ids', [])
    self._pbsState._updateJobIds(pbs_ids)

class PBSState(object):

  def __init__(self, pbsChannel, pollEvery = 10.0):
    self._channel = pbsChannel
    self._privatelisteners = []
    self._listeners = []
    self._privatelisteners.append(_AddRemoveListener(self._listeners))
    self._jobIds = set()
    self._pollEvery = pollEvery
    self._pollingGreenlet = None
    self._cb = None

    # Register msg callback with the pbsChannel
    self._registerChannelCallback()

    # Start the timer loop
    self._startTimerLoop()

  def _registerChannelCallback(self):
    self._cb = _PBSStateQSelectCallback(self)
    self._channel.callback.append(self._cb)

  def _startTimerLoop(self):
    self._pollingGreenlet = _QSelectLoop(self)
    self._pollingGreenlet.start()

  def _qselect(self):
    msg = {'msg' : 'QSELECT', 'channel_id' : self._channel.channel_id}
    self._channel.send(msg)

  def _updateJobIds(self, newJobIds):
    oldJobIds = self._jobIds
    newJobIds = set(newJobIds)

    self._jobIds = newJobIds

    if oldJobIds != newJobIds:
      for l in self._allListeners:
        l.jobsChanged(oldJobIds, newJobIds)

  @property
  def _allListeners(self):
    return itertools.chain(self._privatelisteners, self._listeners)

  @property
  def listeners(self):
    return self._listeners

  def hasJobId(self, jobId):
    return jobId in self._jobIds

  def close(self):
    if not self._pollingGreenlet is None:
      self._pollingGreenlet.kill()

    if not self._cb is None:
      del self._channel.callback[self._channel.callback.index(self._cb)]


class _AddRemoveListener(PBSStateListenerAdapter):

  def __init__(self, listeners):
    self._listeners = listeners

  def jobsChanged(self, oldJobs, newJobs):
    jobsRemoved =  oldJobs - newJobs
    jobsAdded = newJobs - oldJobs

    if jobsRemoved:
      self.jobsRemoved(jobsRemoved)

    if jobsAdded:
      self.jobsAdded(jobsAdded)

  def jobsAdded(self, pbsIds):
    for l in self._listeners:
      l.jobsAdded(pbsIds)

  def jobsRemoved(self, pbsIds):
    for l in self._listeners:
      l.jobsRemoved(pbsIds)



