from atsim.pro_fit._channel import AbstractChannel
from atsim.pro_fit._util import MultiCallback, CallbackRegister

from _exceptions import JobKilledException

import _pbs_remote_exec

import logging

from gevent import Greenlet
import gevent
from gevent.event import Event

import itertools
import uuid

class PBSJobKilledException(JobKilledException):
  pass

class PBSStateListenerAdapter(object):

  def jobsChanged(self, oldJobs, newJobs):
    pass

  def jobsAdded(self, pbsId):
    pass

  def jobsRemoved(self, pbsId):
    pass


class PBSChannel(AbstractChannel):

  _logger = logging.getLogger(__name__).getChild("PBSChannel")

  def __init__(self, execnet_gw, channel_id = None, nocb = False):
    super(PBSChannel, self).__init__(
      execnet_gw,
      _pbs_remote_exec,
      channel_id)

    if not nocb:
      self.callback = MultiCallback()

  def make_start_message(self):
    return {'msg' : 'START_CHANNEL', 'channel_id' : self.channel_id}

class PBSJobRecord(object):

  def __init__(self, joblist, callback):
    self._qscallback = None
    self._joblist = joblist
    self._callback = callback
    self._qsubEvent = Event()
    self._finishEvent = Event()
    self._killed = False
    self.pbsId = None

  @property
  def qsubEvent(self):
    return self._qsubEvent

  @property
  def completion_event(self):
    return self._finishEvent

  @property
  def callback(self):
    return self._callback

  def kill(self):
    if not self._killed:
      self._qscallback.kill()
    self._killed = True
    return self.completion_event


class _WaitForMessageCB(object):

  def __init__(self, mtype, transId):
    self.event = Event()
    self.active = True
    self.transaction_id = transId
    self.mtype = mtype

  def __call__(self, msg):
    try:
      mtype = msg["msg"]
    except:
      return

    transaction_id = msg['transaction_id']

    if not (transaction_id == self.transaction_id and mtype == self.mtype):
      return

    self.event.set()
    self.active = False

    return True

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
    if self.jobRecord._killed:
      return
    self._finishJob()

  def _finishJob(self):
    self._unregisterListeners()
    # Indicate that the job is finished
    exc = None

    if self.jobRecord._killed:
      exc = PBSJobKilledException()

    self.jobRecord.callback(exc)
    self.jobRecord.completion_event.set()

  def _unregisterListeners(self):
    # Remove this object as a listener from the _pbsState
    del self.pbsClient._pbsState.listeners[self.pbsClient._pbsState.listeners.index(self)]

  def kill(self):
    self.jobRecord._killed = True
    if self.jobRecord.pbsId is None:
      self.jobRecord.qsubEvent.wait(60)

    if self.jobRecord.pbsId:
      transId = str(uuid.uuid4())
      cb = _WaitForMessageCB('QDEL', transId)
      self.pbsClient._cbregister.append(cb)
      self.pbsClient._qdel(transId, self.jobRecord.pbsId, force = True)
      cb.event.wait(60)
    self._finishJob()


class PBSClient(object):

  def __init__(self, pbsChannel, pollEvery = 10.0):
    self._channel = pbsChannel
    self._pbsState = PBSState(pbsChannel, pollEvery)
    self._cbregister = CallbackRegister()
    self._channel.callback.append(self._cbregister)
    self._closed = False

  def runJobs(self, jobList, callback, header_lines = None):
    jr = PBSJobRecord(jobList, callback)
    transid = str(uuid.uuid4())
    qsubcallback = _QSubCallback(self, transid, jr)
    jr._qscallback = qsubcallback
    self._cbregister.append(qsubcallback)
    self._qsub(transid, jobList, header_lines = header_lines)
    return jr

  def _qsub(self, transId, jobList, header_lines = None):
    msg = {'msg' : 'QSUB', 'transaction_id' : transId, 'channel_id' : self.channel.channel_id, 'jobs' : jobList}

    if header_lines:
      msg['header_lines'] = header_lines

    self.channel.send(msg)

  def _qrls(self, transId, pbsId):
    msg = {'msg' : 'QRLS', 'transaction_id' : transId, 'channel_id' : self.channel.channel_id, 'pbs_id' : pbsId}
    self.channel.send(msg)

  def _qdel(self, transId, pbsId, force = False):
    msg = {'msg' : 'QDEL', 'transaction_id' : transId, 'channel_id' : self.channel.channel_id, 'pbs_ids' : [pbsId], 'force' : force}
    self.channel.send(msg)

  @property
  def channel(self):
    return self._channel

  def close(self, closeChannel = True):
    if not self._closed:
      self._closed = True
      self._pbsState.close()

      if closeChannel:
        self.channel.send(None)


class _PBSStateQSelectCallback(object):

  def __init__(self, pbsState):
    self._pbsState = pbsState
    self._transId = str(uuid.uuid4())

  def __call__(self, msg):
    try:
      mtype = msg.get('msg', None)
    except:
      return

    if mtype != 'QSELECT' or msg.get('transaction_id', None) != self._transId:
      return

    pbs_ids = msg.get('pbs_ids', [])
    self._pbsState._updateJobIds(pbs_ids)

    def qselect_later():
      try:
        self._pbsState._qselect(self._transId)
      except IOError:
        pass

    gevent.spawn_later(self._pbsState._pollEvery, qselect_later)

class PBSState(object):

  def __init__(self, pbsChannel, pollEvery = 10.0):
    self._channel = pbsChannel
    self._privatelisteners = []
    self._listeners = []
    self._privatelisteners.append(_AddRemoveListener(self._listeners))
    self._jobIds = set()
    self._pollEvery = pollEvery
    self._cb = None

    # Register msg callback with the pbsChannel
    self._registerChannelCallback()
    self._qselect(self._cb._transId)

  def _registerChannelCallback(self):
    self._cb = _PBSStateQSelectCallback(self)
    self._channel.callback.append(self._cb)

  def _qselect(self, transId = None):
    msg = {'msg' : 'QSELECT', 'channel_id' : self._channel.channel_id}

    if transId:
      msg['transaction_id'] = transId

    self._channel.send(msg)

  def _updateJobIds(self, newJobIds):
    oldJobIds = self._jobIds
    newJobIds = set(newJobIds)

    self._jobIds = newJobIds

    if oldJobIds != newJobIds:
      for l in list(self._allListeners):
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
    for l in list(self._listeners):
      l.jobsAdded(pbsIds)

  def jobsRemoved(self, pbsIds):
    for l in list(self._listeners):
      l.jobsRemoved(pbsIds)



