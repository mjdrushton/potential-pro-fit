"""Client object to support batch queueing systems running on remote machines. The `QueueingSystemClient` class provides an object
oriented interface to an Execnet channel that supports the protocol originally defined by the `_pbs_remote_exec`.

This is currently defined as follows:

**Note:** messages all support a `transaction_id` record. This is a value that is 
          echoed by the server to the client, allowing message-response pairs to be tracked.

`QSUB`:

Submit jobs to queue.

* Message Format:
  + `{'msg' : 'QSUB', 'jobs' : JOB_LIST}`
  + Where:
    - `JOB_LIST` : List of paths to be run through queue. Each path must refer to a `runjob` file.
  + Jobs are submitted in a held state. In the case of very short jobs, this gives time for 
    client to register the job's existence and maintain consistent state. Once identified by polling
    the queueing system using the `QSELECT` message, the job is released by sending a `QRLS` message.
* Expected Response:
  +  `{'msg' : 'QSUB', 'job_id' : JOB_ID, 'channel_id' : CHANNEL_ID, 'transaction_id' : TRANS_ID }`
  + Where:
    - `JOB_ID` : Queueing system's identifier for submitted job.
    - `CHANNEL_ID`: Execnet channel ID for current response.
    - `TRANS_ID` : See note above.

`QSELECT`:

List queued jobs.

* Message Format:
   + `{'msg' : 'QSELECT'}`
* Expected Response:
   + `{'msg' : 'QSELECT', 'channel_id' : CHANNEL_ID, 'job_ids' : JOB_IDS}`
   + Where:
     - `JOB_IDS` : List of queueing system IDs for running jobs.

`QRLS`:

Release jobs from the held state in which they are submitted.

* Message Format:
  + `{'msg' : 'QRLS`, 'job_id' : JOB_ID }
  + Where:
    - `JOB_ID` : Queueing system ID of job to be released.
* Expected Response:
  + `{'msg' : 'QRLS', 'job_id' : JOB_ID, 'channel_id' : CHANNEL_ID}`
  + Where:
    - `JOB_ID` : ID of released job.

`QDEL`:

Terminate jobs.

* Messgae Format:
  + `{'msg' : 'QDEL', 'job_ids' : JOB_IDS, 'force' : FORCE_FLAG }`
  + Where:
     - `JOB_IDS` : List of queueing system IDs to be terminated.
     - `FORCE_FLAG` : The force attribute is optional. If set to `True`, then
                      jobs will be hard killed.
"""

from atsim.pro_fit._channel import AbstractChannel
from atsim.pro_fit._util import MultiCallback, CallbackRegister

from _exceptions import JobKilledException

import logging

from gevent import Greenlet
import gevent
from gevent.event import Event

import itertools
import uuid

class QueueingSystemJobKilledException(JobKilledException):
  pass

class QueueingSystemStateListenerAdapter(object):

  def jobsChanged(self, oldJobs, newJobs):
    pass

  def jobsAdded(self, jobId):
    pass

  def jobsRemoved(self, jobId):
    pass

class QueueingSystemJobRecord(object):

  def __init__(self, joblist, callback):
    self._qscallback = None
    self._joblist = joblist
    self._callback = callback
    self._qsubEvent = Event()
    self._finishEvent = Event()
    self._killed = False
    self.jobId = None

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

    transaction_id = msg.get('transaction_id', None)

    if not (transaction_id == self.transaction_id and mtype == self.mtype):
      return

    self.event.set()
    self.active = False

    return True

class _QSubCallback(QueueingSystemStateListenerAdapter):

  def __init__(self, qsClient, transaction_id, jobRecord):
    self.qsClient = qsClient
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

    jobId = msg['job_id']
    self.jobRecord.jobId = jobId

    # Register event handlers with the qsState object, the first to trigger QRLS, then second to indicate job completion.
    self.qsClient._qsState.listeners.append(self)
    if self.qsClient._qsState.hasJobId(jobId):
      self._jobSeen()

    self.active = False
    return True

  def jobsAdded(self, jobIds):
    if not self.jobRecord.jobId in jobIds:
      return
    self._jobSeen()

  def jobsRemoved(self, jobIds):
    if not self.jobRecord.jobId in jobIds:
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
    self.qsClient._qrls(transId, self.jobRecord.jobId)

  def _jobRemoved(self):
    if self.jobRecord._killed:
      return
    self._finishJob()

  def _finishJob(self):
    self._unregisterListeners()
    # Indicate that the job is finished
    exc = None

    if self.jobRecord._killed:
      exc = QueueingSystemJobKilledException()

    self.jobRecord.callback(exc)
    self.jobRecord.completion_event.set()

  def _unregisterListeners(self):
    # Remove this object as a listener from the _qsState
    del self.qsClient._qsState.listeners[self.qsClient._qsState.listeners.index(self)]

  def kill(self):
    self.jobRecord._killed = True
    if self.jobRecord.jobId is None:
      self.jobRecord.qsubEvent.wait(60)

    if self.jobRecord.jobId:
      transId = str(uuid.uuid4())
      cb = _WaitForMessageCB('QDEL', transId)
      self.qsClient._cbregister.append(cb)
      self.qsClient._qdel(transId, self.jobRecord.jobId, force = True)
      cb.event.wait(60)
    self._finishJob()

class QueueingSystemClient(object):

  def __init__(self, qsChannel, pollEvery = 10.0):
    """Create queueing system client.

    Args:
      qsChannel (atsim.pro_fit._channel.AbstractChannel): Instance of AbstractChannel sub-class supporting the protocol described in this module's comments.
      pollEvery (float): Interval for queueing system state (QSELECT) to be polled, in seconds."""

    self._channel = qsChannel
    self._qsState = QueueingSystemState(qsChannel, pollEvery)
    self._cbregister = CallbackRegister()
    self._channel.callback.append(self._cbregister)
    self._closed = False

  def runJobs(self, jobList, callback, header_lines = None):
    jr = QueueingSystemJobRecord(jobList, callback)
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

  def _qrls(self, transId, jobId):
    msg = {'msg' : 'QRLS', 'transaction_id' : transId, 'channel_id' : self.channel.channel_id, 'job_id' : jobId}
    self.channel.send(msg)

  def _qdel(self, transId, jobId, force = False):
    msg = {'msg' : 'QDEL', 'transaction_id' : transId, 'channel_id' : self.channel.channel_id, 'job_ids' : [jobId], 'force' : force}
    self.channel.send(msg)

  @property
  def channel(self):
    return self._channel

  def close(self, closeChannel = True):
    if not self._closed:
      self._closed = True
      self._qsState.close()

      if closeChannel:
        self.channel.send(None)

class _QueueingSystemStateQSelectCallback(object):

  def __init__(self, qsState):
    self._qsState = qsState
    self._transId = str(uuid.uuid4())

  def __call__(self, msg):
    try:
      mtype = msg.get('msg', None)
    except:
      return

    if mtype != 'QSELECT' or msg.get('transaction_id', None) != self._transId:
      return

    job_ids = msg.get('job_ids', [])
    self._qsState._updateJobIds(job_ids)

    def qselect_later():
      try:
        self._qsState._qselect(self._transId)
      except IOError:
        pass

    gevent.spawn_later(self._qsState._pollEvery, qselect_later)

class QueueingSystemState(object):

  def __init__(self, qsChannel, pollEvery = 10.0):
    self._channel = qsChannel
    self._privatelisteners = []
    self._listeners = []
    self._privatelisteners.append(_AddRemoveListener(self._listeners))
    self._jobIds = set()
    self._pollEvery = pollEvery
    self._cb = None

    # Register msg callback with the qsChannel
    self._registerChannelCallback()
    self._qselect(self._cb._transId)

  def _registerChannelCallback(self):
    self._cb = _QueueingSystemStateQSelectCallback(self)
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

class _AddRemoveListener(QueueingSystemStateListenerAdapter):

  def __init__(self, listeners):
    self._listeners = listeners

  def jobsChanged(self, oldJobs, newJobs):
    jobsRemoved =  oldJobs - newJobs
    jobsAdded = newJobs - oldJobs

    if jobsRemoved:
      self.jobsRemoved(jobsRemoved)

    if jobsAdded:
      self.jobsAdded(jobsAdded)

  def jobsAdded(self, jobIds):
    for l in list(self._listeners):
      l.jobsAdded(jobIds)

  def jobsRemoved(self, jobIds):
    for l in list(self._listeners):
      l.jobsRemoved(jobIds)



