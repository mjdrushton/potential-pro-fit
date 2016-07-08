"""Classes to make the use of _run_remote_exec channels easier"""


from atsim.pro_fit._channel import AbstractChannel, MultiChannel
from atsim.pro_fit._util import CallbackRegister, NamedEvent, eventWait_or

import itertools
import logging
import multiprocessing
import threading
import uuid
import sys
import traceback
from collections import deque

_ncpu = multiprocessing.cpu_count()

_logger = logging.getLogger("atsim.pro_fit.runners")

class RunChannelException(Exception):
  pass

class RunChannel(AbstractChannel):

  _logger = _logger.getChild("RunChannel")

  def __init__(self, execnet_gw, channel_id = None, shell = "/bin/bash", hardkill_timeout = 60,  connection_timeout = 60):
    """Summary

    Args:
        execnet_gw (execnet.Gateway): Execnet gateway used to create channel managed by this object.
        channel_id (None, optional): Description
        shell (str, optional): Shell that should be used on remote host to run commands.
        hardkill_timeout (int, optional): When jobs are terminated, an initial attempt is made to kill the job by
          sending the termination signal. If this has failed, after this timeout the SIGKILL signal is sent to hard stop the process.
        connection_timeout (int, optional): Time limit for connection attempt.
    """
    import _run_remote_exec
    self.shell = shell
    self.hardkill_timeout = hardkill_timeout
    super(RunChannel, self).__init__(execnet_gw, _run_remote_exec, channel_id, connection_timeout)

  def make_start_message(self):
    return {'msg' : 'START_CHANNEL', 'channel_id' : self.channel_id, 'shell' : self.shell, 'hardkill_timeout' : self.hardkill_timeout }

class RunChannelFactory(object):

  def __init__(self, shell, hardkill_timeout, connection_timeout):
    self.shell = shell
    self.hardkill_timeout = hardkill_timeout
    self.connection_timeout = connection_timeout

  def createChannel(self, execnet_gw, channel_id):
    return RunChannel(execnet_gw,
      channel_id,
      self.shell,
      self.hardkill_timeout,
      self.connection_timeout)

class RunChannels(MultiChannel):
  _logger = _logger.getChild("RunChannels")

  def __init__(self, execnet_gw, channel_id = None,  shell = "/bin/bash", hardkill_timeout = 60,  connection_timeout = 60, num_channels = _ncpu):
    factory = RunChannelFactory(shell, hardkill_timeout, connection_timeout)
    super(RunChannels, self).__init__(execnet_gw, factory, num_channels, channel_id)
    self._channel_map = dict(zip([c.channel_id for c in self._channels], self._channels))



  def getChannel(self, channel_id):
    """Return underlying channel that has the given channel_id"""
    return self._channel_map[channel_id]

def _NullCallback(*args, **kwargs):
  pass

class RunClient(object):

  _logger = _logger.getChild("RunClient")

  def __init__(self, channel):
    self.channel = channel
    self._base_transid = uuid.uuid4()
    self._id_count = itertools.count()
    self._cbregister = CallbackRegister()
    self._submissionCallback = SubmissionCallback(self.channel, self._transid, self._cbregister)
    self._cbregister.append(self._submissionCallback)
    self.channel.setcallback(self._cbregister)
    self.channel.broadcast({'msg' : 'READY'})

  def runCommand(self, workingDirectory, callback = None):
    """Executes `runjob` command in the given working directory.

    If a callback is specified, this will be invoked with any exception raised
    during execution (or None if no error encountered). When a callback is given
    this method returns immediately.

    If `callback` is None, then this method will block until the command finishes,
    any exception encountered is then raised in the calling thread.

    Args:
        workingDirectory (TYPE): Description
        callback (None, optional): Description

    """
    self._logger.debug("runCommand called for workingDirectory = '%s'", workingDirectory)
    cbobj = self._registerCallback(workingDirectory, callback)
    if callback is None:
      cbobj.event.wait()
      cbobj.raise_exception()
      return None
    else:
      jobRecord = JobRecord(cbobj, self)
      return jobRecord

  def _registerCallback(self, workingDirectory, callback):
    if callback is None:
      cbobj = RunJobCallback(workingDirectory, _NullCallback, self._transid, self)
      cbobj.should_raise = True
    else:
      cbobj = RunJobCallback(workingDirectory, callback, self._transid, self)
    self._submissionCallback.registerJob(cbobj)
    return cbobj

  @property
  def _transid(self):
    return "%s-%d" % (self._base_transid, self._id_count.next())

class JobRecord(object):

  def __init__(self, rjc, runClient):
    self._runjobcallback = rjc
    self._runClient = runClient
    self.pid = None

  @property
  def workingDirectory(self):
    return self._runjobcallback.workingDirectory

  @property
  def trans_id(self):
    return self._runjobcallback.trans_id

  @property
  def completion_event(self):
    return self._runjobcallback.finishEvent

  def kill(self):
    """Kill the job.

    Returns:
        threading.Event: Event that is set once job completes.
    """
    self._runjobcallback.killEvent.set()
    return self.completion_event

class JobAlreadyFinishedException(Exception):
  pass

class JobStartException(Exception):
  pass

class RunJobKilledException(Exception):
  pass

class _RunJobState(threading.Thread):
  """Class that keeps track of a job's run state"""

  _logger = logging.getLogger("atsim.pro_fit.runners._run_remote_client._RunJobState")

  def __init__(self, runJobCallback):
    # REMOVE
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    # END REMOVE

    threading.Thread.__init__(self)

    self.runJobCallback = runJobCallback
    self.readyEvent = threading.Event()
    self.jobStartEvent = threading.Event()
    self.jobEndEvent = threading.Event()
    self.killEvent = threading.Event()
    self.finishEvent = threading.Event()
    self.errorEvent = threading.Event()

  def run(self):
    waitEvent = eventWait_or([self.readyEvent, self.killEvent])
    while not waitEvent.wait(0.1):
      pass

    try:
      if self.killEvent.is_set():
        self.cancelJobBeforeRun()
        return

      if self.readyEvent.is_set():
        self.startJob()

      self.monitorJob()
      self.finishJob(None)
    except:
      exc = sys.exc_info()
      #I don't want to log RunJobKilledException hence this...
      try:
        raise exc
      except RunJobKilledException:
        pass
      except:
        tbstring = traceback.format_exception(*exc)
        self._logger.debug("Exception in run: %s", tbstring)
      self.finishJob(exc)

  def cancelJobBeforeRun(self):
    # Cancel and remove job
    self.runJobCallback.unregisterCallback()
    raise RunJobKilledException()

  def getChannel(self):
    channel_id = self.runJobCallback.channel_id
    channel = self.runJobCallback._runClient.channel.getChannel(channel_id)
    return channel

  def startJob(self):
    # Submit the job
    jobid = self.runJobCallback.trans_id
    workingDirectory = self.runJobCallback.workingDirectory
    sendmsg = {'msg' : 'JOB_START', 'job_id' : jobid, 'job_path' : workingDirectory }

    channel = self.getChannel()
    self._logger.debug("Submitting job to channel_id = %s: %s", channel.channel_id, sendmsg)
    channel.send(sendmsg)

  def finishJob(self, exception):
    if exception:
      self.runJobCallback.exception = exception

    self._logger.debug("finishJob called")
    self.finishEvent.set()
    self.runJobCallback.callback(exception, self.runJobCallback)

  def monitorJob(self):
    waitEvent = eventWait_or([self.jobStartEvent, self.errorEvent])
    while not waitEvent.wait(0.1):
      pass

    if self.errorEvent.is_set():
      raise self.runJobCallback.exception

    if self.jobStartEvent.is_set():
      if self.runJobCallback.exception:
        raise self.runJobCallback.exception

      waitEvent = eventWait_or([self.jobEndEvent, self.errorEvent, self.killEvent])
      while not waitEvent.wait(0.1):
        pass

      if self.killEvent.is_set():
        if self.jobEndEvent.is_set():
          raise RunJobKilledException()
        else:
          self.killJob()
          return

      if self.errorEvent.is_set():
        raise self.runJobCallback.exception

      if self.jobEndEvent.is_set():
        return
    else:
      # Shouldn't get here
      raise Exception("Shouldn't get here")

  def killJob(self):
    # Get the channel_id
    killchannel = self.getChannel()
    jobid = self.runJobCallback.trans_id
    killchannel.send({'msg' : 'JOB_KILL', 'job_id' : jobid })

    # Now wait for the job to finish
    waitEvent = eventWait_or([self.jobEndEvent, self.errorEvent])
    while not waitEvent.wait(0.1):
      pass

    if self.errorEvent.is_set():
      raise self.job.exception

    raise RunJobKilledException()

class RunJobCallback(object):
  """Callback for use with CallbackRegister.

  Responds to messages with matching channel and transaction ID."""

  _logger = _logger.getChild("atsim.pro_fit.runners.RunJobCallback")

  def __init__(self, workingDirectory, callback, trans_id, runClient):

    # REMOVE
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    # END REMOVE

    self.workingDirectory = workingDirectory
    self.callback  = callback
    self.trans_id = trans_id
    self._runClient = runClient
    self.exception = None
    self.active = True
    self.should_raise = False
    self.channel_id = None

    self._runjobState = _RunJobState(self)
    self.finishEvent = self._runjobState.finishEvent
    self.killEvent = self._runjobState.killEvent
    self._runjobState.start()

  def __call__(self, msg):
    if not self.active:
      return False

    self._logger.debug("Callback called job_id=%s, msg = %s", self.trans_id, msg)
    try:
      channel_id = msg['channel_id']
      mtype = msg.get('msg', "")

      if not mtype in ["JOB_START_ERROR", "JOB_END", "ERROR", "JOB_START"]:
        self._logger.debug("Message not relevant to callback job_id=%s, msg = %s", self.trans_id, msg)
        return False

      transid = msg.get('job_id', None)

      if transid != self.trans_id:
        self._logger.debug("Message not relevant to callback job_id=%s, msg = %s", self.trans_id, msg)
        return False

      if mtype == 'ERROR':
        self.exception =  self.error(msg)
        self._runjobState.errorEvent.set()
        self.finish()
        return True
      elif mtype == "JOB_START_ERROR":
        self.exception = JobStartException()
        self._runjobState.jobStartEvent.set()
        self.finish()
        return True

      if mtype == "JOB_START":
        pid = msg.get('pid', None)
        # self._parentRecord.pid = pid
        self.channel_id = channel_id
        self._logger.debug("Job id=%s started in directory='%s' with pid=%s", transid, self.workingDirectory, pid)
        self._runjobState.jobStartEvent.set()
        return True

      if mtype == "JOB_END":
        if msg.get('killed', False):
          self._logger.warning("Job id=%s (working directory='%s') was killed", transid, self.workingDirectory)
          self.exception = RunJobKilledException()
          self._runjobState.jobEndEvent.set()
          self.finish()
          return True
        else:
          self._logger.debug("Job id=%s (working directory='%s') finished with return code %d", transid, self.workingDirectory, msg.get("returncode", "Unknown"))
          self._runjobState.jobEndEvent.set()
          self.finish()
          return True
      return False
    except Exception:
      self.exception = sys.exc_info()
      self._runjobState.errorEvent.set()
      self.finish()
      return True

  @property
  def isKilled(self):
    return self.killEvent.is_set()

  def unregisterCallback(self):
    self.active = False
    self._runClient._submissionCallback.removeCallback(self)

  def error(self, msg):
    self._logger.warning("Callback job_id=%s, received ERROR message. msg = %s", self.trans_id, msg)
    reason = msg.get('reason', '')
    self.exception = RunChannelException("Error, received error: %s. Msg: %s", reason, msg)
    self._runJobState.errorEvent.set()

  def finish(self):
    self._logger.debug("finish called, %s", self.workingDirectory)
    self.active = False
    return True

  def ready(self):
    self._runjobState.readyEvent.set()

  def raise_exception(self):
    if self.should_raise:
      if self.exception:
        self._runJobState.errorEvent.set()
        raise self.exception

class SubmissionCallback(object):
  """Callback placed first in RunClient callback register.

  Is responsible for listening to 'READY' messages and submitting jobs as necessary."""

  _logger = _logger.getChild("SubmissionCallback")
  submitted = True
  active = True

  def __init__(self, channel, trans_id, callbacklist):
    self.lock = threading.RLock()
    self.channel = channel
    self.trans_id = trans_id
    self.callbacklist = callbacklist
    self.pendingJobs = deque()

  def __call__(self, msg):
    with self.lock:
      mtype = msg.get("msg")
      if mtype in ["BUSY", "READY"]:
        channel_id = msg.get("channel_id")
        self._statusPing(msg, channel_id)
        if mtype == "BUSY":
          return True
        elif mtype == "READY":
          cb = self._findNextJob()
          if not cb is None:
            self._submitJob(channel_id, cb)
          return True
      return False

  def registerJob(self, cb):
      self.pendingJobs.appendleft(cb)

  def removeCallback(self, cb):
    with self.lock:
      cb.active = False
      if cb in self.callbacklist:
        self.callbacklist.remove(cb)
      elif cb in self.pendingJobs:
        self.pendingJobs.remove(cb)

  def _findNextJob(self):
    try:
      job = None
      while job is None:
        job = self.pendingJobs.pop()
        if job.isKilled:
          job = None
      return job
    except IndexError:
      return None

  def _statusPing(self, msg, channel_id):
    channel = self.channel.getChannel(channel_id)
    channel.send(msg)

  def _submitJob(self, channel_id, job):
    job.channel_id = channel_id
    self.callbacklist.append(job)
    job.ready()



