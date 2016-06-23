"""Classes to make the use of _run_remote_exec channels easier"""


from atsim.pro_fit._channel import AbstractChannel, MultiChannel
from atsim.pro_fit._util import CallbackRegister

import itertools
import logging
import multiprocessing
import threading
import uuid
import sys
import traceback

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

  def broadcast(self, msg):
    """Send msg to all channels registered with RunChannels"""
    self._logger.debug("Broadcasting message to %d channels: %s", len(self), msg)
    for channel in self._channels:
      channel.send(msg)

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
      cbobj = RunJobCallback(workingDirectory, _NullCallback, self._transid)
      cbobj.should_raise = True
    else:
      cbobj = RunJobCallback(workingDirectory, callback, self._transid)
      cbobj.event.set()
    self._submissionCallback.addCallback(cbobj)
    return cbobj

  @property
  def _transid(self):
    return "%s-%d" % (self._base_transid, self._id_count.next())

  def _kill(self, trans_id, channel_id):
    # Get the channel_id
    killchannel = self.channel.getChannel(channel_id)
    killchannel.send({'msg' : 'JOB_KILL', 'job_id' : trans_id })

class JobRecord(object):

  def __init__(self, rjc, runClient):
    self._runjobcallback = rjc
    self._runClient = runClient

  @property
  def workingDirectory(self):
    return self._runjobcallback.workingDirectory

  @property
  def trans_id(self):
    return self._runjobcallback.trans_id

  @property
  def completion_event(self):
    return self._runjobcallback.event

  @property
  def pid(self):
    return self._runjobcallback.pid

  def kill(self):
    """Kill the job.

    Returns:
        threading.Event: Event that is set once job completes.
    """
    with self._runjobcallback.lock, self._runClient._submissionCallback.lock:
      if not self._runjobcallback.active:
        raise JobAlreadyFinishedException()

      if self._runjobcallback.submitted:
        self._runClient._kill(self._runjobcallback.trans_id, self._runjobcallback.channel_id)
      else:
        self._runClient._submissionCallback.removeCallback(self._runjobcallback)
        self._runjobcallback.exception = RunJobKilledException()
        self._runjobcallback.finish()
      return self.completion_event

class JobAlreadyFinishedException(Exception):
  pass

class RunJobKilledException(Exception):
  pass

class RunJobCallback(object):
  """Callback for use with CallbackRegister.

  Responds to messages with matching channel and transaction ID."""

  _logger = _logger.getChild("atsim.pro_fit.runners.RunJobCallback")

  def __init__(self, workingDirectory, callback, trans_id):
    self.lock = threading.RLock()
    self.workingDirectory = workingDirectory
    self.submitted = False
    self.callback  = callback
    self.trans_id = trans_id
    self.exception = None
    self.active = True
    self.event = threading.Event()
    self.should_raise = False
    self.pid = None
    self.channel_id = None

  def __call__(self, msg):
    with self.lock:
      if not self.submitted or not self.active:
        return False

      self._logger.debug("Callback called job_id=%s, msg = %s", self.trans_id, msg)
      try:
        # transid = msg['id']
        channel_id = msg['channel_id']
        mtype = msg.get('msg', "")

        if not mtype in ["JOB_START_ERROR", "JOB_END", "ERROR", "JOB_START"]:
          self._logger.debug("Message not relevant to callback job_id=%s, msg = %s", self.trans_id, msg)
          return False

        transid = msg.get('job_id', None)

        if transid != self.trans_id:
          self._logger.debug("Message not relevant to callback job_id=%s, msg = %s", self.trans_id, msg)
          return False

        if mtype == 'ERROR' or mtype == "JOB_START_ERROR":
          self.error(msg)
          return True

        if mtype == "JOB_START":
          # import pdb;pdb.set_trace()
          pid = msg.get('pid', None)
          self.pid = pid
          self.channel_id = channel_id
          self._logger.debug("Job id=%s started in directory='%s' with pid=%s", transid, self.workingDirectory, pid)
          return True

        if mtype == "JOB_END":
          if msg.get('killed', False):
            self._logger.warning("Job id=%s (working directory='%s') was killed", transid, self.workingDirectory)
            self.exception = RunJobKilledException()
            self.finish()
          else:
            self._logger.debug("Job id=%s (working directory='%s') finished with return code %d", transid, self.workingDirectory, msg.get("returncode", "Unknown"))
            self.finish()
        return False
      except Exception:
        self.exception = sys.exc_info()
        self.finish()

  def error(self, msg):
    self._logger.warning("Callback job_id=%s, received ERROR message. msg = %s", self.trans_id, msg)
    reason = msg.get('reason', '')
    raise RunChannelException("Error, received error: %s. Msg: %s", reason, msg)

  def finish(self):
    with self.lock:
      self.callback(self.exception)
      self.active = False
      self.event.set()
      return True

  def raise_exception(self):
    if self.should_raise:
      if self.exception:
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

  def __call__(self, msg):
    with self.lock:
      self._logger.debug("Ready Callback, msg = %s", msg)
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

  def addCallback(self, cb):
    with self.lock:
      self.callbacklist.append(cb)

  def removeCallback(self, cb):
    with self.lock:
      cb.active = False
      self.callbacklist.remove(cb)

  def _findNextJob(self):
    for cb in self.callbacklist:
      if not cb.submitted:
        return cb
    return None

  def _statusPing(self, msg, channel_id):
    channel = self.channel.getChannel(channel_id)
    channel.send(msg)

  def _submitJob(self, channel_id, callback):
    jobid = callback.trans_id
    sendmsg = {'msg' : 'JOB_START', 'job_id' : jobid, 'job_path' : callback.workingDirectory }
    channel = self.channel.getChannel(channel_id)
    callback.submitted = True
    self._logger.debug("Submitting job to channel_id = %s: %s", channel.channel_id, sendmsg)
    channel.send(sendmsg)
