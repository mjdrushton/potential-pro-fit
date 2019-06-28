"""Classes to make the use of _run_remote_exec channels easier"""

from atsim.pro_fit._channel import AbstractChannel, MultiChannel
from atsim.pro_fit._util import CallbackRegister, NamedEvent
from ._exceptions import JobKilledException, NonZeroExitStatus
from atsim.pro_fit._keepalive import KeepAlive

import itertools
import logging
import sys
import uuid

import gevent
import gevent.event


class RunChannelException(Exception):
    pass


class RunChannel(AbstractChannel):

    _logger = logging.getLogger(__name__).getChild("RunChannel")

    def __init__(
        self,
        execnet_gw,
        channel_id=None,
        nprocesses=None,
        shell="/bin/bash",
        hardkill_timeout=60,
        connection_timeout=60,
        keepAlive=10,
    ):
        """Summary

    Args:
        execnet_gw (execnet.Gateway): Execnet gateway used to create channel managed by this object.
        channel_id (None, optional): Description
        shell (str, optional): Shell that should be used on remote host to run commands.
        hardkill_timeout (int, optional): When jobs are terminated, an initial attempt is made to kill the job by
          sending the termination signal. If this has failed, after this timeout the SIGKILL signal is sent to hard stop the process.
        connection_timeout (int, optional): Time limit for connection attempt.
        keepAlive (int, optional): Send a `KEEP_ALIVE` message to the server every `keepAlive` seconds. If `None` do not send `KEEP_ALIVE` messages.

    """
        from . import _run_remote_exec

        self.shell = shell
        self.hardkill_timeout = hardkill_timeout
        self.nprocesses = nprocesses
        super(RunChannel, self).__init__(
            execnet_gw, _run_remote_exec, channel_id, connection_timeout
        )

        if not keepAlive > 0:
            self._keepAlive = None
        else:
            self._keepAlive = KeepAlive(self, keepAlive)
            self._keepAlive.start()

    def make_start_message(self):
        return {
            "msg": "START_CHANNEL",
            "channel_id": self.channel_id,
            "shell": self.shell,
            "hardkill_timeout": self.hardkill_timeout,
            "nprocesses": self.nprocesses,
        }

    def close(self, error=None):
        if not self._keepAlive is None:
            self._keepAlive.kill()
        super(RunChannel, self).close(error)

    def waitclose(self, timeout=None):
        if not self._keepAlive is None:
            self._keepAlive.kill()
        super(RunChannel, self).waitclose(timeout)


def _NullCallback(*args, **kwargs):
    pass


class RunClient(object):

    _logger = logging.getLogger(__name__).getChild("RunClient")

    def __init__(self, channel):
        self.channel = channel
        self._base_transid = uuid.uuid4()
        self._id_count = itertools.count()
        self._cbregister = CallbackRegister()
        self.channel.setcallback(self._cbregister)

    def runCommand(self, workingDirectory, callback=None):
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
        self._logger.debug(
            "runCommand called for workingDirectory = '%s'", workingDirectory
        )
        cbobj = self._registerCallback(workingDirectory, callback)
        if callback is None:
            cbobj.finishEvent.wait()
            cbobj.raise_exception()
            return None
        else:
            jobRecord = JobRecord(cbobj, self)
            return jobRecord

    def _registerCallback(self, workingDirectory, callback):
        if callback is None:
            cbobj = RunJobCallback(
                workingDirectory, _NullCallback, self._transid, self
            )
            cbobj.should_raise = True
        else:
            cbobj = RunJobCallback(
                workingDirectory, callback, self._transid, self
            )
        self._submitJob(cbobj)
        return cbobj

    @property
    def _transid(self):
        return "%s-%d" % (self._base_transid, next(self._id_count))

    def _submitJob(self, cbobj):
        cbobj.channel_id = self.channel.channel_id
        self._cbregister.append(cbobj)
        self.channel.send(
            {
                "msg": "JOB_START",
                "job_path": cbobj.workingDirectory,
                "job_id": cbobj.trans_id,
            }
        )
        gevent.sleep(0)


class JobRecord(object):

    logger = logging.getLogger(__name__).getChild("JobRecord")

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
        return self._runjobcallback.finishEvent

    @property
    def pidSetEvent(self):
        return self._runjobcallback.pidSetEvent

    @property
    def jobRunEvent(self):
        return self.pidSetEvent

    @property
    def pid(self):
        return self._runjobcallback.pid

    def kill(self):
        """Kill the job.

    Returns:
        gevent.event.Event: Event that is set once job completes.
    """
        if self.completion_event.is_set():
            raise JobAlreadyFinishedException()

        self._runjobcallback.kill()
        return self.completion_event


class JobAlreadyFinishedException(Exception):
    pass


class JobStartException(Exception):
    pass


class RunJobKilledException(JobKilledException):
    pass


class RunJobCallback(object):
    """Callback for use with CallbackRegister.

  Responds to messages with matching channel and transaction ID."""

    _logger = logging.getLogger(__name__).getChild("RunJobCallback")

    def __init__(self, workingDirectory, callback, trans_id, runClient):
        self.workingDirectory = workingDirectory
        self.callback = callback
        self.trans_id = trans_id
        self._runClient = runClient
        self.exception = None
        self.active = True
        self.should_raise = False
        self.channel_id = None

        self.pidSetEvent = gevent.event.Event()
        self._pid = None
        self.finishEvent = gevent.event.Event()
        self.killEvent = gevent.event.Event()

    def __call__(self, msg):
        if not self.active:
            return False

        self._logger.debug(
            "Callback called job_id=%s, msg = %s", self.trans_id, msg
        )
        try:
            channel_id = msg["channel_id"]
            mtype = msg.get("msg", "")

            if not mtype in [
                "JOB_START_ERROR",
                "JOB_END",
                "ERROR",
                "JOB_START",
            ]:
                self._logger.debug(
                    "Message not relevant to callback job_id=%s, msg = %s",
                    self.trans_id,
                    msg,
                )
                return False

            transid = msg.get("job_id", None)

            if transid != self.trans_id:
                self._logger.debug(
                    "Message not relevant to callback job_id=%s, msg = %s",
                    self.trans_id,
                    msg,
                )
                return False

            if mtype == "ERROR":
                self.exception = self.error(msg)
                # self._runjobState.errorEvent.set()
                self.finish()
                return True
            elif mtype == "JOB_START_ERROR":
                self.exception = JobStartException()
                # self._runjobState.jobStartEvent.set()
                self.finish()
                return True

            if mtype == "JOB_START":
                pid = msg.get("pid", None)
                self.pid = pid
                self.channel_id = channel_id
                self._logger.debug(
                    "Job id=%s started in directory='%s' with pid=%s",
                    transid,
                    self.workingDirectory,
                    pid,
                )
                # self._runjobState.jobStartEvent.set()
                return True

            if mtype == "JOB_END":
                if msg.get("killed", False):
                    self._logger.warning(
                        "Job id=%s (working directory='%s') was killed",
                        transid,
                        self.workingDirectory,
                    )
                    self.exception = RunJobKilledException()
                    # self._runjobState.jobEndEvent.set()
                    self.finish()
                    return True
                else:
                    self._logger.debug(
                        "Job id=%s (working directory='%s') finished with return code %d",
                        transid,
                        self.workingDirectory,
                        msg.get("returncode", "Unknown"),
                    )

                    if msg.get("returncode", 0) != 0:
                        self.exception = NonZeroExitStatus(
                            "Job finished with return code %d"
                            % msg.get("returncode", 0)
                        )
                    self.finish()
                    return True
            return False
        except Exception:
            self.exception = sys.exc_info()
            # self._runjobState.errorEvent.set()
            self.finish()
            return True

    # @property
    # def isKilled(self):
    #   return self.killEvent.is_set()

    def kill(self):
        self._runClient.channel.send(
            {"msg": "JOB_KILL", "job_id": self.trans_id}
        )

    def error(self, msg):
        self._logger.warning(
            "Callback job_id=%s, received ERROR message. msg = %s",
            self.trans_id,
            msg,
        )
        reason = msg.get("reason", "")
        self.exception = RunChannelException(
            "Error, received error: %s. Msg: %s", reason, msg
        )
        self._runJobState.errorEvent.set()

    def finish(self):
        self._logger.debug("finish called, %s", self.workingDirectory)
        self.active = False
        self.callback(self.exception, self)
        self.finishEvent.set()
        return True

    def raise_exception(self):
        if self.should_raise:
            if self.exception:
                self._runjobState.errorEvent.set()
                raise self.exception

    def _setPid(self, pid):
        self._logger.getChild("_setPid").debug(
            "setting pid:%d for object id:%s", pid, id(self)
        )

        self._pid = pid
        self.pidSetEvent.set()

    def _getPid(self):
        return self._pid

    pid = property(_getPid, _setPid)
