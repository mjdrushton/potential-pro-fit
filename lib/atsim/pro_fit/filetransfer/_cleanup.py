from atsim.pro_fit._channel import AbstractChannel
from remote_exec import file_cleanup_remote_exec
from atsim.pro_fit._util import CallbackRegister, NamedEvent

import logging
import threading
import traceback
import uuid
import itertools
import sys

class CleanupChannel(AbstractChannel):
  """Channel type for use with CleanupClient"""

  _logger = logging.getLogger('atsim.pro_fit.filetransfer.CleanupChannel')

  def __init__(self, execnet_gw, remote_path,  channel_id = None, connection_timeout = 60):
    self._remote_path = remote_path
    super(CleanupChannel, self).__init__(execnet_gw, file_cleanup_remote_exec, channel_id, connection_timeout)

  def make_start_message(self):
    return {'msg' : 'START_CLEANUP_CHANNEL', 'channel_id' : self.channel_id, 'remote_path' : self.remote_path }

  def ready(self, msg):
    self._channel_id = msg.get('channel_id', self.channel_id)
    self._remote_path = msg.get('remote_path', self.remote_path)

  @property
  def remote_path(self):
    return self._remote_path

  @property
  def channel_id(self):
    return self._channel_id

class CleanupChannelException(Exception):
  pass

class CleanupAgentCallback(object):
  """Callback for use with CallbackRegister.

  Responds to messages with matching channel and transaction ID."""

  _logger = logging.getLogger("atsim.pro_fit.filetransfer.CleanupAgentCallback")

  def __init__(self, callback, expected_msg, channel_id, trans_id):
    self.callback  = callback
    self.channel_id = channel_id
    self.trans_id = trans_id
    self.exception = None
    self.expected_msg = expected_msg
    self.active = True
    self.event = NamedEvent("CleanupAgentCallback")
    self.should_raise = False

  def __call__(self, msg):
    self._logger.debug("Callback called trans_id=%s, msg = %s", self.trans_id, msg)
    try:
      transid = msg['id']
      channel_id = msg['channel_id']

      if not (transid == self.trans_id and channel_id == self.channel_id):
        self._logger.debug("Message not relevant to callback trans_id=%s, msg = %s", self.trans_id, msg)
        return False

      mtype = msg['msg']
      if mtype == 'ERROR':
        self.error(msg)
        return True

      if mtype != self.expected_msg:
        raise CleanupChannelException("Was expecting '%s' message but received '%s': %s", self.expectd_msg, mtype, msg)

      return self.finish()

    except Exception:
      self.exception = sys.exc_info()
      self.finish()

  def error(self, msg):
    self._logger.warning("Callback trans_id=%s, received ERROR message. msg = %s", self.trans_id, msg)
    reason = msg.get('reason', '')
    raise CleanupChannelException("Error, received error: %s. Msg: %s", reason, msg)

  def finish(self):
    self.callback(self.exception)
    self.active = False
    self.event.set()
    return True

  def raise_exception(self):
    if self.should_raise:
      if self.exception:
        raise self.exception

def _NullCallback(*args, **kwargs):
  pass

class CleanupClient(object):
  """Object oriented interface to the `file_cleanup_remote_exec`.

  Directories and files registered through `lock()` are protected from deletion.
  When the files are unlocked, through `unlock()`, the files become eligible for
  deletion. Deletion takes place when a directory is unlocked and there are
  no files beneath it in the directory hierarchy. All registered files are
  deleted when the cleanup agent terminates."""

  def __init__(self, cleanup_channel):
    """Create `CleanupAgent`

    Args:
        cleanup_channel (atsim.pro_fit.filetransfer.CleanupChannel): Cleanup channel.
    """
    self.channel = cleanup_channel
    self._cbregister = CallbackRegister()
    self._base_transid = uuid.uuid4()
    self._id_count = itertools.count()
    self._init_channel()
    self.block_timeout = 60

  def lock(self, *paths, **kwargs):
    """Lock specified paths.

    If callback is specified, `lock()` will act asynchronously, with callback
    being called on completion of the lock request.

    If no callback is given then call will block until request completes.

    Args:
        *paths (str): Paths to be locked
        callback (None, optional): Unary function  that takes an exception as its
          argument. If an error is encountered during locking then the resultant
          exception is passed into the callback.
    """
    callback = kwargs.get('callback', None)
    cbobj = self._registerCallback(callback, "LOCKED")
    msg = {'msg' : 'LOCK', 'remote_path' : paths, 'id' : cbobj.trans_id}
    self.channel.send(msg)
    cbobj.event.wait(self.block_timeout)
    cbobj.raise_exception()

  def unlock(self, *paths, **kwargs):
    """Unlock specified paths.

    If callback is specified, `unlock()` will act asynchronously, with callback
    being called on completion of the unlock request.

    If no callback is given then call will block until request completes.

    Args:
        *paths (str): Paths to be unlocked.
        callback (None, optional): Unary function  that takes an exception as its
          argument. If an error is encountered during locking then the resultant
          exception is passed into the callback.
    """
    callback = kwargs.get('callback', None)
    cbobj = self._registerCallback(callback, "UNLOCKED")
    msg = {'msg' : 'UNLOCK', 'remote_path' : paths, 'id' : cbobj.trans_id}
    self.channel.send(msg)
    cbobj.event.wait(self.block_timeout)
    cbobj.raise_exception()

  def flush(self, callback = None):
    """Force any outstanding file deletions to be processed.

    If callback is specified, `flush()` will act asynchronously, with callback
    being called on completion of the flush request.

    If no callback is given then call will block until request completes.

    Args:
        callback (None, optional): Unary function  that takes an exception as its
          argument. If an error is encountered during locking then the resultant
          exception is passed into the callback.
    """
    cbobj = self._registerCallback(callback, "FLUSHED")
    msg = {'msg' : 'FLUSH', 'id' : cbobj.trans_id}
    self.channel.send(msg)

    if callback is None:
      cbobj.event.wait(self.block_timeout)
      if cbobj.should_raise:
        cbobj.raise_exception()
      return None
    else:
      return cbobj.event

  def _registerCallback(self, callback, expectedMessage):
    if callback is None:
      cbobj = CleanupAgentCallback(_NullCallback, expectedMessage, self.channel.channel_id, self._transid)
      cbobj.should_raise = True
    else:
      cbobj = CleanupAgentCallback(callback, expectedMessage, self.channel.channel_id, self._transid)
    self._cbregister.append(cbobj)
    return cbobj

  def _init_channel(self):
    self.channel.setcallback(self._cbregister)

  @property
  def _transid(self):
    return "%s-%d" % (self._base_transid, self._id_count.next())
