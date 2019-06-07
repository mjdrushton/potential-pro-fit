from atsim.pro_fit._channel import AbstractChannel

import logging
import itertools

from .._keepalive import KeepAlive

class BaseChannel(AbstractChannel):
  """Base class for DownloadChannel and UploadChannel."""

  _logger = logging.getLogger("atsim.pro_fit._channel.BaseChannel")

  def __init__(self, execnet_gw, startmsg, remote_path,  channel_id = None, connection_timeout = 60, keepAlive = 10):
    """Create an execnet channel (which is wrapped in this object) using the `_file_transfer_remote_exec` as its
    code.

    Args:
        execnet_gw (excenet.Gateway): Gateway used to create channel.
        startmsg (str): The value of the `msg` key in the dictionary sent as first message through channel to initialise it.
        remote_path (str): Path on remote host providing upload/download root directory.
        channel_id (None, optional): Channel id - if not specified a uuid will be generated.
        connection_timeout (int, optional): Timeout in seconds after which connection will fail if 'READY' message not received.
        keepAlive (int, optional): Send a `KEEP_ALIVE` message to the server every `keepAlive` seconds. If `None` do not send `KEEP_ALIVE` messages.
    """
    from .remote_exec import file_transfer_remote_exec
    self._startmsg = startmsg
    self._remote_path = remote_path
    super(BaseChannel, self).__init__(execnet_gw, file_transfer_remote_exec, channel_id, connection_timeout)

    if not keepAlive > 0:
      self._keepAlive = None
    else:
      self._keepAlive = KeepAlive(self, keepAlive)
      self._keepAlive.start()

  def make_start_message(self):
    return {'msg' : self._startmsg, 'channel_id' : self.channel_id, 'remote_path' : self.remote_path }

  def ready(self, msg):
    self._channel_id = msg.get('channel_id', self.channel_id)
    self._remote_path = msg.get('remote_path', self.remote_path)

  @property
  def remote_path(self):
    return self._remote_path

  def close(self, error = None):
    if not self._keepAlive is None:
      self._keepAlive.kill()
    super(BaseChannel, self).close(error)

  def waitclose(self, timeout =  None):
    if not self._keepAlive is None:
      self._keepAlive.kill()
    super(BaseChannel,self).waitclose(timeout)
    

class ChannelFactory(object):
  """Factory class for use with MultiChannel"""

  def __init__(self, channelClass, remotePath, keepAlive):
    self.remotePath = remotePath
    self.channelClass = channelClass
    self.keepAlive = keepAlive

  def createChannel(self, execnet_gw, channel_id):
    return self.channelClass(execnet_gw, self.remotePath, channel_id, self.keepAlive)
