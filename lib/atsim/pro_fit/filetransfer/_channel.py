import logging
import uuid
import itertools

from remote_exec import file_transfer_remote_exec

class ChannelCallback(object):
  """Execnet channels can only have a single callback associated with them. This object is a forwarding callback.
  It holds its own callback that can be changed and when registered with an execnet channel, forwards to its own callback"""

  def __init__(self, callback = None):
    self.callback = callback

  def __call__(self, msg):
    if self.callback:
      return self.callback(msg)
    return

class ChannelException(Exception):

  def __init__(self, message, wiremsg = None):
    super(ChannelException, self).__init__(message)
    self.wiremsg = wiremsg

class BaseChannel(object):
  """Base class for DownloadChannel and UploadChannel."""

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.BaseChannel")

  def __init__(self, execnet_gw, startmsg, remote_path,  channel_id = None, connection_timeout = 60):
    """Create an execnet channel (which is wrapped in this object) using the `_file_transfer_remote_exec` as its
    code.

    Args:
        execnet_gw (excenet.Gateway): Gateway used to create channel.
        startmsg (str): The value of the `msg` key in the dictionary sent as first message through channel to initialise it.
        remote_path (str): Path on remote host providing upload/download root directory.
        channel_id (None, optional): Channel id - if not specified a uuid will be generated.
        connection_timeout (int, optional): Timeout in seconds after which connection will fail if 'READY' message not received.
    """
    if channel_id is None:
      self._channel_id = str(uuid.uuid4())
    else:
      self._channel_id = channel_id

    self._callback = None
    self._remote_path = remote_path
    self._logger.info("Starting channel, id='%s', remote_path='%s'", self.channel_id, self.remote_path)
    self._channel = self._startChannel(execnet_gw, startmsg, connection_timeout)
    self._logger.info("Channel started id='%s', remote_path='%s'", self.channel_id, self.remote_path)


  def _startChannel(self, execnet_gw, startmsg, connection_timeout):
    from remote_exec import file_transfer_remote_exec
    channel = execnet_gw.remote_exec(file_transfer_remote_exec)
    channel.send({'msg' : startmsg, 'channel_id' : self.channel_id, 'remote_path' : self.remote_path })
    msg = channel.receive(connection_timeout)
    mtype = msg.get('msg', None)

    if mtype is None or not mtype in ['READY', 'ERROR']:
      self._logger.warning("Couldn't start channel, id='%s', remote_path='%s'", self.channel_id, self.remote_path)
      raise ChannelException("Couldn't create channel for channel_id: '%s', was expecting 'READY' got: %s" % (self.channel_id, msg), msg)

    if mtype == 'READY':
      self._channel_id = msg.get('channel_id', self.channel_id)
      self._remote_path = msg.get('remote_path', self.remote_path)
      return channel
    elif mtype == 'ERROR':
      self._logger.warning("Couldn't start channel, id='%s', remote_path='%s': %s", self.channel_id, self.remote_path, msg.get('reason', ""))
      raise ChannelException("Couldn't create channel for channel_id: '%s', %s" % (self.channel_id, msg.get('reason', '')), msg)

  @property
  def remote_path(self):
    return self._remote_path

  @property
  def channel_id(self):
    return self._channel_id

  def setcallback(self, callback):
    if self._callback is None:
      self._callback = ChannelCallback(callback)
      self._channel.setcallback(self._callback)
    else:
      self._callback.callback = callback

  def __iter__(self):
    return self

  def next(self):
    return self

  def send(self, msg):
    self._channel.send(msg)

  def __len__(self):
    return 1

class MultiChannel(object):

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.MultiChannel")

  def __init__(self, execnet_gw, channel_class, remote_path, num_channels = 1, channel_id = None):
    """Factory class and container for managing multiple Download/UploadChannel instances.

    This class implements a subset of the BaseChannel methods. Importantly, the send() method is not implemented.
    To send a message, the client must first obtain a channel instance by iterating over this MultiChannel instance
    (for instance, by calling next() ).

    Args:
        execnet_gw (execnet.Gateway): Gateway used to create execnet channels.
        channel_class (class): Class of channel type, this should be DownloadChannel or UploadChannel
        remote_path (str): Remote path passed to channel_class constructor.
        num_channels (int): Number of channels that should be created and managed.
        channel_id (None, optional): Base channel id, this will be appended by the number of each channel managed by multichannel.
          If `None` an ID will be automatically generated using `uuid.uuid4()`.
    """

    if channel_id is None:
      self._channel_id = str(uuid.uuid4())
    else:
      self._channel_id = channel_id

    self._logger.info("Starting %d channels with remote_path = '%s' and base channel_id='%s'", num_channels, remote_path, self._channel_id)
    self._channels = self._start_channels(execnet_gw, channel_class, remote_path, num_channels)
    self._iter = itertools.cycle(self._channels)

  def _start_channels(self, execnet_gw, channel_class, remote_path, num_channels):
    channels = []
    for i in xrange(num_channels):
      chan_id = "_".join([str(self._channel_id), str(i)])
      ch = channel_class(execnet_gw, remote_path, chan_id)
      channels.append(ch)
    return channels

  def __iter__(self):
    return self._iter

  def next(self):
    return self._iter.next()

  def setcallback(self, callback):
    for ch in self._channels:
      ch.setcallback(callback)

  def __len__(self):
    return len(self._channels)
