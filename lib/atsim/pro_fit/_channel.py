import logging
import uuid
import itertools
import sys

from gevent.queue import Queue
from gevent import Greenlet
import gevent.lock
import gevent

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


class AbstractChannel(object):
  """Abstract base class for making execnet channels nicer to work with.

  At a minimum client code should override the make_start_message() method.

  The start_response() method can also be used to customise channel start behaviour.

  """


  def __init__(self, execnet_gw, channel_remote_exec, channel_id = None, connection_timeout = 60):
    """Create an execnet channel (which is wrapped in this object) using the `_file_transfer_remote_exec` as its
    code.

    Args:
        execnet_gw (excenet.Gateway): Gateway used to create channel.
        channel_remote_exec (module): Module that should be used to start execnet channel.
        channel_id (None, optional): Channel id - if not specified a uuid will be generated.
        connection_timeout (int, optional): Timeout in seconds after which connection will fail if 'READY' message not received.
    """
    self._logger = logging.getLogger("__name__").getChild("BaseChannel")
    if channel_id is None:
      self._channel_id = str(uuid.uuid4())
    else:
      self._channel_id = channel_id

    self._callback = None
    self._logger.info("Starting channel, id='%s'", self.channel_id)
    self._channel = self._startChannel(execnet_gw, channel_remote_exec, connection_timeout)
    self._logger.info("Channel started id='%s'", self.channel_id)

  def _startChannel(self, execnet_gw, channel_remote_exec, connection_timeout):
    channel = execnet_gw.remote_exec(channel_remote_exec)

    # Was getting reentrant io error when sending, add a lock to the channel that can be used to synchronize message sending.
    if not hasattr(channel.gateway, '_sendlock'):
      channel.gateway._sendlock = gevent.lock.Semaphore()

    startmsg = self.make_start_message()
    self._logger.debug("Channel start message: %s", startmsg)
    self._send(channel, startmsg)
    msg = channel.receive(connection_timeout)
    self.start_response(msg)
    return channel

  def make_start_message(self):
    """Returns the message that should be sent to channel to initialise the remote exec.

    Returns:
        Start message.
    """
    raise Exception("This class needs to be implemented in child classes.")

  def start_response(self, msg):
    """Called with the response to sending start message to execnet channel

    Args:
        msg : Message received after starting channel.

    """
    mtype = msg.get('msg', None)

    if mtype is None or not mtype in ['READY', 'ERROR']:
      self._logger.warning("Couldn't start channel, id='%s', remote_path='%s'", self.channel_id, self.remote_path)
      raise ChannelException("Couldn't create channel for channel_id: '%s', was expecting 'READY' got: %s" % (self.channel_id, msg), msg)

    if mtype == 'READY':
      self.ready(msg)
    elif mtype == 'ERROR':
      self.error(msg)

  def ready(self, msg):
    pass

  def error(self, msg):
    self._logger.warning("Couldn't start channel, id='%s': %s", self.channel_id, msg.get('reason', ""))
    raise ChannelException("Couldn't create channel for channel_id: '%s', %s" % (self.channel_id, msg.get('reason', '')), msg)

  @property
  def channel_id(self):
    return self._channel_id

  def setcallback(self, callback):
    if self._callback is None:
      self._callback = ChannelCallback(callback)
      self._channel.setcallback(self._callback)
    else:
      self._callback.callback = callback

  def getcallback(self):
    if self._callback is None:
      return None
    return self._callback.callback

    return self._callback

  callback = property(fget = getcallback, fset = setcallback)

  def __iter__(self):
    return self._channel

  def next(self):
    msg = self._channel.next()
    self._logger.debug("_next, %s: %s", self.channel_id, msg)
    return msg

  def _send(self, ch, msg):
    with ch.gateway._sendlock:
      self._logger.debug("_send, %s: %s", self.channel_id, msg)
      ch.send(msg)

  def send(self, msg):
    return self._send(self._channel, msg)

  def __len__(self):
    return 1

  def close(self, error = None):
    return self._channel.close(error)

  def waitclose(self, timeout = None):
    return self._channel.waitclose(timeout)


class MultiChannel(object):

  _logger = logging.getLogger("atsim.pro_fit._channel.MultiChannel")

  def __init__(self, execnet_gw, channel_factory, num_channels = 1, channel_id = None):
    """Factory class and container for managing multiple Download/UploadChannel instances.

    This class implements a subset of the BaseChannel methods. Importantly, the send() method is not implemented.
    To send a message, the client must first obtain a channel instance by iterating over this MultiChannel instance
    (for instance, by calling next() ).

    Args:
        execnet_gw (execnet.Gateway): Gateway used to create execnet channels.
        channel_factor (ChannelFactory): ChannelFactory that has `.createChannel(execnet_gw, channel_id)` returning new channel instances.
        num_channels (int): Number of channels that should be created and managed.
        channel_id (None, optional): Base channel id, this will be appended by the number of each channel managed by multichannel.
          If `None` an ID will be automatically generated using `uuid.uuid4()`.
    """
    if channel_id is None:
      self._channel_id = str(uuid.uuid4())
    else:
      self._channel_id = channel_id

    self._logger.info("Starting %d channels with base channel_id='%s'", num_channels, self._channel_id)
    self._channels = self._start_channels(execnet_gw, channel_factory, num_channels)
    self._iter = itertools.cycle(self._channels)
    self._callback = None

  def _start_channels(self, execnet_gw, channel_factory, num_channels):
    channels = []
    for i in xrange(num_channels):
      chan_id = "_".join([str(self._channel_id), str(i)])
      ch = channel_factory.createChannel(execnet_gw, chan_id)
      channels.append(ch)
    return channels

  def __iter__(self):
    return self._iter

  def next(self):
    return self._iter.next()

  def setcallback(self, callback):
    self._callback = callback
    for ch in self._channels:
      ch.setcallback(callback)

  def getcallback(self):
    return self._callback

  callback = property(fget = getcallback, fset = setcallback)

  def __len__(self):
    return len(self._channels)

  def waitclose(self, timeout = None):
    for channel in self._channels:
      channel.waitclose(timeout)

  def broadcast(self, msg):
    """Send msg to all channels registered with MultiChannel"""
    self._logger.debug("Broadcasting message to %d channels: %s", len(self), msg)
    for channel in self._channels:
      channel.send(msg)
