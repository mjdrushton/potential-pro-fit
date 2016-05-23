import logging
import uuid

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

  At a minimum vlient code should override the make_start_message() method.

  The start_response() method can also be used to customise channel start behaviour.

  """

  _logger = logging.getLogger("atsim.pro_fit._channel.BaseChannel")

  def __init__(self, execnet_gw, channel_remote_exec, channel_id = None, connection_timeout = 60):
    """Create an execnet channel (which is wrapped in this object) using the `_file_transfer_remote_exec` as its
    code.

    Args:
        execnet_gw (excenet.Gateway): Gateway used to create channel.
        channel_remote_exec (module): Module that should be used to start execnet channel.
        channel_id (None, optional): Channel id - if not specified a uuid will be generated.
        connection_timeout (int, optional): Timeout in seconds after which connection will fail if 'READY' message not received.
    """
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
    startmsg = self.make_start_message()
    self._logger.debug("Channel start message: %s", startmsg)
    channel.send(startmsg)
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

  def __iter__(self):
    return self

  def next(self):
    return self

  def send(self, msg):
    self._channel.send(msg)

  def __len__(self):
    return 1

