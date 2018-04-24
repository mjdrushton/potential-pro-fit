
import gevent
import itertools

class KeepAlive(object):
  """Send `KEEP_ALIVE` messages through a `BaseChannel` instance at regular intervals"""

  def __init__(self, channel, interval):
    """Create a keep alive loop object.

    Args:
      channel (atsim.profit.filetransfer.BaseChannel): Channel through which `KEEP_ALIVE` messages should be sent.
      interval (int): Interval (in seconds) between `KEEP_ALIVE` messages"""
    self.interval = interval
    self.channel = channel
    self._counter_prefix = "keepalive_"
    self._counter = itertools.count()
    self._greenlet = None

    if not self.interval > 0:
      raise ValueError("Interval should be positive number.")

    self.start()

  def _msgid(self):
    return self._counter_prefix+str(self._counter.next())

  def _createMessage(self):
    msg = dict(msg =  'KEEP_ALIVE', channel_id = self.channel.channel_id, id = self._msgid())
    return msg

  def start(self):

    def loop():
      while True:
        try:
          self.channel.send(self._createMessage())
          gevent.sleep(self.interval)
        except IOError:
          return

    greenlet = gevent.spawn(loop)
    self._greenlet = greenlet

  def kill(self):
    if self._greenlet:
      self._greenlet.kill()