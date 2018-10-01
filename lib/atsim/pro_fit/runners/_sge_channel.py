from atsim.pro_fit._channel import AbstractChannel
from atsim.pro_fit._util import MultiCallback
from . import _sge_remote_exec

import logging

class SGEChannel(AbstractChannel):

  _logger = logging.getLogger(__name__).getChild("SGEChannel")

  def __init__(self, execnet_gw, channel_id = None, nocb = False):
    super(SGEChannel, self).__init__(
      execnet_gw,
      _sge_remote_exec,
      channel_id)

    if not nocb:
      self.callback = MultiCallback()

  def make_start_message(self):
    return {'msg' : 'START_CHANNEL', 'channel_id' : self.channel_id}