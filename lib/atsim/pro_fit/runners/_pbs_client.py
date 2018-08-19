from atsim.pro_fit._channel import AbstractChannel
# from atsim.pro_fit._util import MultiCallback, CallbackRegister
from atsim.pro_fit._util import MultiCallback

# from _exceptions import JobKilledException

import _pbs_remote_exec

import logging

# from gevent import Greenlet
# import gevent
# from gevent.event import Event

# import itertools
# import uuid

from ._generic_queueing_system_client import GenericQueueingSystemClient


class PBSChannel(AbstractChannel):

  _logger = logging.getLogger(__name__).getChild("PBSChannel")

  def __init__(self, execnet_gw, channel_id = None, nocb = False):
    super(PBSChannel, self).__init__(
      execnet_gw,
      _pbs_remote_exec,
      channel_id)

    if not nocb:
      self.callback = MultiCallback()

  def make_start_message(self):
    return {'msg' : 'START_CHANNEL', 'channel_id' : self.channel_id}


class PBSClient(GenericQueueingSystemClient):
  pass