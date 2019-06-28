from atsim.pro_fit._channel import AbstractChannel
from atsim.pro_fit._util import MultiCallback
from . import _slurm_remote_exec

import logging


class SlurmChannel(AbstractChannel):

    _logger = logging.getLogger(__name__).getChild("SlurmChannel")

    def __init__(self, execnet_gw, channel_id=None, nocb=False):
        super(SlurmChannel, self).__init__(
            execnet_gw, _slurm_remote_exec, channel_id
        )

        if not nocb:
            self.callback = MultiCallback()

    def make_start_message(self):
        return {"msg": "START_CHANNEL", "channel_id": self.channel_id}
