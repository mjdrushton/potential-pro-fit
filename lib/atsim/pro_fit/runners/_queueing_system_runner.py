from _queueing_system_client import QueueingSystemClient
from _queueing_system_runner_batch import QueueingSystemRunnerBatch

import logging
import os

from _base_remoterunner import BaseRemoteRunner
from _base_remoterunner import RemoteRunnerCloseThreadBase


class _QueueingSystemRunnerCloseThread(RemoteRunnerCloseThreadBase):

  def closeRunClient(self):
    self.runner._pbsclient.close(closeChannel = False)
    evts = self._closeChannel(self.runner._pbsclient.channel, 'closeRunClient')
    return evts


class QueueingSystemRunnerBaseClass(BaseRemoteRunner):
  """Generic runner class for batch queueing systems. This is an abstract base class designed to be used as the basis for publicly accesible runners.
  
  Sub-classes should override this class and provide an implementation of the `makeRunChannel` method."""

  """Suffix appended to uuid to produce channel's ID"""
  id_suffix = "_qs"

  def __init__(self, name, url, header_include,  batch_size, qselect_poll_interval,  identityfile = None, extra_ssh_options = [], do_cleanup = True):
    """Create runner.

    Args:
      name (str): Runner name.
      url (str): SSH remote host job directory url and path, as accepted by BaseRemoteRunner.
      batch_size (int): Number of pprofit jobs to be included in each queueing system array job.
      qselect_poll_interval (float): Time interval (seconds) at which queueing system is queried to establish state of array jobs.
      identityfile (str) : Path to ssh private key file used to log into remote host (or None if system defaults are to be used).
      extra_ssh_options (list): List of strings giving any extra ssh configuration options.
      do_cleanup (bool): If True perform file clean-up on queueing system submission host. If False leave temporary files on remote machine (for debugging)."""
    
    self.header_include = []
    if not header_include is None:
      self.header_include = header_include.split(os.linesep)

    self.qselect_poll_interval = qselect_poll_interval
    self.batch_size = batch_size
    super(QueueingSystemRunnerBaseClass, self).__init__(name, url, identityfile, extra_ssh_options, do_cleanup)

  @property
  def _logger(self):
    return self._get_logger()

  def _get_logger(self):
    return logging.getLogger(__name__).getChild("QueueingSystemRunnerBaseClass")

  def initialiseRun(self):
    channel_id = self._uuid + self.id_suffix
    self._pbschannel = self.makeRunChannel(channel_id)
    self._pbsclient = QueueingSystemClient(self._pbschannel, pollEvery = self.qselect_poll_interval)

  def makeRunChannel(self, channel_id):
    """Method which returns an execnet channel suitable for use as argument to atsim.pro_fit.runners._queuing_system_client.QueueingSystemClient constructor.

    Args:
      channel_id (str) : String used to identify created channel.
      
    Returns:
      atsim.pro_fit._channel.AbstractChannel : Channel instance that supports the protocol expected by QueueingSystemClient."""
    raise NotImplementedError("Sub-classes must provide implementation for makeRunChannel.")

  def createBatch(self, batchDir, jobs, batchId):
    return QueueingSystemRunnerBatch(self, batchDir, jobs, batchId, self._pbsclient, self.header_include)

  def makeCloseThread(self):
    return _QueueingSystemRunnerCloseThread(self)


