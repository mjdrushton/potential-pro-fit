from atsim.pro_fit.fittool import ConfigException
from atsim.pro_fit import filetransfer

from atsim.pro_fit._util import NamedEvent
from _util import BatchNameIterator
from _exceptions import RunnerClosedException
from _pbs_client import PBSChannel, PBSClient

from atsim.pro_fit.filetransfer import UploadHandler, DownloadHandler

from atsim.pro_fit import _execnet
import execnet

import gevent
import gevent.event

import itertools
import logging
import posixpath
import uuid
import traceback
import sys
import os

from _base_remoterunner import BaseRemoteRunner
from _pbsrunner_batch import PBSRunnerBatch

EXECNET_TERM_TIMEOUT=10


class InnerPBSRunner(BaseRemoteRunner):
  """Runner class held by PBSRunner that does all the work."""

  def __init__(self, name, url, pbsinclude,  pbsbatch_size, qselect_poll_interval,  identityfile = None, extra_ssh_options = []):
    self.pbsinclude = pbsinclude
    self.pbsqselect_poll_interval = qselect_poll_interval
    self.pbsbatch_size = pbsbatch_size
    self._logger = logging.getLogger(__name__).getChild("InnerPBSRunner")
    super(InnerPBSRunner, self).__init__(name, url, identityfile, extra_ssh_options)

  def initialiseRun(self):
    channel_id = self._uuid + "_pbs"
    self._pbschannel = PBSChannel(self._gw, channel_id)
    self._pbsclient = PBSClient(self._pbschannel, pollEvery = self.pbsqselect_poll_interval)

  def createBatch(self, batchDir, jobs, batchId):
    return PBSRunnerBatch(self, batchDir, jobs, batchId, self._pbsclient)



class PBSRunner(object):
  """Runner that allows a remote PBS queuing system to be used to run jobs.

  SSH is used to communicate with server to submit jobs and copy files."""

  def __init__(self, name, url, pbsinclude, qselect_poll_interval = 10.0, pbsbatch_size = None, identityfile = None, extra_ssh_options = []):
    """Create PBSRunner instance

    Args:
        name (str): Name of runner
        url (str): Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
        pbsinclude (str): String that will be inserted at top of PBS submission script, this can be used to customise job requirements.
        qselect_poll_interval (float, optional): qselect will be polled using this interval (seconds).
        pbsbatch_size (None, optional): Maximum number of jobs (i.e. PBS array size). Qsub is invoked when files have been uploaded for this number of jobs.
                                        If this argument is `None` then all the jobs for a particular pprofit batch will be included in the same array job.
        identityfile (str, optional): Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                                       the platform's ssh command are used.
        extra_ssh_options (list, optional): List of (key,value) tuples that are added to the ssh_config file used when making ssh connections.
    """
    self._inner = InnerPBSRunner(name,
      url,
      pbsinclude,
      pbsbatch_size,
      qselect_poll_interval,
      identityfile,
      extra_ssh_options)

  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    Args:
        jobs (): List of `atsim.pro_fit.jobfactories.Job` as created by a JobFactory.

    Returns:
        object: An object that supports .join() which when joined will block until batch completion """
    return self._inner.runBatch(jobs)

  def close(self):
    """Shuts down the runner

    Returns:
      gevent.event.Event: Event that will be set() once shutdown has been completed.
    """
    return self._inner.close()

  @property
  def name(self):
    return self._inner.name

  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    allowedkeywords = set(['type', 'remotehost', 'pbsinclude'])
    cfgdict = dict(cfgitems)

    for k in cfgdict.iterkeys():
      if not k in allowedkeywords:
        raise ConfigException("Unknown keyword for Remote runner '%s'" % k)

    try:
      remotehost = cfgdict['remotehost']
    except KeyError:
      raise ConfigException("remotehost configuration item not found")

    if not remotehost.startswith("ssh://"):
      raise ConfigException("remotehost configuration item must start with ssh://")

    username, host, port, path = PBSRunner._urlParse(remotehost)
    if not host:
      raise ConfigException("remotehost configuration item should be of form ssh://[username@]hostname/remote_path")

    # Attempt connection and check remote directory exists
    group = _execnet.Group()
    try:
      if username:
        gwurl = "ssh=%s@%s" % (username, host)
      else:
        gwurl = "ssh=%s" % host
      gw = group.makegateway(gwurl)
      channel = gw.remote_exec(_execnet._remoteCheckPBS)
      channel.send(path)
      status = channel.receive()

      if not status:
        raise ConfigException("Remote directory does not exist or is not read/writable:'%s'" % path)

      # Check that qselect can be run
      status = channel.receive()
      if not status.startswith('qselect okay'):
        raise ConfigException("Cannot run 'qselect' on remote host.")

      # Check that qsub can be run
      status = channel.receive()
      if not status.startswith('qsub okay'):
        raise ConfigException("Cannot run 'qsub' on remote host.")

      # Configure runner from the version string
      identifyRecord = pbsIdentify(status[9:])

      channel.waitclose()

    except execnet.gateway_bootstrap.HostNotFound:
      raise ConfigException("Couldn't connect to host: %s" % gwurl)
    finally:
        group.terminate(EXECNET_TERM_TIMEOUT)

    pbsinclude = cfgdict.get('pbsinclude', None)
    if pbsinclude:
      try:
        pbsinclude = open(pbsinclude, 'rb').read()
      except IOError:
        raise ConfigException("Could not open file specified by 'pbsinclude' directive: %s" % pbsinclude)

    return PBSRunner(runnerName, remotehost, pbsinclude)

