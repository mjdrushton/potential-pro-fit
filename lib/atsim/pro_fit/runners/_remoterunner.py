from atsim.pro_fit.fittool import ConfigException

from atsim.pro_fit import _execnet

import execnet
import logging

from _base_remoterunner import BaseRemoteRunner, RemoteRunnerCloseThreadBase

EXECNET_TERM_TIMEOUT=10

from _runner_batch import RunnerBatch
from _run_remote_client import RunChannel, RunClient

import gevent
from gevent.event import Event

class _RemoteRunnerCloseThread(RemoteRunnerCloseThreadBase):

  _logger = logging.getLogger(__name__).getChild("_RemoteRunnerCloseThread")

  def closeRunClient(self):
    return self._closeChannel(self.runner._runChannel, "run")

class InnerRemoteRunner(BaseRemoteRunner):
  """The actual implementation of RemoteRunner.

  InnerRemotRunner is used to allow RemoteRunner to just expose the public Runner interface whilst InnerRemoteRunner
  has a much more extensive interface (required by the RunnerBatch)"""

  _logger = logging.getLogger(__name__).getChild("InnerRemoteRunner")

  def __init__(self, name, url, nprocesses, identityfile=None, extra_ssh_options = [], do_cleanup = True):
    """Instantiate RemoteRunner.

    Args:
        name (str): Name of this runner.
        url (str): Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
        nprocesses (int): Number of processes that can be run in parallel by this runner
        identityfile (file, optional): Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                      the platform's ssh command are used.
        extra_ssh_options (list, optional): List of (key,value) tuples that are added to the ssh_config file used when making ssh connections.
        do_cleanup (bool): If `True` file clean-up will be automatically performed following a run and on termination of the runner. If `False` this
                behaviour is disabled. This option is provided for the purposes of debugging.
    """
    self._nprocesses = nprocesses
    super(InnerRemoteRunner, self).__init__(name, url, identityfile, extra_ssh_options, do_cleanup)

  def initialiseRun(self):
    # Initialise the remote runners their client.
    self._runChannel = self._makeRunChannel()
    self._runClient = RunClient(self._runChannel)

  def createBatch(self, batchDir, jobs, batchId):
    batch = RunnerBatch(self, batchDir, jobs, batchId)
    return batch

  def _makeRunChannel(self):
    """Creates the RunChannels instance associated with this runner.

    Args:
        nprocesses (int): Number of runner channels that will be instantiated within RunChannels object.
    """
    channel = RunChannel(self._gw, '%s-Run' % self.name, nprocesses = self._nprocesses)
    return channel

  def makeCloseThread(self):
    return _RemoteRunnerCloseThread(self)

  def startJobRun(self, handler):
    """Run the given job defined by handler.

    Handler is an object with the following properties:
      * `workingDirectory`: gives the path of this job on the remote machine.
      * `callback`: Unary callback,  accepting throwable as its argument, which is called on completion of the job.

    Args:
        handler (object): See above

    Returns:
        (_run_remote_client.JobRecord): Record supporting kill() method.
    """
    return self._runClient.runCommand(handler.workingDirectory, handler.callback)

class RemoteRunner(object):
  """Runner that uses SSH to run jobs in parallel on a remote machine"""

  def __init__(self, name, url, nprocesses, identityfile=None, extra_ssh_options = [], do_cleanup = True):
    """Instantiate RemoteRunner.

    Args:
        name (str): Name of this runner.
        url (str): Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
        nprocesses (int): Number of processes that can be run in parallel by this runner
        identityfile (file, optional): Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                      the platform's ssh command are used.
        extra_ssh_options (list, optional): List of (key,value) tuples that are added to the ssh_config file used when making ssh connections.
        do_cleanup (bool): If `True` file clean-up will be automatically performed following a run and on termination of the runner. If `False` this
                behaviour is disabled. This option is provided for the purposes of debugging.

    """
    self._inner = InnerRemoteRunner(name, url, nprocesses, identityfile, extra_ssh_options, do_cleanup)

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

  @property
  def observers(self):
    return self._inner.observers

  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    allowedkeywords = set(['nprocesses', 'type', 'remotehost', 'debug.disable-cleanup'])
    cfgdict = dict(cfgitems)

    for k in cfgdict.iterkeys():
      if not k in allowedkeywords:
        raise ConfigException("Unknown keyword for Remote runner '%s'" % k)

    try:
      nprocesses = cfgdict['nprocesses']
    except KeyError:
      raise ConfigException("nprocesses configuration item not found")

    try:
      nprocesses = int(nprocesses)
    except ValueError:
      raise ConfigException("Could not convert nprocesses configuration item into an integer")

    try:
      remotehost = cfgdict['remotehost']
    except KeyError:
      raise ConfigException("remotehost configuration item not found")

    if not remotehost.startswith("ssh://"):
      raise ConfigException("remotehost configuration item must start with ssh://")

    username, host, port,  path = _execnet.urlParse(remotehost)
    if not host:
      raise ConfigException("remotehost configuration item should be of form ssh://[username@]hostname/remote_path")

    # Attempt connection and check remote directory exists
    group = _execnet.Group()
    try:
      gwurl, sshcfg = _execnet.makeExecnetConnectionSpec(username, host, port)
      gw = group.makegateway(gwurl)

      # Check existence of remote directory
      channel = gw.remote_exec(_execnet._remoteCheck)
      channel.send(path)
      status = channel.receive()
      if not status:
        raise ConfigException("Remote directory does not exist or is not read/writable:'%s'" % path)

      channel.waitclose()
      if sshcfg:
        sshcfg.close()
    except execnet.gateway_bootstrap.HostNotFound:
      raise ConfigException("Couldn't connect to host: %s" % gwurl)
    finally:
        group.terminate(EXECNET_TERM_TIMEOUT)

    kwargs = BaseRemoteRunner.parseConfigItem_debug_disable_cleanup(runnerName, fitRootPath, cfgitems)

    return RemoteRunner(runnerName, remotehost,nprocesses, **kwargs)
