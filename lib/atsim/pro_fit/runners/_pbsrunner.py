from atsim.pro_fit.fittool import ConfigException
from atsim.pro_fit._channel import ChannelException

from _pbs_channel import PBSChannel

from _queueing_system_runner import QueueingSystemRunnerBaseClass

from atsim.pro_fit import _execnet
from atsim.pro_fit._execnet import urlParse
import execnet

EXECNET_TERM_TIMEOUT=10

class InnerPBSRunner(QueueingSystemRunnerBaseClass):
  """Runner class held by PBSRunner that does all the work."""

  id_suffix = "_pbs"

  def makeRunChannel(self, channel_id):
    return PBSChannel(self._gw, channel_id)

class PBSRunner(object):
  """Runner that allows a remote PBS queuing system to be used to run jobs.

  SSH is used to communicate with server to submit jobs and copy files."""

  def __init__(self, name, url, pbsinclude, qselect_poll_interval = 10.0, pbsbatch_size = None, identityfile = None, extra_ssh_options = [], do_cleanup = True):
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
        do_cleanup (bool): If `True` file clean-up will be automatically performed following a run and on termination of the runner. If `False` this
                                      behaviour is disabled. This option is provided for the purposes of debugging.

    """
    self._inner = InnerPBSRunner(name,
      url,
      pbsinclude,
      pbsbatch_size,
      qselect_poll_interval,
      identityfile,
      extra_ssh_options,
      do_cleanup)

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
    allowedkeywords = set(['type', 'remotehost', 'pbsinclude', 'pbsarraysize', 'pbspollinterval'])
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

    username, host, port, path = urlParse(remotehost)
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
      channel = gw.remote_exec(_execnet._remoteCheck)
      channel.send(path)
      status = channel.receive()
      channel.waitclose()

      if not status:
        raise ConfigException("Remote directory does not exist or is not read/writable:'%s'" % path)

      try:
        pbschannel = PBSChannel(gw, 'test_channel')
      except ChannelException as e:
        raise ConfigException("Error starting PBS: '%s'" % e.message)
      finally:
        pbschannel.close()

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

    pbsarraysize = cfgdict.get('pbsarraysize', None)
    if pbsarraysize != None and pbsarraysize.strip() == 'None':
      pbsarraysize = None

    if not pbsarraysize is None:

      try:
        pbsarraysize = int(pbsarraysize)
      except ValueError:
        raise ConfigException("Invalid numerical value for 'pbsarraysize' configuration option: %s" % pbsarraysize)

      if not pbsarraysize >= 1:
        raise ConfigException("Value of 'pbsarraysize' must >= 1. Value was %s" % pbsarraysize)

    pbspollinterval = cfgdict.get('pbspollinterval', 30.0)
    try:
      pbspollinterval = float(pbspollinterval)
    except ValueError:
      raise ConfigException("Invalid numerical value for 'pbspollinterval': %s" % pbspollinterval)

    if not pbspollinterval > 0.0:
      raise ConfigException("Value of 'pbspollinterval' must > 0.0. Value was %s" % pbspollinterval)

    kwargs = dict(qselect_poll_interval = pbspollinterval, pbsbatch_size = pbsarraysize)
    kwargs.update(QueueingSystemRunnerBaseClass._parseConfigItem_debug_disable_cleanup(runnerName, fitRootPath, cfgitems))
    return PBSRunner(runnerName, remotehost, pbsinclude, **kwargs)

