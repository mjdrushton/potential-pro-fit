from atsim.pro_fit.fittool import ConfigException
from atsim.pro_fit._channel import ChannelException

from _pbs_channel import PBSChannel

from _queueing_system_runner import QueueingSystemRunnerBaseClass

from atsim.pro_fit import _execnet
import execnet


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

    # When PBSRunner was the only runner to support queueing systems,
    # its options, prefixed with 'pbs' made some sense. Now they're non
    # standard. To ensure some conformity across the the batch queue style
    # runners, allow both the old and new key names.
    # This synonyms dictionary is used to rename the old options later in
    # this function.
    synonyms = {'pbsinclude' : 'header_include',
                'pbsarraysize' : 'arraysize',
                'pbspollinterval' : 'pollinterval'}

    allowedkeywords = ['type']
    allowedkeywords.extend(synonyms.keys())
    allowedkeywords.extend(InnerPBSRunner.allowedConfigKeywords())
    allowedkeywords = set(allowedkeywords)

    # Now rename pbs prefixed options to their standard forms
    newcfgitems = []
    for k,v in cfgitems:
      if not k in allowedkeywords:
        raise ConfigException("Unknown keyword for Remote runner '%s'" % k)
      if k in synonyms:
        newcfgitems.append((synonyms[k], v))
      else:
        newcfgitems.append((k,v))
    cfgitems = newcfgitems

    options = InnerPBSRunner.parseConfig(runnerName, fitRootPath, cfgitems)

    return PBSRunner(runnerName, 
      options['remotehost'], 
      options['header_include'],
      options['arraysize'],
      options['pollinterval'],
      do_cleanup = options['do_cleanup'])

