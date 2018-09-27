from atsim.pro_fit.fittool import ConfigException
from atsim.pro_fit._channel import ChannelException

from _slurm_channel import SlurmChannel

from _queueing_system_runner import QueueingSystemRunnerBaseClass

from atsim.pro_fit import _execnet
import execnet


class InnerSlurmRunner(QueueingSystemRunnerBaseClass):
  """Runner class held by PBSRunner that does all the work."""

  id_suffix = "_slurm"

  def makeRunChannel(self, channel_id):
    return SlurmChannel(self._gw, channel_id)

class SlurmRunner(object):
  """Runner that allows a remote Slurm based queuing system to be used to run jobs.

  SSH is used to communicate with server to submit jobs and copy files."""

  def __init__(self, name, url, header_include, poll_interval = 10.0, batch_size = None, identityfile = None, extra_ssh_options = [], do_cleanup = True):
    """Create SlurmRunner instance

    Args:
        name (str): Name of runner
        url (str): Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
        header_include (str): String that will be inserted at top of Slurm submission script, this can be used to customise job requirements.
        poll_interval (float, optional): Slurm's squeue command will be polled using this interval (seconds) to determine job status.
        batch_size (None, optional): Maximum number of jobs (i.e. Slurm array job size). The `sbatch` command is invoked when files have been uploaded for this number of jobs.
                                        If this argument is `None` then all the jobs for a particular pprofit batch will be included in the same array job.
        identityfile (str, optional): Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                                       the platform's ssh command are used.
        extra_ssh_options (list, optional): List of (key,value) tuples that are added to the ssh_config file used when making ssh connections.
        do_cleanup (bool): If `True` file clean-up will be automatically performed following a run and on termination of the runner. If `False` this
                                      behaviour is disabled. This option is provided for the purposes of debugging.

    """
    self._inner = InnerSlurmRunner(name,
      url,
      header_include,
      batch_size,
      poll_interval,
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
    allowedkeywords = ['type']
    allowedkeywords.extend(InnerSlurmRunner.allowedConfigKeywords())
    allowedkeywords = set(allowedkeywords)

    # Now rename pbs prefixed options to their standard forms
    for k,v in cfgitems:
      if not k in allowedkeywords:
        raise ConfigException("Unknown keyword for Remote runner '%s'" % k)

    options = InnerSlurmRunner.parseConfig(runnerName, fitRootPath, cfgitems)

    return SlurmRunner(runnerName, 
      options['remotehost'], 
      options['header_include'],
      options['arraysize'],
      options['pollinterval'],
      do_cleanup = options['do_cleanup'])

