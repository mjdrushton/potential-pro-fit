from atsim.pro_fit.fittool import ConfigException
from atsim.pro_fit import _execnet
from _localrunner_batch import LocalRunnerBatch
from _run_remote_client import RunChannel, RunClient
from _base_remoterunner import BaseRemoteRunner, RemoteRunnerCloseThreadBase

import execnet
import gevent
from gevent.event import Event

import logging
import os
import shutil
import tempfile


EXECNET_TERM_TIMEOUT=10

class _CopyDirectory(object):
  _logger = logging.getLogger(__name__).getChild("_CopyDirectory")

  def __init__(self, source_path, dest_path):
    self.source_path = os.path.abspath(source_path)
    self.dest_path = os.path.abspath(dest_path)
    self.exception = None
    self._greenlet = None
    self.finishEvent = gevent.event.Event()

  def doCopy(self, non_blocking = False):
    logger = self._logger.getChild("doCopy")
    logger.debug("Copying files from '%s' to '%s'.", self.source_path, self.dest_path)

    self.finishEvent.clear()

    def copyfiles():
      shutil.copytree(self.source_path, self.dest_path)

    def after(grn):
      self.exception = grn.exception
      self.finishEvent.set()

    self._greenlet = gevent.Greenlet(copyfiles)
    self._greenlet.link(after)

    if non_blocking:
      self._greenlet.start()
      return self.finishEvent
    else:
      self._greenlet.start()
      self._greenlet.join()

  def cancel(self):
    return self.finishEvent

class _CopyDirectoryUp(_CopyDirectory):
  """Class that copies directories whilst fulfilling the filetransfer.UploadDirectory interface"""

  _logger = logging.getLogger(__name__).getChild("_CopyDirectoryUp")

  def upload(self, non_blocking = False):
    return self.doCopy()

class _CopyDirectoryDown(_CopyDirectory):
  """Class that copies directories whilst fulfilling the filetransfer.UploadDirectory interface"""

  _logger = logging.getLogger(__name__).getChild("_CopyDirectoryDown")

  def download(self, non_blocking = False):
    return self.doCopy()

class _LocalRunnerCloseThread(RemoteRunnerCloseThreadBase):

  _logger = logging.getLogger(__name__).getChild("_LocalRunnerCloseThread")

  def closeUpload(self):
    return None

  def closeDownload(self):
    return None

  def closeRunClient(self):
    return self._closeChannel(self.runner._runChannel, 'closeRunClient')


class InnerLocalRunner(BaseRemoteRunner):
  """The actual implementation of LocalRunner.

  InnerLocalRunner is used to allow LocalRunner to just expose the public Runner interface whilst InnerLocalRunner
  has a much more extensive interface (required by the RunnerBatch)"""

  _logger = logging.getLogger(__name__).getChild("InnerLocalRunner")

  def __init__(self, name, nprocesses):
    """Instantiate LocalRunner.

    Args:
        name (str): Name of this runner.
        nprocesses (int): Number of processes that can be run in parallel by this runner"""
    self._nprocesses = nprocesses
    super(InnerLocalRunner, self).__init__(name, None)

  def makeExecnetGateway(self, url, identityfile, extra_ssh_options):
    self._remotePath = None
    group = _execnet.Group()
    # group.set_execmodel("gevent", "thread")
    gw = group.makegateway()
    return gw

  def initialiseUpload(self):
    pass

  def initialiseDownload(self):
    pass

  def initialiseTemporaryRemoteDirectory(self):
    self._remotePath = tempfile.mkdtemp()

  # def initialiseCleanup(self):
  #   pass

  def initialiseRun(self):
    # Initialise the remote runners their client.
    self._runChannel = self._makeRunChannel(self._nprocesses)
    self._runClient = RunClient(self._runChannel)

  def createBatch(self, batchDir, jobs, batchId):
    batch = LocalRunnerBatch(self, batchDir, jobs, batchId)
    return batch

  def _makeRunChannel(self, nprocesses):
    """Creates the RunChannels instance associated with this runner.

    Args:
        nprocesses (int): Number of runner channels that will be instantiated within RunChannels object.
    """
    channel = RunChannel(self._gw, '%s-Run' % self.name, nprocesses = nprocesses)
    return channel

  def makeCloseThread(self):
    return _LocalRunnerCloseThread(self)

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

  def createUploadDirectory(self, job):
    # Copy from job.sourcePath to batch directory
    upload = _CopyDirectoryUp(job.sourcePath, job.remotePath)
    return upload.finishEvent, upload

  def createDownloadDirectory(self, job):
    download = _CopyDirectoryDown(os.path.join(job.remotePath,'job_files'), job.outputPath)
    return download.finishEvent, download

class LocalRunner(object):
  """Runner that uses SSH to run jobs in parallel on a remote machine"""

  def __init__(self, name, nprocesses):
    """Instantiate LocalRunner.

    Args:
        name (str): Name of this runner.
        nprocesses (int): Number of processes that can be run in parallel by this runner
    """
    self._inner = InnerLocalRunner(name, nprocesses)

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
  def _makeException(runnerName, msg):
    errmsg = "%s for Local runner '%s'" % (msg, runnerName)
    return ConfigException(errmsg)

  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    allowedkeywords = set(['nprocesses', 'type'])
    cfgdict = dict(cfgitems)

    for k in cfgdict.iterkeys():
      if not k in allowedkeywords:
        raise LocalRunner._makeException(runnerName, "Unknown keyword '%s'" % k)

    try:
      nprocesses = cfgdict['nprocesses']
    except KeyError:
      raise LocalRunner._makeException(runnerName, "'nprocesses' configuration item not found")

    try:
      nprocesses = int(nprocesses)
    except ValueError:
      raise LocalRunner._makeException(runnerName, "Could not convert 'nprocesses' configuration item into an integer. Value was = '%s'" % nprocesses)

    return LocalRunner(runnerName, nprocesses)
