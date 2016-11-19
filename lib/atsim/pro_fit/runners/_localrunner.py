from atsim.pro_fit.fittool import ConfigException
from atsim.pro_fit import filetransfer

from atsim.pro_fit._util import NamedEvent
from _util import BatchNameIterator

from _exceptions import RunnerClosedException

from atsim.pro_fit import _execnet
import execnet


import gevent
import gevent.event

import logging
import tempfile
import os
import shutil
import uuid
import posixpath

from _runner_batch import RunnerBatch
from _run_remote_client import RunChannels, RunClient


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
    logger.info("Copying files from '%s' to '%s'.", self.source_path, self.dest_path)

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
    if self._greenlet is None:
      event = gevent.event.Event()
      event.set()
      return event

    self._greenlet.kill(block = False)
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


class _LocalRunner(object):

  _logger = logging.getLogger(__name__).getChild("_LocalRunner")

  def __init__(self, name, nprocesses):
    """@param name Name of this runner.
    @param nprocesses Number of processes that can be run in parallel by this runner"""
    self.name = name
    self._nprocesses = nprocesses
    self._batchNameIterator = BatchNameIterator()

    self._uuid = str(uuid.uuid4())
    self._closed = False

    group = _execnet.Group()
    self._gw = group.makegateway()

    # Initialise the remote runners their client.
    self._runChannel = self._makeRunChannel(nprocesses)
    self._runClient = RunClient(self._runChannel)

    self._remotePath = self._makeTemporaryDirectory()
    self._cleanupChannel = self._makeCleanupChannel()
    self._cleanupClient = filetransfer.CleanupClient(self._cleanupChannel)
    # Register the remote directory with cleanup agent, such that it will be deleted at shutdown.
    self._cleanupClient.lock(self._remotePath)

    self._extantBatches = []

  def _makeRunChannel(self, nprocesses):
    """Creates the RunChannels instance associated with this runner.

    Args:
        nprocesses (int): Number of runner channels that will be instantiated within RunChannels object.
    """
    channel = RunChannels(self._gw, '%s-Run' % self.name, num_channels = nprocesses)
    return channel

  def _makeTemporaryDirectory(self):
    logger = self._logger.getChild("_makeTemporaryDirectory")
    tmpdir = tempfile.mkdtemp()
    logger.debug("Local Runner '%s' created temporary directory for runs: %s", self.name, tmpdir)
    return tmpdir

  def _makeCleanupChannel(self):
    channel = filetransfer.CleanupChannel(self._gw, self._remotePath, channel_id = "%s-Cleanup" % self.name)
    self._remotePath = channel.remote_path
    return channel

  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    Args:
        jobs (): List of `atsim.pro_fit.jobfactories.Job` as created by a JobFactory.

    Returns:
        object: An object that supports .join() which when joined will block until batch completion """
    logger = self._logger.getChild('runBatch')

    if self._closed:
      raise RunnerClosedException()

    batchId = self._batchNameIterator.next()
    batchDir = posixpath.join(self._remotePath, batchId)

    logger.debug("Starting batch %s, containing %d jobs.", batchId, len(jobs))
    logger.debug("%s directory is: '%s'", batchId, batchDir)

    batch = RunnerBatch(self, batchDir, jobs, batchId)
    self._extantBatches.append(batch)
    batch.startBatch()
    return batch

  def lockPath(self, path, callback):
    """Protect the remote path `path` from deletion by the cleanup agent."""
    self._cleanupClient.lock(path, callback = callback)

  def unlockPath(self, path):
    """Unprotect the remote path `path` from deletion by the cleanup agent."""
    unlockedEvent = NamedEvent("RemoteRunner unlock path: %s" % path)

    def callback(exception):
      unlockedEvent.set()

    self._cleanupClient.unlock(path, callback = callback)
    return unlockedEvent

  def batchFinished(self, batch, exception):
    """Called by a batch when it is complete

    Args:
        batch (RunnerBatch): Batch that has just completed.
        exception (Exception): None if no errors experienced. An object that can
          be raised otherwise.
    """
    if exception is None:
      self._logger.info("Batch finished successfully: %s", batch.name)
    else:
      self._logger.warning("Batch %s finished with errors: %s", batch.name, exception)
    self._extantBatches.remove(batch)

  def createUploadDirectory(self, job):
    """As this runner doesn't actually upload any files, this method creates an
    object that has the same interface as a `filetransfer.UploadDirectory` instance.
    Rather than performing an upload, the object copies files to the runners temporary
    directory directly.

    Args:
        sourcePath (str): Path to copy from (local)
        remotePath (str): Destination path (remote)
        uploadHandler (filetransfer.UploadHandler): Acts as callback for upload completion.

    Returns:
        (gevent.event.Event, UploadDirectory) Tuple of an event set when upload completes and
          a correctly instantiated directory upload instance.    """

    cpdir = _CopyDirectoryUp(job.sourcePath, job.remotePath)
    return cpdir.finishEvent, cpdir

  def createDownloadDirectory(self, job):
    # Create a filetransfer.DownloadDirectory alike object
    # there shouldn't be anything to do here as the output files
    # should be generated directly in the output directory.
    cpdir = _CopyDirectoryDown(os.path.join(job.remotePath, "job_files"), job.outputPath)
    remotePath = posixpath.join(job.remotePath, "job_files")
    destPath = job.outputPath

    return cpdir.finishEvent, cpdir

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


  def close(self):
    if self._closed:
      raise RunnerClosedException()
    self._closed = True
    closeThread = _LocalRunnerCloseThread(self)
    closeThread.start()
    return closeThread.afterEvent

class _LocalRunnerCloseThread(gevent.Greenlet):

  _logger = logging.getLogger(__name__).getChild("_LocalRunnerCloseThread")

  def __init__(self, runner):
    gevent.Greenlet.__init__(self)
    self.runner = runner
    self.afterEvent = gevent.event.Event()
    self.killevents = [ b.terminate() for b in runner._extantBatches]

  def _run(self):
    gevent.wait(objects = self.killevents)
    self.after()

  def after(self):
    try:
      # Close channels associated with the runner.
      self.runner._runChannel.broadcast(None)
      self.runner._runChannel.waitclose(60)
      self._logger.info("Runner's run channels closed.")

      self.runner._cleanupChannel.send(None)
      self.runner._cleanupChannel.waitclose(60)
      self._logger.info("Runner's cleanup channel closed.")
    except:
      exception = sys.exc_info()
      tbstring = traceback.format_exception(*exception)
      self._logger.warning("Exception raised during close(): %s", tbstring)
    finally:
      self.afterEvent.set()


class LocalRunner(object):
  """Runner that coordinates parallel job submission to local machine"""

  def __init__(self, name, nprocesses):
    """@param name Name of this runner.
    @param nprocesses Number of processes that can be run in parallel by this runner"""
    self._inner = _LocalRunner(name, nprocesses)

  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    @param jobs List of job instances as created by a JobFactory
    @return LocalRunnerFuture a job future that supports .join() to block until completion"""
    return self._inner.runBatch(jobs)

  def _getname(self):
    return self._inner.name

  def _setname(self, v):
    self._inner.name = v

  name = property(_getname, _setname)

  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    allowedkeywords = set(['nprocesses', 'type'])
    cfgdict = dict(cfgitems)

    for k in cfgdict.iterkeys():
      if not k in allowedkeywords:
        raise ConfigException("Unknown keyword for Local runner '%s'" % k)

    try:
      nprocesses = cfgdict['nprocesses']
    except KeyError:
      raise ConfigException("nprocesses configuration item not found")

    try:
      nprocesses = int(nprocesses)
    except ValueError:
      raise ConfigException("Could not convert nprocesses configuration item into an integer")

    return LocalRunner(runnerName, nprocesses)
