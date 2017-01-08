import logging
import uuid

from _exceptions import RunnerClosedException
from atsim.pro_fit._util import NamedEvent
from _util import BatchNameIterator
from atsim.pro_fit import filetransfer
from atsim.pro_fit import _execnet
from atsim.pro_fit.filetransfer import UploadHandler, DownloadHandler

import posixpath
import os
import sys
import traceback

import gevent
from gevent.event import Event


class RemoteRunnerCloseThreadBase(gevent.Greenlet):

  _logger = logging.getLogger(__name__).getChild("RemoteRunnerCloseThreadBase")

  def __init__(self, runner):
    super(RemoteRunnerCloseThreadBase, self).__init__()
    # gevent.Greenlet.__init__(self)
    self.runner = runner
    self.afterEvent = gevent.event.Event()
    self.batchKillEvents = None
    self.closeRunClientEvents = None
    self.closeUploadEvents = None
    self.closeDownloadEvents = None

  def terminateBatches(self):
    return [ b.terminate() for b in self.runner._extantBatches]

  def closeRunClient(self):
    event = Event()
    event.set()
    return [event]

  def _closeChannel(self, channel, logname):
    def closechannel(channel, evt):
      if hasattr(channel, 'broadcast'):
        channel.broadcast(None)
      else:
        channel.send(None)
      gevent.sleep(0)
      try:
        channel.waitclose(60)
      except EOFError:
        #Channel already closed
        pass
      self._logger.info("Runner (%s) %s channels closed.", self.runner.name, logname)
      evt.set()
    evt = Event()
    gevent.spawn(closechannel, channel, evt)
    return [evt]

  def closeUpload(self):
    return self._closeChannel(self.runner._uploadChannel, 'upload')

  def closeDownload(self):
    return self._closeChannel(self.runner._downloadChannel, 'download')

  def closeCleanup(self):
    return self._closeChannel(self.runner._cleanupChannel, 'cleanup')

  def _run(self):
    try:
      self.batchKillEvents = self.terminateBatches()
      if self.batchKillEvents:
        gevent.wait(objects = self.batchKillEvents, timeout = 120)

      allEvents = []
      self.closeRunClientEvents = self.closeRunClient()
      if self.closeRunClientEvents:
        allEvents.extend(self.closeRunClientEvents)

      self.closeUploadEvents = self.closeUpload()
      if self.closeUploadEvents:
        allEvents.extend(self.closeUploadEvents)

      self.closeDownloadEvents = self.closeDownload()
      if self.closeDownloadEvents:
        allEvents.extend(self.closeDownloadEvents)

      self.closeCleanupEvents = self.closeCleanup()
      if self.closeCleanupEvents:
        allEvents.extend(self.closeCleanupEvents)

      gevent.wait(objects = allEvents, timeout = 120)
    except:
      exception = sys.exc_info()
      tbstring = traceback.format_exception(*exception)
      self._logger.warning("Exception raised during close(): %s", tbstring)
    finally:
      self.afterEvent.set()

class _RunnerJobUploadHandler(UploadHandler):
  """Handler that acts as finish() callback for RunnerJob.

  Taks RunnerJob instance and will call its `finishUpload()` method when download
  completes"""

  _logger = logging.getLogger(__name__).getChild('_RunnerJobUploadHandler')

  def __init__(self, job):
    super(_RunnerJobUploadHandler, self).__init__(job.sourcePath, job.remotePath)
    self.job = job
    self.finishEvent = gevent.event.Event()

  def finish(self, exception = None):
    self._logger.debug("finish called for %s", self.job)
    self.job.exception = exception
    self.finishEvent.set()
    return None

class _RunnerJobDownloadHandler(DownloadHandler):
  """Handler that acts as finish() callback for RunnerJob.

  Takes RunnerJob instance and will call its `finishDownload()` method when download
  completes"""

  _logger = logging.getLogger(__name__).getChild('_RunnerJobDownloadHandler')

  def __init__(self,  job):
    self.remoteOutputPath = posixpath.join(job.remotePath, "job_files")
    super(_RunnerJobDownloadHandler, self).__init__(self.remoteOutputPath, job.outputPath)
    self.job = job
    self.finishEvent = gevent.event.Event()

  def finish(self, exception = None):
    self._logger.debug("finish called for %s", self.job)
    self.job.exception = exception
    self.finishEvent.set()
    return None


class BaseRemoteRunner(object):
  """
    Base class for runners that run jobs on remote machines. This runner class
    provides the `create*()` methods required by the `RunnerBatch` class.

    Child classes at a minimum need to implement these methods:

      * `createBatch()`
      * `startJobRun()`

    Initialisation of the object can be customised by overriding these methods:

      * `initialiseUpload()`
      * `initialiseDownload()`
      * `initialiseCleanup()`
      * `initialiseRun()`
      * `makeUploadChannel()`
      * `makeDownloadChannel()`
      * `makeCleanupChannel()`
  """

  _logger = logging.getLogger(__name__).getChild("BaseRemoteRunner")

  def __init__(self, name, url, identityfile=None, extra_ssh_options = []):
    """Instantiate RemoteRunner.

    Args:
        name (str): Name of this runner.
        url (str): Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
        nprocesses (int): Number of processes that can be run in parallel by this runner
        identityfile (file, optional): Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                      the platform's ssh command are used.
        extra_ssh_options (list, optional): List of (key,value) tuples that are added to the ssh_config file used when making ssh connections.
    """
    self.name = name
    self._uuid = str(uuid.uuid4())
    self._closed = False

    self._username, self._hostname, self._port, self._remotePath = _execnet.urlParse(url)

    # Create a common url used to create execnet gateways
    self._gwurl, sshcfgfile = _execnet.makeExecnetConnectionSpec(self._username, self._hostname, self._port, identityfile, extra_ssh_options)

    if sshcfgfile:
      self._sshcfgfile = sshcfgfile

    group = _execnet.Group()
    self._gw = group.makegateway(self._gwurl)

    # Initialise the remote runners their client.
    # self._runChannel = self._makeRunChannel(nprocesses)
    # self._runClient = RunClient(self._runChannel)

    # Upload and download channel initialisation
    if not self._remotePath:
      # Create an appropriate remote path.
      self._createTemporaryRemoteDirectory()
    else:
      self._remotePath = posixpath.join(self._remotePath, self._uuid)

    self._numUpload = self._numDownload = 1
    self.initialiseUpload()
    self.initialiseDownload()
    self.initialiseCleanup()
    self.initialiseRun()

    self._batchNameIterator = BatchNameIterator()
    self._extantBatches = []

    # Register the remote directory with cleanup agent, such that it will be deleted at shutdown.
    self._cleanupClient.lock(self._remotePath)

  def initialiseUpload(self):
    self._uploadChannel = self.makeUploadChannel()

  def initialiseDownload(self):
    self._downloadChannel = self.makeDownloadChannel()

  def initialiseCleanup(self):
    #Cleanup channel initialisation.
    self._cleanupChannel = self.makeCleanupChannel()
    self._cleanupClient = filetransfer.CleanupClient(self._cleanupChannel)

  def initialiseRun(self):
    """Set up classes associated with any client required to process run files."""
    pass

  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    Args:
        jobs (): List of `atsim.pro_fit.jobfactories.Job` as created by a JobFactory.

    Returns:
        object: An object that supports .join() which when joined will block until batch completion """

    if self._closed:
      raise RunnerClosedException()

    batchId = self._batchNameIterator.next()
    batchDir = posixpath.join(self._remotePath, batchId)

    self._logger.debug("Starting batch %s, containing %d jobs.", batchId, len(jobs))
    self._logger.debug("%s directory is: '%s'", batchId, batchDir)

    batch = self.createBatch(batchDir, jobs, batchId)
    self._extantBatches.append(batch)
    batch.startBatch()
    return batch

  def createBatch(self, batchDir, jobs, batchId):
    """Create a RunnerBatch instance.

    Args:
        batchDir (str): Remote batch directory
        jobs (list): List of jobfactory Job instances describing the jobs.
        batchId (str): Unique ID for the batch.

    Returns:
        RunnerBatch: Object implementing the RunnerBatch interface.
    """
    raise Exception("Not Implemented")

  def makeUploadChannel(self):
    """Creates the UploadChannel instance associated with this runner. It is actually an instance
    of filetransfer.UploadChannels() the number of channels held by UploadChannels is determined by the
    self._numUpload property"""
    channel = filetransfer.UploadChannels(self._gw, self._remotePath, self._numUpload, "_".join([self.name, 'upload']))
    return channel

  def makeDownloadChannel(self):
    """Creates the DownloadChannel instance associated with this runner. It is actually an instance
    of filetransfer.DownloadChannels() the number of channels held by DownloadChannels is determined by the
    self._numDownload property"""
    channel = filetransfer.DownloadChannels(self._gw, self._remotePath, self._numDownload, "_".join([self.name, 'download']))
    return channel

  def makeCleanupChannel(self):
    channel = filetransfer.CleanupChannel(self._gw, self._remotePath, channel_id = "%s-Cleanup" % self.name)
    self._remotePath = channel.remote_path
    return channel

  def _createTemporaryRemoteDirectory(self):
    """Makes a remote call to tempfile.mkdtemp() and sets
      * self._remotePath to the name of the created directory.
      * self._remoted_is_temp is set to True to support correct cleanup behaviour.
    """
    channel = self._gw.remote_exec(_execnet._makeTemporaryDirectory)
    tmpdir = channel.receive()
    self._logger.getChild("_createTemporaryRemoteDirectory").debug("Remote temporary directory: %s", tmpdir)
    self._remotePath = tmpdir
    channel.close()
    channel.waitclose()

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

  def _createUploadDirectory(self, sourcePath, remotePath, uploadHandler):
    """Create an `filetransfer.UploadDirectory` instance configured to this runner's remote
    directory.

    Args:
        sourcePath (str): Path to copy from (local)
        remotePath (str): Destination path (remote)
        uploadHandler (filetransfer.UploadHandler): Acts as callback for upload completion.

    Returns:
        (gevent.event.Event, UploadDirectory) Tuple of an event set when upload completes and
          a correctly instantiated directory upload instance.    """
    uploadDirectory = filetransfer.UploadDirectory(
      self._uploadChannel,
      sourcePath, remotePath, uploadHandler)

    finishEvent = uploadHandler.finishEvent
    return finishEvent, uploadDirectory

  def createUploadDirectory(self, job):
    """Create an `atsim.pro_fit.filetransfer.UploadDirectory` instance
    for `job`. The `UploadDirectory` will be registered with an upload handler
    that will call the job's  `finishUpload()` method on completion.

    Upload will occur between `job.sourcePath` and `job.remotePath`.

    Args:
        job (RunnerJob): Job

    Returns:
        (gevent.event.Event, UploadDirectory) Tuple of an event set when upload completes and
          a correctly instantiated directory upload instance.
    """
    sourcePath = job.sourcePath
    remotePath = job.remotePath
    handler = _RunnerJobUploadHandler(job)
    finishEvent, upload = self._createUploadDirectory(sourcePath, remotePath, handler)
    return finishEvent, upload

  def createDownloadDirectory(self, job):
    """Create an `filetransfer.DownloadDirectory` instance configured to this runner's remote
    directory.

    Args:
      job : RunnerJob
    Returns:
        finishEvent, filetransfer.DownloadDirectory: Directory instance ready for `download()` method to be called.
    """
    downloadHandler = _RunnerJobDownloadHandler(job)
    remotePath = downloadHandler.remoteOutputPath
    destPath = job.outputPath
    os.mkdir(destPath)

    downloadDirectory = filetransfer.DownloadDirectory(
      self._downloadChannel,
      remotePath, destPath, downloadHandler)
    return downloadHandler.finishEvent, downloadDirectory

  def startJobRun(self, handler):
    """Run the given job defined by handler.

    Handler is an object with the following properties:
      * `workingDirectory`: gives the path of this job on the remote machine.
      * `callback`: Unary callback,  accepting throwable as its argument, which is called on completion of the job.

    Args:
        handler (object): See above

    Returns:
        (JobRecord): Record supporting kill() method.
    """
    raise Exception("Not Implemented")

  def cleanupFlush(self):
    """Tell the cleanup agent associated with this runner to process any outstanding  file deletion jobs.

    Returns:
        gevent.event.Event: Event that will be set() once cleanup has completed.
    """
    event = gevent.event.Event()
    def callback(exception = None):
      event.set()

    self._cleanupClient.flush(callback)

    return event

  def makeCloseThread(self):
    """Creates thread like object that can be `started` to shutdown the runner.

    The returned object must have an `afterEvent` property that is `set()` when
    this object's resources have all been closed.

    Returns:
        Thread like object: See above
    """
    raise Exception("Not Implemented")
    # closeThread = _RemoteRunnerCloseThread(self)
    # return closeThread

  def close(self):
    """Shuts down the runner

    Returns:
      gevent.event.Event: Event that will be set() once shutdown has been completed.
    """
    if self._closed:
      raise RunnerClosedException()
    self._closed = True
    closeThread = self.makeCloseThread()
    closeThread.start()
    return closeThread.afterEvent
