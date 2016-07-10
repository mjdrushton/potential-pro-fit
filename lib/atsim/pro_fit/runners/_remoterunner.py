from atsim.pro_fit.fittool import ConfigException
from atsim.pro_fit._util import EventWaitThread, NamedEvent
from atsim.pro_fit import filetransfer

import _execnet
import execnet

import itertools
import logging
import posixpath
import uuid
import threading
import traceback
import sys

EXECNET_TERM_TIMEOUT=10

from _runner_batch import RunnerBatch
from _run_remote_client import RunChannels, RunClient

class RunnerClosedException(Exception):
  pass

class InnerRemoteRunner(object):
  """The actual implementation of RemoteRunner.

  InnerRemotRunner is used to allow RemoteRunner to just expose the public Runner interface whilst InnerRemoteRunner
  has a much more extensive interface (required by the RunnerBatch)"""

  _logger = logging.getLogger("atsim.pro_fit.runners.RemoteRunner")

  def __init__(self, name, url, nprocesses, identityfile=None, extra_ssh_options = []):
    """Instantiate RemoteRunner.

    Args:
        name (str): Name of this runner.
        url (str): Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
        nprocesses (int): Number of processes that can be run in parallel by this runner
        identityfile (file, optional): Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                      the platform's ssh command are used.
        extra_ssh_options (list, optional): List of (key,value) tuples that are added to the ssh_config file used when making ssh connections.
    """
    self.lock = threading.RLock()
    self.name = name
    self._uuid = str(uuid.uuid4())
    self._closed = False

    self._username, self._hostname, self._port, self._remotePath = _execnet.urlParse(url)

    # Create a common url used to create execnet gateways
    self._gwurl, sshcfgfile = _execnet.makeExecnetConnectionSpec(self._username, self._hostname, self._port, identityfile, extra_ssh_options)

    if sshcfgfile:
      self._sshcfgfile = sshcfgfile

    group = execnet.Group()
    # group.set_execmodel("eventlet", "thread")
    self._gw = group.makegateway(self._gwurl)

    # Initialise the remote runners their client.
    self._runChannel = self._makeRunChannel(nprocesses)
    self._runClient = RunClient(self._runChannel)

    # Upload and download channel initialisation
    if not self._remotePath:
      # Create an appropriate remote path.
      self._createTemporaryRemoteDirectory()
    else:
      self._remotePath = posixpath.join(self._remotePath, self._uuid)

    self._numUpload = self._numDownload = 4
    self._uploadChannel = self._makeUploadChannel()
    self._downloadChannel = self._makeDownloadChannel()

    #Cleanup channel initialisation.
    self._cleanupChannel = self._makeCleanupChannel()
    self._cleanupClient = filetransfer.CleanupClient(self._cleanupChannel)

    self._batchCount = itertools.count()
    self._extantBatches = []

    # Register the remote directory with cleanup agent, such that it will be deleted at shutdown.
    self._cleanupClient.lock(self._remotePath)

  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    Args:
        jobs (): List of `atsim.pro_fit.jobfactories.Job` as created by a JobFactory.

    Returns:
        object: An object that supports .join() which when joined will block until batch completion """

    if self._closed:
      raise RunnerClosedException()

    batchId = self._get_batch_id()
    batchDir = posixpath.join(self._remotePath, batchId)

    self._logger.debug("Starting batch %s, containing %d jobs.", batchId, len(jobs))
    self._logger.debug("%s directory is: '%s'", batchId, batchDir)

    batch = RunnerBatch(self, batchDir, jobs, batchId)
    with self.lock:
      self._extantBatches.append(batch)
    batch.startBatch()
    return batch

  def _get_batch_id(self):
    with self.lock:
      return "Batch-%d" % self._batchCount.next()

  def _makeUploadChannel(self):
    """Creates the UploadChannel instance associated with this runner. It is actually an instance
    of filetransfer.UploadChannels() the number of channels held by UploadChannels is determined by the
    self._numUpload property"""
    with self.lock:
      channel = filetransfer.UploadChannels(self._gw, self._remotePath, self._numUpload, "_".join([self.name, 'upload']))
    return channel

  def _makeDownloadChannel(self):
    """Creates the DownloadChannel instance associated with this runner. It is actually an instance
    of filetransfer.DownloadChannels() the number of channels held by DownloadChannels is determined by the
    self._numDownload property"""
    with self.lock:
      channel = filetransfer.DownloadChannels(self._gw, self._remotePath, self._numDownload, "_".join([self.name, 'download']))
    return channel

  def _makeCleanupChannel(self):
    with self.lock:
      channel = filetransfer.CleanupChannel(self._gw, self._remotePath, channel_id = "%s-Cleanup" % self.name)
      self._remotePath = channel.remote_path
    return channel

  def _makeRunChannel(self, nprocesses):
    """Creates the RunChannels instance associated with this runner.

    Args:
        nprocesses (int): Number of runner channels that will be instantiated within RunChannels object.
    """
    with self.lock:
      channel = RunChannels(self._gw, '%s-Run' % self.name, num_channels = nprocesses)
    return channel

  def _createTemporaryRemoteDirectory(self):
    """Makes a remote call to tempfile.mkdtemp() and sets
      * self._remotePath to the name of the created directory.
      * self._remoted_is_temp is set to True to support correct cleanup behaviour.
    """
    from _execnet import _makeTemporaryDirectory
    channel = self._gw.remote_exec(_makeTemporaryDirectory)
    tmpdir = channel.receive()
    self._logger.getChild("_createTemporaryRemoteDirectory").debug("Remote temporary directory: %s", tmpdir)
    with self.lock:
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
    with self.lock:
      if exception is None:
        self._logger.info("Batch finished successfully: %s", batch.name)
      else:
        self._logger.warning("Batch %s finished with errors: %s", batch.name, exception)
      self._extantBatches.remove(batch)

  def createUploadDirectory(self, sourcePath, remotePath, uploadHandler):
    """Create an `filetransfer.UploadDirectory` instance configured to this runner's remote
    directory.

    Args:
        sourcePath (str): Path to copy from (local)
        remotePath (str): Destination path (remote)
        uploadHandler (filetransfer.UploadHandler): Acts as callback for upload completion.

    Returns:
        filetransfer.UploadDirectory : Directory instance ready for `upload()` method to be called.
    """
    with self.lock:
      uploadDirectory = filetransfer.UploadDirectory(
        self._uploadChannel,
        sourcePath, remotePath, uploadHandler)
    return uploadDirectory

  def createDownloadDirectory(self, remotePath, localPath, downloadHandler):
    """Create an `filetransfer.DownloadDirectory` instance configured to this runner's remote
    directory.

    Args:
        remotePath (str): Source path (remote)
        localPath (str): Destination path for download (local)
        downloadHandler (filetransfer.DownloadHandler): Description

    Returns:
        filetransfer.DownloadDirectory: Directory instance ready for `download()` method to be called.
    """
    with self.lock:
      downloadDirectory = filetransfer.DownloadDirectory(
        self._downloadChannel,
        remotePath, localPath, downloadHandler)
    return downloadDirectory

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

  def cleanupFlush(self):
    """Tell the cleanup agent associated with this runner to process any outstanding  file deletion jobs.

    Returns:
        threading.Event: Event that will be set() once cleanup has completed.
    """
    event = threading.Event()
    def callback(exception = None):
      event.set()

    self._cleanupClient.flush(callback)

    return event

  def close(self):
    """Shuts down the runner

    Returns:
      threading.Event: Event that will be set() once shutdown has been completed.
    """
    with self.lock:
      if self._closed:
        raise RunnerClosedException()
      self._closed = True
      closeThread = _RemoteRunnerCloseThread(self)
      closeThread.start()
      return closeThread.afterEvent

class _RemoteRunnerCloseThread(EventWaitThread):

  _logger = logging.getLogger("atsim.pro_fit.runners.RemoteRunner._RemoteRunnerCloseThread")

  def __init__(self, runner):
    self.runner = runner
    self.afterEvent = threading.Event()
    killevents = [ b.terminate() for b in runner._extantBatches]
    EventWaitThread.__init__(self, killevents)


  def after(self):
    try:
      # Unlock batch path
      # try:
      #   self.runner._cleanupClient.unlock(self.runner._remotePath)
      #   self._logger.info("Runner's remote directory unlocked during close()")
      # except:
      #   exception = sys.exc_info()
      #   tbstring = traceback.format_exception(*exception)
      #   self._logger.warning("Exception raised during cleanup client unlock in close(): %s", exception)

      # Close channels associated with the runner.
      self.runner._runChannel.broadcast(None)
      self.runner._runChannel.waitclose(60)
      self._logger.info("Remote runner's run channels closed.")

      self.runner._uploadChannel.broadcast(None)
      self.runner._uploadChannel.waitclose(60)
      self._logger.info("Remote runner's upload channels closed.")

      self.runner._downloadChannel.broadcast(None)
      self.runner._downloadChannel.waitclose(60)
      self._logger.info("Remote runner's download channels closed.")

      self.runner._cleanupChannel.send(None)
      self.runner._cleanupChannel.waitclose(60)
      self._logger.info("Remote runner's cleanup channel closed.")
    except:
      exception = sys.exc_info()
      tbstring = traceback.format_exception(*exception)
      self._logger.warning("Exception raised during close(): %s", tbstring)
    finally:
      self.afterEvent.set()


class RemoteRunner(object):
  """Runner that uses SSH to run jobs in parallel on a remote machine"""

  def __init__(self, name, url, nprocesses, identityfile=None, extra_ssh_options = []):
    """Instantiate RemoteRunner.

    Args:
        name (str): Name of this runner.
        url (str): Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
        nprocesses (int): Number of processes that can be run in parallel by this runner
        identityfile (file, optional): Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                      the platform's ssh command are used.
        extra_ssh_options (list, optional): List of (key,value) tuples that are added to the ssh_config file used when making ssh connections.
    """
    self._inner = InnerRemoteRunner(name, url, nprocesses, identityfile, extra_ssh_options)

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
      threading.Event: Event that will be set() once shutdown has been completed.
    """
    return self._inner.close()

  @property
  def name(self):
    return self._inner.name

  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    allowedkeywords = set(['nprocesses', 'type', 'remotehost'])
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
    group = execnet.Group()
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

    return RemoteRunner(runnerName, remotehost,nprocesses)
