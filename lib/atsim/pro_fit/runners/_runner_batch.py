import functools
import logging
import operator
import os
import posixpath
import Queue
import sys
import threading
import traceback

from _runner_job import RunnerJob
from atsim.pro_fit._util import EventWaitThread, eventWait_or


_logger = logging.getLogger("atsim.pro_fit.runners")

class BatchAlreadyFinishedException(Exception):
  pass

class BatchKilledException(Exception):
  pass

class BatchDirectoryLockException(Exception):
  pass

class _BatchMonitorThread(threading.Thread):

  def __init__(self, batch):
    threading.Thread.__init__(self)
    self._logger = batch._logger.getChild("_BatchMonitorThread")
    self.batch = batch
    self.extantJobs = set(batch.jobs)
    self.killedEvent = threading.Event()
    self.killFinishEvent = threading.Event()
    self.finishedEvent = threading.Event()
    self.completedJobQueue = Queue.Queue()
    self.dirLocked = False
    self.exception = None

  def run(self):
    try:
      self.doLock()
      self.startJobs()
      self.monitorJobs()
      self.finishBatch(None)
    except:
      self._logger.debug("run, exception")
      self.finishBatch(sys.exc_info())

  def doLock(self):
    self._logger.debug("doLock")
    lockEvent = threading.Event()
    def callback(exception):
      if not exception is None:
        self._logger.warning("Could not start batch %s, directory lock failed: %s",self.name)
        self.exception = exception
      lockEvent.set()
    self.batch.parentRunner.lockPath(self.batch.remoteBatchDir, callback)

    combinedEvent = eventWait_or([lockEvent, self.killedEvent])
    while not combinedEvent.wait(0.1):
      pass

    if lockEvent.is_set() and not self.exception:
      self.dirLocked = True

    if self.killedEvent.is_set():
      raise BatchKilledException()

  def startJobs(self):
    for job in self.extantJobs:
      job.start()

  def areJobsRunning(self):
    if self.extantJobs:
      return True
    return False

  def monitorJobs(self):
    self._logger.debug("monitorJobs")
    while self.areJobsRunning() and not self.killedEvent.is_set():
      try:
        job = self.completedJobQueue.get(True, 0.1)
        self.extantJobs.remove(job)
        self._logger.debug("monitorJobs job remove - %d jobs remaining", len(self.extantJobs))
      except Queue.Empty:
        pass

    if self.killedEvent.is_set():
        self._logger.debug("monitorJobs killed event")
        self.killJobs()

  def killJobs(self):
    self._logger.debug("killJobs")
    killEvents = [ job.kill() for job in self.extantJobs]
    ewt = EventWaitThread(killEvents)
    ewt.start()
    while not ewt.completeEvent.wait(0.1):
      pass
    self.killFinishEvent.set()
    self._logger.debug("killJobs finished")

  def finishBatch(self, exception):
    self.exception = exception

    if exception:
      self._logger.warning("Batch finished with exception: %s", traceback.format_exception(*exception))

    if self.dirLocked:
      unlockEvent = self.batch.parentRunner.unlockPath(self.batch.remoteBatchDir)
    else:
      unlockEvent = threading.Event()
      unlockEvent.set()

    finishedEvent = self.finishedEvent
    class WaitUnlock(EventWaitThread):
      def after(slf):
        self.batch.parentRunner.batchFinished(self.batch, self.exception)
        finishedEvent.set()

    WaitUnlock([unlockEvent]).start()
    # self.finishedEvent.set()


class RunnerBatch(object):

  _logger = _logger.getChild("RunnerBatch")

  def __init__(self, parentRunner, remoteBatchDir, jobs, name):
    self.name = name
    self.parentRunner = parentRunner
    self.remoteBatchDir = remoteBatchDir
    self.jobs = self._createJobInstances(jobs)
    self._monitorThread = _BatchMonitorThread(self)
    self._finishedEvent = self._monitorThread.finishedEvent
    self._killedEvent = self._monitorThread.killedEvent

  def startBatch(self):
    """Register batch remote path with the cleanup agent and call start() on this batch's
    jobs"""
    self._logger.debug("Starting batch: %s", self.name)
    self._monitorThread.start()

  @property
  def isFinished(self):
    return self._finishedEvent.is_set()

  def createUploadDirectory(self, job, handler):
    """Create an `atsim.pro_fit.filetransfer.UploadDirectory` instance
    for `job`. The `UploadDirectory` will be registered with an upload handler
    that will call the job's  `finishUpload()` method on completion.

    Upload will occur between `job.sourcePath` and `job.remotePath`.

    Args:
        job (RunnerJob): Job
        handler (UploadHandler): Handler for this upload directory.

    Returns:
        UploadDirectory: Correctly instantiated directory upload instance.
    """
    sourcePath = job.sourcePath
    remotePath = job.remotePath
    upload = self.parentRunner.createUploadDirectory(sourcePath, remotePath, handler)
    return upload

  def createDownloadDirectory(self, job, handler):
    """Create an `atsim.pro_fit.filetransfer.DownloadDirectory` instance
    for `job`. The `DownloadDirectory` will be registered with a download handler
    that will call the job's  `finishDownload()` method on completion.

    Download will occur between `job.remotePath` and `job.outputPath`.

    Args:
        job (RunnerJob): Job
        handler (DownloadHandler): Handler for the job

    Returns:
        DownloadDirectory: Correctly instantiated directory download instance.
    """
    destPath = job.outputPath
    os.mkdir(destPath)
    download = self.parentRunner.createDownloadDirectory(handler.remoteOutputPath, destPath, handler)
    return download

  def startJobRun(self, job, handler):
    """Register this job with the parent runner's `RunClient`"""
    # handler = job.createRunJobHandler()
    return self.parentRunner.startJobRun(handler)

  def jobFinished(self, job):
    self._monitorThread.completedJobQueue.put(job)

  def lockJobDirectory(self, job, callback):
    """Register the remote directory of `job` with the cleanup agent associated
    with this batch's runner.

    Args:
        job (RunnerJob): RunnerJob whose directory is to be protected from deletion.
        callback (callable): Callback to be invoked when lock confirmation is granted.
            The callback takes a single argument `exception` which is `None` if
            locking was successful or an exception object if there was a problem
            during locking.
    """
    self.parentRunner.lockPath(job.remotePath, callback)

  def unlockJobDirectory(self, job):
    """Called to indicated that the remote directory belonging to `job` is no
    longer required and can be deleted by the cleanup agent associated with this
    batch's runner.

    Args:
        job (RunnerJob): Job for which remote directory should be unlocked.
    """
    return self.parentRunner.unlockPath(job.remotePath)

  def _createJobInstances(self, jobs):
    """Create RunnerJob instances from jobfactories.Job instances"""
    retlist = []
    numjobs = len(jobs)
    for i, job in enumerate(jobs):
      job_name = "%s-%d" % (job.name,i)
      rjob = RunnerJob(self, job, job_name)
      retlist.append(rjob)
    return retlist

  def join(self, timeout = None):
    """Join blocks until batch completes (or until timeout expires).

    This allows batch to have similar behaviour to threading.Thread object.

    Args:
        timeout (int): Timeout in seconds or None
    """
    return self._finishedEvent.wait(timeout)

  def terminate(self):
    self._killedEvent.set()
    return self._finishedEvent

  @property
  def finishedEvent(self):
    return self._finishedEvent

