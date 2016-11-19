import functools
import logging
import operator
import os
import posixpath
import sys
import traceback

from _runner_job import RunnerJob

import gevent
import gevent.event
import gevent.queue

_logger = logging.getLogger("atsim.pro_fit.runners")

class BatchAlreadyFinishedException(Exception):
  pass

class BatchKilledException(Exception):
  pass

class BatchDirectoryLockException(Exception):
  pass

class _BatchMonitorThread(object):

  def __init__(self, batch):
    self._logger = batch._logger.getChild("_BatchMonitorThread")
    self.batch = batch
    self.extantJobs = set(batch.jobs)
    self.killedEvent = gevent.event.Event()
    self.killFinishEvent = gevent.event.Event()
    self.finishedEvent = gevent.event.Event()
    self.completedJobQueue = gevent.queue.Queue()
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
    lockEvent = gevent.event.Event()
    def callback(exception):
      if not exception is None:
        self._logger.warning("Could not start batch %s, directory lock failed: %s",self.name)
        self.exception = exception
      lockEvent.set()
    self.batch.parentRunner.lockPath(self.batch.remoteBatchDir, callback)
    gevent.wait(objects = [lockEvent, self.killedEvent], count = 1)

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
      except gevent.queue.Empty:
        pass
      gevent.sleep(0)

    if self.killedEvent.is_set():
        self._logger.debug("monitorJobs killed event")
        self.killJobs()

  def killJobs(self):
    self._logger.debug("killJobs")
    killEvents = [ job.kill() for job in self.extantJobs]
    gevent.wait(objects = killEvents)
    self._logger.debug("killJobs finished")

  def finishBatch(self, exception):
    self.exception = exception

    if exception:
      self._logger.warning("Batch finished with exception: %s", traceback.format_exception(*exception))

    if self.dirLocked:
      unlockEvent = self.batch.parentRunner.unlockPath(self.batch.remoteBatchDir)
    else:
      unlockEvent = gevent.event.Event()
      unlockEvent.set()

    finishedEvent = self.finishedEvent

    def after():
      unlockEvent.wait()
      self.batch.parentRunner.batchFinished(self.batch, self.exception)
      finishedEvent.set()

    gevent.Greenlet.spawn(after)

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
    gevent.Greenlet.spawn(self._monitorThread.run)

  @property
  def isFinished(self):
    return self._finishedEvent.is_set()

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
    return self.parentRunner.createUploadDirectory(job)

  def createDownloadDirectory(self, job):
    """Create an `atsim.pro_fit.filetransfer.DownloadDirectory` instance
    for `job`.

    Download will occur between `job.remotePath` and `job.outputPath`.

    Args:
        job (RunnerJob): Job

    Returns:
        finishEvent, DownloadDirectory: Correctly instantiated directory download instance.
    """
    finishEvent, download = self.parentRunner.createDownloadDirectory(job)
    return finishEvent, download

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

