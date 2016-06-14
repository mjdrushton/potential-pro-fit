
import logging

import os
import threading

import posixpath

from _runner_job import RunnerJob

_logger = logging.getLogger("atsim.pro_fit.runners")

class RunnerBatch(object):

  _logger = _logger.getChild("RunnerBatch")

  def __init__(self, parentRunner, remoteBatchDir, jobs, name):
    self.name = name
    self.parentRunner = parentRunner
    self.remoteBatchDir = remoteBatchDir
    self._remoteBatchDirLocked = False

    self.jobs = self._createJobInstances(jobs)
    self.exception = None
    self._extantJobs = set()
    self._event = threading.Event()

  def startBatch(self):
    """Register batch remote path with the cleanup agent and call start() on this batch's
    jobs"""
    self._event.clear()
    def callback(exception):
      if not exception is None:
        self._logger.warning("Could not start batch %s, directory lock failed: %s",self.name, exception)
        self.batchFinished(exception)
      else:
        self._logger.debug("Starting batch: %s", self.name)
        self._remoteBatchDirLocked = True
        for job in self.jobs:
          self._extantJobs.add(job)
          job.start()
    self.parentRunner.lockPath(self.remoteBatchDir, callback)

  def createUploadDirectory(self, job):
    """Create an `atsim.pro_fit.filetransfer.UploadDirectory` instance
    for `job`. The `UploadDirectory` will be registered with an upload handler
    that will call the job's  `finishUpload()` method on completion.

    Upload will occur between `job.sourcePath` and `job.remotePath`.

    Args:
        job (RunnerJob): Job

    Returns:
        UploadDirectory: Correctly instantiated directory upload instance.
    """
    handler = job.createUploadHandler()
    sourcePath = job.sourcePath
    remotePath = job.remotePath
    upload = self.parentRunner.createUploadDirectory(sourcePath, remotePath, handler)
    return upload

  def createDownloadDirectory(self, job):
    """Create an `atsim.pro_fit.filetransfer.DownloadDirectory` instance
    for `job`. The `DownloadDirectory` will be registered with a download handler
    that will call the job's  `finishDownload()` method on completion.

    Download will occur between `job.remotePath` and `job.outputPath`.

    Args:
        job (RunnerJob): Job

    Returns:
        DownloadDirectory: Correctly instantiated directory download instance.
    """
    handler = job.createDownloadHandler()
    destPath = job.outputPath

    # self._logger.getChild('createDownloadDirectory').debug("remotePath = %s", remotePath)
    # self._logger.getChild('createDownloadDirectory').debug("destPath = %s", destPath)
    os.mkdir(destPath)
    download = self.parentRunner.createDownloadDirectory(handler.remoteOutputPath, destPath, handler)
    return download

  def startJobRun(self, job):
    """Register this job with the parent runner's `RunClient`"""
    handler = job.createRunJobHandler()
    self.parentRunner.startJobRun(handler)

  def jobFinished(self, job):
    self._extantJobs.remove(job)
    if not self._extantJobs:
      self.batchFinished(None)

  def batchFinished(self, exception):
    if self._remoteBatchDirLocked:
      self.parentRunner.unlockPath(self.remoteBatchDir)
      self._remoteBatchDirLocked = False
    self.exception = exception
    self.parentRunner.batchFinished(self, exception)
    self._event.set()

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
    self.parentRunner.unlockPath(job.remotePath)

  def _createJobInstances(self, jobs):
    """Create RunnerJob instances from jobfactories.Job instances"""
    retlist = []
    numjobs = len(jobs)
    for i, job in enumerate(jobs):
      job_name = "%s-%d/%d" % (job.name,i,numjobs)
      rjob = RunnerJob(self, job, job_name)
      retlist.append(rjob)
    return retlist

  def join(self, timeout = None):
    """Join blocks until batch completes (or until timeout expires).

    This allows batch to have similar behaviour to threading.Thread object.

    Args:
        timeout (int): Timeout in seconds or None
    """
    self._event.wait(timeout)
