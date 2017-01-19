import logging
import os
import posixpath
import sys
import traceback
import uuid

import gevent
import gevent.event

from atsim.pro_fit.filetransfer import  DownloadHandler, DownloadCancelledException, UploadCancelledException

from _run_remote_client import RunJobKilledException

class _RunnerJobThread(object):

  def __init__(self, job):
    self.job = job
    self.killedEvent = self.job._killedEvent
    self.finishedEvent = gevent.event.Event()
    self._logger = self.job._logger.getChild("_RunnerJobThread")
    self._waitTimeout = 0.5
    self.exception = None

  def run(self):
    try:
      if not self.doLock():
        return

      if not self.doUpload():
        return

      if not self.doRun():
        return

      if not self.doDownload():
        return

      self._logger.debug("%s calling finishJob", self.job)
      self.finishJob(None)
    except:
      exception = sys.exc_info()
      tbstring = traceback.format_exception(*exception)
      self._logger.warning("%s run exception: %s", self.job, tbstring)
      self.finishJob(exception)
      raise exception

  def doLock(self):
    if self.killedEvent.is_set():
      self.finishJob(JobKilledException())
      return False

    lockEvent = self.job._lockDirectory()
    lockEvent.wait()
    exc = self.job.exception
    if not exc is None:
      self._logger.warning("Could not start upload for %s, directory lock failed: %s",self.job, exc)
      self.finishJob(exc)
      return False
      self._logger.info("Directory locked for %s.", self.job)
    return True
    self.startUpload()

  def doUpload(self):
    if self.killedEvent.is_set():
      self.finishJob(JobKilledException())
      return False

    self.job.status.append("start upload")
    uploadedEvent, uploadDirectory = self.job._startUpload()
    mergedEvent = self._makeEventOrKillEvent(uploadedEvent)
    mergedEvent.wait()
    # Which event occurred?
    if self.killedEvent.is_set():
      if not uploadedEvent.is_set():
        self.cancelUpload(uploadDirectory)
        return False
      else:
        self.finishJob(RunJobKilledException())
        return False
    elif uploadedEvent.is_set():
      self.job.status.append("finish upload")
      exc = self.job.exception
      if not exc is None:
        self._logger.info("Upload finished for job %s, with exception: %s", self.job, exc)
        self.finishJob(exc)
        return False
      else:
        self._logger.debug("Upload finished successfully for %s", self.job)
        return True

  def cancelUpload(self, uploadDirectory):
    self._logger.debug("Cancelling upload for %s", self.job)
    cancelEvent = uploadDirectory.cancel()
    cancelEvent.wait()
    self.finishJob(UploadCancelledException())

  def doRun(self):
    if self.killedEvent.is_set():
      self.finishJob(JobKilledException())
      return False

    self.job.status.append("start job run")
    jobRunEvent, jobRun = self.job._startJobRun()
    mergedEvent = self._makeEventOrKillEvent(jobRunEvent)
    mergedEvent.wait()

    # Which event occurred?
    if self.killedEvent.is_set():
      if not jobRunEvent.is_set():
        self.killJob(jobRun)
        return False
      else:
        self.job.status.append("finish job run killed")
        self.finishJob(RunJobKilledException())
        return False
    elif jobRunEvent.is_set():
      self.job.status.append("finish job run")
      exc = self.job.exception
      self._logger.debug("finish job run: %s", exc)
      if not exc is None:
        self._logger.warning("Job execution finished (finishJobRun) for %s, with exception: %s" , self.job, exc)
        self.finishJob(exc)
        return False
      else:
        self._logger.info("Job execution finished successfully (finishJobRun) for %s" , self.job)
        return True

  def killJob(self, jobRun):
    killEvent = jobRun.kill()
    killEvent.wait()
    self.job.status.append("finish job run killed")
    self.finishJob(RunJobKilledException())
    return False

  def doDownload(self):
    if self.killedEvent.is_set():
      self.finishJob(JobKilledException())
      return False

    self.job.status.append("start download")
    downloadedEvent, downloadDirectory = self.job._startDownload()
    mergedEvent = self._makeEventOrKillEvent(downloadedEvent)
    mergedEvent.wait()

    # Which event occurred?
    if self.killedEvent.is_set():
      if not downloadedEvent.is_set():
        self.cancelDownload(downloadDirectory)
        return False
      else:
        self.finishJob(RunJobKilledException())
        return False
    elif downloadedEvent.is_set():
      self.job.status.append("finish download")
      exc = self.job.exception
      if not exc is None:
        self._logger.warning("Download finished for job %s, with exception: %s", self.job, exc)
        self.finishJob(exc)
        return False
      else:
        self._logger.debug("Download finished successfully for %s", self.job)
        return True

  def cancelDownload(self, downloadDirectory):
    cancelEvent = downloadDirectory.cancel()
    cancelEvent.wait()
    self.finishJob(RunJobKilledException())

  def finishJob(self, exception):
    if self.exception:
      tbstring = traceback.format_exception(*self.exception)
      self._logger.debug("Finish job, job had exception: %s", tbstring)

    unlockWait = None

    try:
      self._logger.info("%s, finished.", self.job)
      if exception is None:
        self.job.status.append("finish job")
      else:
        try:
          raise exception
        except (RunJobKilledException, DownloadCancelledException, UploadCancelledException):
          self._logger.debug("finish job killed")
          self.job.status.append("finish job killed")
        except:
          self._logger.debug("finish job error")
          self.job.status.append("finish job error")
          self.exception = sys.exc_info()

      if self.job._directoryLocked:
        unlockEvent = self.job.parentBatch.unlockJobDirectory(self.job)
        def after():
          unlockEvent.wait()
          self.job._directoryLocked = False
          self.finishedEvent.set()
        gevent.Greenlet.spawn(after)

      self.job.parentBatch.jobFinished(self.job)
      self._logger.debug("Finished set")
    finally:

      if not unlockWait is None:
        self.finishedEvent.set()

      if not self.exception is None:
        self._logger.warning("%s finished with exception: %s", self, traceback.format_exception(*self.exception))

  def _makeEventOrKillEvent(self, e):
    events = [e, self.killedEvent]
    mergedEvent = gevent.event.Event()
    def after():
      gevent.wait(objects = events, count =1)
      mergedEvent.set()
    gevent.Greenlet.spawn(after)
    return mergedEvent

class RunnerJob(object):

  _logger = logging.getLogger(__name__).getChild("RunnerJob")

  # Job workflow:
  # 1. Upload job directory:
  #     * Create UploadDirectory for the job instantiating with remote-batch-dir.
  #     * Register RunnerJobUploadHandler  whose .finish() method triggers step 2.
  # 2. Run job:
  #     * Register this job and its finish callback with RunClient.
  #     * When the callback is invoked, this starts step 3.
  # 3. Download job directory:
  #     * Create DownloadDirectory instance.
  #     * Register a DownloadHandler responsible for translating remote-path to local path.
  #     * DownloadHandler's .finish() method is overidden to set RunnerJob's state to finished.

  def __init__(self, parentBatch, job, jobid = None):
    """Create RunnerJob.

    Args:
        parentBatch (RunnerBatch): Batch to which this job belongs.
        job (jobfactories.Job): Job to be run in this batch.
    """
    self.job = job

    if jobid is None:
      self.jobid = uuid.uuid4()
    else:
      self.jobid = jobid

    self.parentBatch = parentBatch

    self._jobRun = None

    # Local path which will be uploaded to remote-host
    self.sourcePath = job.path
    # The ultimate destination for the job on completion.
    self.outputPath = job.outputPath
    # The remote directory into which  the job should be copied.
    self._remotePath = None

    self.status = []

    self._directoryLocked = False
    self._killedEvent = gevent.event.Event()
    self._jobThread = _RunnerJobThread(self)

  @property
  def isFinished(self):
    return self._jobThread.finishedEvent.is_set()

  @property
  def killed(self):
    return self._killedEvent.is_set()

  @property
  def name(self):
    return self.job.name

  @property
  def remotePath(self):
    if self._remotePath is None:
      junk, job_name  = os.path.split(self.sourcePath)
      # Make the job_name unique (for population minimizer use)
      # by using the jobid as a suffix
      job_name = "_".join([job_name, self.jobid])
      batchdir = self.parentBatch.remoteBatchDir
      self._remotePath = os.path.join(batchdir, job_name)
    return self._remotePath

  @property
  def pid(self):
    if not self._jobRun:
      return None
    return self._jobRun.pid

  @property
  def pidSetEvent(self):
    if not self._jobRun:
      return None
    return self._jobRun.pidSetEvent


  def start(self):
    self._logger.info("Starting job %s from batch %s" % (self.jobid, self.parentBatch.name))
    self._logger.debug("Job %s from batch %s has properties, sourcePath = '%s', remotePath = '%s' and outputPath = '%s'" % (self.jobid,
      self.parentBatch.name,
      self.sourcePath,
      self.remotePath,
      self.outputPath))
    gevent.Greenlet.spawn(self._jobThread.run)

  def _lockDirectory(self):
    self._logger.debug("Locking directory for job %s from batch %s" % (self.jobid, self.parentBatch.name))
    lockEvent = gevent.event.Event()
    def callback(exception):
      self.exception = exception
      self._directoryLocked = True
      lockEvent.set()
    self.parentBatch.lockJobDirectory(self, callback)
    return lockEvent

  def _startUpload(self):
    self._logger.debug("(startUpload) for %s" % self)
    # event = gevent.event.Event()
    # handler = RunnerJobUploadHandler(self)
    finishEvent, uploadDirectory = self.parentBatch.createUploadDirectory(self)
    uploadDirectory.upload(non_blocking = True)
    return finishEvent, uploadDirectory

  def _startJobRun(self):
    self._logger.debug("Starting job execution (startJobRun) for %s" % self)
    handler = RunnerJobRunClientJob(self)
    jobRun = self.parentBatch.startJobRun(self, handler)
    self._jobRun = jobRun
    return handler.finishEvent, jobRun

  def _startDownload(self):
      self._logger.debug("Starting download for %s" % self)
      finishEvent, downloadDirectory = self.parentBatch.createDownloadDirectory(self)
      downloadDirectory.download(non_blocking = True)
      return finishEvent, downloadDirectory

  def kill(self):
    """Kill the current job and perform any required cleanup

    Returns:
        gevent.event.Event: Event that is set() once the job has terminated successfully.
    """
    self._killedEvent.set()
    return self._jobThread.finishedEvent

  def __repr__(self):
    return "job %s from batch %s" % (self.jobid, self.parentBatch.name)

class RunnerJobRunClientJob(object):

    def __init__(self, job):
      self.job = job
      self.finishEvent = gevent.event.Event()

    @property
    def workingDirectory(self):
      return posixpath.join(self.job.remotePath, "job_files")

    @property
    def callback(self):
      return self

    def __call__(self, exception, rjc):
      self.job.exception = exception
      self.finishEvent.set()

