import logging
import os
import uuid
import posixpath

from atsim.pro_fit.filetransfer import UploadHandler, DownloadHandler

_logger = logging.getLogger("atsim.pro_fit.runners")

class RunnerJob(object):

  _logger = _logger.getChild("RunnerJob")

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

    # Local path which will be uploaded to remote-host
    self.sourcePath = job.path
    # The ultimate destination for the job on completion.
    self.outputPath = job.outputPath
    # The remote directory into which  the job should be copied.
    self._remotePath = None

  @property
  def remotePath(self):
    if self._remotePath is None:
      junk, job_name  = os.path.split(self.sourcePath)
      batchdir = self.parentBatch.remoteBatchDir
      self._remotePath = os.path.join(batchdir, job_name)
    return self._remotePath

  def start(self):
    self._logger.info("Starting job %s from batch %s" % (self.jobid, self.parentBatch.name))
    self._logger.debug("Job %s from batch %s has properties, sourcePath = '%s', remotePath = '%s' and outputPath = '%s'" % (self.jobid,
      self.parentBatch.name,
      self.sourcePath,
      self.remotePath,
      self.outputPath))

    self.startUpload()

  def startUpload(self):
    self._logger.debug("Starting upload for job %s from batch %s" % (self.jobid, self.parentBatch.name))
    upload = self.parentBatch.createUploadDirectory(self)

    def callback(exception):
      if not exception is None:
        self._logger.warning("Could not start upload for job %s from batch %s, directory lock failed: %s",self.jobid, self.parentBatch.name, exception)
        self.finishJob(exception)
      else:
        self._logger.debug("Starting upload for job %s from batch %s" % (self.jobid, self.parentBatch.name))
        upload.upload(non_blocking = True)

    self.parentBatch.lockJobDirectory(self, callback)

  def finishUpload(self, exception):
    if not exception is None:
      self._logger.warning("Upload finished for job %s from batch %s, with exception: %s" % (self.jobid, self.parentBatch.name, exception))
      self.finishJob(exception)
    self._logger.debug("Upload finished successfully for job %s from batch %s" % (self.jobid, self.parentBatch.name))
    self.startJobRun()

  def startJobRun(self):
    self._logger.debug("Starting job exection (startJobRun) for job %s from batch %s" % (self.jobid, self.parentBatch.name))
    self.parentBatch.startJobRun(self)

  def finishJobRun(self, exception):
    if not exception is None:
      self._logger.warning("Job execution finished (finishJobRun) for job %s from batch %s, with exception: %s" % (self.jobid, self.parentBatch.name, exception))
      self.finishJob(exception)
    else:
      self._logger.info("Job execution finished successfully (finishJobRun) for job %s from batch %s" % (self.jobid, self.parentBatch.name))
    self.startDownload()

  def startDownload(self):
    self._logger.debug("Starting download for job %s from batch %s" % (self.jobid, self.parentBatch.name))
    download = self.parentBatch.createDownloadDirectory(self)
    download.download(non_blocking = True)

  def finishDownload(self, exception):
    if exception is None:
      self._logger.debug("Download finished successfully for job %s from batch %s" % (self.jobid, self.parentBatch.name))
    else:
      self._logger.warning("Download finished for job %s from batch %s, with exception: %s" % (self.jobid, self.parentBatch.name, exception))
    self.finishJob(exception)

  def finishJob(self, exception):
    self._logger.info("Job %s from batch %s, finished." % (self.jobid, self.parentBatch.name))
    self.parentBatch.unlockJobDirectory(self)
    self.parentBatch.jobFinished(self)

  def createDownloadHandler(self):
    return RunnerJobDownloadHandler(self)

  def createUploadHandler(self):
    return RunnerJobUploadHandler(self)

  def createRunJobHandler(self):
    return RunnerJobRunClientJob(self)

class RunnerJobDownloadHandler(DownloadHandler):
  """Handler that acts as finish() callback for RunnerJob.

  Taks RunnerJob instance and will call its `finishDownload()` method when download
  completes"""

  _logger = _logger.getChild('RunnerJobDownloadHandler')

  def __init__(self,  job):
    self.remoteOutputPath = posixpath.join(job.remotePath, "job_files")
    super(RunnerJobDownloadHandler, self).__init__(self.remoteOutputPath, job.outputPath)
    self.job = job

  def finish(self, exception = None):
    self.job.finishDownload(exception)
    return None

class RunnerJobUploadHandler(UploadHandler):
  """Handler that acts as finish() callback for RunnerJob.

  Taks RunnerJob instance and will call its `finishUpload()` method when download
  completes"""

  _logger = _logger.getChild('RunnerJobUploadHandler')

  def __init__(self, job):
    super(RunnerJobUploadHandler, self).__init__(job.sourcePath, job.remotePath)
    self.job = job

  def finish(self, exception = None):
    self.job.finishUpload(exception)
    return None

class RunnerJobRunClientJob(object):

    def __init__(self, job):
      self.job = job

    @property
    def workingDirectory(self):
      return posixpath.join(self.job.remotePath, "job_files")

    @property
    def callback(self):
      return self

    def __call__(self, exception):
      self.job.finishJobRun(exception)

