
from atsim.pro_fit._util import MultiCallback, linkevent_spawn
from _runner_batch import RunnerBatch
from _exceptions import JobKilledException

import logging
import weakref
import posixpath
import os
import pkgutil

from gevent.event import Event
import gevent

class PBSRunnerJobRecordException(Exception):
  pass

class PBSRunnerJobRecord(object):

  def __init__(self, name, pbsbatchsize, pbsclient, pbsinclude):
    self.name = name
    self._pbsclient = weakref.proxy(pbsclient)
    self._pbsbatchsize = pbsbatchsize
    self._handlers = []
    self._pbsclient_record = None
    self._pbs_submit_event = Event()
    self._pbsinclude = pbsinclude

  @property
  def jobRunEvent(self):
    return self._pbs_submit_event

  @property
  def pidSetEvent(self):
    return self.jobRunEvent

  @property
  def pbs_submit_event(self):
    return self._pbs_submit_event

  @property
  def pbsId(self):
    if self._pbsclient_record:
      return self._pbsclient_record.jobId
    return None

  @property
  def jobId(self):
    return self.pbsId

  @property
  def isFull(self):
    if self._pbsbatchsize is None:
      return False
    return len(self._handlers) >= self._pbsbatchsize

  def append(self, handler):
    if self.isFull:
      raise IndexError("JobRecord cannot accept any more handlers.")
    self._handlers.append(handler)

  def submit(self):
    if not self._handlers:
      raise PBSRunnerJobRecordException("No jobs registered with PBSRunnerJobRecord")
    joblist = [ h.workingDirectory for h in self._handlers]
    joblist = [ posixpath.join(h, posixpath.pardir, 'runjob') for h in joblist]

    handlers = [ self._handler_wrap(h) for h in self._handlers ]
    callback = MultiCallback()
    callback.extend(handlers)

    pbsclient_record = self._pbsclient.runJobs(joblist, callback, header_lines = self._pbsinclude)
    self._pbsclient_record = pbsclient_record

    linkevent_spawn(self._pbsclient_record.qsubEvent, self._pbs_submit_event)


  def _handler_wrap(self, handler):
    # The handlers passed into append expect two arguments, PBSClient expects a single argument
    # wrap the handler so it receives the number of arguments it expects.
    def handler_wrap(exc):
      return handler(exc, None)
    return handler_wrap

  def kill(self):
    if self._pbsclient_record:
      return self._pbsclient_record.kill()
    else:
      killevent = Event()
      exc = JobKilledException()
      self._handlers(exc)
      killevent.set()
      return killevent


class _ModifiedPathJob(object):

  def __init__(self, job):
    self._job = job
    self.sourcePath = job.sourcePath
    self.remotePath = job.remotePath
    self.outputPath = job.outputPath

  def setexception(self, exc):
    self._job.exception = exc

  def getexception(self, exc):
    return self._job.exception

  exception = property(fget = getexception, fset = setexception)



class PBSRunnerBatch(RunnerBatch):

  def __init__(self, parentRunner, remoteBatchDir, jobs, name, pbsclient, pbsinclude):
    self._logger = logging.getLogger(__name__).getChild("PBSRunnerBatch")
    super(PBSRunnerBatch, self).__init__(parentRunner, remoteBatchDir, jobs, name)
    self._pbsinclude = pbsinclude
    self.pbsclient = pbsclient
    self._subBatchCount = 0
    self.resetPBSJobRecord()
    self._startJobRunCount = 0
    self._submittedPBSRecords = []

  def startJobRun(self, job, handler):
    self._logger.debug("startJobRun called for batch (%s) and job: %s", self.name, job)
    self._startJobRunCount += 1
    jr = self.addJobToPBSJobRecord(handler)
    return jr

  def resetPBSJobRecord(self):
    name = self.name + "_sub: %d" % self._subBatchCount
    self._logger.debug("Resetting PBSJobRecord for batch: '%s'. PBSJobRecord name  = '%s'", self.name, name)
    self.pbsjobrecord = PBSRunnerJobRecord(name, self.parentRunner.pbsbatch_size, self.pbsclient, self._pbsinclude)
    self._subBatchCount += 1

  def addJobToPBSJobRecord(self, handler):
    self.pbsjobrecord.append(handler)
    self._logger.debug("Job added to sub-batch: '%s'", self.pbsjobrecord.name)
    if self.shouldSubmit:
      return self.submitPBSJobRecord()
    return self.pbsjobrecord

  def createUploadDirectory(self, job):
    # Add runjob to root of jobdirectory
    self._addAdditionalFilesToJobDir(job)
    return super(PBSRunnerBatch, self).createUploadDirectory(job)

  def createDownloadDirectory(self, job):
    # The output directory is created in the root of the job directory
    # not in job_files/output. Need to rewrite the remotePath to account for this.
    modified_job = _ModifiedPathJob(job)
    modified_job.remotePath = posixpath.join(modified_job.remotePath, 'output')
    # modified_job.sourcePath = os.path.join(modified_job.sourcePath, 'job_files')
    return super(PBSRunnerBatch, self).createDownloadDirectory(modified_job)

  def _addAdditionalFilesToJobDir(self, job):
    # Add a new runjob to sourcePath
    runjobpath = os.path.join(job.sourcePath, 'runjob')
    dat = pkgutil.get_data(__name__, 'templates/pbsrunner_jobrun')
    with open(runjobpath, 'w') as runjob:
      runjob.write(dat)

  @property
  def shouldSubmit(self):
    return self.pbsjobrecord.isFull or self._startJobRunCount >= len(self.jobs)

  def submitPBSJobRecord(self):
    jr = self.pbsjobrecord
    self._logger.debug("Submitting sub-batch to PBS: '%s' which contains %d jobs", jr.name, len(jr._handlers))
    self.resetPBSJobRecord()
    jr.submit()
    self._submittedPBSRecords.append(jr)
    return jr


