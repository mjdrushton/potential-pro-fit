from atsim.pro_fit._util import MultiCallback
from atsim.pro_fit._util import linkevent_spawn
from ._runner_batch import RunnerBatch
from ._exceptions import JobKilledException

import logging
import weakref
import posixpath
import os
import importlib.resources

from gevent.event import Event


class QueueingSystemRunnerJobRecordException(Exception):
    pass


class QueueingSystemRunnerJobRecord(object):
    def __init__(self, name, batch_size, qs_client, header_include):
        self.name = name
        self._qsclient = weakref.proxy(qs_client)
        self._batch_size = batch_size
        self._handlers = []
        self._qs_client_record = None
        self._qs_submit_event = Event()
        self._header_include = header_include
        self._jobId = None

    @property
    def jobRunEvent(self):
        return self._qs_submit_event

    @property
    def pidSetEvent(self):
        return self.jobRunEvent

    @property
    def submit_event(self):
        return self._qs_submit_event

    @property
    def jobId(self):
        if self._qs_client_record:
            return self._qs_client_record.jobId
        return None

    @property
    def isFull(self):
        if self._batch_size is None:
            return False
        return len(self._handlers) >= self._batch_size

    def append(self, handler):
        if self.isFull:
            raise IndexError("JobRecord cannot accept any more handlers.")
        self._handlers.append(handler)

    def submit(self):
        if not self._handlers:
            raise QueueingSystemRunnerJobRecordException(
                "No jobs registered with QueueingSystemRunnerJobRecord"
            )
        joblist = [h.workingDirectory for h in self._handlers]
        joblist = [
            posixpath.join(h, posixpath.pardir, "runjob") for h in joblist
        ]

        handlers = [self._handler_wrap(h) for h in self._handlers]
        callback = MultiCallback()
        callback.extend(handlers)

        qs_client_record = self._qsclient.runJobs(
            joblist, callback, header_lines=self._header_include
        )
        self._qs_client_record = qs_client_record

        linkevent_spawn(self._qs_client_record.qsubEvent, self._qs_submit_event)

    def _handler_wrap(self, handler):
        # The handlers passed into append expect two arguments, QueueingSystemClient expects a single argument
        # wrap the handler so it receives the number of arguments it expects.
        def handler_wrap(exc):
            return handler(exc, None)

        return handler_wrap

    def kill(self):
        if self._qs_client_record:
            return self._qs_client_record.kill()
        else:
            killevent = Event()
            exc = JobKilledException()
            for h in self._handlers:
                h(exc)
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

    exception = property(fget=getexception, fset=setexception)


class QueueingSystemRunnerBatch(RunnerBatch):
    def __init__(
        self,
        parentRunner,
        remoteBatchDir,
        jobs,
        name,
        qs_client,
        header_include,
    ):
        self._logger = logging.getLogger(__name__).getChild(
            "QueueingSystemRunnerBatch"
        )
        super(QueueingSystemRunnerBatch, self).__init__(
            parentRunner, remoteBatchDir, jobs, name
        )
        self._header_include = header_include
        self.qs_client = qs_client
        self._subBatchCount = 0
        self.resetQSJobRecord()
        self._startJobRunCount = 0
        self._submittedQSRecords = []

    def startJobRun(self, job, handler):
        self._logger.debug(
            "startJobRun called for batch (%s) and job: %s", self.name, job
        )
        self._startJobRunCount += 1
        jr = self.addJobToQSJobRecord(handler)
        return jr

    def resetQSJobRecord(self):
        name = self.name + "_sub: %d" % self._subBatchCount
        self._logger.debug(
            "Resetting QueueingSystemRunnerJobRecord for batch: '%s'. QueueingSystemRunnerJobRecord name  = '%s'",
            self.name,
            name,
        )
        self.qs_jobrecord = QueueingSystemRunnerJobRecord(
            name,
            self.parentRunner.batch_size,
            self.qs_client,
            self._header_include,
        )
        self._subBatchCount += 1

    def addJobToQSJobRecord(self, handler):
        self.qs_jobrecord.append(handler)
        self._logger.debug(
            "Job added to sub-batch: '%s'", self.qs_jobrecord.name
        )
        if self.shouldSubmit:
            return self.submitQSJobRecord()
        return self.qs_jobrecord

    def createUploadDirectory(self, job):
        # Add runjob to root of jobdirectory
        self._addAdditionalFilesToJobDir(job)
        return super(QueueingSystemRunnerBatch, self).createUploadDirectory(job)

    def createDownloadDirectory(self, job):
        # The output directory is created in the root of the job directory
        # not in job_files/output. Need to rewrite the remotePath to account for this.
        modified_job = _ModifiedPathJob(job)
        modified_job.remotePath = posixpath.join(
            modified_job.remotePath, "output"
        )
        # modified_job.sourcePath = os.path.join(modified_job.sourcePath, 'job_files')
        return super(QueueingSystemRunnerBatch, self).createDownloadDirectory(
            modified_job
        )

    def _addAdditionalFilesToJobDir(self, job):
        # Add a new runjob to sourcePath
        runjobpath = os.path.join(job.sourcePath, "runjob")
        from . import templates

        dat = importlib.resources.read_text(templates, "queueing_system_jobrun")
        with open(runjobpath, "w") as runjob:
            runjob.write(dat)

    @property
    def shouldSubmit(self):
        return self.qs_jobrecord.isFull or self._startJobRunCount >= len(
            self.jobs
        )

    def submitQSJobRecord(self):
        jr = self.qs_jobrecord
        self._logger.debug(
            "Submitting sub-batch to queueing system: '%s' which contains %d jobs",
            jr.name,
            len(jr._handlers),
        )
        self.resetQSJobRecord()
        jr.submit()
        self._submittedQSRecords.append(jr)
        return jr
