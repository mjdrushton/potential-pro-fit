
from ._runner_batch import RunnerBatch
from ._exceptions import JobKilledException

import logging
import os

from gevent.event import Event
import gevent


class RunClientHandlerWrap(object):

  def __init__(self, handler):
    self.job = handler.job
    self.finishEvent = handler.finishEvent
    self.handler = handler

  @property
  def workingDirectory(self):
    p = os.path.join(self.job.remotePath, 'job_files')
    return p

  @property
  def callback(self):
    return self.handler

class LocalRunnerBatch(RunnerBatch):

  def __init__(self, parentRunner, remoteBatchDir, jobs, name):
    self._logger = logging.getLogger(__name__).getChild("LocalRunnerBatch")
    super(LocalRunnerBatch, self).__init__(parentRunner, remoteBatchDir, jobs, name)

  def startJobRun(self, job, handler):
    self._logger.debug("startJobRun called for batch (%s) and job: %s", self.name, job)
    wrappedHandler = RunClientHandlerWrap(handler)
    return super(LocalRunnerBatch, self).startJobRun(job, wrappedHandler)

