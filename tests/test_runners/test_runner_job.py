from atsim.pro_fit.runners._runner_job import RunnerJob

from atsim.pro_fit.jobfactories import Job

def test_remotePath():
  job = Job(None, "/this/is/the/job_path", [])

  class MockBatch(object):

    def __init__(self):
      self.name = "MockBatch"
      self.remoteBatchDir = "/this/is/the/remote/path"

  batch = MockBatch()

  rj = RunnerJob(batch, job)
  rp = rj.remotePath
  assert "/this/is/the/remote/path/job_path_"+str(rj.jobid) == rp
