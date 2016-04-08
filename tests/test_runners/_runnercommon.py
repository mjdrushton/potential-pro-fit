import os
import shutil
import stat
import tempfile

from .. import common
from atsim import pro_fit

import pytest

from assertpy import assert_that

DIR = 0
FILE = 1

def _compareDir(path):
  actual = []
  for f in os.listdir(path):
    fullpath = os.path.join(path, f)
    mode = os.stat(fullpath).st_mode
    if stat.S_ISDIR(mode):
      m = DIR
    elif stat.S_ISREG(mode):
      m = FILE
    else:
      m = None
    actual.append((f, m))
  return actual

def runnertestjob(runfixture, jobid):
  """Test running a single job"""

  job_path = runfixture.jobs[jobid].path
  jfdir = os.path.join(job_path, 'job_files')
  rfdir = os.path.join(job_path, 'runner_files')

  expect = [ ('job_files', DIR),
             ('runner_files', DIR)]
  actual = _compareDir(job_path)

  # Check job_files directory contents
  expect = [ ('runjob', FILE),
             ('output', DIR)]
  actual = _compareDir(jfdir)
  assert_that(sorted(actual)).is_equal_to(sorted(expect))

  # Check output directory contents
  expect = [ ('runjob', FILE),
           ('STATUS', FILE),
           ('runner_files_contents', FILE),
           ('output.res', FILE)]

  output_dir = os.path.join(jfdir, 'output')
  actual = _compareDir(output_dir)
  assert_that(sorted(actual)).is_equal_to(sorted(expect))

  # Check file contents
  status = open(os.path.join(output_dir, 'STATUS')).readline()[:-1]
  assert_that(status).is_equal_to('0')

  common.logger.debug("output.res: %s" % open(os.path.join(output_dir, 'output.res')).read())
  assert_that(runfixture.jobs[jobid].variables.id).is_equal_to(jobid)

  d = runfixture.jobfactory.evaluators[0](runfixture.jobs[jobid])
  assert_that(jobid).is_equal_to(d[0].meritValue)




class FixtureObj(object):
  def __init__(self, tempd, remoted, jobfactory, jobs):
    self.tempd = tempd
    self.remoted = remoted
    self.jobfactory = jobfactory
    self.jobs = jobs


@pytest.fixture(scope="function")
def runfixture(request):
  tempd = tempfile.mkdtemp()
  # remoted = tempfile.mkdtemp()
  remoted = None
  evaluator = common.MockEvaluator(lambda d: d['A'])

  # Create some jobs
  jobfactory = common.MockJobFactory('Runner', 'Test', [evaluator])
  jobs = []

  for i in xrange(12):
    variables = pro_fit.fittool.Variables([('A', i, True)])
    variables.id = i
    jd = os.path.join(tempd, str(i))
    os.mkdir(jd)
    jobs.append(
        jobfactory.createJob( jd, variables))
  jobs = jobs

  fixture = FixtureObj(tempd, remoted, jobfactory, jobs)

  def tearDown():
    shutil.rmtree(fixture.tempd, ignore_errors = True)
    # shutil.rmtree(fixture.remoted, ignore_errors = True)

  request.addfinalizer(tearDown)
  return fixture
