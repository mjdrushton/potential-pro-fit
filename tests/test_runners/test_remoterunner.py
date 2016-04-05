"""Tests for atsim.pro_fit.runners.RemoteRunner"""

import execnet
import os
import pytest
import shutil
import tempfile
import stat

from assertpy import assert_that

from .. import common

from atsim import pro_fit

from ..testutil import vagrant_box

DIR = 0
FILE = 1


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

def _createRunner(runfixture, vagrantbox, ncpu=1):
  username = vagrantbox.user()
  hostname = vagrantbox.hostname()
  port = vagrantbox.port()
  keyfilename = vagrantbox.keyfile()

  extraoptions = [("StrictHostKeyChecking","no")]

  if runfixture.remoted:
    runner = pro_fit.runners.RemoteRunner('RemoteRunner', "ssh://%s@%s:%s/%s" % (username, hostname, port, runfixture.remoted),
      ncpu,
      identityfile = keyfilename,
      extra_ssh_options = extraoptions)
  else:
    runner = pro_fit.runners.RemoteRunner('RemoteRunner', "ssh://%s@%s:%s" % (username, hostname, port),
      ncpu,
      identityfile = keyfilename,
      extra_ssh_options = extraoptions)

  return runner

def _runBatch(runner, jobs):
  return runner.runBatch(jobs)


def testUrlParse(runfixture):
  """Test parsing of host directory string"""
  username, host, port, path = pro_fit.runners.RemoteRunner._urlParse("ssh://username@localhost/remote/path")
  assert_that(username).is_equal_to('username')
  assert_that(host).is_equal_to('localhost')
  assert_that(path).is_equal_to("/remote/path")
  assert_that(port).is_none()

  username, host, port, path = pro_fit.runners.RemoteRunner._urlParse("ssh://localhost/remote/path")
  assert_that(username).is_equal_to('')
  assert_that(host).is_equal_to('localhost')
  assert_that(path).is_equal_to("/remote/path")
  assert_that(port).is_none()

  username, host, port, path = pro_fit.runners.RemoteRunner._urlParse("ssh://localhost:2222/remote/path")
  assert_that(username).is_equal_to('')
  assert_that(host).is_equal_to('localhost')
  assert_that(path).is_equal_to("/remote/path")
  assert_that(port).is_equal_to(2222)

  username, host, port, path = pro_fit.runners.RemoteRunner._urlParse("ssh://username@localhost:2222")
  assert_that(username).is_equal_to('username')
  assert_that(host).is_equal_to('localhost')
  assert_that(path).is_equal_to('')
  assert_that(port).is_equal_to(2222)

def testSingle(runfixture, vagrant_box):
  runner = _createRunner(runfixture,vagrant_box, 1)
  _runBatch(runner, [runfixture.jobs[0]]).join()
  _testjob(runfixture, 0)

def testAllInSingleBatch(runfixture, vagrant_box):
  runner = _createRunner(runfixture, vagrant_box, 3)
  _runBatch(runner, runfixture.jobs).join()
  for job in runfixture.jobs:
    _testjob(runfixture, job.variables.id)

def testAllInMultipleBatch(runfixture, vagrant_box):
  runner = _createRunner(runfixture, vagrant_box, 3)

  f1 = _runBatch(runner, runfixture.jobs[:6])
  f2 = _runBatch(runner, runfixture.jobs[6:])
  f2.join()
  f1.join()
  for job in runfixture.jobs:
    _testjob(runfixture, job.variables.id)

def _testjob(runfixture, jobid):
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


def testTempDirectoryCleanup(runfixture, vagrant_box):
  runfixture.remoted = ""
  runner = _createRunner(runfixture,vagrant_box, 1)
  assert_that(runner.remotePath).is_not_none()
  assert_that(runner._remoted_is_temp).is_true()

  # Check that a temporary remote path has been created
  # on the execution host.
  from atsim.pro_fit.runners._remoterunner import _remoteCheck

  group = execnet.Group()
  gw = group.makegateway(runner.gwurl)
  try:
    channel = gw.remote_exec(_remoteCheck)
    path = runner.remotePath
    channel.send(runner.remotePath)
    status = channel.receive()
    assert_that(status).is_true()
    # msg="Remote directory does not exist or is not read/writable:'%s'" % path)
    channel.waitclose()

  finally:
    group.terminate(10)


def testTerminate():
  """Test runner's .terminate() method."""
  assert(False)

def testClose():
  """Test runner's .close() method."""
  assert(False)
