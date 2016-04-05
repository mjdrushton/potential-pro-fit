import os
import shutil
import stat
import StringIO
import tempfile
import unittest
import ConfigParser

from .. import common

from atsim import pro_fit


class LocalRunnerTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.runners.LocalRunner"""

  DIR = 0
  FILE = 1

  def setUp(self):
    self.tempd = tempfile.mkdtemp()

    evaluator = common.MockEvaluator(lambda d: d['A'])

    # Create some jobs
    self.jobfactory = common.MockJobFactory('Runner', 'Test', [evaluator])
    jobs = []

    for i in xrange(12):
      variables = pro_fit.fittool.Variables([('A', i, True)])
      variables.id = i
      jd = os.path.join(self.tempd, str(i))
      os.mkdir(jd)
      jobs.append(
          self.jobfactory.createJob( jd, variables))

    self.jobs = jobs

  def tearDown(self):
    shutil.rmtree(self.tempd, ignore_errors = True)


  def _compareDir(self, path):
    actual = []
    for f in os.listdir(path):
      fullpath = os.path.join(path, f)
      mode = os.stat(fullpath).st_mode
      if stat.S_ISDIR(mode):
        m = self.DIR
      elif stat.S_ISREG(mode):
        m = self.FILE
      else:
        m = None
      actual.append((f, m))
    return actual

  def testSingle(self):
    runner = pro_fit.runners.LocalRunner('LocalRunner', 1)
    runner.runBatch([self.jobs[0]]).join()
    self._testjob(runner, 0)

  def testAllInSingleBatch(self):
    runner = pro_fit.runners.LocalRunner('LocalRunner', 3)
    runner.runBatch(self.jobs).join()
    for job in self.jobs:
      self._testjob(runner, job.variables.id)

  def testAllInMultipleBatch(self):
    runner = pro_fit.runners.LocalRunner('LocalRunner', 3)

    f1 = runner.runBatch(self.jobs[:6])
    f2 = runner.runBatch(self.jobs[6:])
    f2.join()
    f1.join()
    for job in self.jobs:
      self._testjob(runner, job.variables.id)

  def _testjob(self, runner, jobid):
    """Test running a single job"""

    expect = [ ('runner_files', self.DIR),
           ('job_files', self.DIR)]

    actual = self._compareDir(self.jobs[jobid].path)
    self.assertEqual(sorted(expect), sorted(actual))

    jfdir = os.path.join(self.jobs[jobid].path, "job_files")

    # Check directory contents
    expect = [ ('runjob', self.FILE),
               ('output', self.DIR)]

    actual = self._compareDir(jfdir)
    self.assertEqual(sorted(expect), sorted(actual))

    # Check output directory contents
    expect = [ ('runjob', self.FILE),
             ('STATUS', self.FILE),
             ('output.res', self.FILE),
             ('runner_files_contents', self.FILE)]

    ddir = os.path.join(jfdir, 'output')
    actual = self._compareDir(ddir)
    self.assertEqual(sorted(expect), sorted(actual))

    # Check file contents
    status = open(os.path.join(ddir, 'STATUS')).readline()[:-1]
    self.assertEqual('0', status)

    common.logger.debug("output.res: %s" % open(os.path.join(ddir, 'output.res')).read())
    self.assertEqual(jobid, self.jobs[jobid].variables.id)

    d = self.jobfactory.evaluators[0](self.jobs[jobid])
    self.assertEqual(d[0].meritValue, jobid)

    rfcontents = open(os.path.join(ddir, 'runner_files_contents')).read()
    common.logger.debug("runner_files_contents: %s" % rfcontents)

    self.assertEqual("testfile",rfcontents[:-1])

  def testErrorHandling(self):
    """Check that error handling works"""

    # Create an error condition by deleting runjob form a couple of the jobs
    os.remove( os.path.join(self.jobs[0].path, 'job_files', 'runjob') )
    os.remove( os.path.join(self.jobs[5].path, 'job_files', 'runjob') )
    os.remove( os.path.join(self.jobs[8].path, 'job_files', 'runjob') )
    runner = pro_fit.runners.LocalRunner('LocalRunner', 3)
    future = runner.runBatch(self.jobs)
    future.join()

    self.assertTrue(future.errorFlag)
    expect = [0,5,8]
    actual = [ dict(j.variables.variablePairs)['A'] for (j,msg) in future.jobsWithErrors ]
    self.assertEqual(expect, actual)

  def testCreateFromConfig(self):
    """Test createFromConfig()"""
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    sio = StringIO.StringIO("""[Runner:RunnerName]
type: Local
nprocesses : 5
""")
    parser.readfp(sio)
    runner = pro_fit.runners.LocalRunner.createFromConfig('RunnerName', self.tempd, parser.items('Runner:RunnerName'))
    self.assertEquals(pro_fit.runners.LocalRunner, type(runner))
    self.assertEquals('RunnerName', runner.name)
    self.assertEquals(5, runner._runner._nprocs)

    with self.assertRaises(pro_fit.fittool.ConfigException):
      runner = pro_fit.runners.LocalRunner.createFromConfig('RunnerName', self.tempd, [])


  def testTerminate(self):
    """Test runner's .terminate() method."""
    self.fail("Not implemented")

  def testClose(self):
    """Test runner's .close() method."""
    self.fail("Not implemented")

