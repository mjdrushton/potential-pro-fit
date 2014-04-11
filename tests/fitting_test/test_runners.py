import unittest

import tempfile
import shutil
import stat
import os

import common
from atsim import pro_fit

import ConfigParser

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

    # Check directory contents
    expect = [ ('runjob', self.FILE),
               ('output', self.DIR)]

    actual = self._compareDir(self.jobs[jobid].path)
    self.assertEqual(sorted(expect), sorted(actual))

    # Check output directory contents
    expect = [ ('runjob', self.FILE),
             ('STATUS', self.FILE),
             ('output.res', self.FILE)]

    ddir = os.path.join(self.jobs[jobid].path, 'output')
    actual = self._compareDir(ddir)
    self.assertEqual(sorted(expect), sorted(actual))

    # Check file contents
    status = open(os.path.join(ddir, 'STATUS')).readline()[:-1]
    self.assertEqual('0', status)

    common.logger.debug("output.res: %s" % open(os.path.join(ddir, 'output.res')).read())
    self.assertEqual(jobid, self.jobs[jobid].variables.id)

    d = self.jobfactory.evaluators[0](self.jobs[jobid])
    self.assertEqual(d[0].meritValue, jobid)

  def testErrorHandling(self):
    """Check that error handling works"""

    # Create an error condition by deleting runjob form a couple of the jobs
    os.remove( os.path.join(self.jobs[0].path, 'runjob') )
    os.remove( os.path.join(self.jobs[5].path, 'runjob') )
    os.remove( os.path.join(self.jobs[8].path, 'runjob') )
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
    import StringIO
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


class RemoteRunnerTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.runners.RemoteRunner"""

  DIR = 0
  FILE = 1

  def setUp(self):

    self.tempd = tempfile.mkdtemp()
    self.remoted = tempfile.mkdtemp()
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
    shutil.rmtree(self.remoted, ignore_errors = True)


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

  def testUrlParse(self):
    """Test parsing of host directory string"""
    username, host, path = pro_fit.runners.RemoteRunner._urlParse("ssh://username@localhost/%s" % self.remoted)
    self.assertEqual('username', username)
    self.assertEqual('localhost', host)
    self.assertEqual(self.remoted, path)

    username, host, path = pro_fit.runners.RemoteRunner._urlParse("ssh://localhost/%s" % self.remoted)
    self.assertEqual('', username)
    self.assertEqual('localhost', host)
    self.assertEqual(self.remoted, path)

  def testSingle(self):
    runner = pro_fit.runners.RemoteRunner('RemoteRunner', "ssh://localhost/%s" % self.remoted, 1)
    runner.runBatch([self.jobs[0]]).join()
    # import pudb;pudb.set_trace()
    self._testjob(runner, 0)

  def testAllInSingleBatch(self):
    runner = pro_fit.runners.RemoteRunner('RemoteRunner', "ssh://localhost/%s" % self.remoted, 3)
    runner.runBatch(self.jobs).join()
    for job in self.jobs:
      self._testjob(runner, job.variables.id)

  def testAllInMultipleBatch(self):
    runner = pro_fit.runners.RemoteRunner('RemoteRunner', "ssh://localhost/%s" % self.remoted, 3)

    f1 = runner.runBatch(self.jobs[:6])
    f2 = runner.runBatch(self.jobs[6:])
    f2.join()
    f1.join()
    for job in self.jobs:
      self._testjob(runner, job.variables.id)

  def _testjob(self, runner, jobid):
    """Test running a single job"""

    # Check directory contents
    expect = [ ('runjob', self.FILE),
               ('output', self.DIR)]

    actual = self._compareDir(self.jobs[jobid].path)
    self.assertEqual(sorted(expect), sorted(actual))

    # Check output directory contents
    expect = [ ('runjob', self.FILE),
             ('STATUS', self.FILE),
             ('output.res', self.FILE)]

    ddir = os.path.join(self.jobs[jobid].path, 'output')
    actual = self._compareDir(ddir)
    self.assertEqual(sorted(expect), sorted(actual))

    # Check file contents
    status = open(os.path.join(ddir, 'STATUS')).readline()[:-1]
    self.assertEqual('0', status)

    common.logger.debug("output.res: %s" % open(os.path.join(ddir, 'output.res')).read())
    self.assertEqual(jobid, self.jobs[jobid].variables.id)

    d = self.jobfactory.evaluators[0](self.jobs[jobid])
    self.assertEqual(d[0].meritValue, jobid)



class PBSRunnerTestCase(unittest.TestCase):


  def testPBSIdentify(self):
    """Given a string from qstat --version identify PBS system as Torque or PBSPro"""

    from atsim.pro_fit.runners._remoterunner import pbsIdentify

    # Test output from TORQUE
    versionString = "version: 2.4.16"
    actual = pbsIdentify(versionString)
    self.assertEquals("-t", actual.arrayFlag)
    self.assertEquals("PBS_ARRAYID", actual.arrayIDVariable)

    versionString = "pbs_version = PBSPro_11.1.0.111761"
    actual = pbsIdentify(versionString)
    self.assertEquals("-J", actual.arrayFlag)
    self.assertEquals("PBS_ARRAY_INDEX", actual.arrayIDVariable)








