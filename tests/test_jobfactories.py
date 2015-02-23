import unittest

import shutil
import tempfile
import os
import ConfigParser

from common import *
from atsim import pro_fit

def _getResourceDir():
  return os.path.join(
      os.path.dirname(__file__),
      'resources')


class TemplateJobFactoryTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.jobfactories.TemplateJobFactory"""

  def setUp(self):
    self.tempd = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tempd, ignore_errors = True)

  def testCreateJob(self):
    """Test TemplateJobFactory.createJob()"""
    srcpath = os.path.join(
        _getResourceDir(),
        'template_job_factory')

    logger.debug('srcpath: %s' % srcpath)
    logger.debug('destpath: %s' % self.tempd)

    factory = pro_fit.jobfactories.TemplateJobFactory(srcpath, 'Runner',
        'Job',
        [])

    variables = pro_fit.fittool.Variables([
      ('NAME', 'Named', True),
      ('A', 5.0,  False) ])

    # Create the directory
    job = factory.createJob(self.tempd, variables)

    # Compare the directory and check it contains what it should
    expect = [ 'runjob', 'Named', 'static', 'fit.cfg']
    actual = os.listdir(self.tempd)

    self.assertEqual( sorted(expect), sorted(actual))

    # Now check that runjob contains what it should.
    expect="""#! /bin/bash

echo 5.0 > output.res
"""
    actual = open(os.path.join(self.tempd, 'runjob')).read()
    self.assertEqual(expect, actual)

    self.assertEqual(self.tempd, job.path)

  def testCreateFromConfig(self):
    """Test atsim.pro_fit.jobfactories.TemplateJobFactory.createFromConfig"""
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    import StringIO
    sio = StringIO.StringIO("""[Job]
type : Template
runner : runner_name
""")
    parser.readfp(sio)
    sect = parser.items('Job')

    import mockeval1
    eval1 = mockeval1.MockEvaluator1Evaluator()
    jf = pro_fit.jobfactories.TemplateJobFactory.createFromConfig('path/to/sourcedir', 'runner_name', 'Blah', [eval1], sect)
    self.assertEqual(pro_fit.jobfactories.TemplateJobFactory, type(jf))
    self.assertEqual('runner_name', jf.runnerName)
    self.assertEqual('Blah', jf.jobName)
    self.assertEqual([eval1], jf.evaluators)
    self.assertEqual('path/to/sourcedir', jf._templatePath)


