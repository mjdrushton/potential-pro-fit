from atsim import pro_fit
import testutil

import unittest

import os
import shutil
import stat
import logging
import sys

from common import *

# import test_monitor


def _getResourceDir():
  return os.path.join(
      os.path.dirname(__file__),
      'resources',
      'example_fit')

class FitConfigTestCase(unittest.TestCase):
  """Tests pro_fit.fittool.FitConfig"""

  def setUp(self):
    cfgFilename = os.path.join( _getResourceDir(), 'fit.cfg')
    import mockrunners
    import mockeval1
    import mockeval2
    import mockfactories

    cfgobject = pro_fit.fittool.FitConfig(cfgFilename,
        runnermodules = [mockrunners],
        evaluatormodules = [mockeval1, mockeval2],
        metaevaluatormodules = [pro_fit.metaevaluators],
        jobfactorymodules = [mockfactories],
        minimizermodules =  [pro_fit.minimizers])
    self.cfgobject = cfgobject

  def testParseName(self):
    """Test parsing of [FittingRun] title variable"""

    # Check default when no name is specified.
    self.assertEquals('fitting_run', self.cfgobject.title)

    # Now try one with a name specified
    cfgFilename = os.path.join( _getResourceDir(), 'name.cfg')
    import mockrunners
    import mockeval1
    import mockeval2
    import mockfactories

    cfgobject = pro_fit.fittool.FitConfig(cfgFilename,
        runnermodules = [mockrunners],
        evaluatormodules = [mockeval1, mockeval2],
        metaevaluatormodules = [pro_fit.metaevaluators],
        jobfactorymodules = [mockfactories],
        minimizermodules =  [pro_fit.minimizers])

    self.assertEquals('This is the name of the run', cfgobject.title)

  def testParseVariables(self):
    """Test creation of pro_fit._Variables Variables section of fit.cfg"""
    variables = self.cfgobject.variables

    expect = [
    ('buck_OU_A', 405.66942 ),
    ('buck_OU_rho', 0.397000  ),
    ('buck_OU_C', 0.0       ),
    ('morse_OU_D', .75271639),
    ('morse_OU_a', 1.8640),
    ('morse_OU_r0', 2.39700),
    ('buck_OO_A', 1078.2322     ),
    ('buck_OO_rho', 0.342200  ),
    ('buck_OO_C', 3.9960000      ),
    ('buck_UU_A', 187.03000),
    ('buck_UU_rho', 0.327022),
    ('buck_UU_C', 0.0000000 ),
    ('mb_OU_rmin', 1.5),
    ('mb_OU_rmax', 11.000),
    ('mb_OO_rmin', 1.5),
    ('mb_OO_rmax', 11.000),
    ('mb_UU_rmin', 1.5),
    ('mb_UU_rmax', 11.000),
    ('mb_U_A', 1.003919),
    ('mb_O_A', 0.872509),
    ('mb_U_dens',2197.767782 ),
    ('mb_O_dens',438.831314),
    ('bounded_1', 1.0),
    ('bounded_2', 1.0)]

    testutil.compareCollection(self, expect, variables.variablePairs)

    expect = ['buck_OU_A', 'buck_OU_rho', 'buck_OU_C', 'bounded_2']
    self.assertEquals(expect, variables.fitKeys)

    expect = [405.66942 ,  0.397000  ,  0.0, 1.0]
    testutil.compareCollection(self, expect, variables.fitValues)

    for lbound,hbound in variables.bounds[:-2]:
      self.assertEqual(float("-inf"), lbound)
      self.assertEqual(float("inf"), hbound)

    bounds = variables.bounds[-2:]
    self.assertAlmostEquals(0.2, bounds[0][0])
    self.assertAlmostEquals(0.3, bounds[0][1])
    self.assertAlmostEquals(float("-inf"), bounds[1][0])
    self.assertAlmostEquals(5.0, bounds[1][1])


  def testErrorOnNoRunners(self):
    """Test that configuration error is thrown when no runners have been instantiated"""
    cfgFilename = os.path.join( _getResourceDir(), os.path.pardir, 'no_runners', 'fit.cfg')
    import mockrunners
    import mockeval1
    import mockeval2
    import mockfactories

    with self.assertRaises(pro_fit.fittool.ConfigException):
      cfgobject = pro_fit.fittool.FitConfig(cfgFilename,
          runnermodules = [mockrunners],
          evaluatormodules = [mockeval1, mockeval2],
          metaevaluatormodules = [pro_fit.metaevaluators],
          jobfactorymodules = [mockfactories],
          minimizermodules =  [pro_fit.minimizers])

  def testErrorOnNoEvaluators(self):
    """Test that configuration error is thrown when no jobs assigned to any of the runners"""
    cfgFilename = os.path.join( _getResourceDir(), os.path.pardir, 'no_evaluators', 'fit.cfg')
    import mockrunners
    import mockeval1
    import mockeval2
    import mockfactories

    with self.assertRaises(pro_fit.fittool.ConfigException):
      cfgobject = pro_fit.fittool.FitConfig(cfgFilename,
          runnermodules = [mockrunners],
          evaluatormodules = [mockeval1, mockeval2],
          metaevaluatormodules = [pro_fit.metaevaluators],
          jobfactorymodules = [mockfactories],
          minimizermodules =  [pro_fit.minimizers])

  def testErrorOnMultipleMinimizers(self):
    """Test that a configuration error is thrown when multiple minimizers are specified"""
    cfgFilename = os.path.join( _getResourceDir(), os.path.pardir, 'multiple_minimizers', 'fit.cfg')
    import mockrunners
    import mockeval1
    import mockeval2
    import mockfactories

    with self.assertRaises(pro_fit.fittool.MultipleSectionConfigException):
      cfgobject = pro_fit.fittool.FitConfig(cfgFilename,
          runnermodules = [mockrunners],
          evaluatormodules = [mockeval1, mockeval2],
          metaevaluatormodules = [pro_fit.metaevaluators],
          jobfactorymodules = [mockfactories],
          minimizermodules =  [pro_fit.minimizers])



  def testParseBounds(self):
    """Test bound parsing for pro_fit.Variables"""
    neginf = float("-inf")
    inf = float("inf")

    inputExpect = [("( - inf, inf )", [neginf, inf]),
     ("(-inf, inf)", [neginf, inf]),
     ("(-10.0,)", [-10.0, inf]),
     ("(,10.0)", [neginf, 10.0]),
     ("(-100.0, 1000.0)", [-100.0, 1000.0])]

    for i,e in inputExpect:
      actual = pro_fit.fittool.Variables._parseBounds(i)
      testutil.compareCollection(self,e,actual)

    # Check some error conditions
    inputs = [("(10.0, 1.0)"),
      "()",
      "(A,B)"]

    for i in inputs:
      with self.assertRaises(pro_fit.fittool.ConfigException):
        pro_fit.fittool.Variables._parseBounds(i)


  def testRunners(self):
    runners = self.cfgobject.runners
    self.assertEquals(sorted(['Short', 'EightCPU']), sorted(runners.keys()))

    # Test that Short was created correctly
    r = runners['Short']
    import mockrunners
    self.assertEquals(mockrunners.MockRunner1Runner, r.__class__)
    self.assertEquals('Short', r.name)
    self.assertEquals('mjdr@login.cx1.hpc.ic.ac.uk:/work/mjdr/jobs', r.remote_dir)
    self.assertEquals('short.pbs', r.header_filename)
    self.assertEquals(_getResourceDir(), r.fitpath)

    # Test the EightCPU was created correctly
    r = runners['EightCPU']
    self.assertEquals(mockrunners.MockRunner2Runner, r.__class__)
    self.assertEquals('EightCPU', r.name)
    self.assertEquals('mjdr@login.cx1.hpc.ic.ac.uk:/work/mjdr/jobs', r.remote_dir)
    self.assertEquals(int(5), r.ncpus)

  def testMinimizer(self):
    minimizer = self.cfgobject.minimizer
    self.assertEquals(pro_fit.minimizers.NelderMeadMinimizer, type(minimizer))
    self.assertEquals(self.cfgobject.variables, minimizer._initialVariables)

  def testMetaEvaluators(self):
    metaevaluators = self.cfgobject.metaEvaluators
    self.assertEquals(1, len(metaevaluators))
    self.assertEquals(pro_fit.metaevaluators.FormulaMetaEvaluator, type(metaevaluators[0]))
    self.assertEquals("SumThing", metaevaluators[0].name)

  def testCalculatedVariables(self):
    cvars = self.cfgobject.calculatedVariables
    self.assertEquals(pro_fit.fittool.CalculatedVariables, type(cvars))
    transvars = cvars(self.cfgobject.variables)
    self.assertAlmostEquals(405.66942 + .75271639, dict(transvars.variablePairs)['cvar'])

    # Check that fittool throws if a calculated variable name shadows an existing variable
    # name.
    badcfgfilename = os.path.join(_getResourceDir(), "bad_calcvars.cfg")
    self.cfgobject._cfg = self.cfgobject._parseConfig(badcfgfilename)
    self.cfgobject._variables = self.cfgobject._createVariables()

    with self.assertRaises(pro_fit.fittool.ConfigException):
      self.cfgobject._createCalculatedVariables()


