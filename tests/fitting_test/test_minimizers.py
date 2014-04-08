import unittest


import ConfigParser

from atomsscripts import fitting
from atomsscripts import testutil

import mystic.models

import StringIO

class MockJob(object):

  def __init__(self, variables):
    self.variables = variables

  def __repr__(self):
    return repr(self.variables)

class MockMerit(object):

  def __init__(self):
    self.afterMerit = None

  def calculate(self, candidates):
    c = candidates[0]
    v = [mystic.models.rosen(c.fitValues)]

    j = MockJob(c)

    if self.afterMerit:
      self.afterMerit(v, [(c, [j])])

    return v


class StepCallBack(object):

  def __init__(self):
    self.stepDicts = []
    self.stepNum = 0

  def __call__(self, minimizerResults):
    if self.stepNum > 11:
      return
    self.stepNum += 1

    indexOfBest = minimizerResults.indexOfBest
    mval = minimizerResults.meritValues[indexOfBest]
    variables = minimizerResults.candidateJobList[indexOfBest][0]
    cdict = {}
    cdict['meritval'] = mval
    vdict = dict(variables.variablePairs)
    cdict.update(vdict)
    self.stepDicts.append(cdict)


class NelderMeadTestCase(unittest.TestCase):
  """Test atomsscripts.fitting.minimizers"""

  def testNelderMeadGetBounds(self):
    config = """[Minimizer]
type : NelderMead
function_tolerance : 1.0E-3
value_tolerance : 1.0E-3
max_iterations : 30

"""
    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    variables = fitting.fittool.Variables([
      ('A', 1.0, False),
      ('B', 2.0, True),
      ('C', 3.0, False),
      ('D', 4.0, True)], 
      [(2.0, 3.0),
       (3.0, 4.0),
       (5.0, 6.0),
       (7.0, 8.0)])

    minimizer = fitting.minimizers.NelderMeadMinimizer.createFromConfig(variables, configitems)
    expect = [(3.0, 4.0), (7.0,8.0)] 
    testutil.compareCollection(self, expect, minimizer._getBounds())

  def testNelderMeadSingleConfig(self):
    """Tests for the atomsscripts.fitting.minimizers.NelderMead wrapper"""
    variables = fitting.fittool.Variables([
      ('A', 1.0, False),
      ('B', 2.0, True),
      ('C', 3.0, False),
      ('D', 4.0, True)])

    config = """[Minimizer]
type : NelderMead
value_tolerance : 1.0E-3

"""
    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    minimizer = fitting.minimizers.NelderMeadMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(fitting.minimizers.NelderMeadMinimizer, type(minimizer))

    config = """[Minimizer]
type : NelderMead
max_iterations : 30

"""
    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    minimizer = fitting.minimizers.NelderMeadMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(fitting.minimizers.NelderMeadMinimizer, type(minimizer))    

  def testNelderMead(self):
    """Tests for the atomsscripts.fitting.minimizers.NelderMead wrapper"""

    config = """[Minimizer]
type : NelderMead
function_tolerance : 1.0E-3
value_tolerance : 1.0E-3
max_iterations : 30

"""
    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    variables = fitting.fittool.Variables([
      ('A', 1.0, False),
      ('B', 2.0, True),
      ('C', 3.0, False),
      ('D', 4.0, True)])

    minimizer = fitting.minimizers.NelderMeadMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(fitting.minimizers.NelderMeadMinimizer, type(minimizer))

    args = minimizer._initialArgs()
    testutil.compareCollection(self, [2.0, 4.0], args)

    self.assertEquals(None, minimizer._getBounds())

    variables = minimizer._argsToVariables([5.0, 6.0])
    testutil.compareCollection(self,
      [('A', 1.0), ('B', 5.0), ('C', 3.0), ('D', 6.0)],
      variables.variablePairs)

    minimizer.stepCallback = StepCallBack()
    # Perform minimization of Rosenbrock function and check we get correct answers
    optimized = minimizer.minimize(MockMerit())

    finalmeritval = optimized.bestMeritValue
    optimized = optimized.bestVariables

    self.assertAlmostEquals(0.0668765184732, finalmeritval)
    testutil.compareCollection(self, 
      [('A', 1.0),
       ('B', 1.25066832),
       ('C', 3.0),
       ('D', 1.57052885)], optimized.variablePairs)

    stepcallbackexpect = [ 
      dict(A=1.000000, B=2.000000, C=3.000000, D=4.000000, meritval = 1.0),
      dict(A=1.000000, B=2.000000, C=3.000000, D=4.200000, meritval = 5.0000000000000071),
      dict(A=1.000000, B=2.050000, C=3.000000, D=4.050000, meritval = 3.4281249999999952),
      dict(A=1.000000, B=2.012500, C=3.000000, D=4.112500, meritval = 1.4138305664062523),
      dict(A=1.000000, B=2.028125, C=3.000000, D=4.053125, meritval = 1.4190359592437922),
      dict(A=1.000000, B=2.017188, C=3.000000, D=4.054688, meritval = 1.0552853685617489),
      dict(A=1.000000, B=2.010547, C=3.000000, D=4.069922, meritval = 1.0975087642320434),
      dict(A=1.000000, B=2.009570, C=3.000000, D=4.048633, meritval = 1.0297589176429021),
      dict(A=1.000000, B=1.998584, C=3.000000, D=4.009131, meritval = 1.0190530125207142),
      dict(A=1.000000, B=1.978735, C=3.000000, D=3.916431, meritval = 0.95803044034335361),
      dict(A=1.000000, B=1.980151, C=3.000000, D=3.907300, meritval = 0.97946469507950373),
      dict(A=1.000000, B=1.938330, C=3.000000, D=3.735596, meritval = 0.92680790388294665)]
    testutil.compareCollection(self,stepcallbackexpect, minimizer.stepCallback.stepDicts)


class InspyredSupportTestCase(unittest.TestCase):
  """Tests for adapters contained in fitting.minimizers._inspyred"""

  def testBounderGenerator(self):
    """Test Bounder and Generator"""
    from atomsscripts.fitting.minimizers import _inspyred

    # Test BounderGenerator
    # ... first check it throws when unbounded variables used for instantiation
    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Bounder(
          fitting.fittool.Variables([('A', 1.0, True)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Generator(
        fitting.fittool.Variables([('A', 1.0, True)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Bounder(
          fitting.fittool.Variables([('A', 1.0, True)], [(None, 10.0)]) )
    
    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Generator(
          fitting.fittool.Variables([('A', 1.0, True)], [(None, 10.0)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Bounder(
          fitting.fittool.Variables([('A', 1.0, False), ('B', 1.0, True)], [(None, 10.0), (-10.0, float("inf"))]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Generator(
          fitting.fittool.Variables([('A', 1.0, False), ('B', 1.0, True)], [(None, 10.0), (-10.0, float("inf"))]) )

    # ... or throws if non of the variables are fit parameters
    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Bounder(
          fitting.fittool.Variables([('A', 1.0, False), ('B', 1.0, False)], [(-10.0, 10.0), (-10.0, 10.0)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Generator(
          fitting.fittool.Variables([('A', 1.0, False), ('B', 1.0, False)], [(-10.0, 10.0), (-10.0, 10.0)]) )

    # Check we can access initial arguments
    
    # self.assertEquals(variables.flaggedVariablePairs, 
    #   bounderGenerator.initialVariables.flaggedVariablePairs)
    
    # Check the bounder
    variables = fitting.fittool.Variables([('A', 1.0, False), ('B', 2.0, True), ('C', 3.0, True), ('D', 4.0, False)], [(None, None), (-10.0, 10.0), (-20.0, 20.0), (-30.0, 30.0)])
    bounder= _inspyred.Bounder(variables)

    expect = [[-10.0, -20.0], [10.0, 20.0]]
    actual = bounder._bounds
    self.assertEquals(expect, actual)

    import inspyred
    self.assertEquals(inspyred.ec.Bounder, type(bounder._bounder))

    # Check the generator
    import random
    generator = _inspyred.Generator(variables)
    actual = generator(random.Random(), {})
    self.assertEquals(2, len(actual))



class MinimizerResultsTestCase(unittest.TestCase):
  """Tests for fitting.minimizers.MinimizerResults"""

  def testMinimizerResults(self):
    meritVals = [2.0, 3.0, 1.0]

    V = fitting.fittool.Variables
    c1 = V([ ('A', 1.0, False), ('B', 2.0, True)])
    c2 = V([ ('A', 1.0, False), ('B', 2.0, True)])
    c3 = V([ ('A', 1.0, False), ('B', 2.0, True)])

    j1 = MockJob(c1)
    j2 = MockJob(c2)
    j3 = MockJob(c3)

    candidateJobList = [ 
      (c1, [j1]),
      (c2, [j2]),
      (c3, [j3])]

    results = fitting.minimizers.MinimizerResults(meritVals, candidateJobList)

    self.assertEquals(meritVals, results.meritValues)
    self.assertEquals(candidateJobList, results.candidateJobList)

    self.assertEquals(1.0, results.bestMeritValue)
    self.assertEquals(2, results.indexOfBest)
    self.assertEquals(candidateJobList[2][1], results.bestJobList)

  def testComparison(self):
    meritVals = [2.0, 3.0, 1.0]
    meritVals2 = [2.0, 0.0, 1.0]

    V = fitting.fittool.Variables
    c1 = V([ ('A', 1.0, False), ('B', 2.0, True)])
    c2 = V([ ('A', 1.0, False), ('B', 2.0, True)])
    c3 = V([ ('A', 1.0, False), ('B', 2.0, True)])

    j1 = MockJob(c1)
    j2 = MockJob(c2)
    j3 = MockJob(c3)

    candidateJobList = [ 
      (c1, [j1]),
      (c2, [j2]),
      (c3, [j3])]

    results1 = fitting.minimizers.MinimizerResults(meritVals, candidateJobList)
    results2 = fitting.minimizers.MinimizerResults(meritVals2, candidateJobList)

    self.assertTrue(results2 < results1)
    self.assertTrue(results1 > results2)
    self.assertTrue(results1 == results1)
    self.assertTrue(results2 == results2)

