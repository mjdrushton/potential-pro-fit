import unittest

from atsim import pro_fit

class InspyredSupportTestCase(unittest.TestCase):
  """Tests for adapters contained in fitting.minimizers._inspyred"""

  def testBounderGenerator(self):
    """Test Bounder and Generator"""
    from atsim.pro_fit.minimizers import _inspyred

    # Test BounderGenerator
    # ... first check it throws when unbounded variables used for instantiation
    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Bounder(
          pro_fit.fittool.Variables([('A', 1.0, True)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Generator(
        pro_fit.fittool.Variables([('A', 1.0, True)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Bounder(
          pro_fit.fittool.Variables([('A', 1.0, True)], [(None, 10.0)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Generator(
          pro_fit.fittool.Variables([('A', 1.0, True)], [(None, 10.0)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Bounder(
          pro_fit.fittool.Variables([('A', 1.0, False), ('B', 1.0, True)], [(None, 10.0), (-10.0, float("inf"))]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Generator(
          pro_fit.fittool.Variables([('A', 1.0, False), ('B', 1.0, True)], [(None, 10.0), (-10.0, float("inf"))]) )

    # ... or throws if non of the variables are fit parameters
    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Bounder(
          pro_fit.fittool.Variables([('A', 1.0, False), ('B', 1.0, False)], [(-10.0, 10.0), (-10.0, 10.0)]) )

    with self.assertRaises(_inspyred.VariableException):
      _inspyred.Generator(
          pro_fit.fittool.Variables([('A', 1.0, False), ('B', 1.0, False)], [(-10.0, 10.0), (-10.0, 10.0)]) )

    # Check we can access initial arguments

    # self.assertEquals(variables.flaggedVariablePairs,
    #   bounderGenerator.initialVariables.flaggedVariablePairs)

    # Check the bounder
    variables = pro_fit.fittool.Variables([('A', 1.0, False), ('B', 2.0, True), ('C', 3.0, True), ('D', 4.0, False)], [(None, None), (-10.0, 10.0), (-20.0, 20.0), (-30.0, 30.0)])
    bounder= _inspyred.Bounder(variables)

    expect = [[-10.0, -20.0], [10.0, 20.0]]
    actual = bounder._bounds
    self.assertEqual(expect, actual)

    import inspyred
    self.assertEqual(inspyred.ec.Bounder, type(bounder._bounder))

    # Check the generator
    import random
    generator = _inspyred.Generator(variables)
    actual = generator(random.Random(), {})
    self.assertEqual(2, len(actual))
