import unittest

from atomsscripts import testutil
from atomsscripts import fitting

import ConfigParser


class VariablesTestCase(unittest.TestCase):
  """Tests fitting.fittool._Variables class"""
  
  def testFlaggedVariablePairs(self):
    """Test flaggedVariablePairs property of Variables class"""
    expect = [
      ('A', 1.0, False),
      ('B', 2.0, True),
      ('C', 3.0, False),
      ('D', 4.0, True),
      ('E', 5.0, True) ]
    
    v = fitting.fittool.Variables(expect)
    actual = v.flaggedVariablePairs
    testutil.compareCollection(self, expect, actual)

  def testCreateUpdated(self):
    """Ensure correct behaviour of fitting.fittool._Variables.createUpdated()"""
    initialVariables = fitting.fittool.Variables(
        [ ('A', 1.0, False),
          ('B', 2.0, True),
          ('C', 3.0, True) ])

    candidate1 = initialVariables.createUpdated()
    candidate2 = initialVariables.createUpdated([5.0, 6.0])
    
    testutil.compareCollection(self,
        [ ('A', 1.0),
          ('B', 2.0),
          ('C', 3.0) ],
        initialVariables.variablePairs)
    self.assertEquals( ['B', 'C'],  initialVariables.fitKeys)
    self.assertEquals( [2.0, 3.0],  initialVariables.fitValues)
    
    testutil.compareCollection(self,
        [ ('A', 1.0),
          ('B', 2.0),
          ('C', 3.0) ],
        candidate1.variablePairs)
    self.assertEquals( ['B', 'C'],  candidate1.fitKeys)
    self.assertEquals( [2.0, 3.0],  candidate1.fitValues)
    
    testutil.compareCollection(self,
        [ ('A', 1.0),
          ('B', 2.0),
          ('C', 3.0) ],
        candidate1.variablePairs)
    self.assertEquals( ['B', 'C'],  candidate1.fitKeys)
    self.assertEquals( [2.0, 3.0],  candidate1.fitValues)

    testutil.compareCollection(self,
        [ ('A', 1.0),
          ('B', 5.0),
          ('C', 6.0) ],
        candidate2.variablePairs)
    self.assertEquals( ['B', 'C'],  candidate2.fitKeys)
    self.assertEquals( [5.0, 6.0],  candidate2.fitValues)


class CalculatedVariables(unittest.TestCase):
  """Tests for synthetic variables"""

  def testNoExpressions(self):
    """Test that variables pass through CalculatedVariables unchanged when no expressions specified"""
    variables = fitting.fittool.Variables(
      [("A", 1.23, False),
      ("B", 4.56, False),
      ("electroneg", 0.4, True)],
      bounds = [None, None, (0,1)] )

    calculatedVariables = fitting.fittool.CalculatedVariables([])
    outVars = calculatedVariables(variables)

    expect = [
      ("A", 1.23, False),
      ("B", 4.56, False),
      ("electroneg", 0.4, True)]

    testutil.compareCollection(self,
        expect, outVars.flaggedVariablePairs)

  def testCalculatedVariables(self):
    """Test the creation and calculation of synthetic variables"""
    # import pudb;pudb.set_trace()
    expression1 = "5 + 6 + 8"
    expression2 = "-electroneg * 2"
    expression3 = "electroneg * 4"


    variables = fitting.fittool.Variables(
      [("A", 1.23, False),
      ("B", 4.56, False),
      ("electroneg", 0.4, True)],
      bounds = [None, None, (0,1)] )

    calculatedVariables = fitting.fittool.CalculatedVariables(
      [("sum", expression1),
      ("Ocharge", expression2),
      ("Ucharge", expression3)] )

    outVars = calculatedVariables(variables)

    expect = [
      ("A", 1.23, False),
      ("B", 4.56, False),
      ("electroneg", 0.4, True),
      ("sum", 19, False),
      ("Ocharge", -0.4*2, False),
      ("Ucharge", 0.4*4, False) ]

    testutil.compareCollection(self,
      expect, outVars.flaggedVariablePairs)

    # Check bounds
    expect = [None, None, (0,1), None, None, None]

    testutil.compareCollection(self, expect, outVars.bounds)


  def testCreateFromConfig(self):
    """Test creation of CalculatedVariables from [CalculatedVariables] configuration directives"""

    config = """[CalculatedVariables]
sum : 5+6+8
Ocharge : -electroneg * 2
Ucharge : electroneg * 4
"""
    
    import StringIO

    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('CalculatedVariables')

    variables = fitting.fittool.Variables(
      [("A", 1.23, False),
      ("B", 4.56, False),
      ("electroneg", 0.4, True)],
      bounds = [None, None, (0,1)] )

    calculatedVariables = fitting.fittool.CalculatedVariables.createFromConfig(cfg.items("CalculatedVariables"))
    outVars = calculatedVariables(variables)

    expect = [
      ("A", 1.23, False),
      ("B", 4.56, False),
      ("electroneg", 0.4, True),
      ("sum", 19, False),
      ("Ocharge", -0.4*2, False),
      ("Ucharge", 0.4*4, False) ]

    testutil.compareCollection(self,
      expect, outVars.flaggedVariablePairs)
    

    # Now check that an exception is thrown if a bad expression is used.
    config = """[CalculatedVariables]
sum : abs(5+6+8
Ocharge : -electroneg * 2
Ucharge : electroneg * 4
"""
    
    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('CalculatedVariables')

    with self.assertRaises(fitting.fittool.ConfigException):
      fitting.fittool.CalculatedVariables.createFromConfig(cfg.items("CalculatedVariables"))
