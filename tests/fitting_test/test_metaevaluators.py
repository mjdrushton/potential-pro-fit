import unittest


from atomsscripts import fitting
from atomsscripts import testutil

class MockJob(object):

  def __init__(self, name, evaluatorRecords):
    self.name = name
    self.evaluatorRecords = evaluatorRecords

#TODO: Test errors

class FormulaMetaEvaluator(unittest.TestCase):

  def _parseListAsConfig(self, config):
    import ConfigParser
    import os
    config = os.linesep.join(config)
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    import StringIO
    sio = StringIO.StringIO(config)   
    parser.readfp(sio)
    return parser

  def testSplitVariables(self):
    variables = [
      fitting.metaevaluators.FormulaVariable("A", "MgO:Gulp:lattice_energy"),
      fitting.metaevaluators.FormulaVariable("B", "MgO:Gulp:lattice_energy:extracted_value"),
      fitting.metaevaluators.FormulaVariable("C", "MgO:Gulp:lattice_energy:merit_value"),
      fitting.metaevaluators.FormulaVariable("D", "MgO:Gulp:lattice_energy:weight"),
      fitting.metaevaluators.FormulaVariable("E", "MgO:Gulp:lattice_energy:expected_value")]

    SVK = fitting.metaevaluators.SplitVariableKey
    expect = [ ('A', SVK("MgO", "Gulp", "lattice_energy", "merit_value")),
               ('B', SVK("MgO", "Gulp", "lattice_energy", "extracted_value")),
               ('C', SVK("MgO", "Gulp", "lattice_energy", "merit_value")),
               ('D', SVK("MgO", "Gulp", "lattice_energy", "weight")),
               ('E', SVK("MgO", "Gulp", "lattice_energy", "expected_value"))]

    testutil.compareCollection(self, expect, fitting.metaevaluators.FormulaMetaEvaluator._splitVariables(variables))

    with self.assertRaises(fitting.fittool.ConfigException):
      fitting.metaevaluators.FormulaMetaEvaluator._splitVariables([
        fitting.metaevaluators.FormulaVariable("A", "MgO:Gulp:lattice_energy:blibble")])


  def testGetVariableValues(self):
    """Test fitting.metaevaluators.FormulaMetaEvaluator"""
    Job = fitting.jobfactories.Job
    ER = fitting.evaluators.EvaluatorRecord

    le = ER("lattice_energy", 1000.0, 100.0, 2.0,123.0, "MgO:Gulp")
    e1v1 = ER("value1", 2000.0, 200.0, 3.0,124.0, "CaO:Eval1")
    e2v1 = ER("value1", 3000.0, 300.0, 4.0,125.0, "CaO:Eval2")
    e2v2 = ER("value2", 4000.0, 400.0, 5.0,126.0, "CaO:Eval2")

    mgojob = MockJob("MgO", [[le]])
    caojob = MockJob("CaO", [[e1v1], [e2v1, e2v2]])

    variables = [
      fitting.metaevaluators.FormulaVariable("A", "MgO:Gulp:lattice_energy"),
      fitting.metaevaluators.FormulaVariable("B", "MgO:Gulp:lattice_energy:extracted_value"),
      fitting.metaevaluators.FormulaVariable("C", "MgO:Gulp:lattice_energy:merit_value"),
      fitting.metaevaluators.FormulaVariable("D", "MgO:Gulp:lattice_energy:weight"),
      fitting.metaevaluators.FormulaVariable("E", "MgO:Gulp:lattice_energy:expected_value"),
      fitting.metaevaluators.FormulaVariable("F", "CaO:Eval1:value1"),
      fitting.metaevaluators.FormulaVariable("G", "CaO:Eval2:value1"),
      fitting.metaevaluators.FormulaVariable("H", "CaO:Eval2:value2")]
    variables = fitting.metaevaluators.FormulaMetaEvaluator._splitVariables(variables)
    metaeval = fitting.metaevaluators.FormulaMetaEvaluator("Meta", "A+B", variables)
    variabledict = metaeval._makeVariableDict([mgojob, caojob])

    expect = { "A" : 123.0,
      "B" : 100.0,
      "C" : 123.0,
      "D" : 2.0,
      "E" : 1000.0,
      "F" : 124.0,
      "G" : 125.0,
      "H" : 126.0}

    testutil.compareCollection(self, expect, variabledict)

  def testEvaluate(self):
    """Test that formula FormulaMetaEvaluator performs calculations correctly"""
    Job = fitting.jobfactories.Job
    ER = fitting.evaluators.EvaluatorRecord

    le = ER("lattice_energy", 1000.0, 100.0, 2.0,123.0, "MgO:Gulp")
    e1v1 = ER("value1", 2000.0, 200.0, 3.0,124.0, "CaO:Eval1")
    e2v1 = ER("value1", 3000.0, 300.0, 4.0,125.0, "CaO:Eval2")
    e2v2 = ER("value2", 4000.0, 400.0, 5.0,126.0, "CaO:Eval2")

    mgojob = MockJob("MgO", [[le]])
    caojob = MockJob("CaO", [[e1v1], [e2v1, e2v2]])

    variables = [
      fitting.metaevaluators.FormulaVariable("A", "MgO:Gulp:lattice_energy"),
      fitting.metaevaluators.FormulaVariable("B", "MgO:Gulp:lattice_energy:extracted_value"),
      fitting.metaevaluators.FormulaVariable("C", "MgO:Gulp:lattice_energy:merit_value"),
      fitting.metaevaluators.FormulaVariable("D", "MgO:Gulp:lattice_energy:weight"),
      fitting.metaevaluators.FormulaVariable("E", "MgO:Gulp:lattice_energy:expected_value"),
      fitting.metaevaluators.FormulaVariable("F", "CaO:Eval1:value1"),
      fitting.metaevaluators.FormulaVariable("G", "CaO:Eval2:value1"),
      fitting.metaevaluators.FormulaVariable("H", "CaO:Eval2:value2")]
    variables = fitting.metaevaluators.FormulaMetaEvaluator._splitVariables(variables)
    expression1 = fitting.metaevaluators.Expression("Expression1", "A + B+C+D+E+F+G+H", 1.0, None)
    metaeval = fitting.metaevaluators.FormulaMetaEvaluator("Meta", [expression1], variables)
    expect1 = 123.0 + 100.0 + 123.0 + 2.0 + 1000.0 + 124.0 + 125.0 + 126.0
    actual = metaeval([mgojob, caojob])[0]
    self.assertAlmostEquals(expect1, actual.meritValue)
    self.assertEquals("Expression1", actual.name)

    expression2 = fitting.metaevaluators.Expression("Expression2", "(A+B)/2.0 - (2*(G+H+E+F))/3", 2.0, None)
    metaeval = fitting.metaevaluators.FormulaMetaEvaluator("Meta", [expression2], variables)
    expect2 = ((123.0 + 100.0)/2.0 - (2.0*(125.0+126.0+1000.0+124.0)/3.0)) * 2.0
    actual = metaeval([mgojob, caojob])[0]
    self.assertAlmostEquals(expect2, actual.meritValue)
    self.assertAlmostEquals(((123.0 + 100.0)/2.0 - (2.0*(125.0+126.0+1000.0+124.0)/3.0)), actual.extractedValue)

    metaeval = fitting.metaevaluators.FormulaMetaEvaluator("Meta", [expression1, expression2], variables)
    actual = metaeval([mgojob, caojob])
    testutil.compareCollection(self, [expect1, expect2], [er.meritValue for er in actual])
    testutil.compareCollection(self, ["Expression1", "Expression2"], [er.name for er in actual])
    testutil.compareCollection(self, ["Meta", "Meta"], [er.evaluatorName for er in actual])


    # Check RMS evaluation
    rmsexpression = fitting.metaevaluators.Expression('rmsvalue', "A+B", 1.0, 22.0)
    rmsexpression2 = fitting.metaevaluators.Expression('rmsvalue', "A+B", 0.5, 22.0)

    # import pdb;pdb.set_trace()
    metaeval = fitting.metaevaluators.FormulaMetaEvaluator("Meta", [rmsexpression, rmsexpression2], variables)
    actual = metaeval([mgojob, caojob])
    testutil.compareCollection(self, [201.0, 201.0/2.0], [er.meritValue for er in actual])
    testutil.compareCollection(self, ["rmsvalue", "rmsvalue"], [er.name for er in actual])
    testutil.compareCollection(self, ["Meta", "Meta"], [er.evaluatorName for er in actual])

    # Check bad evaluation
    badexpression = fitting.metaevaluators.Expression("BadExpression1", "J+K", 1.0, None)
    metaeval = fitting.metaevaluators.FormulaMetaEvaluator("Meta", [badexpression], variables)
    actual = metaeval([mgojob, caojob])
    self.assertEquals(1, len(actual))
    actual = actual[0]
    self.assertEquals(fitting.evaluators.ErrorEvaluatorRecord, type(actual))
    self.assertEquals("BadExpression1", actual.name)

  def testCreateFromConfig(self):
    """Test creation of FormulaMetaEvaluator from config items"""
    parser = self._parseListAsConfig(
      ["[MetaEvaluator:Sumthings]",
      "type : Formula",
      "variable_A : MgO:Gulp:lattice_energy_weight",
      "variable_B : CaO:Eval1:value1",
      "expression_summed : A + B",
      "expression_product : A*B",
      "weight_summed : 5.0"])
    cfgitems = parser.items("MetaEvaluator:Sumthings")
    evaluator = fitting.metaevaluators.FormulaMetaEvaluator.createFromConfig("Sumthings", "/a/path", cfgitems)
    self.assertEquals(fitting.metaevaluators.FormulaMetaEvaluator, type(evaluator))

    testutil.compareCollection(self, 
      [("summed", "A + B", 5.0, None),
       ("product", "A*B", 1.0, None)],
      evaluator.expressionList)


  def testCreateFromConfigWithExpect(self):
    """Test creation of FormulaMetaEvaluator from config items containing expected values"""
    parser = self._parseListAsConfig(
      ["[MetaEvaluator:Sumthings]",
      "type : Formula",
      "variable_A : MgO:Gulp:lattice_energy_weight",
      "variable_B : CaO:Eval1:value1",
      "expression_summed : A + B",
      "expression_product : 25.0=A*B",
      "expression_product2 : 2e-10 =A*B",
      "expression_product3 : 25.0 = A*B",
      "weight_summed : 5.0"])
    cfgitems = parser.items("MetaEvaluator:Sumthings")
    evaluator = fitting.metaevaluators.FormulaMetaEvaluator.createFromConfig("Sumthings", "/a/path", cfgitems)
    self.assertEquals(fitting.metaevaluators.FormulaMetaEvaluator, type(evaluator))

    testutil.compareCollection(self, 
      [("summed", "A + B", 5.0, None),
       ("product", "A*B", 1.0, 25.0),
       ("product2", "A*B", 1.0, 2e-10 ),
       ("product3", "A*B", 1.0, 25.0) ],
      evaluator.expressionList)

  def testCreateFromConfigWithBadExpect(self):
    """Test creation of FormulaMetaEvaluator from config items containing expected values with bad syntax"""
    from atomsscripts.fitting.fittool import ConfigException
    parser = self._parseListAsConfig(
      ["[MetaEvaluator:Sumthings]",
      "type : Formula",
      "variable_A : MgO:Gulp:lattice_energy_weight",
      "variable_B : CaO:Eval1:value1",
      "expression_product : =A*B",
      "weight_summed : 5.0"])
    cfgitems = parser.items("MetaEvaluator:Sumthings")
    with self.assertRaises(ConfigException):
      evaluator = fitting.metaevaluators.FormulaMetaEvaluator.createFromConfig("Sumthings", "/a/path", cfgitems)

    parser = self._parseListAsConfig(
      ["[MetaEvaluator:Sumthings]",
      "type : Formula",
      "variable_A : MgO:Gulp:lattice_energy_weight",
      "variable_B : CaO:Eval1:value1",
      "expression_product : Blah =A*B",
      "weight_summed : 5.0"])
    cfgitems = parser.items("MetaEvaluator:Sumthings")
    with self.assertRaises(ConfigException):
      evaluator = fitting.metaevaluators.FormulaMetaEvaluator.createFromConfig("Sumthings", "/a/path", cfgitems)

