import unittest
import ConfigParser

import os
import shutil

from atsim import pro_fit
import testutil

def _getResourceDir():
  return os.path.join(
      os.path.dirname(__file__),
      'resources',
      'table_evaluator')


class TableEvaluatorTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.evaluators.TableEvaluator"""


  def testEndToEnd(self):
    """Test extraction and comparison of single row"""
    resdir = _getResourceDir()
    resdir = os.path.join(resdir, 'end_to_end')

    # Configure the evaluator
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    with open(os.path.join(resdir, 'evaluator.cfg')) as infile:
      parser.readfp(infile)

    evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
      'Table',
      resdir,
      parser.items('Evaluator:Table'))

    job = pro_fit.jobfactories.Job(None, resdir, None)
    # import pdb;pdb.set_trace()
    evaluated = evaluator(job)

    self.assertEqual(1, len(evaluated))

    rec = evaluated[0]
    self.assertAlmostEquals(0.0, rec.expectedValue)
    self.assertAlmostEquals(46.983161698072, rec.extractedValue)
    self.assertAlmostEquals(2.0 * 46.983161698072, rec.meritValue)

  def testRowCompareValidation(self):
    """Check field validation for 'row_compare' configuration option."""
    import StringIO
    # Test a malformed expression
    expression = "sqrt( (e_x - rx)^2"

    with self.assertRaises(pro_fit.evaluators._table.BadExpressionException):
      pro_fit.evaluators.TableEvaluator._validateExpression(expression, StringIO.StringIO())

    # Test the case when expression references a column that is not in in the csv file.
    expression = "e_x + e_y + e_z + e_bad"

    sio = StringIO.StringIO()
    print >>sio, "x,y,z,expect"
    print >>sio, "1,2,3,0"
    sio.seek(0)

    try:
      pro_fit.evaluators.TableEvaluator._validateExpression(expression, StringIO.StringIO())
      self.fail("_validateExpression() did not raise UnknownVariableException")
    except pro_fit.evaluators._table.UnknownVariableException, e:
      self.assertEqual(["bad"], e.unknownVariables)

    # Check when expression references column that doesn't exist in expect
    with self.assertRaises(pro_fit.fittool.ConfigException):
      pro_fit.evaluators.TableEvaluator._validateExpression("e_bad + e_A", sio)

    # Check when non e_ and r_ variables are specified.
    sio.seek(0)
    with self.assertRaises(pro_fit.evaluators._table.UnknownVariableException):
      pro_fit.evaluators.TableEvaluator._validateExpression("e_x + e_y + r_n + really + bad", sio)

  def testExpectColumnValidation(self):
    """Test TableEvaluator._validateExpectColumns()"""

    expression = "(e_A+e_B) - (r_A + r_B)"

    import StringIO
    sio = StringIO.StringIO()
    print >>sio, "A,B"
    print >>sio, "1,2"
    sio.seek(0)

    with self.assertRaises(pro_fit.fittool.ConfigException):
      pro_fit.evaluators.TableEvaluator._validateExpectColumns(sio)

    #
    sio = StringIO.StringIO()
    print >>sio, "A,B,expect"
    print >>sio, "1,2,1.0"
    sio.seek(0)

    pro_fit.evaluators.TableEvaluator._validateExpectColumns(sio)


  def testParseVariablesUnknownSymbolResolver(self):
    """Test the cexprtk unknown symbol resolver callback used to identify row_compare variables"""
    import cexprtk
    callback = pro_fit.evaluators._table._UnknownVariableResolver()
    st = cexprtk.Symbol_Table({}, add_constants = True)
    expression = "sqrt( (e_jabble + e_x - r_x)^2)"
    cexprtk.Expression(expression, st, callback)
    self.assertEquals(["e_jabble", "e_x", "r_x"], callback.variables)

    self.assertEquals(["jabble", "x"], callback.expectVariables)
    self.assertEquals(["x"], callback.resultsVariables)

    callback = pro_fit.evaluators._table._UnknownVariableResolver()
    st = cexprtk.Symbol_Table({}, add_constants = True)
    expression = "pi + blah + r_A_B  - r_E_B + sqrt(booble) + e_Z "
    cexprtk.Expression(expression, st, callback)
    self.assertEquals(["blah", "booble", "e_Z", "r_A_B", "r_E_B"], callback.variables)
    self.assertEquals(["Z"], callback.expectVariables)
    self.assertEquals(["A_B", "E_B"], callback.resultsVariables)
    self.assertEquals(["blah", "booble"], callback.otherVariables)

  def testRowValidation(self):
    """Test row by row validation of table files"""
    from atsim.pro_fit.evaluators._table import TableEvaluator, TableEvaluatorConfigException

    import StringIO
    sio = StringIO.StringIO()

    # Test that should pass
    print >> sio, "A,B,C,expect"
    print >> sio, "1,2,3,4"
    sio.seek(0)

    TableEvaluator._validateExpectRows("e_A + e_B + e_C", sio)

    # Test non-numeric value in the 'expect' column.
    sio = StringIO.StringIO()
    print >> sio, "A,B,C,expect"
    print >> sio, "1,2,3,X"
    sio.seek(0)

    with self.assertRaises(TableEvaluatorConfigException):
      TableEvaluator._validateExpectRows("e_A + e_B + e_C", sio)

    # Test that non-numeric values in fields un-used by expression passes.
    sio = StringIO.StringIO()
    print >> sio, "A,B,C,expect"
    print >> sio, "1,X,X,2"
    print >> sio, "3,4,5,6"
    print >> sio, "7,8,X,9"
    print >> sio, "1,X,7,10"
    sio.seek(0)

    with self.assertRaises(TableEvaluatorConfigException):
      TableEvaluator._validateExpectRows("e_B + 5", sio)

    # Test that bad value in e_ field used by expression fails.
    sio.seek(0)
    with self.assertRaises(TableEvaluatorConfigException):
      TableEvaluator._validateExpectRows("e_A + e_B", sio)


class RowComparatorTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.evaluators._table._RowComparator"""

  def testRowCompare(self):
    comparator = pro_fit.evaluators._table._RowComparator("(r_x - e_x) + (r_y - e_y)")
    actual = comparator.compare(
      {'label' : 'hello', 'x' : '1.0', 'y' : '2.1', 'expect' : '0.0'},
      {'label' : 'boom', 'x' : '15.0', 'y' : '2.2', 'ignored' : '5.0'})
    expect = (15.0 - 1.0) + (2.2 - 2.1)
    self.assertAlmostEquals(expect, actual)

  def testPopulateSymbolTable(self):
    comparator = pro_fit.evaluators._table._RowComparator("(r_x - e_x) + (r_y - e_y)")

    expect = [
      ('r_x', 0.0),
      ('e_x', 0.0),
      ('r_y', 0.0),
      ('e_y', 0.0)]

    testutil.compareCollection(self,
      sorted(expect),
      sorted(comparator._expression.symbol_table.variables.items()))

    comparator._populateSymbolTableWithExpect({'label' : 'hello', 'x' : '1.0', 'y' : '2.1', 'expect' : '0.0'})

    expect = [
      ('r_x', 0.0),
      ('e_x', 1.0),
      ('r_y', 0.0),
      ('e_y', 2.1)]

    testutil.compareCollection(self,
      sorted(expect),
      sorted(comparator._expression.symbol_table.variables.items()))

    comparator._populateSymbolTableWithResults({'label' : 'boom', 'x' : '15.0', 'y' : '2.2', 'ignored' : '5.0'})

    expect = [
      ('r_x', 15.0),
      ('e_x', 1.0),
      ('r_y', 2.2),
      ('e_y', 2.1)]

    testutil.compareCollection(self,
      sorted(expect),
      sorted(comparator._expression.symbol_table.variables.items()))

