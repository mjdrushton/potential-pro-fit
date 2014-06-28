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


from tests.common import MockJobFactory
mockJobFactory = MockJobFactory('Runner', "TableJob", [])

class TableEvaluatorTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.evaluators.TableEvaluator"""

  def testEndToEnd_onlysum(self):
    """Test TableEvaluator"""
    resdir = _getResourceDir()
    resdir = os.path.join(resdir, 'end_to_end')

    # Configure the evaluator
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    with open(os.path.join(resdir, 'evaluator.cfg')) as infile:
      parser.readfp(infile)

    # import pdb;pdb.set_trace()
    evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
      'Table',
      resdir,
      parser.items('Evaluator:Table'))

    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)
    evaluated = evaluator(job)

    self.assertEqual(1, len(evaluated))

    rec = evaluated[0]
    self.assertAlmostEquals(0.0, rec.expectedValue)
    self.assertAlmostEquals(46.983161698072, rec.extractedValue)
    self.assertAlmostEquals(2.0 * 46.983161698072, rec.meritValue)

  def testEndToEnd(self):
    """Test for when individual evaluator records are returned for each row"""
    resdir = _getResourceDir()
    resdir = os.path.join(resdir, 'end_to_end')

    # Configure the evaluator
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    with open(os.path.join(resdir, 'evaluator_individual_rows.cfg')) as infile:
      parser.readfp(infile)

    evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
      'Table',
      resdir,
      parser.items('Evaluator:Table'))

    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)
    # import pdb;pdb.set_trace()
    evaluated = evaluator(job)

    ER = pro_fit.evaluators._common.RMSEvaluatorRecord
    # def __init__(self, name, expectedValue, extractedValue, weight = 1.0, evaluatorName = None):

    weight = 2.0
    expectedRecords = [
      ER('row_0'       , 0   , 18.6010752377383        , weight , "Table") ,
      ER('row_1'       , 1   , 14.3527000944073        , weight , "Table") ,
      ER('row_2'       , 2   , 17.0293863659264        , weight , "Table") ,
      ER('table_sum'   , 0.0  , weight *46.983161698072         , 0.0    , "Table") ]

    def todicts(records):
      dicts = []
      for r in records:
        d = dict(name = r.name,
          expectedValue = r.expectedValue,
          extractedValue = r.extractedValue,
          weight = r.weight,
          evaluatorName = r.evaluatorName,
          errorFlag = r.errorFlag)
        dicts.append(d)
      return dicts

    testutil.compareCollection(self,
      todicts(expectedRecords),
      todicts(evaluated))


class TableEvaluatorCreateFromConfigTestCase(unittest.TestCase):
  """Test TableEvaluator methods related to TableEvaluator.createFromConfig()"""

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

  def testSumOnlyValidate(self):
    """Test sum_only configuration option for TableEvaluator"""

    optionDict = {
    }

    self.assertEquals(False, pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict))

    optionDict['sum_only'] = 'false'
    self.assertEquals(False, pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict))
    optionDict['sum_only'] = 'False'
    self.assertEquals(False, pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict))
    optionDict['sum_only'] = 'true'
    self.assertEquals(True, pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict))
    optionDict['sum_only'] = 'True'
    self.assertEquals(True, pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict))

    with self.assertRaises(pro_fit.evaluators._table.TableEvaluatorConfigException):
      optionDict['sum_only'] = 'booom'
      pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict)


class TableEvaluatorErrorConditionTestCase(unittest.TestCase):
  """Tests for TableEvaluator error conditions"""

  def testHeaderExceptions(self):
    """Test _validateExpectColumns() and _validateExpectRows() raises TableHeaderException under error conditions."""
    import StringIO
    sio = StringIO.StringIO()

    # Completely empty file
    with self.assertRaises(pro_fit.evaluators._table.TableHeaderException):
      pro_fit.evaluators.TableEvaluator._validateExpectColumns(sio)

    # File where header has different number of columns than body
    sio = StringIO.StringIO()
    print >>sio, "A,B,expect"
    print >>sio, "1,2,3"
    print >>sio, "2,3,4,5"

    sio.seek(0)
    with self.assertRaises(pro_fit.evaluators._table.TableHeaderException):
      pro_fit.evaluators.TableEvaluator._validateExpectRows("e_A", sio)

    # File where header has different number of columns than body
    sio = StringIO.StringIO()
    print >>sio, "A,B,expect"
    print >>sio, "1,2,3,4"
    print >>sio, "2,3,4"

    sio.seek(0)
    with self.assertRaises(pro_fit.evaluators._table.TableHeaderException):
      pro_fit.evaluators.TableEvaluator._validateExpectRows("e_A", sio)

  def testMissingOutputFile(self):
    """Test error condition when results table is missing"""

    evaluator = pro_fit.evaluators.TableEvaluator("Table",
      [{"A": '1.0', "B" : '2.0', "expect": 3.0}],
      "nofile.csv",
      "r_A",
      1.0,
      0.0,
      False)

    resdir = _getResourceDir()
    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

    evaluated = evaluator(job)
    self.assertEqual(1, len(evaluated))
    r = evaluated[0]

    # Test that error-flag is set
    self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertEquals(True, r.errorFlag)
    self.assertEquals(IOError, type(r.exception))
    self.assertEquals("Table", r.evaluatorName)

  def testEmptyOutputFile(self):
    """Test error condition when results table is empty"""

    # Create empty file
    import tempfile
    import shutil
    import os
    tempdir = tempfile.mkdtemp()
    oldir = os.getcwd()
    try:
      # Create an empty file
      os.chdir(tempdir)
      os.mkdir('output')
      f = open(os.path.join('output', 'output.csv'),'w')
      f.close()

      # Perform the test
      job = pro_fit.jobfactories.Job(mockJobFactory, tempdir, None)
      evaluator = pro_fit.evaluators.TableEvaluator("Table",
            [{"A": '1.0', "B" : '2.0', "expect": 3.0}],
            "output.csv",
            "r_A",
            1.0,
            0.0,
            False)

      evaluated = evaluator(job)
      self.assertEqual(1, len(evaluated))
      r = evaluated[0]

      self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
      self.assertEquals(True, r.errorFlag)
      self.assertEquals(pro_fit.evaluators._table.TableHeaderException, type(r.exception))
      self.assertEquals("Table", r.evaluatorName)

    finally:
      shutil.rmtree(tempdir, ignore_errors = True)

  def testMissingResultsColumn(self):
    """Check error condition when results table is missing column required by row_compare expression"""
    evaluator = pro_fit.evaluators.TableEvaluator("Table",
      [{"A": '1.0', "B" : '2.0', "expect": 3.0}],
      "missing_column.csv",
      "r_A",
      1.0,
      0.0,
      False)

    resdir = os.path.join(_getResourceDir(), 'error_conditions')
    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

    evaluated = evaluator(job)
    self.assertEqual(1, len(evaluated))
    r = evaluated[0]

    # Test that error-flag is set
    self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertEquals(True, r.errorFlag)
    self.assertEquals(pro_fit.evaluators._table.UnknownVariableException, type(r.exception))
    self.assertEquals(['r_A'], r.exception.unknownVariables)
    self.assertEquals('r_A', r.exception.expression)
    self.assertEquals("Table", r.evaluatorName)

  def testExpectResultsDifferentLengths_ExpectLonger(self):
    """Test correct behaviour when expect table and results table have different lengths"""

    # Expect longer than results
    evaluator = pro_fit.evaluators.TableEvaluator("Table",
      [
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'}
      ],
      "three_rows.csv",
      "r_A",
      1.0,
      0.0,
      False)

    resdir = os.path.join(_getResourceDir(), 'error_conditions')
    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

    evaluated = evaluator(job)
    self.assertEqual(1, len(evaluated))
    r = evaluated[0]

    # Test that error-flag is set
    self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertEquals(True, r.errorFlag)
    self.assertEquals(pro_fit.evaluators._table.TableLengthException, type(r.exception))
    self.assertEquals(False, r.exception.isResultsLonger)
    self.assertEquals("Table", r.evaluatorName)

  def testExpectResultsDifferentLengths_ResultsLonger(self):
    # Results longer than expect
    evaluator = pro_fit.evaluators.TableEvaluator("Table",
      [
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
      ],
      "three_rows.csv",
      "r_A",
      1.0,
      0.0,
      False)

    resdir = os.path.join(_getResourceDir(), 'error_conditions')
    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

    evaluated = evaluator(job)
    self.assertEqual(1, len(evaluated))
    r = evaluated[0]

    # Test that error-flag is set
    self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertEquals(True, r.errorFlag)
    self.assertEquals(pro_fit.evaluators._table.TableLengthException, type(r.exception))
    self.assertEquals(True, r.exception.isResultsLonger)
    self.assertEquals("Table", r.evaluatorName)

  def testValueError(self):
    """Check error handling when expectation and results table values cannot be converted to float"""
    evaluator = pro_fit.evaluators.TableEvaluator("Table",
      [
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'}
      ],
      "bad_value.csv",
      "r_B",
      1.0,
      0.0,
      False)

    resdir = os.path.join(_getResourceDir(), 'error_conditions')
    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

    evaluated = evaluator(job)
    self.assertEqual(1, len(evaluated))
    r = evaluated[0]

    # Test that error-flag is set
    self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertEquals(True, r.errorFlag)
    self.assertEquals(ValueError, type(r.exception))
    self.assertEquals("Table", r.evaluatorName)

  def testMathDomainError(self):
    """Check correct behaviour when row_compare expression yields bad values"""
    evaluator = pro_fit.evaluators.TableEvaluator("Table",
      [
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
      ],
      "three_rows.csv",
      "1.0/0",
      1.0,
      0.0,
      False)

    resdir = os.path.join(_getResourceDir(), 'error_conditions')
    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

    evaluated = evaluator(job)
    self.assertEqual(1, len(evaluated))
    r = evaluated[0]

    # Test that error-flag is set
    self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertEquals(True, r.errorFlag)
    self.assertEquals(ValueError, type(r.exception))
    self.assertEquals("Table", r.evaluatorName)

    evaluator = pro_fit.evaluators.TableEvaluator("Table",
      [
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
        {"A": '1.0', "B" : '2.0', "expect": '3.0'},
      ],
      "three_rows.csv",
      "log(-1)",
      1.0,
      0.0,
      False)

    job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

    evaluated = evaluator(job)
    self.assertEqual(1, len(evaluated))
    r = evaluated[0]

    self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertEquals(True, r.errorFlag)
    self.assertEquals(ValueError, type(r.exception))
    self.assertEquals("Table", r.evaluatorName)


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

