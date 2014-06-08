import unittest
import ConfigParser

import os
import shutil

from atsim import pro_fit

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

  def testExpectColumnValidation(self):
    """Test TableEvaluator._validateExpectColumns()"""

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





