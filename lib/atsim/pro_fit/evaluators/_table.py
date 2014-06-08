from _common import *
from atsim.pro_fit.fittool import ConfigException

import logging
import csv
import os

import cexprtk


class TableEvaluator(object):
  """Evaluator allowing comparison between two sets of tabulated data"""

  _logger = logging.getLogger("atsim.pro_fit.evaluators.TableEvaluator")

  @staticmethod
  def createFromConfig(name, jobpath, cfgitems):

    cfgdict = dict(cfgitems)
    del cfgdict['type']

    supportedFields = set([
      'results_filename',
      'expect_filename',
      'row_compare',
      'weight'])

    for k,v in cfgdict.iteritems():
      if not k in supportedFields:
        raise ConfigException("unknown configuration option for Table evaluator: '%s'" % k)

    # Get the required fields
    try:
      results_filename = cfgdict['results_filename']
    except KeyError, e:
      raise ConfigException("required field not found: 'results_filename'")

    try:
      expect_filename = cfgdict['expect_filename']
    except KeyError, e:
      raise ConfigException("required field not found: 'expect_filename'")

    try:
      row_compare = cfgdict['row_compare']
    except KeyError, e:
      raise ConfigException("required field not found: 'row_compare'")


    try:
      weight = float(cfgdict.get('weight', 1.0))
    except ValueError, e:
      raise ConfigException("couldn't parse 'weight' configuration option into float: ''" % weight)

    # Check that expect file exists
    if not os.path.isabs(expect_filename):
      csvfilename = os.path.join(jobpath, expect_filename)
    else:
      csvfilename = expect_filename

    try:
      with open(csvfilename, 'rUb') as csvfile:
      # Check that expect file contains the columns that it should.
        TableEvaluator._validateExpectColumns(csvfile)
    except IOError:
      raise ConfigException("Could not open file given by 'expect_filename': %s" % csvfilename)

    # Check that row_compare can be parsed into an expression.
    with open(csvfilename, 'rUb') as csvfile:
      TableEvaluator._validateExpression(row_compare, csvfile)


    # TODO: Step through the expect file, checking its integrity. (needs modifications to cexprtk)

    # TODO: Check that row_compare doesn't reference columns within expect file that it shouldn't. (needs modifications to cexprtk)

    TableEvaluator._logger.debug("Creating TableEvaluator using the following settings:")
    TableEvaluator._logger.debug("   results_filename : '%s'" % results_filename)
    TableEvaluator._logger.debug("   expect_filename  : '%s'" % csvfilename)
    TableEvaluator._logger.debug("   row_compare      : '%s'" % row_compare)
    TableEvaluator._logger.debug("   weight           : '%s'" % weight)


  @staticmethod
  def _validateExpression(expression, expectCSVFile):
    """Validate ``expression`` using ``cexprtk`` and raise exceptions if error
    conditions detected.

    :param expression: String giving cexprtk compatible ``row_compare`` expression.
    :param expectCSVFile: Python file object containing CSV data against which
      evaluator will compare data. Here this is used to provide column and hence
      valid variable names.

    :raises BadExpressionException: if ``expression`` can't be parsed by ``cexprtk``.
    :raises UnknownVariableException: if ``expression`` contains variables that can't be
      found in the expectCSV file or do not begin with ``r_`` (as ``r_`` variables are
      taken from results file that don't exist until after job is run)."""

    try:
      cexprtk.check_expression(expression)
    except cexprtk.ParseException, e:
      raise BadExpressionException("Could not parse 'row_compare' expression: %s" % e.message)

    #TODO: Raise when unknown variables found. Will require changes to cexprtk.

  @staticmethod
  def _validateExpectColumns(expectCSVFile):
    """Check that expectCSVFile contains necessary columns, based on ``expression``.

    Objects that are sub-classes of ConfigException are raised if problems are detected.

    :param file expectCSVFile: Python file object containing CSV data for expected values table."""

    dr = csv.DictReader(expectCSVFile)
    if not 'expect' in dr.fieldnames:
      TableEvaluator._logger.debug("'expect' column not found, 'expect_filename' columns: %s" % dr.fieldnames)
      raise ConfigException("File given by 'expect_filename' did no contain column named 'expect'.")


class TableEvaluatorConfigException(ConfigException):
  pass

class BadExpressionException(TableEvaluatorConfigException):
  pass

class UnknownVariableException(TableEvaluatorConfigException):

  def __init__(self, expression, unknownVariables):
    super(TableEvaluatorConfigException,self).__init__(
      "Expression refers to unknown variables '%s' : %s" % (
        ",".join([str(v) for v in unknownVariables]),
        expression ))
    self.expression = expression
    self.unknownVariables = unknownVariables

