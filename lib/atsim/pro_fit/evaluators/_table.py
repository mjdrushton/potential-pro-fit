from _common import *
from atsim.pro_fit.fittool import ConfigException

import logging
import csv
import os
import itertools

import cexprtk


class TableEvaluator(object):
  """Evaluator allowing comparison between two sets of tabulated data"""

  _logger = logging.getLogger("atsim.pro_fit.evaluators.TableEvaluator")


  def __init__(self, name, expectCSVReader, resultsFilename, row_compare, weight):
    """Create TableEvaluator.

    :param str name: evaluator name.
    :param expectCSVReader: csv.DictReader like iterator, returning expectation table.
    :param str resultsFilename: Output filename from which simulation results are read and compared with
      expectation table.
    :param str row_compare: Expression used to perform row-by-row comparison of expected and actual results.
    :param float weight: Weight applied to merit value calculated by this evaluator"""

    self._name = name
    self._expectedTable = list(expectCSVReader)
    self._resultsFilename = resultsFilename
    self._rowCompare = _RowComparator(row_compare)
    self._weight = weight

  def __call__(self, job):
    resultsFilename = os.path.join(job.path, 'output', self._resultsFilename)
    self._logger.debug("TableEvaluator processing ouput file: '%s'" % self._resultsFilename)

    with open(resultsFilename) as resultsFile:
      resultsTable = csv.DictReader(resultsFile)
      rowrecords = []
      for rowid, (expectRow, resultsRow) in enumerate(itertools.izip_longest(self._expectedTable, resultsTable)):
        #TODO: If either of these comes up None then throw appropriate exception.
        cmpval = self._rowCompare.compare(expectRow, resultsRow)
        expect = float(expectRow['expect'])
        rowrecord = RMSEvaluatorRecord(
          "row_" + str(rowid),
          expect,
          cmpval,
          evaluatorName = self._name)
        self._logger.debug("Result of row comparison: %s" % rowrecord )
        rowrecords.append(rowrecord)

    # Sum over the meritValues of the individual row evaluator records
    meritValue = sum([r.meritValue for r in rowrecords])
    overallRecord =   EvaluatorRecord("table_sum",
      0.0,        # Expected value
      meritValue, # Extracted value
      weight = self._weight,
      meritValue = self._weight * meritValue,
      evaluatorName = self._name)

    self._logger.info("Result of table comparison: %s" % overallRecord)

    # TODO: Provide an option to dump individual row records

    return [overallRecord]

  @classmethod
  def createFromConfig(cls, name, jobpath, cfgitems):
    """Create TableEvaluator from job.cfg data"""

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
        cls._validateExpectColumns(csvfile)
        csvfile.seek(0)
        # Check that row_compare can be parsed into an expression.
        cls._validateExpression(row_compare, csvfile, csvfilename)
        csvfile.seek(0)
        # Step through the expect file, checking its integrity.
        cls._validateExpectRows(row_compare, csvfile, csvfilename)

        # Now actually create the TableEvaluator
        cls._logger.debug("Creating TableEvaluator using the following settings:")
        cls._logger.debug("   results_filename : '%s'" % results_filename)
        cls._logger.debug("   expect_filename  : '%s'" % csvfilename)
        cls._logger.debug("   row_compare      : '%s'" % row_compare)
        cls._logger.debug("   weight           : '%s'" % weight)

        csvfile.seek(0)
        dr = csv.DictReader(csvfile)
        return cls(name, dr, results_filename, row_compare, weight)
    except IOError:
      raise ConfigException("Could not open file given by 'expect_filename': %s" % csvfilename)

  @staticmethod
  def _validateExpression(expression, expectCSVFile, csvFilename = ""):
    """Validate ``expression`` using ``cexprtk`` and raise exceptions if error
    conditions detected.

    :param str expression: String giving cexprtk compatible ``row_compare`` expression.
    :param file expectCSVFile: Python file object containing CSV data against which
      evaluator will compare data. Here this is used to provide column and hence
      valid variable names.
    :param str csvFilename: Name of expectCSVFile (used for error and logging purposes)

    :raises BadExpressionException: if ``expression`` can't be parsed by ``cexprtk``.
    :raises UnknownVariableException: if ``expression`` contains ``e_`` prefix variables
      that can't be found in the expectCSV file or do not begin with ``r_`` (as ``r_``
      variables are taken from results file that don't exist until after job is run)."""

    try:
      cexprtk.check_expression(expression)
    except cexprtk.ParseException, e:
      raise BadExpressionException("Could not parse 'row_compare' expression: %s" % e.message)

    # Throw when unknown variables found. Will require changes to cexprtk.
    callback = _UnknownVariableResolver()
    st = cexprtk.Symbol_Table({}, add_constants = True)
    cexprExpression = cexprtk.Expression(expression, st, callback)

    evars = callback.expectVariables

    # Check for missing e_ columns within expectCSVFile
    dr = csv.DictReader(expectCSVFile)
    if dr.fieldnames == None:
      csvFieldNames = set()
    else:
      csvFieldNames = set(dr.fieldnames)

    for evar in evars:
      if not evar in csvFieldNames:
        raise UnknownVariableException(expression,
          [evar],
          msg ="No column for variable 'e_%s' found within table file '%s' for 'row_compare' expression: %s " % (evar, csvFilename, expression))

    # Check for non r_ or non e_ columns
    if callback.otherVariables:
      raise UnknownVariableException(expression, callback.otherVariables)


  @staticmethod
  def _validateExpectColumns(expectCSVFile):
    """Check that expectCSVFile contains necessary columns, based on ``expression``.

    Objects that are sub-classes of ConfigException are raised if problems are detected.

    :param file expectCSVFile: Python file object containing CSV data for expected
      values table.
    """
    dr = csv.DictReader(expectCSVFile)
    csvFieldNames = set(dr.fieldnames)
    if not 'expect' in csvFieldNames:
      TableEvaluator._logger.debug("'expect' column not found, 'expect_filename' columns: %s" % dr.fieldnames)
      raise ConfigException("File given by 'expect_filename' did no contain column named 'expect'.")

  @classmethod
  def _validateExpectRows(cls, expression, expectCSVFile, csvFilename = ""):
    """Steps through expectCSVFile and raises a TableEvaluatorConfigException if
    any value required by e_ prefix variable within ``expression`` cannot be converted
    into a float.

    :param str expression: ``row_compare`` expression.
    :param file expectCSVFile: File containing CSV table data.
    :param str csvFilename: Name of file containing CSV data (for logging and error reporting)"""

    callback = _UnknownVariableResolver()
    st = cexprtk.Symbol_Table({})
    cexprExpression = cexprtk.Expression(expression, st, callback)
    requiredVariables = callback.expectVariables
    requiredVariables.append('expect')
    cls._logger.debug("Validating rows of CSV file: %s" % csvFilename)
    cls._logger.debug("Checking following keys in rows: %s" % (requiredVariables,))

    dr = csv.DictReader(expectCSVFile)
    for i,row in enumerate(dr):
      for k in requiredVariables:
        v = row[k]
        try:
          v = float(v)
        except ValueError:
          raise TableEvaluatorConfigException(
            "Error converting '%s' value into float in row %d of '%s': %s" % (
              k, i+1, csvFilename, v ))
    cls._logger.debug("Row validation completed.")

class TableEvaluatorConfigException(ConfigException):
  pass

class BadExpressionException(TableEvaluatorConfigException):
  pass

class UnknownVariableException(TableEvaluatorConfigException):

  def __init__(self, expression, unknownVariables, msg = None):
    if not msg:
      super(TableEvaluatorConfigException,self).__init__(
        "Expression refers to unknown variables '%s' : %s" % (
          ",".join([str(v) for v in unknownVariables]),
          expression ))
    else:
      super(TableEvaluatorConfigException,self).__init__(msg)
    self.expression = expression
    self.unknownVariables = unknownVariables


class _UnknownVariableResolver(object):
  """Class used a unknown symbol resolver with cexprtk.Expression,
  unknown variable names accessible through the .variables property, after
  cexprtk.Expression has instantiated with an instance of this class"""

  def __init__(self):
    self._variables = []

  def __call__(self, symbol):
    self._variables.append(symbol)
    return (True, cexprtk.USRSymbolType.VARIABLE, 0.0, "")

  @property
  def variables(self):
    return sorted(self._variables)

  @property
  def expectVariables(self):
    """:return: variables parsed from expression that start with ``e_`` (prefix is removed)"""
    return [ vname.split("_", 1)[1] for vname in self.variables if vname.startswith("e_")]

  @property
  def resultsVariables(self):
    """:return: variables parsed from expression that start with ``r_`` (prefix is removed)"""
    return [ vname.split("_", 1)[1] for vname in self.variables if vname.startswith("r_")]

  @property
  def otherVariables(self):
    """:return: non ``r_`` or ``e_`` prefix variables."""
    prefixes = ['r_', 'e_']
    def predicate(v):
      for prefix in prefixes:
        if v.startswith(prefix):
          return True
      return False
    return [ vname for vname in self.variables if not predicate(vname)]


class _RowComparator(object):
  """Class used to compare rows from the expect and results table by the TableEvaluator.
  Evaluation takes place using a provided row_compare expression."""

  def __init__(self, expression):
    """:param str expression: row_compare expression"""
    symbol_table = cexprtk.Symbol_Table({}, add_constants = True)
    callback = _UnknownVariableResolver()
    self._expression = cexprtk.Expression(expression, symbol_table, callback)
    self._expectVariables = callback.expectVariables
    self._resultsVariables = callback.resultsVariables


  def compare(self, expectRow, resultsRow):
    """Return float giving result of comparison between expectRow and resultsRow.
    Comparison is made using the expression passed to the _RowComparator constructor.

    :param dict expectRow: Row extracted from expectation table. Keys give variable names
      and dict values, the variable values.
    :param dict resultsRow: Row extracted from results table.

    :return float: Number giving result of expression evaluation"""

    self._populateSymbolTableWithExpect(expectRow)
    self._populateSymbolTableWithResults(resultsRow)

    return self._expression.value()

  def _populateSymbolTableWithExpect(self, expectRow):
    #TODO: raise appropriate exception if resultsRow doesn't contain required variable.
    for var in self._expectVariables:
      varkey = "e_"+var
      #TODO: raise appropriate exception if value cannot be converted to a float.
      self._expression.symbol_table.variables[varkey] = float(expectRow[var])

  def _populateSymbolTableWithResults(self, resultsRow):
    #TODO: raise appropriate exception if resultsRow doesn't contain required variable.
    for var in self._resultsVariables:
      varkey = "r_"+var
      #TODO: raise appropriate exception if value cannot be converted to a float.
      self._expression.symbol_table.variables[varkey] = float(resultsRow[var])

