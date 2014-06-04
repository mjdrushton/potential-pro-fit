from _common import *

import logging

class SpreadsheetMinimizer(object):
  """Minimizer that iteratively steps through rows of a spreadsheet, evaluating Merit function for each row.
  With each column of the spreadsheet representing a variable within pprofit. """

  _logger = logging.getLogger('atsim.pro_fit.minimizers.SpreadsheetMinimizer')

  def __init__(self, spreadsheetRowIterator):
    """Create SpreadsheetMinimizer.

    :param variables: Variables instance giving run values.
    :param spreadsheetRowIterator: Iterator returning one Variables instance per spreadsheet row."""
    self._rowIterator = spreadsheetRowIterator
    self.stepCallback = None

  def minimize(self, merit):
    """Perform minimization.

    :param merit: atsim.pro_fit.fittool.Merit instance.
    :return: MinimizerResults containing values obtained after merit function evaluation"""

    minimizerResults = None

    for i,variables in enumerate(self._rowIterator):
      self._logger.info("Minimizer iteration: %d" % i)
      candidates = [variables]
      meritValues, candidateJobPairs = merit.calculate(
        candidates,
        returnCandidateJobPairs = True)
      currentMinimizerResults = MinimizerResults(meritValues, candidateJobPairs)
      
      if not minimizerResults:
        minimizerResults = currentMinimizerResults
        self._logger.info("Iteration %d. Initial solution found, candidate: %d. %s. Merit = %f" % (
          i,
          currentMinimizerResults.indexOfBest,
          currentMinimizerResults.bestVariables,
          currentMinimizerResults.bestMeritValue))
      elif currentMinimizerResults < minimizerResults:
        minimizerResults = currentMinimizerResults
        self._logger.info("Iteration %d. Improved solution found, candidate: %d. %s. Merit = %f" % (
          i,
          currentMinimizerResults.indexOfBest,
          currentMinimizerResults.bestVariables,
          currentMinimizerResults.bestMeritValue))
      
      if self.stepCallback:
        self.stepCallback(currentMinimizerResults)
  

    return minimizerResults

  @staticmethod
  def createFromConfig(variables, configitems):
    cfgdict = dict(configitems)
    del cfgdict['type']

    allowedkeys = set(['filename'])

    for k in cfgdict.iterkeys():
      if not (k in allowedkeys):
        raise ConfigException("Unknown configuration option: '%s'" % k)

    # Extract required configuration items
    # ... filename
    filename = cfgdict['filename']

    try:
      with open(filename) as infile:
        pass
    except IOError as e:
      raise ConfigException("Could not open spreadsheet with 'filename' '%s': %s" % (filename, e.strerror))

    # Do a test-run through the spreadsheet
    with open(filename, 'rUb') as infile:
      SpreadsheetMinimizer._logger.info("Checking integrity of spreadsheet: '%s'" % filename)
      rowit = _SpreadsheetRowIterator(variables, infile)

      try:
        for row in rowit:
          pass
      except _RowColException as rce:
        msg = "In spreadsheet: '%s', %s for col: '%s', line: %d, value = '%s'" % (filename, rce.message, rce.columnKey, rce.lineno)
        raise ConfigException(msg)
      except  _MissingColumnException as mce:
        raise ConfigException("Spreadsheet did not contain column for fitting variable named '%s'" % mce.columnKey)

      SpreadsheetMinimizer._logger.info("Spreadsheet integrity test, passed")

    infile = open(filename, 'rUb')
    SpreadsheetMinimizer._logger.info("Creating Spreadsheet minimizer with options:")
    SpreadsheetMinimizer._logger.info("  'filename' : %s" % filename)

    rowit = _SpreadsheetRowIterator(variables, infile)
    minimizer = SpreadsheetMinimizer(rowit)
    return minimizer


class _RowColException(Exception):
  def __init__(self, msgPrefix, columnKey, value, lineno):
    super(_RowColException, self).__init__(
      "%s column '%s' on line %d: '%s'" % (msgPrefix, columnKey, lineno, value))
    self.columnKey = columnKey
    self.value = value
    self.lineno = lineno

class _MissingColumnException(Exception):
  def __init__(self, columnKey):
    super(_MissingColumnException, self).__init__("Required column missing in spreadsheet: '%s'" % columnKey )
    self.columnKey = columnKey


class _BadValueException(_RowColException):
  def __init__(self, columnKey, value, lineno):
    super(_BadValueException, self).__init__(
      "Value could not be converted to float", columnKey, value, lineno)


class _OutOfBoundsException(_RowColException):
  def __init__(self, columnKey, value, lineno):
    super(_OutOfBoundsException, self).__init__(
      "Value outside variable bounds ", columnKey, value, lineno)


class _SpreadsheetRowIterator(object):
  """Iterator class that reads CSV data and yields one row per spreadsheet row.

  Functionally, this class is similar to csv.DictReader however, the rows it returns
  are Variables instances. Only fitting variables are updated within the Variables instances
  yielded by iterator."""

  _logger = logging.getLogger('atsim.pro_fit.minimizers.SpreadsheetMinimizer._SpreadsheetRowIterator')

  def __init__(self, variables, fileObj):
    """Initialise row iterator from Variables instance and python file object containing
    delimited data.

    :param variables: Initial Variables instance.
    :param fileObj: Python file object providing the spreadsheet data."""

    self._logger.debug("Creating _SpreadsheetRowIterator with initial Variable: %s"  % variables)
    self._iter = self._createIterator(variables, fileObj)

  def _createIterator(self, variables, fileObj):
    import csv
    dr = csv.DictReader(fileObj)

    reqkeys = variables.fitKeys

    for rowidx,row in enumerate(dr):
      self._logger.debug("Read row from spreadsheet: %s" % row)
      lineno = rowidx + 2
      updatevals = []
      for k in reqkeys:
        try:
          v = row[k]
        except KeyError:
          raise _MissingColumnException(k)

        try:
          v = float(v)
        except ValueError:
          raise _BadValueException(k, v, lineno)

        if not variables.inBounds(k, v):
          raise _OutOfBoundsException(k, v, lineno)

        updatevals.append(v)
      yieldvariables = variables.createUpdated(updatevals)
      self._logger.debug("Created variables from spreadsheet row: %s" % yieldvariables)
      yield yieldvariables


  def __iter__(self):
    return self._iter

