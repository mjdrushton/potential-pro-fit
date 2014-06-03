from _common import *

import logging

class SpreadsheetMinimizer(object):
  """Minimizer that iteratively steps through rows of a spreadsheet, evaluating Merit function for each row.
  With each column of the spreadsheet representing a variable within pprofit. """

  _logger = logging.getLogger('atsim.pro_fit.minimizers.SpreadsheetMinimizer')

  def __init__(self, variables, spreadsheetRowIterator):
    """Create SpreadsheetMinimizer.

    :param variables: Variables instance giving run values.
    :param spreadsheetRowIterator: Iterator returning one Variables instance per spreadsheet row."""
    pass

  def minimize(self, merit):
    """Perform minimization.

    :param merit: atsim.pro_fit.fittool.Merit instance.
    :return: MinimizerResults containing values obtained after merit function evaluation"""
    pass

  @staticmethod
  def createFromConfig(variables, configitems):
    pass


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

