from ._common import *
from atsim.pro_fit.exceptions import ConfigException
import gevent

import logging


class SpreadsheetMinimizer(object):
    """Minimizer that iteratively steps through rows of a spreadsheet, evaluating Merit function for each row.
  With each column of the spreadsheet representing a variable within pprofit. """

    _logger = logging.getLogger(
        "atsim.pro_fit.minimizers.SpreadsheetMinimizer"
    )

    def __init__(self, spreadsheetRowIterator, batchSize):
        """Create SpreadsheetMinimizer.

    :param variables: Variables instance giving run values.
    :param spreadsheetRowIterator: Iterator returning one Variables instance per spreadsheet row.
    :param int batchSize: Number of candidates to evaluate at each minimizer iteration.
    :param int rowIncrement: Increment between spreadsheet rows."""
        self._rowIterator = spreadsheetRowIterator
        self.stepCallback = None
        self.batchSize = batchSize
        self._greenlet = None

    def minimize(self, merit):
        """Perform minimization.

        :param merit: atsim.pro_fit.merit.Merit instance.
        :return: MinimizerResults containing values obtained after merit function evaluation"""
        self._greenlet = gevent.spawn(self._minimize, merit)
        self._greenlet.name = "SpreadSheet-minimizer-{}".format(
            self._greenlet.name
        )
        gevent.wait([self._greenlet])
        return self._greenlet.value

    def stopMinimizer(self):
        self._greenlet.kill()

    def _minimize(self, merit):
        minimizerResults = None
        for i, candidates in enumerate(self._batchIt()):
            self._logger.info("Minimizer iteration: %d" % i)
            meritValues, candidateJobPairs = merit.calculate(
                candidates, returnCandidateJobPairs=True
            )
            currentMinimizerResults = MinimizerResults(
                meritValues, candidateJobPairs
            )

            if not minimizerResults:
                minimizerResults = currentMinimizerResults
                self._logger.info(
                    "Iteration %d. Initial solution found, candidate: %d. %s. Merit = %f"
                    % (
                        i,
                        currentMinimizerResults.indexOfBest,
                        currentMinimizerResults.bestVariables,
                        currentMinimizerResults.bestMeritValue,
                    )
                )
            elif currentMinimizerResults < minimizerResults:
                minimizerResults = currentMinimizerResults
                self._logger.info(
                    "Iteration %d. Improved solution found, candidate: %d. %s. Merit = %f"
                    % (
                        i,
                        currentMinimizerResults.indexOfBest,
                        currentMinimizerResults.bestVariables,
                        currentMinimizerResults.bestMeritValue,
                    )
                )

            if self.stepCallback:
                self.stepCallback(
                    currentMinimizerResults
                )  # pylint: disable=not-callable
        return minimizerResults

    def _batchIt(self):
        batch = []
        for variables in self._rowIterator:
            if len(batch) == self.batchSize:
                yield batch
                batch = []
            batch.append(variables)
        if batch:
            yield batch

    @staticmethod
    def createFromConfig(variables, configitems):
        cfgdict = dict(configitems)
        del cfgdict["type"]

        allowedkeys = set(
            ["filename", "start_row", "end_row", "batch_size", "row_step"]
        )

        for k in cfgdict.keys():
            if not (k in allowedkeys):
                raise ConfigException("Unknown configuration option: '%s'" % k)

        # Extract required configuration items
        # ... filename
        try:
            filename = cfgdict["filename"]
        except KeyError:
            raise ConfigException(
                "'filename' not specified for Spreadsheet minimizer."
            )

        # ... start_row
        try:
            startRow = int(cfgdict.get("start_row", 0))
        except ValueError:
            raise ConfigException(
                "Could not convert 'start_row' value into an integer"
            )

        # ... end_row
        try:
            endRow = cfgdict.get("end_row", None)
            if endRow != None:
                endRow = int(endRow)
        except ValueError:
            raise ConfigException(
                "Could not convert 'end_row' value into an integer"
            )

        if startRow < 0:
            raise ConfigException("'start_row' should be >= 0")

        if endRow != None and endRow < 0:
            raise ConfigException("'end_row' should be >= 0")

        if endRow != None and endRow < startRow:
            raise ConfigException(
                "'end_row' should be greater than 'start_row'."
            )

        # ... batch_size
        try:
            batchSize = int(cfgdict.get("batch_size", 1))
        except ValueError:
            raise ConfigException(
                "Could not convert 'batch_size' into an integer"
            )

        # ... row_step
        try:
            rowIncrement = int(cfgdict.get("row_step", 1))
        except ValueError:
            raise ConfigException(
                "Could not convert 'row_step' into an intger"
            )

        if not rowIncrement > 0:
            raise ConfigException(
                "'row_step' should be > 0. Instead currently has value: %s"
                % rowIncrement
            )

        # Check that input file can be opened.
        try:
            with open(filename) as infile:
                pass
        except IOError as e:
            raise ConfigException(
                "Could not open spreadsheet with 'filename' '%s': %s"
                % (filename, e.strerror)
            )

        # Do a test-run through the spreadsheet
        with open(filename, "r") as infile:
            SpreadsheetMinimizer._logger.info(
                "Checking integrity of spreadsheet: '%s'" % filename
            )
            rowit = _SpreadsheetRowIterator(
                variables, infile, startRow=startRow, endRow=endRow
            )
            try:
                for row in rowit:
                    pass
            except _RowColException as rce:
                msg = (
                    "In spreadsheet: '%s', %s for col: '%s', line: %d, value = '%s'"
                    % (filename, rce, rce.columnKey, rce.lineno, rce.value)
                )
                raise ConfigException(msg)
            except _MissingColumnException as mce:
                raise ConfigException(
                    "Spreadsheet did not contain column for fitting variable named '%s'"
                    % mce.columnKey
                )
            except _RowRangeException as rre:
                raise ConfigException(str(rre))
            SpreadsheetMinimizer._logger.info(
                "Spreadsheet integrity test, passed"
            )

        # Finally, build the SpreadsheetMinimiser object
        infile = open(filename, "r")
        SpreadsheetMinimizer._logger.info(
            "Creating Spreadsheet minimizer with options:"
        )
        SpreadsheetMinimizer._logger.info("  'filename'   : %s" % filename)
        SpreadsheetMinimizer._logger.info("  'start_row'  : %s" % startRow)
        SpreadsheetMinimizer._logger.info("  'end_row'    : %s" % endRow)
        SpreadsheetMinimizer._logger.info("  'batch_size' : %s" % batchSize)
        SpreadsheetMinimizer._logger.info("  'row_step'   : %s" % rowIncrement)

        rowit = _SpreadsheetRowIterator(
            variables,
            infile,
            startRow=startRow,
            endRow=endRow,
            rowIncrement=rowIncrement,
        )

        minimizer = SpreadsheetMinimizer(rowit, batchSize)
        return minimizer


class _SpreadSheetRowIteratorException(Exception):
    """Base class for exceptions raised by _SpreadsheetRowIterator"""

    pass


class _RowColException(_SpreadSheetRowIteratorException):
    def __init__(self, msgPrefix, columnKey, value, lineno):
        super(_RowColException, self).__init__(
            "%s column '%s' on line %d: '%s'"
            % (msgPrefix, columnKey, lineno, value)
        )
        self.columnKey = columnKey
        self.value = value
        self.lineno = lineno


class _MissingColumnException(_SpreadSheetRowIteratorException):
    def __init__(self, columnKey):
        super(_MissingColumnException, self).__init__(
            "Required column missing in spreadsheet: '%s'" % columnKey
        )
        self.columnKey = columnKey


class _BadValueException(_RowColException):
    def __init__(self, columnKey, value, lineno):
        super(_BadValueException, self).__init__(
            "Value could not be converted to float", columnKey, value, lineno
        )


class _OutOfBoundsException(_RowColException):
    def __init__(self, columnKey, value, lineno):
        super(_OutOfBoundsException, self).__init__(
            "Value outside variable bounds ", columnKey, value, lineno
        )


class _RowRangeException(_SpreadSheetRowIteratorException):
    pass


class _SpreadsheetRowIterator(object):
    """Iterator class that reads CSV data and yields one row per spreadsheet row.

  Functionally, this class is similar to csv.DictReader however, the rows it returns
  are Variables instances. Only fitting variables are updated within the Variables instances
  yielded by iterator."""

    _logger = logging.getLogger(
        "atsim.pro_fit.minimizers.SpreadsheetMinimizer._SpreadsheetRowIterator"
    )

    def __init__(
        self, variables, fileObj, startRow=0, endRow=None, rowIncrement=1
    ):
        """Initialise row iterator from Variables instance and python file object containing
    delimited data.

    :param variables: Initial Variables instance.
    :param fileObj: Python file object providing the spreadsheet data.
    :param startRow: Index of first that row iteration should start.
    :param endRow: Final row for iteration or None if all rows should be used.
    :param int rowIncrement: Step size between rows."""

        self._logger.debug(
            "Creating _SpreadsheetRowIterator with initial Variable: %s"
            % variables
        )
        self.startRow = startRow
        self.endRow = endRow
        self.rowIncrement = rowIncrement
        self._iter = self._createIterator(variables, fileObj)

    def _createIterator(self, variables, fileObj):
        import csv

        dr = csv.DictReader(fileObj)

        reqkeys = variables.fitKeys

        rowidx = None
        for rowidx, row in enumerate(dr):
            self._logger.debug("Read row from spreadsheet: %s" % row)

            if rowidx < self.startRow:
                self._logger.debug(
                    "Skipping row (rowidx < startRow): %s < %s"
                    % (rowidx, self.startRow)
                )
                continue

            if (rowidx - self.startRow) % self.rowIncrement != 0:
                self._logger.debug(
                    "Skipping row. As rowidx-self.startRow (%s - %s) not divisible by self.rowIncrement): %s %% %s != 0"
                    % (
                        rowidx,
                        self.startRow,
                        rowidx - self.startRow,
                        self.rowIncrement,
                    )
                )
                continue

            if self.endRow != None and rowidx > self.endRow:
                self._logger.debug(
                    "rowidx > endRow) end of spreadsheet iteration: %s > %s"
                    % (rowidx, self.endRow)
                )
                return

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
            self._logger.debug(
                "Created variables from spreadsheet row: %s" % yieldvariables
            )
            yield yieldvariables

        if rowidx == None:
            raise _RowRangeException("spreadsheet did not contain any rows.")

        # Did we ever get to the start row? Raise exception so that validation step
        # can pick this up.
        if rowidx < self.startRow:
            raise _RowRangeException(
                "start row > number of rows in spreadsheet (%s/%d)."
                % (self.startRow, rowidx)
            )

        # Was the end row ever reached?
        # Raise exception if not so that validation step can pick this up.
        if self.endRow and rowidx < self.endRow:
            raise _RowRangeException(
                "end row > number of rows in spreadsheet (%s/%d)."
                % (self.endRow, rowidx)
            )

    def __iter__(self):
        return self._iter
