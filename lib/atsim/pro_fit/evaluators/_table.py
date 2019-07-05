from ._common import *
from atsim.pro_fit.exceptions import ConfigException
from atsim.pro_fit._util import SkipWhiteSpaceDictReader

import csv
import itertools
import logging
import math
import os

import cexprtk


class TableEvaluatorConfigException(ConfigException):
    pass


class TableHeaderException(TableEvaluatorConfigException):
    pass


class BadExpressionException(TableEvaluatorConfigException):
    pass


class UnknownVariableException(TableEvaluatorConfigException):
    def __init__(self, expression, unknownVariables, msg=None):
        if not msg:
            super().__init__(
                "Expression refers to unknown variables '%s' : %s"
                % (",".join([str(v) for v in unknownVariables]), expression)
            )
        else:
            super().__init__(msg)
        self.expression = expression
        self.unknownVariables = unknownVariables


class TableLengthException(Exception):
    def __init__(self, msg, isResultsLonger=False):
        super().__init__(msg)
        self.isResultsLonger = isResultsLonger


class TableSumException(Exception):
    pass


class TableEvaluator(object):
    """Evaluator allowing comparison between two sets of tabulated data"""

    _logger = logging.getLogger("atsim.pro_fit.evaluators.TableEvaluator")

    def __init__(
        self,
        name,
        expectCSVReader,
        resultsFilename,
        row_compare,
        sumWeight,
        rowWeight,
        recordPerRow,
        label_column=None,
        weight_column=None,
        expect_value=None,
    ):
        """Create TableEvaluator.

    :param str name: evaluator name.
    :param expectCSVReader: csv.DictReader like iterator, returning expectation table.
    :param str resultsFilename: Output filename from which simulation results are read and compared with
      expectation table.
    :param str row_compare: Expression used to perform row-by-row comparison of expected and actual results.
    :param float sumWeight: Weight applied to ``table_sum`` evaluator record.
    :param float rowWeight: Weight applied ``row_`` evaluator records.
    :param bool recordPerRow: If ``True`` call method will return one evaluator record per table
      row in addition to ``table_sum`` evaluator record.
    :param str label_column: If specified, value from expect table column named ``label_column``
      used as prefix of the name for each row's evaluator record. Record names are ``LABEL_ROWID``.
      If ``label_column`` is ``None`` then record names are ``row_ROWID``.
    :param str weight_column: If specified, then values from column named 'weight_column' in expect table
      will be used to provide row weights. Each row weight is multiplied by the ``rowWeight`` parameter.
    :param float expect_value: When specified, this value is used as 'expect' value for every row_comparison
      and the 'expect' column within the expectation table is not required."""

        self._name = name
        self._expectedTable = list(expectCSVReader)
        self._resultsFilename = resultsFilename
        self._rowCompare = _RowComparator(row_compare)
        self._sumWeight = sumWeight
        self._rowWeight = rowWeight
        self._recordPerRow = recordPerRow
        self._labelColumn = label_column
        self._weightColumn = weight_column
        self._expect_value = expect_value

    def __call__(self, job):
        resultsFilename = os.path.join(job.outputPath, self._resultsFilename)
        self._logger.debug(
            "TableEvaluator processing output file: '%s' for job '%s'"
            % (self._resultsFilename, job.name)
        )

        # Open results file and then compare rows.
        try:
            with open(resultsFilename, "r") as resultsFile:
                rowRecords = self._processRows(
                    resultsFile, job, resultsFilename
                )
        except IOError as ioe:
            self._logger.warning(
                "Table evaluator '%s' could not open results file: '%s' for job '%s'"
                % (self._name, resultsFilename, job.name)
            )
            errorRecord = ErrorEvaluatorRecord(
                "table_sum",
                0.0,
                ioe,
                weight=self._sumWeight,
                evaluatorName=self._name,
            )
            rowRecords = [errorRecord]

        rowRecords = self._makeReturnRecords(rowRecords, job)
        return rowRecords

    def _processRows(self, resultsFile, job, resultsFilename=""):
        # Iterate over rows and perform row comparison.
        resultsTable = SkipWhiteSpaceDictReader(resultsFile)

        # Is the table empty?
        if None == resultsTable.fieldnames:
            self._logger.warning(
                "Table evaluator '%s' could not find any fields in file : '%s' (indicating no header or empty file) for job '%s'"
                % (self._name, resultsFilename, job.name)
            )
            the = TableHeaderException(
                "Could not read field names from file: '%s' (indicating no header or empty file)"
                % resultsFilename
            )
            return [
                ErrorEvaluatorRecord(
                    "table_sum",
                    0.0,
                    the,
                    weight=self._sumWeight,
                    evaluatorName=self._name,
                )
            ]

        rowRecords = []
        for rowid, (expectRow, resultsRow) in enumerate(
            itertools.zip_longest(self._expectedTable, resultsTable)
        ):
            rowName = self._getRowLabel(rowid)

            # Is one of the tables longer than the other?
            errorRecord = self._handleTableLengthErrors(
                rowid, rowName, expectRow, resultsRow, job
            )
            if errorRecord:
                rowrecord = errorRecord
            else:
                # Tables are, up to now, looking fine, perform comparison.
                rowrecord = self._compareRow(
                    rowid, rowName, expectRow, resultsRow, job
                )

            self._logger.debug("Result of row comparison: %s" % rowrecord)
            rowRecords.append(rowrecord)
        return rowRecords

    def _getRowLabel(self, rowidx):
        if self._labelColumn:
            return (
                self._expectedTable[rowidx][self._labelColumn]
                + "_"
                + str(rowidx)
            )
        else:
            return "row_" + str(rowidx)

    def _getRowWeight(self, rowidx):
        if self._weightColumn:
            rowWeight = float(self._expectedTable[rowidx][self._weightColumn])
        else:
            rowWeight = 1.0
        rowWeight *= self._rowWeight
        return rowWeight

    def _getExpectValue(self, row):
        """:return float: Return row['expect'] or self._expect_value if it is set."""
        if self._expect_value != None:
            return self._expect_value
        return float(row["expect"])

    def _handleTableLengthErrors(
        self, rowid, rowName, expectRow, resultsRow, job
    ):
        # Results table longer than expect table ?
        if not expectRow:
            tle = TableLengthException(
                "Results table has more rows than expect table (current row=%d). Results filename: '%s'"
                % (rowid, self._resultsFilename),
                True,
            )
            return self._makeErrorRowRecord(tle, rowName, 0, rowid, job)

        # Expect table long than results table ?
        expect = self._getExpectValue(expectRow)
        if not resultsRow:
            # Expect table longer than results table
            tle = TableLengthException(
                "Expect table has more rows than results table (current row=%d). Results filename: '%s'"
                % (rowid, self._resultsFilename),
                False,
            )
            return self._makeErrorRowRecord(tle, rowName, expect, rowid, job)
        return None

    def _compareRow(self, rowid, rowName, expectRow, resultsRow, job):
        expect = self._getExpectValue(expectRow)
        try:
            cmpval = self._rowCompare.compare(expectRow, resultsRow)
        except ValueError as ve:
            # Some sort of math domain error or floating point conversion error..
            ve = ValueError(
                self._augmentExceptionMessage(ve, rowid, expectRow, resultsRow)
            )
            return self._makeErrorRowRecord(ve, rowName, expect, rowid, job)
        except UnknownVariableException as uve:
            uve = UnknownVariableException(
                uve.expression,
                uve.unknownVariables,
                msg=self._augmentExceptionMessage(
                    uve, rowid, expectRow, resultsRow
                ),
            )
            return self._makeErrorRowRecord(uve, rowName, expect, rowid, job)
        else:
            # No error detected so create RMSEvaluatorRecord
            return RMSEvaluatorRecord(
                rowName,
                expect,
                cmpval,
                weight=self._getRowWeight(rowid),
                evaluatorName=self._name,
            )

    def _augmentExceptionMessage(self, e, rowid, expectRow, resultsRow):
        message = str(e)
        message = (
            "%s when comparing row number %d. Expect table row: '%s'. Results table row: '%s'"
            % (message, rowid, expectRow, resultsRow)
        )
        return message

    def _makeErrorRowRecord(self, e, rowName, expect, rowid, job):
        rowrecord = ErrorEvaluatorRecord(
            rowName,
            expect,
            e,
            weight=self._getRowWeight(rowid),
            evaluatorName=self._name,
        )
        self._logger.warning(
            "Table evaluator (%s, job: %s) could not evaluate expression '%s': %s"
            % (self._name, job.name, self._rowCompare.expressionString, str(e))
        )
        return rowrecord

    def _makeReturnRecords(self, rowRecords, job):
        """Check for errors and either return ErrorEvaluatorRecord or sum of individual rows"""
        retrows = list(rowRecords)

        # If rowRecords has one entry and that is 'table_sum' then an error ocurred
        # before iteration over rows occurred in processRows(). Use the table_sum evaluator record
        # and build row_records containing the same exception.
        if (
            len(retrows) == 1
            and retrows[0].name == "table_sum"
            and retrows[0].errorFlag
        ):
            sumRecord = retrows[0]
            retrows = self._makeErrorRows(sumRecord, job)
        else:
            sumRecord = self._makeTableSumRecord(retrows)

        if self._recordPerRow:
            # Truncate list to number of rows in expect table.
            retrows = retrows[: len(self._expectedTable)]
        else:
            retrows = []
        retrows.append(sumRecord)
        return retrows

    def _makeErrorRows(self, errorRecord, job):
        retrows = []
        for i, expectRow in enumerate(self._expectedTable):
            rowName = self._getRowLabel(i)
            rowRecord = self._makeErrorRowRecord(
                errorRecord.exception,
                rowName,
                self._getExpectValue(expectRow),
                i,
                job,
            )
            retrows.append(rowRecord)
        return retrows

    def _makeTableSumRecord(self, rowRecords):
        """Make the table_sum record"""
        errorRecord = self._getFirstErrorRecord(rowRecords)
        if errorRecord:
            if not self._recordPerRow or len(rowRecords) == 1:
                errorRecord.name = "table_sum"
                errorRecord.weight = self._sumWeight
                return errorRecord
            else:
                if type(errorRecord.exception) == TableLengthException:
                    exc = errorRecord.exception
                else:
                    exc = TableSumException(
                        "Problem calculating table_sum. See individual row evaluator records for reason"
                    )
                errorRecord = ErrorEvaluatorRecord(
                    "table_sum",
                    0,
                    exc,
                    weight=self._sumWeight,
                    evaluatorName=self._name,
                )
                return errorRecord

        # Sum over the meritValues of the individual row evaluator records
        meritValue = sum([r.meritValue for r in rowRecords])
        overallRecord = EvaluatorRecord(
            "table_sum",
            0.0,  # Expected value
            meritValue,  # Extracted value
            weight=self._sumWeight,
            meritValue=self._sumWeight * meritValue,
            evaluatorName=self._name,
        )
        return overallRecord

    def _getFirstErrorRecord(self, rowRecords):
        for record in rowRecords:
            if record.errorFlag:
                return record
        return None

    @classmethod
    def createFromConfig(cls, name, jobpath, cfgitems):
        """Create TableEvaluator from job.cfg data"""

        cfgdict = dict(cfgitems)
        del cfgdict["type"]

        supportedFields = set(
            [
                "results_filename",
                "expect_filename",
                "expect_value",
                "row_compare",
                "weight",
                "weight_column",
                "sum_only",
                "label_column",
            ]
        )

        for k, v in cfgdict.items():
            if not k in supportedFields:
                raise ConfigException(
                    "unknown configuration option for Table evaluator: '%s'"
                    % k
                )

        # Get the required fields
        try:
            results_filename = cfgdict["results_filename"]
        except KeyError as e:
            raise ConfigException(
                "required field not found: 'results_filename'"
            )

        try:
            expect_filename = cfgdict["expect_filename"]
        except KeyError as e:
            raise ConfigException(
                "required field not found: 'expect_filename'"
            )

        try:
            row_compare = cfgdict["row_compare"]
        except KeyError as e:
            raise ConfigException("required field not found: 'row_compare'")

        try:
            weight = float(cfgdict.get("weight", 1.0))
        except ValueError as e:
            raise ConfigException(
                "couldn't parse 'weight' configuration option into float: ''"
                % weight
            )

        # Check that expect file exists
        if not os.path.isabs(expect_filename):
            csvfilename = os.path.join("job_files", jobpath, expect_filename)
        else:
            csvfilename = expect_filename

        sum_only = cls._validateSumOnly(cfgdict)

        try:
            with open(csvfilename, "r") as csvfile:
                label_column = cfgdict.get("label_column", None)
                weight_column = cfgdict.get("weight_column", None)
                expect_value = cfgdict.get("expect_value", None)

                if expect_value != None:
                    try:
                        expect_value = float(expect_value)
                    except ValueError:
                        raise ConfigException(
                            "Could not parse 'expect_value' into float: '%s'"
                            % expect_value
                        )

                # Check that expect file contains the columns that it should.
                cls._validateExpectColumns(
                    csvfile,
                    csvfilename,
                    label_column,
                    weight_column,
                    expect_value,
                )
                csvfile.seek(0)
                # Check that row_compare can be parsed into an expression.
                cls._validateExpression(row_compare, csvfile, csvfilename)
                csvfile.seek(0)
                # Step through the expect file, checking its integrity.
                cls._validateExpectRows(
                    row_compare,
                    csvfile,
                    csvFilename=csvfilename,
                    weight_column=weight_column,
                    expect_value=expect_value,
                )

                # TODO: Log warning if row_compare does not reference any r_ or e_ variables.

                # Now actually create the TableEvaluator
                cls._logger.debug(
                    "Creating TableEvaluator using the following settings:"
                )
                cls._logger.debug(
                    "   results_filename : '%s'" % results_filename
                )
                cls._logger.debug("   expect_filename  : '%s'" % csvfilename)
                cls._logger.debug("   row_compare      : '%s'" % row_compare)
                cls._logger.debug("   weight           : '%s'" % weight)
                cls._logger.debug("   sum_only         : '%s'" % sum_only)

                if label_column:
                    cls._logger.debug(
                        "   label_column     : '%s'" % label_column
                    )

                if weight_column:
                    cls._logger.debug(
                        "   weight_column    : '%s'" % weight_column
                    )

                if expect_value != None:
                    cls._logger.debug(
                        "   expect_value     : '%s'" % expect_value
                    )

                if sum_only:
                    sumWeight = weight
                    rowWeight = 1.0
                else:
                    sumWeight = 0.0
                    rowWeight = weight

                recordPerRow = not sum_only

                csvfile.seek(0)
                dr = SkipWhiteSpaceDictReader(csvfile)

                # Finally, create the table evaulator.
                return cls(
                    name,  # name
                    dr,  # expectCSVReader
                    results_filename,  # resultsFilename
                    row_compare,  # row_compare
                    sumWeight,  # sumWeight
                    rowWeight,  # rowWeight
                    recordPerRow,  # recordPerRow
                    label_column=label_column,
                    weight_column=weight_column,
                    expect_value=expect_value,
                )
        except IOError:
            raise ConfigException(
                "Could not open file given by 'expect_filename': %s"
                % csvfilename
            )

    @staticmethod
    def _validateExpression(expression, expectCSVFile, csvFilename=""):
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
        except cexprtk.ParseException as e:
            raise BadExpressionException(
                "Could not parse 'row_compare' expression: %s" % str(e)
            )

        # Throw when unknown variables found. Will require changes to cexprtk.
        callback = _UnknownVariableResolver()
        st = cexprtk.Symbol_Table({}, add_constants=True)
        cexprExpression = cexprtk.Expression(expression, st, callback)

        evars = callback.expectVariables

        # Check for missing e_ columns within expectCSVFile
        dr = SkipWhiteSpaceDictReader(expectCSVFile)
        if dr.fieldnames == None:
            csvFieldNames = set()
        else:
            csvFieldNames = set(dr.fieldnames)

        for evar in evars:
            if not evar in csvFieldNames:
                raise UnknownVariableException(
                    expression,
                    [evar],
                    msg="No column for variable 'e_%s' found within table file '%s' for 'row_compare' expression: %s "
                    % (evar, csvFilename, expression),
                )

        # Check for non r_ or non e_ columns
        if callback.otherVariables:
            raise UnknownVariableException(expression, callback.otherVariables)

    @staticmethod
    def _validateExpectColumns(
        expectCSVFile,
        csvfilename="",
        label_column=None,
        weight_column=None,
        expect_value=None,
    ):
        """Check that expectCSVFile contains necessary columns, based on ``expression``.

    Objects that are sub-classes of ConfigException are raised if problems are detected.

    :param file expectCSVFile: Python file object containing CSV data for expected
      values table.
    """
        dr = SkipWhiteSpaceDictReader(expectCSVFile)

        if not dr.fieldnames:
            raise TableHeaderException(
                "Could not read column names from header of table : %s "
                % csvfilename
            )

        csvFieldNames = set(dr.fieldnames)
        if expect_value == None and not "expect" in csvFieldNames:
            TableEvaluator._logger.debug(
                "'expect' column not found, 'expect_filename' columns: %s"
                % dr.fieldnames
            )
            raise TableEvaluatorConfigException(
                "File given by 'expect_filename' did no contain column named 'expect'."
            )

        if label_column and not label_column in csvFieldNames:
            TableEvaluator._logger.debug(
                "'%s' column specified by 'label_column' configuration option not found, 'expect_filename' columns: %s"
                % (label_column, dr.fieldnames)
            )
            raise TableEvaluatorConfigException(
                "File given by 'expect_filename' did no contain column named '%s' specified by 'label_column' option."
                % label_column
            )

        if weight_column and not weight_column in csvFieldNames:
            TableEvaluator._logger.debug(
                "'%s' column specified by 'weight_column' configuration option not found, 'expect_filename' columns: %s"
                % (weight_column, dr.fieldnames)
            )
            raise TableEvaluatorConfigException(
                "File given by 'expect_filename' did no contain column named '%s' specified by 'weight_column' option."
                % weight_column
            )

    @classmethod
    def _validateExpectRows(
        cls,
        expression,
        expectCSVFile,
        csvFilename="",
        weight_column=None,
        expect_value=None,
    ):
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
        if expect_value == None:
            requiredVariables.append("expect")

        if weight_column:
            requiredVariables.append(weight_column)

        cls._logger.debug("Validating rows of CSV file: %s" % csvFilename)
        cls._logger.debug(
            "Checking following keys in rows: %s" % (requiredVariables,)
        )

        dr = SkipWhiteSpaceDictReader(expectCSVFile)

        fieldnames = sorted(dr.fieldnames)
        for i, row in enumerate(dr):
            if set(fieldnames) != set(row.keys()):
                raise TableHeaderException(
                    "Number of columns for row %d of '%s' is different to that specified by header row."
                    % (i, csvFilename)
                )
            for k in requiredVariables:
                v = row[k]
                try:
                    v = float(v)
                except ValueError:
                    raise TableEvaluatorConfigException(
                        "Error converting '%s' value into float in row %d of '%s': %s"
                        % (k, i + 1, csvFilename, v)
                    )
        cls._logger.debug("Row validation completed.")

    @classmethod
    def _validateSumOnly(self, cfgdict):
        """Parse 'sum_only' option value into boolean value.

    :param dict cfgdict: Configuration options.
    :return bool: True or False depending on option being set (defaults to False)
    :raises TableEvaluatorConfigException: if value isn't 'True' or 'False'"""
        optionval = cfgdict.get("sum_only", "False")
        optionval = optionval.lower()

        try:
            return {"true": True, "false": False}[optionval]
        except KeyError:
            raise TableEvaluatorConfigException(
                "'sum_only' option can be True or False not '%s'" % optionval
            )


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
        return [
            vname.split("_", 1)[1]
            for vname in self.variables
            if vname.startswith("e_")
        ]

    @property
    def resultsVariables(self):
        """:return: variables parsed from expression that start with ``r_`` (prefix is removed)"""
        return [
            vname.split("_", 1)[1]
            for vname in self.variables
            if vname.startswith("r_")
        ]

    @property
    def otherVariables(self):
        """:return: non ``r_`` or ``e_`` prefix variables."""
        prefixes = ["r_", "e_"]

        def predicate(v):
            for prefix in prefixes:
                if v.startswith(prefix):
                    return True
            return False

        return [vname for vname in self.variables if not predicate(vname)]


class _RowComparator(object):
    """Class used to compare rows from the expect and results table by the TableEvaluator.
  Evaluation takes place using a provided row_compare expression."""

    def __init__(self, expression):
        """:param str expression: row_compare expression"""
        symbol_table = cexprtk.Symbol_Table({}, add_constants=True)
        callback = _UnknownVariableResolver()
        self.expressionString = expression
        self._expression = cexprtk.Expression(
            expression, symbol_table, callback
        )
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

        v = self._expression.value()
        if math.isinf(v):
            raise ValueError(
                "Expression: '%s' evaluated to 'inf'" % self.expressionString
            )
        elif math.isnan(v):
            raise ValueError(
                "Expression: '%s' evaluated to 'inf'" % self.expressionString
            )
        return self._expression.value()

    def _populateSymbolTableWithExpect(self, expectRow):
        for var in self._expectVariables:
            varkey = "e_" + var
            try:
                val = float(expectRow[var])
            except ValueError as ve:
                raise ValueError(
                    "for expect table variable '%s',  %s." % (varkey, ve)
                )
            except KeyError as ke:
                raise UnknownVariableException(self.expressionString, [varkey])

            self._expression.symbol_table.variables[varkey] = val

    def _populateSymbolTableWithResults(self, resultsRow):
        for var in self._resultsVariables:
            varkey = "r_" + var
            try:
                val = float(resultsRow[var])
            except ValueError as ve:
                raise ValueError(
                    "for results table variable '%s', %s." % (varkey, ve)
                )
            except KeyError as ke:
                raise UnknownVariableException(self.expressionString, [varkey])

            self._expression.symbol_table.variables[varkey] = val
