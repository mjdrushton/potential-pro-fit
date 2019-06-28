import configparser
import math
import os
import shutil
import unittest

from atsim import pro_fit
from . import testutil


def _getResourceDir():
    return os.path.join(
        os.path.dirname(__file__), "resources", "table_evaluator"
    )


from tests.common import MockJobFactory

mockJobFactory = MockJobFactory("Runner", "TableJob", [])


def _compareEvaluatorRecords(testcase, expect, actual):
    def todicts(records):
        dicts = []
        for r in records:
            d = dict(
                name=r.name,
                expectedValue=r.expectedValue,
                extractedValue=r.extractedValue,
                weight=r.weight,
                evaluatorName=r.evaluatorName,
                errorFlag=r.errorFlag,
            )

            if (
                r.errorFlag
                and type(r) == pro_fit.evaluators.ErrorEvaluatorRecord
            ):
                d["error_exception"] = type(r.exception)

            dicts.append(d)
        return dicts

    testutil.compareCollection(testcase, todicts(expect), todicts(actual))


class TableEvaluatorTestCase(unittest.TestCase):
    """Tests for atsim.pro_fit.evaluators.TableEvaluator"""

    def testEndToEnd_onlysum(self):
        """Test TableEvaluator"""
        resdir = _getResourceDir()
        resdir_before = os.path.join(
            resdir, "end_to_end", "end_to_end_before_run"
        )
        resdir_after = os.path.join(
            resdir, "end_to_end", "end_to_end_after_run"
        )

        # Configure the evaluator
        parser = configparser.ConfigParser()
        parser.optionxform = str
        with open(os.path.join(resdir_before, "evaluator.cfg")) as infile:
            parser.read_file(infile)

        evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
            "Table", resdir_before, parser.items("Evaluator:Table")
        )

        job = pro_fit.jobfactories.Job(mockJobFactory, resdir_after, None)
        evaluated = evaluator(job)

        self.assertEqual(1, len(evaluated))

        rec = evaluated[0]
        self.assertAlmostEqual(0.0, rec.expectedValue)
        self.assertAlmostEqual(46.983161698072, rec.extractedValue)
        self.assertAlmostEqual(2.0 * 46.983161698072, rec.meritValue)

    def testEndToEnd_expect_value(self):
        """Test TableEvaluator with expect_value cfg option"""
        resdir = _getResourceDir()
        resdir_before = os.path.join(
            resdir, "end_to_end", "end_to_end_before_run"
        )
        resdir_after = os.path.join(
            resdir, "end_to_end", "end_to_end_after_run"
        )

        # Configure the evaluator
        parser = configparser.ConfigParser()
        parser.optionxform = str
        with open(os.path.join(resdir_before, "expect_value.cfg")) as infile:
            parser.read_file(infile)

        evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
            "Table", resdir_before, parser.items("Evaluator:Table")
        )

        job = pro_fit.jobfactories.Job(mockJobFactory, resdir_after, None)
        evaluated = evaluator(job)

        ER = pro_fit.evaluators._common.RMSEvaluatorRecord

        expectedRecords = [
            ER("row_0", 3, 10, 1.0, "Table"),
            ER("row_1", 3, 9, 1.0, "Table"),
            ER("row_2", 3, 8, 1.0, "Table"),
            ER("table_sum", 0, 7 + 6 + 5, 0.0, "Table"),
        ]

        _compareEvaluatorRecords(self, expectedRecords, evaluated)

    def testEndToEnd_expect_value_no_expect_column(self):
        """Test TableEvaluator with expect_value cfg option without expect column in expectation CSV"""
        resdir = _getResourceDir()
        resdir_before = os.path.join(
            resdir, "end_to_end", "end_to_end_before_run"
        )
        resdir_after = os.path.join(
            resdir, "end_to_end", "end_to_end_after_run"
        )

        # Configure the evaluator
        parser = configparser.ConfigParser()
        parser.optionxform = str
        with open(
            os.path.join(resdir_before, "expect_value_nocol.cfg")
        ) as infile:
            parser.read_file(infile)

        evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
            "Table", resdir_before, parser.items("Evaluator:Table")
        )

        job = pro_fit.jobfactories.Job(mockJobFactory, resdir_after, None)
        evaluated = evaluator(job)

        ER = pro_fit.evaluators._common.RMSEvaluatorRecord

        expectedRecords = [
            ER("row_0", 3, 10, 1.0, "Table"),
            ER("row_1", 3, 9, 1.0, "Table"),
            ER("row_2", 3, 8, 1.0, "Table"),
            ER("table_sum", 0, 7 + 6 + 5, 0.0, "Table"),
        ]

        _compareEvaluatorRecords(self, expectedRecords, evaluated)

    def testEndToEnd(self):
        """Test for when individual evaluator records are returned for each row"""
        resdir = _getResourceDir()
        resdir_before = os.path.join(
            resdir, "end_to_end", "end_to_end_before_run"
        )
        resdir_after = os.path.join(
            resdir, "end_to_end", "end_to_end_after_run"
        )

        # Configure the evaluator
        parser = configparser.ConfigParser()
        parser.optionxform = str
        with open(
            os.path.join(resdir_before, "evaluator_individual_rows.cfg")
        ) as infile:
            parser.read_file(infile)

        evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
            "Table", resdir_before, parser.items("Evaluator:Table")
        )

        job = pro_fit.jobfactories.Job(mockJobFactory, resdir_after, None)
        evaluated = evaluator(job)

        ER = pro_fit.evaluators._common.RMSEvaluatorRecord

        weight = 2.0
        expectedRecords = [
            ER("row_0", 0, 18.6010752377383, weight, "Table"),
            ER("row_1", 1, 14.3527000944073, weight, "Table"),
            ER("row_2", 2, 17.0293863659264, weight, "Table"),
            ER("table_sum", 0.0, weight * 46.983161698072, 0.0, "Table"),
        ]

        _compareEvaluatorRecords(self, expectedRecords, evaluated)

    def testLabelColumn(self):
        """Test label_column configuration option."""
        resdir = _getResourceDir()
        resdir_before = os.path.join(
            resdir, "label_column_and_weights", "before_run"
        )
        resdir_after = os.path.join(
            resdir, "label_column_and_weights", "after_run"
        )

        # Configure the evaluator
        parser = configparser.ConfigParser()
        parser.optionxform = str
        with open(os.path.join(resdir_before, "label_column.cfg")) as infile:
            parser.read_file(infile)

        evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
            "Table", resdir_before, parser.items("Evaluator:Table")
        )

        job = pro_fit.jobfactories.Job(mockJobFactory, resdir_after, None)
        evaluated = evaluator(job)

        ER = pro_fit.evaluators._common.RMSEvaluatorRecord

        weight = 1.0

        table_sum = sum(
            [
                math.sqrt((50.0 - (10 + 20 + 30)) ** 2.0),
                math.sqrt((110.0 - (40 + 50 + 60)) ** 2.0),
            ]
        )
        expectedRecords = [
            ER("Hello_0", 50.0, 10 + 20 + 30, weight, "Table"),
            ER("Goodbye_1", 110.0, 40 + 50 + 60, weight, "Table"),
            ER("table_sum", 0.0, table_sum, 0.0, "Table"),
        ]

        _compareEvaluatorRecords(self, expectedRecords, evaluated)

    def testWeight_Column(self):
        """Test weight_column configuration option."""
        resdir = _getResourceDir()
        resdir_before = os.path.join(
            resdir, "label_column_and_weights", "before_run"
        )
        resdir_after = os.path.join(
            resdir, "label_column_and_weights", "after_run"
        )

        # Configure the evaluator
        parser = configparser.ConfigParser()
        parser.optionxform = str
        with open(os.path.join(resdir_before, "weight_column.cfg")) as infile:
            parser.read_file(infile)

        evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
            "Table", resdir_before, parser.items("Evaluator:Table")
        )

        job = pro_fit.jobfactories.Job(mockJobFactory, resdir_after, None)
        evaluated = evaluator(job)

        ER = pro_fit.evaluators._common.RMSEvaluatorRecord

        table_sum = sum(
            [
                math.sqrt((50.0 - (10 + 20 + 30)) ** 2.0),
                math.sqrt((110.0 - (40 + 50 + 60)) ** 2.0) * 2.0,
            ]
        )
        expectedRecords = [
            ER("row_0", 50.0, 10 + 20 + 30, 1.0, "Table"),
            ER("row_1", 110.0, 40 + 50 + 60, 2.0, "Table"),
            ER("table_sum", 0.0, table_sum, 0.0, "Table"),
        ]

        _compareEvaluatorRecords(self, expectedRecords, evaluated)

    def testWeight_ColumnAndWeight(self):
        """Test weight_column together with weight configuration option."""
        resdir = _getResourceDir()
        resdir_before = os.path.join(
            resdir, "label_column_and_weights", "before_run"
        )
        resdir_after = os.path.join(
            resdir, "label_column_and_weights", "after_run"
        )

        # Configure the evaluator
        parser = configparser.ConfigParser()
        parser.optionxform = str
        with open(
            os.path.join(resdir_before, "weight_and_weight_column.cfg")
        ) as infile:
            parser.read_file(infile)

        evaluator = pro_fit.evaluators.TableEvaluator.createFromConfig(
            "Table", resdir_before, parser.items("Evaluator:Table")
        )

        job = pro_fit.jobfactories.Job(mockJobFactory, resdir_after, None)
        evaluated = evaluator(job)

        ER = pro_fit.evaluators._common.RMSEvaluatorRecord

        table_sum = sum(
            [
                math.sqrt((50.0 - (10 + 20 + 30)) ** 2.0) * 5.0,
                math.sqrt((110.0 - (40 + 50 + 60)) ** 2.0) * 2.0 * 5.0,
            ]
        )
        expectedRecords = [
            ER("row_0", 50.0, 10 + 20 + 30, 1.0 * 5.0, "Table"),
            ER("row_1", 110.0, 40 + 50 + 60, 2.0 * 5.0, "Table"),
            ER("table_sum", 0.0, table_sum, 0.0, "Table"),
        ]
        _compareEvaluatorRecords(self, expectedRecords, evaluated)


class TableEvaluatorCreateFromConfigTestCase(unittest.TestCase):
    """Test TableEvaluator methods related to TableEvaluator.createFromConfig()"""

    def testRowCompareValidation(self):
        """Check field validation for 'row_compare' configuration option."""
        import io

        # Test a malformed expression
        expression = "sqrt( (e_x - rx)^2"

        with self.assertRaises(
            pro_fit.evaluators._table.BadExpressionException
        ):
            pro_fit.evaluators.TableEvaluator._validateExpression(
                expression, io.StringIO()
            )

        # Test the case when expression references a column that is not in in the csv file.
        expression = "e_x + e_y + e_z + e_bad"

        sio = io.StringIO()
        print("x,y,z,expect", file=sio)
        print("1,2,3,0", file=sio)
        sio.seek(0)

        try:
            pro_fit.evaluators.TableEvaluator._validateExpression(
                expression, io.StringIO()
            )
            self.fail(
                "_validateExpression() did not raise UnknownVariableException"
            )
        except pro_fit.evaluators._table.UnknownVariableException as e:
            self.assertEqual(["bad"], e.unknownVariables)

        # Check when expression references column that doesn't exist in expect
        with self.assertRaises(pro_fit.fittool.ConfigException):
            pro_fit.evaluators.TableEvaluator._validateExpression(
                "e_bad + e_A", sio
            )

        # Check when non e_ and r_ variables are specified.
        sio.seek(0)
        with self.assertRaises(
            pro_fit.evaluators._table.UnknownVariableException
        ):
            pro_fit.evaluators.TableEvaluator._validateExpression(
                "e_x + e_y + r_n + really + bad", sio
            )

    def testExpectColumnValidation(self):
        """Test TableEvaluator._validateExpectColumns()"""

        expression = "(e_A+e_B) - (r_A + r_B)"

        import io

        sio = io.StringIO()
        print("A,B", file=sio)
        print("1,2", file=sio)
        sio.seek(0)

        with self.assertRaises(pro_fit.fittool.ConfigException):
            pro_fit.evaluators.TableEvaluator._validateExpectColumns(sio)

        sio.seek(0)
        pro_fit.evaluators.TableEvaluator._validateExpectColumns(
            sio, expect_value=4.0
        )
        sio.seek(0)

        #
        sio = io.StringIO()
        print("A,B,expect", file=sio)
        print("1,2,1.0", file=sio)
        sio.seek(0)

        pro_fit.evaluators.TableEvaluator._validateExpectColumns(sio)

        sio.seek(0)
        with self.assertRaises(pro_fit.fittool.ConfigException):
            pro_fit.evaluators.TableEvaluator._validateExpectColumns(
                sio, label_column="Boom"
            )

        sio.seek(0)
        pro_fit.evaluators.TableEvaluator._validateExpectColumns(
            sio, label_column="A"
        )

        sio.seek(0)
        with self.assertRaises(pro_fit.fittool.ConfigException):
            pro_fit.evaluators.TableEvaluator._validateExpectColumns(
                sio, weight_column="Boom"
            )

        sio.seek(0)
        pro_fit.evaluators.TableEvaluator._validateExpectColumns(
            sio, weight_column="A"
        )

    def testParseVariablesUnknownSymbolResolver(self):
        """Test the cexprtk unknown symbol resolver callback used to identify row_compare variables"""
        import cexprtk

        callback = pro_fit.evaluators._table._UnknownVariableResolver()
        st = cexprtk.Symbol_Table({}, add_constants=True)
        expression = "sqrt( (e_jabble + e_x - r_x)^2)"
        cexprtk.Expression(expression, st, callback)
        self.assertEqual(["e_jabble", "e_x", "r_x"], callback.variables)

        self.assertEqual(["jabble", "x"], callback.expectVariables)
        self.assertEqual(["x"], callback.resultsVariables)

        callback = pro_fit.evaluators._table._UnknownVariableResolver()
        st = cexprtk.Symbol_Table({}, add_constants=True)
        expression = "pi + blah + r_A_B  - r_E_B + sqrt(booble) + e_Z "
        cexprtk.Expression(expression, st, callback)
        self.assertEqual(
            ["blah", "booble", "e_Z", "r_A_B", "r_E_B"], callback.variables
        )
        self.assertEqual(["Z"], callback.expectVariables)
        self.assertEqual(["A_B", "E_B"], callback.resultsVariables)
        self.assertEqual(["blah", "booble"], callback.otherVariables)

    def testRowValidation(self):
        """Test row by row validation of table files"""
        from atsim.pro_fit.evaluators._table import (
            TableEvaluator,
            TableEvaluatorConfigException,
        )

        import io

        sio = io.StringIO()

        # Test that should pass
        print("A,B,C,expect", file=sio)
        print("1,2,3,4", file=sio)
        sio.seek(0)

        TableEvaluator._validateExpectRows("e_A + e_B + e_C", sio)

        # Test non-numeric value in the 'expect' column.
        sio = io.StringIO()
        print("A,B,C,expect", file=sio)
        print("1,2,3,X", file=sio)
        sio.seek(0)

        with self.assertRaises(TableEvaluatorConfigException):
            TableEvaluator._validateExpectRows("e_A + e_B + e_C", sio)

        sio = io.StringIO()
        print("A,B,C", file=sio)
        print("1,2,3", file=sio)
        sio.seek(0)
        TableEvaluator._validateExpectRows(
            "e_A + e_B + e_C", sio, expect_value=3.0
        )

        # Test that non-numeric values in fields un-used by expression passes.
        sio = io.StringIO()
        print("A,B,C,expect,weight", file=sio)
        print("1,X,X,2,1", file=sio)
        print("3,4,5,6,X", file=sio)
        print("7,8,X,9,3", file=sio)
        print("1,X,7,10,11", file=sio)
        sio.seek(0)

        with self.assertRaises(TableEvaluatorConfigException):
            TableEvaluator._validateExpectRows("e_B + 5", sio)

        # Test that bad value in e_ field used by expression fails.
        sio.seek(0)
        with self.assertRaises(TableEvaluatorConfigException):
            TableEvaluator._validateExpectRows("e_A + e_B", sio)

        sio.seek(0)
        TableEvaluator._validateExpectRows("e_A", sio)

        sio.seek(0)
        with self.assertRaises(TableEvaluatorConfigException):
            TableEvaluator._validateExpectRows(
                "e_A", sio, weight_column="weight"
            )

    def testSumOnlyValidate(self):
        """Test sum_only configuration option for TableEvaluator"""

        optionDict = {}

        self.assertEqual(
            False,
            pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict),
        )

        optionDict["sum_only"] = "false"
        self.assertEqual(
            False,
            pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict),
        )
        optionDict["sum_only"] = "False"
        self.assertEqual(
            False,
            pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict),
        )
        optionDict["sum_only"] = "true"
        self.assertEqual(
            True, pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict)
        )
        optionDict["sum_only"] = "True"
        self.assertEqual(
            True, pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict)
        )

        with self.assertRaises(
            pro_fit.evaluators._table.TableEvaluatorConfigException
        ):
            optionDict["sum_only"] = "booom"
            pro_fit.evaluators.TableEvaluator._validateSumOnly(optionDict)


class TableEvaluatorErrorConditionTestCase(unittest.TestCase):
    """Tests for TableEvaluator error conditions"""

    def testHeaderExceptions(self):
        """Test _validateExpectColumns() and _validateExpectRows() raises TableHeaderException under error conditions."""
        import io

        sio = io.StringIO()

        # Completely empty file
        with self.assertRaises(pro_fit.evaluators._table.TableHeaderException):
            pro_fit.evaluators.TableEvaluator._validateExpectColumns(sio)

        # File where header has different number of columns than body
        sio = io.StringIO()
        print("A,B,expect", file=sio)
        print("1,2,3", file=sio)
        print("2,3,4,5", file=sio)

        sio.seek(0)
        # import pdb; pdb.set_trace()
        with self.assertRaises(pro_fit.evaluators._table.TableHeaderException):
            pro_fit.evaluators.TableEvaluator._validateExpectRows("e_A", sio)

        # File where header has different number of columns than body
        sio = io.StringIO()
        print("A,B,expect", file=sio)
        print("1,2,3,4", file=sio)
        print("2,3,4", file=sio)

        sio.seek(0)
        with self.assertRaises(pro_fit.evaluators._table.TableHeaderException):
            pro_fit.evaluators.TableEvaluator._validateExpectRows("e_A", sio)

    def testMissingOutputFile_sumonly(self):
        """Test error condition when results table is missing"""

        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [{"A": "1.0", "B": "2.0", "expect": 3.0}],
            "nofile.csv",
            "r_A",
            1.0,
            0.0,
            False,
        )

        resdir = _getResourceDir()
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)
        self.assertEqual(1, len(evaluated))
        r = evaluated[0]

        # Test that error-flag is set
        self.assertEqual(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
        self.assertEqual(True, r.errorFlag)
        self.assertEqual(FileNotFoundError, type(r.exception))
        self.assertEqual("Table", r.evaluatorName)

    def testMissingOutputFile(self):
        """Test error condition when results table is missing when sum_only = False"""

        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [
                {"A": "1.0", "B": "2.0", "expect": 3.0},
                {"A": "1.0", "B": "2.0", "expect": 4.0},
            ],
            "nofile.csv",
            "r_A",
            0.0,
            1.0,
            True,
        )

        resdir = _getResourceDir()
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)

        EER = pro_fit.evaluators._common.ErrorEvaluatorRecord
        # def __init__(self, name, expectedValue, exception, weight = 1.0, evaluatorName = None ):

        weight = 1.0

        exc = FileNotFoundError("")

        expectedRecords = [
            EER("row_0", 3.0, exc, weight, "Table"),
            EER("row_1", 4.0, exc, weight, "Table"),
            EER("table_sum", 0.0, exc, 0.0, "Table"),
        ]

        _compareEvaluatorRecords(self, expectedRecords, evaluated)

    def testEmptyOutputFile_sumonly(self):
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
            os.makedirs(os.path.join("job_files", "output"))
            f = open(os.path.join("job_files", "output", "output.csv"), "w")
            f.close()

            # Perform the test
            job = pro_fit.jobfactories.Job(mockJobFactory, tempdir, None)
            evaluator = pro_fit.evaluators.TableEvaluator(
                "Table",
                [{"A": "1.0", "B": "2.0", "expect": 3.0}],
                "output.csv",
                "r_A",
                1.0,
                0.0,
                False,
            )

            evaluated = evaluator(job)
            self.assertEqual(1, len(evaluated))
            r = evaluated[0]

            self.assertEqual(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
            self.assertEqual(True, r.errorFlag)
            self.assertEqual(
                pro_fit.evaluators._table.TableHeaderException,
                type(r.exception),
            )
            self.assertEqual("Table", r.evaluatorName)

        finally:
            shutil.rmtree(tempdir, ignore_errors=True)

    def testMissingResultsColumn_sumonly(self):
        """Check error condition when results table is missing column required by row_compare expression"""
        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [{"A": "1.0", "B": "2.0", "expect": 3.0}],
            "missing_column.csv",
            "r_A",
            1.0,
            0.0,
            False,
        )

        resdir = os.path.join(_getResourceDir(), "error_conditions")
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)
        self.assertEqual(1, len(evaluated))
        r = evaluated[0]

        # Test that error-flag is set
        self.assertEqual(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
        self.assertEqual(True, r.errorFlag)
        self.assertEqual(
            pro_fit.evaluators._table.UnknownVariableException,
            type(r.exception),
        )
        self.assertEqual(["r_A"], r.exception.unknownVariables)
        self.assertEqual("r_A", r.exception.expression)
        self.assertEqual("Table", r.evaluatorName)

    def testExpectResultsDifferentLengths_ExpectLonger_sumonly(self):
        """Test correct behaviour when expect table and results table have different lengths sum_only = True"""

        # Expect longer than results
        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
            ],
            "three_rows.csv",
            "r_A",
            1.0,
            0.0,
            False,
        )

        resdir = os.path.join(_getResourceDir(), "error_conditions")
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)
        self.assertEqual(1, len(evaluated))
        r = evaluated[0]

        # Test that error-flag is set
        self.assertEqual(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
        self.assertEqual(True, r.errorFlag)
        self.assertEqual(
            pro_fit.evaluators._table.TableLengthException, type(r.exception)
        )
        self.assertEqual(False, r.exception.isResultsLonger)
        self.assertEqual("Table", r.evaluatorName)

    def testExpectResultsDifferentLengths_ExpectLonger(self):
        """Test correct behaviour when expect table and results table have different lengths"""

        # Expect longer than results
        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "4.0"},
                {"A": "1.0", "B": "2.0", "expect": "5.0"},
                {"A": "1.0", "B": "2.0", "expect": "6.0"},
            ],
            "three_rows.csv",
            "r_A",
            0.0,
            1.0,
            True,
        )

        resdir = os.path.join(_getResourceDir(), "error_conditions")
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)

        ER = pro_fit.evaluators._common.RMSEvaluatorRecord
        EER = pro_fit.evaluators._common.ErrorEvaluatorRecord

        weight = 1.0
        exc = pro_fit.evaluators._table.TableLengthException("")

        expectedRecords = [
            ER("row_0", 3, 1, weight, "Table"),
            ER("row_1", 4, 4, weight, "Table"),
            ER("row_2", 5, 7, weight, "Table"),
            EER("row_3", 6, exc, weight, "Table"),
            EER("table_sum", 0.0, exc, 0.0, "Table"),
        ]
        _compareEvaluatorRecords(self, expectedRecords, evaluated)

    def testExpectResultsDifferentLengths_ResultsLonger_sumonly(self):
        # Results longer than expect
        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
            ],
            "three_rows.csv",
            "r_A",
            1.0,
            0.0,
            False,
        )

        resdir = os.path.join(_getResourceDir(), "error_conditions")
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)
        self.assertEqual(1, len(evaluated))
        r = evaluated[0]

        # Test that error-flag is set
        self.assertEqual(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
        self.assertEqual(True, r.errorFlag)
        self.assertEqual(
            pro_fit.evaluators._table.TableLengthException, type(r.exception)
        )
        self.assertEqual(True, r.exception.isResultsLonger)
        self.assertEqual("Table", r.evaluatorName)

    def testExpectResultsDifferentLengths_ResultsLonger(self):
        # Results longer than expect
        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "4.0"},
            ],
            "three_rows.csv",
            "r_A",
            0.0,
            1.0,
            True,
        )

        resdir = os.path.join(_getResourceDir(), "error_conditions")
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)

        ER = pro_fit.evaluators._common.RMSEvaluatorRecord
        EER = pro_fit.evaluators._common.ErrorEvaluatorRecord

        weight = 1.0
        exc = pro_fit.evaluators._table.TableLengthException("")

        expectedRecords = [
            ER("row_0", 3, 1, weight, "Table"),
            ER("row_1", 4, 4, weight, "Table"),
            EER("table_sum", 0.0, exc, 0.0, "Table"),
        ]
        _compareEvaluatorRecords(self, expectedRecords, evaluated)

    def testValueError(self):
        """Check error handling when expectation and results table values cannot be converted to float"""
        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
            ],
            "bad_value.csv",
            "r_B",
            1.0,
            0.0,
            False,
        )

        resdir = os.path.join(_getResourceDir(), "error_conditions")
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)
        self.assertEqual(1, len(evaluated))
        r = evaluated[0]

        # Test that error-flag is set
        self.assertEqual(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
        self.assertEqual(True, r.errorFlag)
        self.assertEqual(ValueError, type(r.exception))
        self.assertEqual("Table", r.evaluatorName)

    def testMathDomainError(self):
        """Check correct behaviour when row_compare expression yields bad values"""
        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
            ],
            "three_rows.csv",
            "1.0/0",
            1.0,
            0.0,
            False,
        )

        resdir = os.path.join(_getResourceDir(), "error_conditions")
        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)
        self.assertEqual(1, len(evaluated))
        r = evaluated[0]

        # Test that error-flag is set
        self.assertEqual(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
        self.assertEqual(True, r.errorFlag)
        self.assertEqual(ValueError, type(r.exception))
        self.assertEqual("Table", r.evaluatorName)

        evaluator = pro_fit.evaluators.TableEvaluator(
            "Table",
            [
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
                {"A": "1.0", "B": "2.0", "expect": "3.0"},
            ],
            "three_rows.csv",
            "log(-1)",
            1.0,
            0.0,
            False,
        )

        job = pro_fit.jobfactories.Job(mockJobFactory, resdir, None)

        evaluated = evaluator(job)
        self.assertEqual(1, len(evaluated))
        r = evaluated[0]

        self.assertEqual(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
        self.assertEqual(True, r.errorFlag)
        self.assertEqual(ValueError, type(r.exception))
        self.assertEqual("Table", r.evaluatorName)


class RowComparatorTestCase(unittest.TestCase):
    """Tests for atsim.pro_fit.evaluators._table._RowComparator"""

    def testRowCompare(self):
        comparator = pro_fit.evaluators._table._RowComparator(
            "(r_x - e_x) + (r_y - e_y)"
        )
        actual = comparator.compare(
            {"label": "hello", "x": "1.0", "y": "2.1", "expect": "0.0"},
            {"label": "boom", "x": "15.0", "y": "2.2", "ignored": "5.0"},
        )
        expect = (15.0 - 1.0) + (2.2 - 2.1)
        self.assertAlmostEqual(expect, actual)

    def testPopulateSymbolTable(self):
        comparator = pro_fit.evaluators._table._RowComparator(
            "(r_x - e_x) + (r_y - e_y)"
        )

        expect = [("r_x", 0.0), ("e_x", 0.0), ("r_y", 0.0), ("e_y", 0.0)]

        testutil.compareCollection(
            self,
            sorted(expect),
            sorted(comparator._expression.symbol_table.variables.items()),
        )

        comparator._populateSymbolTableWithExpect(
            {"label": "hello", "x": "1.0", "y": "2.1", "expect": "0.0"}
        )

        expect = [("r_x", 0.0), ("e_x", 1.0), ("r_y", 0.0), ("e_y", 2.1)]

        testutil.compareCollection(
            self,
            sorted(expect),
            sorted(comparator._expression.symbol_table.variables.items()),
        )

        comparator._populateSymbolTableWithResults(
            {"label": "boom", "x": "15.0", "y": "2.2", "ignored": "5.0"}
        )

        expect = [("r_x", 15.0), ("e_x", 1.0), ("r_y", 2.2), ("e_y", 2.1)]

        testutil.compareCollection(
            self,
            sorted(expect),
            sorted(comparator._expression.symbol_table.variables.items()),
        )
