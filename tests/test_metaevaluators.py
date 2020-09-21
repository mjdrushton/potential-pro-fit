import unittest

from atsim.pro_fit import (
    metaevaluators,
    jobfactories,
    evaluators,
    exceptions,
    variables,
)
from . import testutil


class MockJob(object):
    def __init__(self, name, evaluatorRecords):
        self.name = name
        self.evaluatorRecords = evaluatorRecords


# TODO: Test errors


class FormulaMetaEvaluator(unittest.TestCase):
    def _parseListAsConfig(self, config):
        import configparser
        import os

        config = os.linesep.join(config)
        parser = configparser.ConfigParser()
        parser.optionxform = str
        import io

        sio = io.StringIO(config)
        parser.read_file(sio)
        return parser

    def testSplitVariables(self):
        variables = [
            metaevaluators.FormulaVariable("A", "MgO:Gulp:lattice_energy"),
            metaevaluators.FormulaVariable(
                "B", "MgO:Gulp:lattice_energy:extracted_value"
            ),
            metaevaluators.FormulaVariable(
                "C", "MgO:Gulp:lattice_energy:merit_value"
            ),
            metaevaluators.FormulaVariable(
                "D", "MgO:Gulp:lattice_energy:weight"
            ),
            metaevaluators.FormulaVariable(
                "E", "MgO:Gulp:lattice_energy:expected_value"
            ),
        ]

        SVK = metaevaluators.SplitVariableKey
        expect = [
            ("A", SVK("MgO", "Gulp", "lattice_energy", "merit_value")),
            ("B", SVK("MgO", "Gulp", "lattice_energy", "extracted_value")),
            ("C", SVK("MgO", "Gulp", "lattice_energy", "merit_value")),
            ("D", SVK("MgO", "Gulp", "lattice_energy", "weight")),
            ("E", SVK("MgO", "Gulp", "lattice_energy", "expected_value")),
        ]

        testutil.compareCollection(
            self,
            expect,
            metaevaluators.FormulaMetaEvaluator._splitVariables(variables),
        )

        with self.assertRaises(exceptions.ConfigException):
            metaevaluators.FormulaMetaEvaluator._splitVariables(
                [
                    metaevaluators.FormulaVariable(
                        "A", "MgO:Gulp:lattice_energy:blibble"
                    )
                ]
            )

    def testGetVariableValues(self):
        """Test metaevaluators.FormulaMetaEvaluator"""
        ER = evaluators.EvaluatorRecord

        le = ER("lattice_energy", 1000.0, 100.0, 2.0, 123.0, "MgO:Gulp")
        e1v1 = ER("value1", 2000.0, 200.0, 3.0, 124.0, "CaO:Eval1")
        e2v1 = ER("value1", 3000.0, 300.0, 4.0, 125.0, "CaO:Eval2")
        e2v2 = ER("value2", 4000.0, 400.0, 5.0, 126.0, "CaO:Eval2")

        mgojob = MockJob("MgO", [[le]])
        caojob = MockJob("CaO", [[e1v1], [e2v1, e2v2]])

        variables = [
            metaevaluators.FormulaVariable("A", "MgO:Gulp:lattice_energy"),
            metaevaluators.FormulaVariable(
                "B", "MgO:Gulp:lattice_energy:extracted_value"
            ),
            metaevaluators.FormulaVariable(
                "C", "MgO:Gulp:lattice_energy:merit_value"
            ),
            metaevaluators.FormulaVariable(
                "D", "MgO:Gulp:lattice_energy:weight"
            ),
            metaevaluators.FormulaVariable(
                "E", "MgO:Gulp:lattice_energy:expected_value"
            ),
            metaevaluators.FormulaVariable("F", "CaO:Eval1:value1"),
            metaevaluators.FormulaVariable("G", "CaO:Eval2:value1"),
            metaevaluators.FormulaVariable("H", "CaO:Eval2:value2"),
        ]
        variables = metaevaluators.FormulaMetaEvaluator._splitVariables(
            variables
        )
        metaeval = metaevaluators.FormulaMetaEvaluator(
            "Meta", "A+B", variables
        )
        variabledict = metaeval._makeVariableDict([mgojob, caojob])

        expect = {
            "A": 123.0,
            "B": 100.0,
            "C": 123.0,
            "D": 2.0,
            "E": 1000.0,
            "F": 124.0,
            "G": 125.0,
            "H": 126.0,
        }

        testutil.compareCollection(self, expect, variabledict)

    def testEvaluate(self):
        """Test that formula FormulaMetaEvaluator performs calculations correctly"""
        ER = evaluators.EvaluatorRecord

        le = ER("lattice_energy", 1000.0, 100.0, 2.0, 123.0, "MgO:Gulp")
        e1v1 = ER("value1", 2000.0, 200.0, 3.0, 124.0, "CaO:Eval1")
        e2v1 = ER("value1", 3000.0, 300.0, 4.0, 125.0, "CaO:Eval2")
        e2v2 = ER("value2", 4000.0, 400.0, 5.0, 126.0, "CaO:Eval2")

        mgojob = MockJob("MgO", [[le]])
        caojob = MockJob("CaO", [[e1v1], [e2v1, e2v2]])

        variables = [
            metaevaluators.FormulaVariable("A", "MgO:Gulp:lattice_energy"),
            metaevaluators.FormulaVariable(
                "B", "MgO:Gulp:lattice_energy:extracted_value"
            ),
            metaevaluators.FormulaVariable(
                "C", "MgO:Gulp:lattice_energy:merit_value"
            ),
            metaevaluators.FormulaVariable(
                "D", "MgO:Gulp:lattice_energy:weight"
            ),
            metaevaluators.FormulaVariable(
                "E", "MgO:Gulp:lattice_energy:expected_value"
            ),
            metaevaluators.FormulaVariable("F", "CaO:Eval1:value1"),
            metaevaluators.FormulaVariable("G", "CaO:Eval2:value1"),
            metaevaluators.FormulaVariable("H", "CaO:Eval2:value2"),
        ]
        variables = metaevaluators.FormulaMetaEvaluator._splitVariables(
            variables
        )
        expression1 = metaevaluators.Expression(
            "Expression1", "A + B+C+D+E+F+G+H", 1.0, None
        )
        metaeval = metaevaluators.FormulaMetaEvaluator(
            "Meta", [expression1], variables
        )
        expect1 = 123.0 + 100.0 + 123.0 + 2.0 + 1000.0 + 124.0 + 125.0 + 126.0
        actual = metaeval([mgojob, caojob])[0]
        self.assertAlmostEqual(expect1, actual.meritValue)
        self.assertEqual("Expression1", actual.name)

        expression2 = metaevaluators.Expression(
            "Expression2", "(A+B)/2.0 - (2*(G+H+E+F))/3", 2.0, None
        )
        metaeval = metaevaluators.FormulaMetaEvaluator(
            "Meta", [expression2], variables
        )
        expect2 = (
            (123.0 + 100.0) / 2.0
            - (2.0 * (125.0 + 126.0 + 1000.0 + 124.0) / 3.0)
        ) * 2.0
        actual = metaeval([mgojob, caojob])[0]
        self.assertAlmostEqual(expect2, actual.meritValue)
        self.assertAlmostEqual(
            (
                (123.0 + 100.0) / 2.0
                - (2.0 * (125.0 + 126.0 + 1000.0 + 124.0) / 3.0)
            ),
            actual.extractedValue,
        )

        metaeval = metaevaluators.FormulaMetaEvaluator(
            "Meta", [expression1, expression2], variables
        )
        actual = metaeval([mgojob, caojob])
        testutil.compareCollection(
            self, [expect1, expect2], [er.meritValue for er in actual]
        )
        testutil.compareCollection(
            self, ["Expression1", "Expression2"], [er.name for er in actual]
        )
        testutil.compareCollection(
            self, ["Meta", "Meta"], [er.evaluatorName for er in actual]
        )

        # Check RMS evaluation
        rmsexpression = metaevaluators.Expression("rmsvalue", "A+B", 1.0, 22.0)
        rmsexpression2 = metaevaluators.Expression(
            "rmsvalue", "A+B", 0.5, 22.0
        )

        metaeval = metaevaluators.FormulaMetaEvaluator(
            "Meta", [rmsexpression, rmsexpression2], variables
        )
        actual = metaeval([mgojob, caojob])
        testutil.compareCollection(
            self, [201.0, 201.0 / 2.0], [er.meritValue for er in actual]
        )
        testutil.compareCollection(
            self, ["rmsvalue", "rmsvalue"], [er.name for er in actual]
        )
        testutil.compareCollection(
            self, ["Meta", "Meta"], [er.evaluatorName for er in actual]
        )

        # Check bad evaluation
        badexpression = metaevaluators.Expression(
            "BadExpression1", "J+K", 1.0, None
        )
        metaeval = metaevaluators.FormulaMetaEvaluator(
            "Meta", [badexpression], variables
        )
        actual = metaeval([mgojob, caojob])
        self.assertEqual(1, len(actual))
        actual = actual[0]
        self.assertEqual(evaluators.ErrorEvaluatorRecord, type(actual))
        self.assertEqual("BadExpression1", actual.name)

    def testCreateFromConfig(self):
        """Test creation of FormulaMetaEvaluator from config items"""
        parser = self._parseListAsConfig(
            [
                "[MetaEvaluator:Sumthings]",
                "type : Formula",
                "variable_A : MgO:Gulp:lattice_energy_weight",
                "variable_B : CaO:Eval1:value1",
                "expression_summed : A + B",
                "expression_product : A*B",
                "weight_summed : 5.0",
            ]
        )
        cfgitems = parser.items("MetaEvaluator:Sumthings")
        evaluator = metaevaluators.FormulaMetaEvaluator.createFromConfig(
            "Sumthings", "/a/path", cfgitems
        )
        self.assertEqual(metaevaluators.FormulaMetaEvaluator, type(evaluator))

        testutil.compareCollection(
            self,
            [("summed", "A + B", 5.0, None), ("product", "A*B", 1.0, None)],
            evaluator.expressionList,
        )

    def testCreateFromConfigWithExpect(self):
        """Test creation of FormulaMetaEvaluator from config items containing expected values"""
        parser = self._parseListAsConfig(
            [
                "[MetaEvaluator:Sumthings]",
                "type : Formula",
                "variable_A : MgO:Gulp:lattice_energy_weight",
                "variable_B : CaO:Eval1:value1",
                "expression_summed : A + B",
                "expression_product : 25.0=A*B",
                "expression_product2 : 2e-10 =A*B",
                "expression_product3 : 25.0 = A*B",
                "weight_summed : 5.0",
            ]
        )
        cfgitems = parser.items("MetaEvaluator:Sumthings")
        evaluator = metaevaluators.FormulaMetaEvaluator.createFromConfig(
            "Sumthings", "/a/path", cfgitems
        )
        self.assertEqual(metaevaluators.FormulaMetaEvaluator, type(evaluator))

        testutil.compareCollection(
            self,
            [
                ("summed", "A + B", 5.0, None),
                ("product", "A*B", 1.0, 25.0),
                ("product2", "A*B", 1.0, 2e-10),
                ("product3", "A*B", 1.0, 25.0),
            ],
            evaluator.expressionList,
        )

    def testCreateFromConfigWithBadExpect(self):
        """Test creation of FormulaMetaEvaluator from config items containing expected values with bad syntax"""
        from atsim.pro_fit.exceptions import ConfigException

        parser = self._parseListAsConfig(
            [
                "[MetaEvaluator:Sumthings]",
                "type : Formula",
                "variable_A : MgO:Gulp:lattice_energy_weight",
                "variable_B : CaO:Eval1:value1",
                "expression_product : =A*B",
                "weight_summed : 5.0",
            ]
        )
        cfgitems = parser.items("MetaEvaluator:Sumthings")
        with self.assertRaises(ConfigException):
            metaevaluators.FormulaMetaEvaluator.createFromConfig(
                "Sumthings", "/a/path", cfgitems
            )

        parser = self._parseListAsConfig(
            [
                "[MetaEvaluator:Sumthings]",
                "type : Formula",
                "variable_A : MgO:Gulp:lattice_energy_weight",
                "variable_B : CaO:Eval1:value1",
                "expression_product : Blah =A*B",
                "weight_summed : 5.0",
            ]
        )
        cfgitems = parser.items("MetaEvaluator:Sumthings")
        with self.assertRaises(ConfigException):
            metaevaluators.FormulaMetaEvaluator.createFromConfig(
                "Sumthings", "/a/path", cfgitems
            )
