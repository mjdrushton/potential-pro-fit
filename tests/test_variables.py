import unittest

from . import testutil

import atsim.pro_fit.variables

import configparser


class VariablesTestCase(unittest.TestCase):
    """Tests pro_fit.variables.Variables class"""

    def testFlaggedVariablePairs(self):
        """Test flaggedVariablePairs property of Variables class"""
        expect = [
            ("A", 1.0, False),
            ("B", 2.0, True),
            ("C", 3.0, False),
            ("D", 4.0, True),
            ("E", 5.0, True),
        ]

        v = atsim.pro_fit.variables.Variables(expect)
        actual = v.flaggedVariablePairs
        testutil.compareCollection(self, expect, actual)

    def testCreateUpdated(self):
        """Ensure correct behaviour of atsim.pro_fit.variables.Variables.createUpdated()"""
        initialVariables = atsim.pro_fit.variables.Variables(
            [("A", 1.0, False), ("B", 2.0, True), ("C", 3.0, True)]
        )

        candidate1 = initialVariables.createUpdated()
        candidate2 = initialVariables.createUpdated([5.0, 6.0])

        testutil.compareCollection(
            self,
            [("A", 1.0), ("B", 2.0), ("C", 3.0)],
            initialVariables.variablePairs,
        )
        self.assertEqual(["B", "C"], initialVariables.fitKeys)
        self.assertEqual([2.0, 3.0], initialVariables.fitValues)

        testutil.compareCollection(
            self,
            [("A", 1.0), ("B", 2.0), ("C", 3.0)],
            candidate1.variablePairs,
        )
        self.assertEqual(["B", "C"], candidate1.fitKeys)
        self.assertEqual([2.0, 3.0], candidate1.fitValues)

        testutil.compareCollection(
            self,
            [("A", 1.0), ("B", 2.0), ("C", 3.0)],
            candidate1.variablePairs,
        )
        self.assertEqual(["B", "C"], candidate1.fitKeys)
        self.assertEqual([2.0, 3.0], candidate1.fitValues)

        testutil.compareCollection(
            self,
            [("A", 1.0), ("B", 5.0), ("C", 6.0)],
            candidate2.variablePairs,
        )
        self.assertEqual(["B", "C"], candidate2.fitKeys)
        self.assertEqual([5.0, 6.0], candidate2.fitValues)

    def testFitVariableBounds(self):
        """Test that Variables class will return a filtered list of bounds for fitting variables"""

        varlist = [
            ("A", 1.0, False),
            ("B", 2.0, True),
            ("C", 3.0, False),
            ("D", 4.0, True),
            ("E", 5.0, True),
        ]

        bounds = [None, (1.0, 3.0), (2.0, 4.0), None, (5.0, 6.0)]

        v = atsim.pro_fit.variables.Variables(varlist, bounds)
        # unbound = (float("-inf"), float("inf"))
        unbound = None
        expect_bounds = [unbound, (1.0, 3.0), (2.0, 4.0), unbound, (5.0, 6.0)]

        assert expect_bounds == v.bounds

        expect_bounds = [(1.0, 3.0), None, (5.0, 6.0)]

        assert expect_bounds == v.fitBounds


class CalculatedVariables(unittest.TestCase):
    """Tests for synthetic variables"""

    def testNoExpressions(self):
        """Test that variables pass through CalculatedVariables unchanged when no expressions specified"""
        variables = atsim.pro_fit.variables.Variables(
            [
                ("A", 1.23, False),
                ("B", 4.56, False),
                ("electroneg", 0.4, True),
            ],
            bounds=[None, None, (0, 1)],
        )

        calculatedVariables = atsim.pro_fit.variables.CalculatedVariables([])
        outVars = calculatedVariables(variables)

        expect = [
            ("A", 1.23, False),
            ("B", 4.56, False),
            ("electroneg", 0.4, True),
        ]

        testutil.compareCollection(self, expect, outVars.flaggedVariablePairs)

    def testCalculatedVariables(self):
        """Test the creation and calculation of synthetic variables"""
        # import pudb;pudb.set_trace()
        expression1 = "5 + 6 + 8"
        expression2 = "-electroneg * 2"
        expression3 = "electroneg * 4"

        variables = atsim.pro_fit.variables.Variables(
            [
                ("A", 1.23, False),
                ("B", 4.56, False),
                ("electroneg", 0.4, True),
            ],
            bounds=[None, None, (0, 1)],
        )

        calculatedVariables = atsim.pro_fit.variables.CalculatedVariables(
            [
                ("sum", expression1),
                ("Ocharge", expression2),
                ("Ucharge", expression3),
            ]
        )

        outVars = calculatedVariables(variables)

        expect = [
            ("A", 1.23, False),
            ("B", 4.56, False),
            ("electroneg", 0.4, True),
            ("sum", 19, False),
            ("Ocharge", -0.4 * 2, False),
            ("Ucharge", 0.4 * 4, False),
        ]

        testutil.compareCollection(self, expect, outVars.flaggedVariablePairs)

        # Check bounds
        expect = [None, None, (0, 1), None, None, None]

        testutil.compareCollection(self, expect, outVars.bounds)

    def testCreateFromConfig(self):
        """Test creation of CalculatedVariables from [CalculatedVariables] configuration directives"""

        config = """[CalculatedVariables]
sum : 5+6+8
Ocharge : -electroneg * 2
Ucharge : electroneg * 4
"""

        import io

        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read_file(io.StringIO(config))
        configitems = cfg.items("CalculatedVariables")

        variables = atsim.pro_fit.variables.Variables(
            [
                ("A", 1.23, False),
                ("B", 4.56, False),
                ("electroneg", 0.4, True),
            ],
            bounds=[None, None, (0, 1)],
        )

        calculatedVariables = atsim.pro_fit.variables.CalculatedVariables.createFromConfig(
            cfg.items("CalculatedVariables")
        )
        outVars = calculatedVariables(variables)

        expect = [
            ("A", 1.23, False),
            ("B", 4.56, False),
            ("electroneg", 0.4, True),
            ("sum", 19, False),
            ("Ocharge", -0.4 * 2, False),
            ("Ucharge", 0.4 * 4, False),
        ]

        testutil.compareCollection(self, expect, outVars.flaggedVariablePairs)

        # Now check that an exception is thrown if a bad expression is used.
        config = """[CalculatedVariables]
sum : abs(5+6+8
Ocharge : -electroneg * 2
Ucharge : electroneg * 4
"""

        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read_file(io.StringIO(config))
        configitems = cfg.items("CalculatedVariables")

        with self.assertRaises(atsim.pro_fit.exceptions.ConfigException):
            atsim.pro_fit.variables.CalculatedVariables.createFromConfig(
                cfg.items("CalculatedVariables")
            )

    def testInBounds(self):
        """Test Variables.inBounds method"""

        variables = atsim.pro_fit.variables.Variables(
            [
                ("A", 1.23, False),
                ("B", 4.56, False),
                ("electroneg", 0.4, True),
            ],
            bounds=[None, (float("-inf"), 4.0), (2.0, 3.0)],
        )

        self.assertTrue(variables.inBounds("A", 100))
        self.assertTrue(variables.inBounds("B", -10.0))
        self.assertFalse(variables.inBounds("B", 10.0))
        self.assertTrue(variables.inBounds("electroneg", 2.1))
        self.assertFalse(variables.inBounds("electroneg", 3.1))
