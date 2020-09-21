import configparser
import io
import unittest

from atsim import pro_fit

from .. import testutil
from ._common import MockMeritRosen, StepCallBack


class NelderMeadTestCase(unittest.TestCase):
    """Test atsim.prof_fit.minimizers"""

    def testNelderMeadGetBounds(self):
        config = """[Minimizer]
type : NelderMead
function_tolerance : 1.0E-3
value_tolerance : 1.0E-3
max_iterations : 30

"""
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read_file(io.StringIO(config))
        configitems = cfg.items("Minimizer")

        variables = pro_fit.variables.Variables(
            [
                ("A", 1.0, False),
                ("B", 2.0, True),
                ("C", 3.0, False),
                ("D", 4.0, True),
            ],
            [(2.0, 3.0), (3.0, 4.0), (5.0, 6.0), (7.0, 8.0)],
        )

        minimizer = pro_fit.minimizers.NelderMeadMinimizer.createFromConfig(
            variables, configitems
        )
        expect = [(3.0, 4.0), (7.0, 8.0)]
        testutil.compareCollection(self, expect, minimizer._inner._getBounds())

    def testNelderMeadSingleConfig(self):
        """Tests for the atsim.prof_fit.minimizers.NelderMead wrapper"""
        variables = pro_fit.variables.Variables(
            [
                ("A", 1.0, False),
                ("B", 2.0, True),
                ("C", 3.0, False),
                ("D", 4.0, True),
            ]
        )

        config = """[Minimizer]
type : NelderMead
value_tolerance : 1.0E-3

"""
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read_file(io.StringIO(config))
        configitems = cfg.items("Minimizer")

        minimizer = pro_fit.minimizers.NelderMeadMinimizer.createFromConfig(
            variables, configitems
        )
        self.assertEqual(
            pro_fit.minimizers.NelderMeadMinimizer, type(minimizer)
        )

        config = """[Minimizer]
type : NelderMead
max_iterations : 30

"""
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read_file(io.StringIO(config))
        configitems = cfg.items("Minimizer")

        minimizer = pro_fit.minimizers.NelderMeadMinimizer.createFromConfig(
            variables, configitems
        )
        self.assertEqual(
            pro_fit.minimizers.NelderMeadMinimizer, type(minimizer)
        )

    def testNelderMead(self):
        """Tests for the atsim.prof_fit.minimizers.NelderMead wrapper"""

        config = """[Minimizer]
type : NelderMead
function_tolerance : 1.0E-3
value_tolerance : 1.0E-3
max_iterations : 30

"""
        cfg = configparser.ConfigParser()
        cfg.optionxform = str
        cfg.read_file(io.StringIO(config))
        configitems = cfg.items("Minimizer")

        variables = pro_fit.variables.Variables(
            [
                ("A", 1.0, False),
                ("B", 2.0, True),
                ("C", 3.0, False),
                ("D", 4.0, True),
            ]
        )

        minimizer = pro_fit.minimizers.NelderMeadMinimizer.createFromConfig(
            variables, configitems
        )
        self.assertEqual(
            pro_fit.minimizers.NelderMeadMinimizer, type(minimizer)
        )

        args = minimizer._inner._initialArgs()
        testutil.compareCollection(self, [2.0, 4.0], args)

        self.assertEqual(None, minimizer._inner._getBounds())

        variables = minimizer._inner._argsToVariables([5.0, 6.0])
        testutil.compareCollection(
            self,
            [("A", 1.0), ("B", 5.0), ("C", 3.0), ("D", 6.0)],
            variables.variablePairs,
        )

        minimizer.stepCallback = StepCallBack()
        # Perform minimization of Rosenbrock function and check we get correct answers
        optimized = minimizer.minimize(MockMeritRosen())

        finalmeritval = optimized.bestMeritValue
        optimized = optimized.bestVariables

        self.assertAlmostEqual(0.0668765184732, finalmeritval)
        testutil.compareCollection(
            self,
            [("A", 1.0), ("B", 1.25066832), ("C", 3.0), ("D", 1.57052885)],
            optimized.variablePairs,
        )

        stepcallbackexpect = [
            dict(A=1.000000, B=2.000000, C=3.000000, D=4.000000, meritval=1.0),
            dict(
                A=1.000000,
                B=2.000000,
                C=3.000000,
                D=4.200000,
                meritval=5.0000000000000071,
            ),
            dict(
                A=1.000000,
                B=2.050000,
                C=3.000000,
                D=4.050000,
                meritval=3.4281249999999952,
            ),
            dict(
                A=1.000000,
                B=2.012500,
                C=3.000000,
                D=4.112500,
                meritval=1.4138305664062523,
            ),
            dict(
                A=1.000000,
                B=2.028125,
                C=3.000000,
                D=4.053125,
                meritval=1.4190359592437922,
            ),
            dict(
                A=1.000000,
                B=2.017188,
                C=3.000000,
                D=4.054688,
                meritval=1.0552853685617489,
            ),
            dict(
                A=1.000000,
                B=2.010547,
                C=3.000000,
                D=4.069922,
                meritval=1.0975087642320434,
            ),
            dict(
                A=1.000000,
                B=2.009570,
                C=3.000000,
                D=4.048633,
                meritval=1.0297589176429021,
            ),
            dict(
                A=1.000000,
                B=1.998584,
                C=3.000000,
                D=4.009131,
                meritval=1.0190530125207142,
            ),
            dict(
                A=1.000000,
                B=1.978735,
                C=3.000000,
                D=3.916431,
                meritval=0.95803044034335361,
            ),
            dict(
                A=1.000000,
                B=1.980151,
                C=3.000000,
                D=3.907300,
                meritval=0.97946469507950373,
            ),
            dict(
                A=1.000000,
                B=1.938330,
                C=3.000000,
                D=3.735596,
                meritval=0.92680790388294665,
            ),
        ]
        testutil.compareCollection(
            self, stepcallbackexpect, minimizer.stepCallback.stepDicts
        )
