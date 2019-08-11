import inspyred
import numpy as np
import pytest
import unittest

from atsim import pro_fit

import atsim.pro_fit.minimizers
import atsim.pro_fit.variables

from atsim.pro_fit.exceptions import ConfigException

from atsim.pro_fit.minimizers._inspyred._inspyred_common import (
    Population_To_Generator_Adapter,
)

from atsim.pro_fit.minimizers.population_generators import (
    Uniform_Random_Initial_Population,
    Predefined_Initial_Population,
    Combine_Initial_Population,
    File_Initial_Population,
    Latin_Hypercube_InitialPopulation,
)

from atsim.pro_fit.minimizers.population_generators import (
    PERT_Variable_Distributions,
    Uniform_Variable_Distributions,
)


def UniformGenerator(variables):
    return Population_To_Generator_Adapter(
        variables, Uniform_Random_Initial_Population(variables, 1)
    )


class InspyredSupportTestCase(unittest.TestCase):
    """Tests for adapters contained in fitting.minimizers._inspyred"""

    def testBounderGenerator(self):
        """Test Bounder and Generator"""
        from atsim.pro_fit.minimizers import _inspyred

        # Test BounderGenerator
        # ... first check it throws when unbounded variables used for instantiation
        with self.assertRaises(_inspyred.VariableException):
            _inspyred.Bounder(
                atsim.pro_fit.variables.Variables([("A", 1.0, True)])
            )

        with self.assertRaises(_inspyred.VariableException):
            UniformGenerator(
                atsim.pro_fit.variables.Variables([("A", 1.0, True)])
            )

        with self.assertRaises(_inspyred.VariableException):
            _inspyred.Bounder(
                atsim.pro_fit.variables.Variables(
                    [("A", 1.0, True)], [(None, 10.0)]
                )
            )

        with self.assertRaises(_inspyred.VariableException):
            UniformGenerator(
                atsim.pro_fit.variables.Variables(
                    [("A", 1.0, True)], [(None, 10.0)]
                )
            )

        with self.assertRaises(_inspyred.VariableException):
            _inspyred.Bounder(
                atsim.pro_fit.variables.Variables(
                    [("A", 1.0, False), ("B", 1.0, True)],
                    [(None, 10.0), (-10.0, float("inf"))],
                )
            )

        with self.assertRaises(_inspyred.VariableException):
            UniformGenerator(
                atsim.pro_fit.variables.Variables(
                    [("A", 1.0, False), ("B", 1.0, True)],
                    [(None, 10.0), (-10.0, float("inf"))],
                )
            )

        # ... or throws if non of the variables are fit parameters
        with self.assertRaises(_inspyred.VariableException):
            _inspyred.Bounder(
                atsim.pro_fit.variables.Variables(
                    [("A", 1.0, False), ("B", 1.0, False)],
                    [(-10.0, 10.0), (-10.0, 10.0)],
                )
            )

        with self.assertRaises(_inspyred.VariableException):
            UniformGenerator(
                atsim.pro_fit.variables.Variables(
                    [("A", 1.0, False), ("B", 1.0, False)],
                    [(-10.0, 10.0), (-10.0, 10.0)],
                )
            )

        # Check we can access initial arguments

        # self.assertEquals(variables.flaggedVariablePairs,
        #   bounderGenerator.initialVariables.flaggedVariablePairs)

        # Check the bounder
        variables = atsim.pro_fit.variables.Variables(
            [
                ("A", 1.0, False),
                ("B", 2.0, True),
                ("C", 3.0, True),
                ("D", 4.0, False),
            ],
            [(None, None), (-10.0, 10.0), (-20.0, 20.0), (-30.0, 30.0)],
        )
        bounder = _inspyred.Bounder(variables)

        expect = [[-10.0, -20.0], [10.0, 20.0]]
        actual = bounder.bounds
        self.assertEqual(expect, actual)

        import inspyred

        self.assertEqual(inspyred.ec.Bounder, type(bounder._bounder))

        # Check the generator
        import random

        # generator = UniformGenerator(variables)
        generator = Population_To_Generator_Adapter(
            variables, Uniform_Random_Initial_Population(variables, 1)
        )
        actual = generator(random.Random(), {})
        self.assertEqual(2, len(actual))