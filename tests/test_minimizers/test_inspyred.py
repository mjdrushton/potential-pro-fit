import pytest
import unittest
import inspyred

from atsim import pro_fit

import atsim.pro_fit.minimizers
import atsim.pro_fit.variables

from atsim.pro_fit.minimizers._inspyred._inspyred_common import (
    Population_To_Generator_Adapter,
)
from atsim.pro_fit.minimizers.population_generators import (
    Uniform_Random_Initial_Population,
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


def test_pso_instantiation():
    v = atsim.pro_fit.variables.Variables(
        [("a", 1.0, True), ("b", 2.1, False), ("c", 3.0, True)],
        [(0, 10), (0, 20), (0, 30)],
    )

    topology = inspyred.swarm.topologies.star_topology

    atsim.pro_fit.minimizers.Particle_SwarmMinimizer(
        v,
        topology,
        population_size=100,
        inertia=1.0,
        cognitive_rate=1.0,
        social_rate=1.0,
        max_iterations=500,
        random_seed=8908,
    )
    # pytest.fail()


def test_dea_instantiation():
    v = atsim.pro_fit.variables.Variables(
        [("a", 1.0, True), ("b", 2.1, False), ("c", 3.0, True)],
        [(0, 10), (0, 20), (0, 30)],
    )

    atsim.pro_fit.minimizers.DEAMinimizer(
        v,
        population_size=100,
        random_seed=9786098,
        tournament_size=8,
        crossover_rate=0.9,
        mutation_rate=0.1,
        gaussian_mean=1.0,
        gaussian_stdev=0.1,
        max_iterations=500,
        num_selected=16,
    )


def test_sa_instantiation():
    v = atsim.pro_fit.variables.Variables(
        [("a", 1.0, True), ("b", 2.1, False), ("c", 3.0, True)],
        [(0, 10), (0, 20), (0, 30)],
    )

    # def tbound(candidate, args):
    #     pass

    atsim.pro_fit.minimizers.Simulated_AnnealingMinimizer(
        v,
        None,
        temperature=700.0,
        cooling_rate=1.0,
        mutation_rate=0.1,
        gaussian_mean=1.0,
        gaussian_stdev=0.1,
        max_iterations=500,
        random_seed=907097,
    )
