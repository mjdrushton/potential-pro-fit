import inspyred
import numpy as np
import pytest
import unittest

from atsim import pro_fit

import atsim.pro_fit.minimizers
import atsim.pro_fit.variables

from atsim.pro_fit.minimizers._inspyred._inspyred_common import (
    Population_To_Generator_Adapter,
)

from atsim.pro_fit.minimizers._inspyred._config import (
    Initial_Population_Config_Helper,
    _Initial_Population_Factory,
)

from atsim.pro_fit.minimizers.population_generators import (
    Uniform_Random_Initial_Population,
    Predefined_Initial_Population,
    Combine_Initial_Population,
    File_Initial_Population,
    Latin_Hypercube_InitialPopulation,
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

    cfg_opts = Initial_Population_Config_Helper("Helper").parse(v, {})

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
        generator=cfg_opts["generator"],
        initial_population=cfg_opts["initial_population"],
    )
    # pytest.fail()


def test_dea_instantiation():
    v = atsim.pro_fit.variables.Variables(
        [("a", 1.0, True), ("b", 2.1, False), ("c", 3.0, True)],
        [(0, 10), (0, 20), (0, 30)],
    )

    cfg_opts = Initial_Population_Config_Helper("Helper").parse(v, {})

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
        generator=cfg_opts["generator"],
        initial_population=cfg_opts["initial_population"],
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


def test_initial_population_config_helper():
    config_helper = Initial_Population_Config_Helper(
        "Helper", population_size=128
    )
    assert config_helper.default_keys == sorted(
        [
            "population_size",
            "population_include_orig_vars",
            "random_seed",
            "max_iterations",
            "population_load_from_csv",
            "population_load_from_ppdump",
        ]
    )

    # Get defaults
    v = atsim.pro_fit.variables.Variables(
        [("a", 1.0, True), ("b", 2.1, False), ("c", 3.0, True)],
        [(0, 10), (0, 20), (0, 30)],
    )

    popn_dict = config_helper.parse(v, {})

    assert "initial_population" in popn_dict
    assert "generator" in popn_dict

    init_population = popn_dict["initial_population"]
    generator = popn_dict["generator"]

    del popn_dict["initial_population"]
    del popn_dict["generator"]

    assert "random_seed" in popn_dict
    del popn_dict["random_seed"]

    expect_defaults = {
        "population_size": 128,
        "population_include_orig_vars": True,
        "max_iterations": 1000,
        "population_load_from_csv": None,
        "population_load_from_ppdump": None,
    }
    assert expect_defaults == popn_dict

    assert init_population.population_size == 128
    assert np.allclose([[1.0, 3.0]], init_population.generate_candidates()[0])

    # TODO: Need test for when population_include_orig_vars is False

    # TODO: Need test for generator


def test_initial_population_factory(tmpdir):
    v = atsim.pro_fit.variables.Variables(
        [("a", 1.0, True), ("b", 2.1, False), ("c", 3.0, True)],
        [(0, 10), (0, 20), (0, 30)],
    )

    table_file = tmpdir.join("variables.csv")

    table_file.write(
        """a,b,c
1.0,2.0,3.0
4.0,5.0,6.0"""
    )

    popn_factory = _Initial_Population_Factory(
        v,
        {
            "population_size": 6,
            "population_load_from_csv": table_file.strpath,
            "population_include_orig_vars": True,
        },
    )

    popn = popn_factory.population
    assert popn.population_size == 6

    assert type(popn) == Combine_Initial_Population
    assert len(popn.populations) == 3

    assert type(popn.populations[0]) == Predefined_Initial_Population
    assert popn.populations[0].population_size == 1

    assert type(popn.populations[1]) == File_Initial_Population
    assert popn.populations[1].population_size == 2

    assert type(popn.populations[2]) == Latin_Hypercube_InitialPopulation
    assert popn.populations[2].population_size == 3

    # Don't include original variables
    popn_factory = _Initial_Population_Factory(
        v,
        {
            "population_size": 6,
            "population_load_from_csv": table_file.strpath,
            "population_include_orig_vars": False,
        },
    )

    popn = popn_factory.population
    assert popn.population_size == 6

    assert type(popn) == Combine_Initial_Population
    assert len(popn.populations) == 2

    assert type(popn.populations[0]) == File_Initial_Population
    assert popn.populations[0].population_size == 2

    assert type(popn.populations[1]) == Latin_Hypercube_InitialPopulation
    assert popn.populations[1].population_size == 4

    # Don't include file
    popn_factory = _Initial_Population_Factory(
        v, {"population_size": 6, "population_include_orig_vars": True}
    )

    popn = popn_factory.population
    assert popn.population_size == 6

    assert type(popn) == Combine_Initial_Population
    assert len(popn.populations) == 2

    assert type(popn.populations[0]) == Predefined_Initial_Population
    assert popn.populations[0].population_size == 1

    assert type(popn.populations[1]) == Latin_Hypercube_InitialPopulation
    assert popn.populations[1].population_size == 5

    # Don't include file or original value
    popn_factory = _Initial_Population_Factory(
        v, {"population_size": 6, "population_include_orig_vars": False}
    )

    popn = popn_factory.population
    assert popn.population_size == 6

    assert type(popn) == Latin_Hypercube_InitialPopulation

    # Only file
    popn_factory = _Initial_Population_Factory(
        v,
        {
            "population_size": 2,
            "population_include_orig_vars": False,
            "population_load_from_csv": table_file.strpath,
        },
    )

    popn = popn_factory.population
    assert popn.population_size == 2

    assert type(popn) == File_Initial_Population
