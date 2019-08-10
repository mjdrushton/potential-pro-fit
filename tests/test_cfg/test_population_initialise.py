from atsim.pro_fit.cfg import Create_From_Config_Parser
from atsim.pro_fit.cfg import add_initial_population_options

import atsim.pro_fit.variables

from atsim.pro_fit.minimizers.population_generators import (
    Uniform_Random_Initial_Population,
    Predefined_Initial_Population,
    Combine_Initial_Population,
    File_Initial_Population,
    Latin_Hypercube_InitialPopulation,
)


def test_defaults():
    v = atsim.pro_fit.variables.Variables(
        [("a", 1.0, True), ("b", 2.1, False), ("c", 3.0, True)],
        [(0, 10), (0, 20), (0, 30)],
    )

    cfgparser = Create_From_Config_Parser("Population Test")
    # import pdb; pdb.set_trace()
    add_initial_population_options(cfgparser)

    parsed_options = cfgparser.parse([], [])
    assert len(parsed_options) == 1

    o = parsed_options[0].value
    popn = o(v)

    assert popn.population_size == 100
    # assert hasattr(o, "random_seed")
    assert popn.population_include_orig_vars == True
    assert popn.population_load_from_csv == None
    assert popn.population_load_from_ppdump == None
    assert popn.population_distribution == "uniform"


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

    options = [
        ("population_size", 6),
        ("population_load_from_csv", table_file.strpath),
        ("population_include_orig_vars", True),
    ]

    cfgparser = Create_From_Config_Parser("Test Parser")
    add_initial_population_options(cfgparser)

    parsed_options = cfgparser.parse(options, [])
    population_factory = parsed_options[0].value
    popn = population_factory(v).population

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
    options = [
        ("population_size", 6),
        ("population_load_from_csv", table_file.strpath),
        ("population_include_orig_vars", False),
    ]

    cfgparser = Create_From_Config_Parser("Test Parser")
    add_initial_population_options(cfgparser)

    parsed_options = cfgparser.parse(options, [])
    population_factory = parsed_options[0].value
    popn = population_factory(v).population

    assert popn.population_size == 6

    assert type(popn) == Combine_Initial_Population
    assert len(popn.populations) == 2

    assert type(popn.populations[0]) == File_Initial_Population
    assert popn.populations[0].population_size == 2

    assert type(popn.populations[1]) == Latin_Hypercube_InitialPopulation
    assert popn.populations[1].population_size == 4

    # Don't include file
    options = [("population_size", 6), ("population_include_orig_vars", True)]

    cfgparser = Create_From_Config_Parser("Test Parser")
    add_initial_population_options(cfgparser)

    parsed_options = cfgparser.parse(options, [])
    population_factory = parsed_options[0].value
    popn = population_factory(v).population

    assert popn.population_size == 6

    assert type(popn) == Combine_Initial_Population
    assert len(popn.populations) == 2

    assert type(popn.populations[0]) == Predefined_Initial_Population
    assert popn.populations[0].population_size == 1

    assert type(popn.populations[1]) == Latin_Hypercube_InitialPopulation
    assert popn.populations[1].population_size == 5

    # Don't include file or original value
    options = [("population_size", 6), ("population_include_orig_vars", False)]

    cfgparser = Create_From_Config_Parser("Test Parser")
    add_initial_population_options(cfgparser)

    parsed_options = cfgparser.parse(options, [])
    population_factory = parsed_options[0].value
    popn = population_factory(v).population

    assert popn.population_size == 6
    assert type(popn) == Latin_Hypercube_InitialPopulation

    # Only file
    options = [
        ("population_size", 2),
        ("population_include_orig_vars", False),
        ("population_load_from_csv", table_file.strpath),
    ]

    cfgparser = Create_From_Config_Parser("Test Parser")
    add_initial_population_options(cfgparser)

    parsed_options = cfgparser.parse(options, [])
    population_factory = parsed_options[0].value
    popn = population_factory(v).population

    assert popn.population_size == 2
    assert type(popn) == File_Initial_Population
