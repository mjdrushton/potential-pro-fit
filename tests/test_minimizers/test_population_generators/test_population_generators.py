import pytest
import numpy as np

import random

from atsim.pro_fit.minimizers.population_generators import (
    Candidate_Generator,
    Missing_Distribution_Exception,
    Candidate_Length_Exception,
    Variable_Distribution,
    Variable_Distributions,
    Latin_Hypercube_InitialPopulation,
    Uniform_Random_Initial_Population,
    Uniform_Variable_Distribution,
)

from atsim.pro_fit.variables import Variables, VariableException


def c(n):
    def f(v):
        return n

    return f


def test_candidate_generator():

    v = Variables(
        ([("a", 1.0, True), ("b", 2.0, False), ("c", 3.0, True)]),
        bounds=[(0, 10), (1, 10), (2, 10)],
    )

    vd_a = Variable_Distribution("a", c(1))
    vd_c = Variable_Distribution("c", c(3))

    vd = Variable_Distributions(v, [vd_a, vd_c])

    cg = Candidate_Generator(vd)

    # Test with 1D list
    expect = np.array([1, 3])
    actual = cg.generate_candidates([0.1, 0.2])
    assert np.allclose(expect, actual)

    # Test with 1D np.array
    actual = cg.generate_candidates(np.array([0.1, 0.2]))
    assert np.allclose(expect, actual)

    # Test with 2d list
    expect = np.array([[1, 3], [1, 3]])

    actual = cg.generate_candidates([[0.1, 0.2], [0.1, 0.2]])
    assert np.allclose(expect, actual)

    # Test with 2D np.array
    actual = cg.generate_candidates(np.array([[0.1, 0.2], [0.1, 0.2]]))
    assert np.allclose(expect, actual)

    # Check that exception if thrown if too many or too few candidates specified
    with pytest.raises(Candidate_Length_Exception):
        cg.generate_candidates([])

    with pytest.raises(Candidate_Length_Exception):
        cg.generate_candidates([0.1, 0.2, 0.3])

    with pytest.raises(Candidate_Length_Exception):
        cg.generate_candidates([[0.1, 0.2], [0.1, 0.2, 0.3]])


def test_variable_distributions_missing_distribution():
    v = Variables(
        ([("a", 1.0, True), ("b", 2.0, False), ("c", 3.0, True)]),
        bounds=[(0, 10), (1, 10), (2, 10)],
    )

    vd_a = Variable_Distribution("a", c(1))

    with pytest.raises(Missing_Distribution_Exception):
        Variable_Distributions(v, [vd_a])


def test_latin_hyper_cube_initial_population():
    v = Variables(
        ([("a", 1.0, True), ("b", 2.0, False), ("c", 3.0, True)]),
        bounds=[(0, 10), (1, 10), (2, 10)],
    )

    ip = Latin_Hypercube_InitialPopulation(
        v, 4, criterion=Latin_Hypercube_InitialPopulation.Criterion.center
    )

    cd = ip.generate_candidates()
    assert type(cd) == np.ndarray
    assert (4, 2) == cd.shape

    vd = ip.candidate_generator.variable_distributions

    assert list(vd.variable_distribution_dict.keys()) == ["a", "c"]

    vd_a = vd.variable_distribution_dict["a"]
    vd_c = vd.variable_distribution_dict["c"]

    assert type(vd_a) == Uniform_Variable_Distribution
    assert "a" == vd_a.variable_label
    assert vd_a.lower_bound == 0.0
    assert vd_a.upper_bound == 10

    assert 0.0 == vd_a._distn_function(0)
    assert 10.0 == vd_a._distn_function(1)

    assert type(vd_c) == Uniform_Variable_Distribution
    assert "c" == vd_c.variable_label
    assert vd_c.lower_bound == 2
    assert vd_c.upper_bound == 10

    assert 2.0 == vd_c._distn_function(0)
    assert 10.0 == vd_c._distn_function(1)

    v = Variables(
        ([("a", 1.0, True), ("b", 2.0, False), ("c", 3.0, True)]),
        bounds=[(0, 10), (1, 10), (2, float("inf"))],
    )

    with pytest.raises(VariableException):
        Latin_Hypercube_InitialPopulation(v, 4)


def test_uniform_random_initial_population():
    v = Variables(
        ([("a", 1.0, True), ("b", 2.0, False), ("c", 3.0, True)]),
        bounds=[(0, 10), (1, 10), (2, 10)],
    )

    ip = Uniform_Random_Initial_Population(v, 4)

    cd = ip.generate_candidates()
    assert type(cd) == np.ndarray
    assert (4, 2) == cd.shape
