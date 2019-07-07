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
    Predefined_Initial_Population,
    Combine_Initial_Population
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


def test_predefined_initial_population():
    # Test with good variables
    v = Variables(
        ([("b", 2.0, False), ("c", 3.0, True), ("a", 1.0, True)]),
        bounds=[(2, 10), (1, 10), (0, 10)],
    )

    # .. initialise from dictionary
    candidates = [
        {"a": 1.0, "b": 2.0, "c": 3.0},
        {"a": 4.0, "b": 5.0, "c": 6.0},
        {"a": 8.0, "b": 5.0, "c": 4.0},
    ]

    expect = np.array([[3, 1], [6, 4], [4, 8]], dtype=np.double)

    ip = Predefined_Initial_Population(v, from_dict=candidates)
    actual = ip.generate_candidates()

    assert np.allclose(expect, actual)
    assert 3 == ip.population_size

    # Test with unbounded variables
    v = Variables(([("b", 2.0, False), ("c", 3.0, True), ("a", 1.0, True)]))
    ip = Predefined_Initial_Population(v, from_dict=candidates)
    actual = ip.generate_candidates()

    assert np.allclose(expect, actual)
    assert 3 == ip.population_size

    v = Variables(
        ([("b", 2.0, False), ("c", 3.0, True), ("a", 1.0, True)]),
        bounds=[(2, 10), (1, 10), (0, 10)],
    )

    # ... check that exception is thrown if variables are out of bounds
    with pytest.raises(VariableException):
        Predefined_Initial_Population(v, from_dict=[{"a": 1.0, "c": 20.0}])

    with pytest.raises(VariableException):
        Predefined_Initial_Population(v, from_dict=[{"a": float("nan")}])

    with pytest.raises(VariableException):
        Predefined_Initial_Population(v, from_dict=[{"a": None, "c": 3.0}])

    with pytest.raises(VariableException):
        Predefined_Initial_Population(
            v, from_dict=[{"a": float("inf"), "c": 3.0}]
        )

    # ... check that exception is thrown if a key is missing
    with pytest.raises(VariableException):
        Predefined_Initial_Population(v, from_dict=[{"a": 1.0}])

    # Same tests but with arrays
    candidates = [[3.0, 1.0], [6.0, 4.0]]

    ip = Predefined_Initial_Population(v, from_array=candidates)
    actual = ip.generate_candidates()

    assert np.allclose(np.array(candidates, dtype=np.double), actual)
    assert 2 == ip.population_size

    # ... check that exception is thrown if shape is wrong
    with pytest.raises(VariableException):
        Predefined_Initial_Population(v, from_array=[[1.0]])

    # check that an exception is raised if from_array and from_dict specified at same time
    with pytest.raises(ValueError):
        Predefined_Initial_Population(
            v,
            from_dict=[{"a": 1.0, "b": 2.0, "c": 3.0}],
            from_array=[[3.0, 1.0]],
        )

    with pytest.raises(ValueError):
        Predefined_Initial_Population(v, from_dict=None, from_array=None)


def test_combine_initial_population():
    v_a = Variables([("a", 1.0, True)])

    popn_a = Predefined_Initial_Population(v_a, from_array=[[0], [1], [2]])
    popn_b = Predefined_Initial_Population(v_a, from_array=[[3], [4], [5]])

    combined = Combine_Initial_Population(popn_a, popn_b)
    assert 6 == combined.population_size

    expect = np.arange(6, dtype=np.double).reshape(6, 1)
    actual = combined.generate_candidates()

    assert np.allclose(expect, actual)

    # Fit keys and bounds must be the same

    v_b = Variables([("b", 2.0, True)])

    popn_a = Predefined_Initial_Population(v_a, from_array=[[0], [1], [2]])
    popn_b = Predefined_Initial_Population(v_b, from_array=[[3], [4], [5]])

    # Different variables
    with pytest.raises(VariableException):
        Combine_Initial_Population(popn_a, popn_b)

    v_a = Variables([("b", 2.0, True), ("a", 1.0, True)])
    v_b = Variables([("a", 1.0, True), ("b", 2.0, True)])
    popn_a = Predefined_Initial_Population(
        v_a, from_array=[[0, 1], [1, 2], [2, 3]]
    )
    popn_b = Predefined_Initial_Population(
        v_b, from_array=[[3, 4], [4, 5], [5, 6]]
    )

    with pytest.raises(VariableException):
        Combine_Initial_Population(popn_a, popn_b)

    # This should be fine (different value but some fit key order)
    v_a = Variables([("a", 2.0, True), ("b", 1.0, True)])
    v_b = Variables([("a", 1.0, True), ("b", 2.0, True)])
    popn_a = Predefined_Initial_Population(
        v_a, from_array=[[0, 1], [1, 2], [2, 3]]
    )
    popn_b = Predefined_Initial_Population(
        v_b, from_array=[[3, 4], [4, 5], [5, 6]]
    )

    Combine_Initial_Population(popn_a, popn_b)

    # Different bounds
    v_a = Variables([("b", 2.0, True), ("a", 4.0, True)], [(1, 3), (4, 5)])
    v_b = Variables([("b", 2.0, True), ("a", 1.0, True)])
    popn_a = Predefined_Initial_Population(
        v_a, from_array=[[2, 4.5], [2.5, 4.1], [2, 4.2]]
    )
    popn_b = Predefined_Initial_Population(
        v_b, from_array=[[3, 4], [4, 5], [5, 6]]
    )

    with pytest.raises(VariableException):
        Combine_Initial_Population(popn_a, popn_b)
