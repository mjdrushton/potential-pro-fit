import random
import enum

import pyDOE2

from ._variable_distributions import (
    Uniform_Variable_Distribution,
    Uniform_Variable_Distributions,
    Variable_Distributions,
)

from ._candidate_generator import Candidate_Generator
from ._bounded_variable_initialpopulation_base import (
    Bounded_Variable_InitialPopulation_Base,
)


class Latin_Hypercube_InitialPopulation(
    Bounded_Variable_InitialPopulation_Base
):
    """Generates initial populations using the Latin Hypercube method.

    This class makes use of the functionality provided by the pyDOE2
    package."""

    class Criterion(enum.Enum):
        random = None
        center = "center"
        maximin = "maximin"
        centermaximin = "centermaximin"
        correlation = "correlation"

    def __init__(
        self,
        initial_variables,
        population_size,
        criterion=Criterion.random,
        candidate_generator=None,
    ):
        """Create an initial population of candidates based on the bounds in 

        
        Arguments:
            initial_variables {atsim.pro_fit.variables.Variables} -- Initial variables
            population_size {int} -- Number of candidate sets to generate.
        
        Keyword Arguments:
            criterion {Criterion or None} -- Determines how points are arranged in interval.
                * "random": randomizes the points within the intervals
                * “center”: center the points within the sampling intervals
                * “maximin”: maximize the minimum distance between points, but place the point in a randomized location within its interval
                * “centermaximin”: same as “maximin”, but centered within the intervals
                * “correlation”: minimize the maximum correlation coefficient
            candidate_generator {atsim.pro_fit.minimizers.population_generators.Candidate_Generator} Object used to convert 
                values from fractional values into coordinate space. If this is None, then an object using Variable_Distribution
                based around a uniform distribution between each variable's upper and lower bounds will be used.
        """
        super().__init__(
            initial_variables, population_size, candidate_generator
        )
        self.criterion = criterion

    def _init_variable_distributions(self):
        return Uniform_Variable_Distributions(self.initialVariables)

    def _generate_norm_candidates(self):
        n_factors = len(self.initialVariables.fitKeys)
        norm_candidates = pyDOE2.lhs(
            n_factors,
            samples=self.population_size,
            criterion=self.criterion.value,
        )
        return norm_candidates
