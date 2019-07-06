import random
import enum

import pyDOE2

from ._variable_distributions import (
    Uniform_Variable_Distribution,
    Variable_Distributions,
)

from ._candidate_generator import Candidate_Generator


class Latin_Hypercube_InitialPopulation(object):
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
        self.initial_variables = initial_variables
        self.population_size = population_size
        self.criterion = criterion

        if candidate_generator is not None:
            self.candidate_generator = candidate_generator
        else:
            self.candidate_generator = self._init_default_candidate_generator()

    def _init_default_candidate_generator(self):
        vd = [
            Uniform_Variable_Distribution(fk, self.initial_variables)
            for fk in self.initial_variables.fitKeys
        ]
        vd_obj = Variable_Distributions(self.initial_variables, vd)
        cg = Candidate_Generator(vd_obj)
        return cg

    def generate_candidates(self):
        n_factors = len(self.initial_variables.fitKeys)
        norm_candidates = pyDOE2.lhs(
            n_factors,
            samples=self.population_size,
            criterion=self.criterion.value,
        )
        candidates = self.candidate_generator.generate_candidates(
            norm_candidates
        )
        return candidates
