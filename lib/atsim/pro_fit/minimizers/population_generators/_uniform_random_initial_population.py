from ._bounded_variable_initialpopulation_base import (
    Bounded_Variable_InitialPopulation_Base,
)

from ._variable_distributions import Uniform_Variable_Distributions

import numpy as np


class Uniform_Random_Initial_Population(
    Bounded_Variable_InitialPopulation_Base
):
    """Initialise population from a continuous random distribution"""

    def __init__(self, initial_variables, population_size):
        super().__init__(initial_variables, population_size)

    def _init_variable_distributions(self):
        return Uniform_Variable_Distributions(self.initialVariables)

    def _generate_norm_candidates(self):
        s = (self.population_size, self.initialVariables.numFitVariables)
        norm_candidates = np.random.rand(*s)
        return norm_candidates
