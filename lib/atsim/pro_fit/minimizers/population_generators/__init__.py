from atsim.pro_fit.variables import BoundedVariableBaseClass

from ._exceptions import (
    Missing_Distribution_Exception,
    Candidate_Length_Exception,
)

from ._candidate_generator import Candidate_Generator

from ._variable_distributions import (
    Variable_Distribution,
    Variable_Distributions,
    Uniform_Variable_Distribution,
    PERT_Variable_Distribution
)

from ._latin_hypercube_initial_population import (
    Latin_Hypercube_InitialPopulation,
)

from ._uniform_random_initial_population import (
    Uniform_Random_Initial_Population,
)

from ._predefined_initial_population import Predefined_Initial_Population

from ._combine_initial_population import Combine_Initial_Population

from ._file_initial_population import (
    File_Initial_Population,
    Ppdump_File_Initial_Population,
)

import numpy as np


class Null_Initial_Population(object):
    """Returns zero sized population"""

    def __init__(self):
        self.population_size = 0

    def generate_candidates(self):
        return np.array([])
