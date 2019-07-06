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
)

from ._latin_hypercube_initial_population import (
    Latin_Hypercube_InitialPopulation,
)


class UniformGenerator(BoundedVariableBaseClass):
    """Inspyred generator that generates bounded candidates from bounds stored in a Variables instance.
    Candidates are selected from a random uniform distribution."""

    def __call__(self, random, args):
        """Inspyred generator.

    @param random random.Random instance passed in by Inspyred
    @param args Args dictionary (not used here)
    @return Candidate with length == adjustable parameters in self.initialVariables sitting within
      limits defined by variable bounds."""
        candidate = []
        for (l, h) in zip(self._bounds[0], self._bounds[1]):
            candidate.append(random.uniform(l, h))
        return candidate
