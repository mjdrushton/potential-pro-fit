from atsim.pro_fit.variables import BoundedVariableBaseClass

from ._candidate_generator import Candidate_Generator


class Bounded_Variable_InitialPopulation_Base(BoundedVariableBaseClass):
    def __init__(
        self, initial_variables, population_size, candidate_generator=None
    ):
        super().__init__(initial_variables)
        self.population_size = population_size

        if candidate_generator is not None:
            self.candidate_generator = candidate_generator
        else:
            self.candidate_generator = self._init_default_candidate_generator()

    def _init_variable_distributions(self):
        raise NotImplementedError("Child classes should implement this method")

    def _init_default_candidate_generator(self):
        vd = self._init_variable_distributions()
        cg = Candidate_Generator(vd)
        return cg

    def _generate_norm_candidates(self):
        raise NotImplementedError("Child classes should implement this method")

    def generate_candidates(self):
        norm_candidates = self._generate_norm_candidates()
        candidates = self.candidate_generator.generate_candidates(
            norm_candidates
        )
        return candidates
