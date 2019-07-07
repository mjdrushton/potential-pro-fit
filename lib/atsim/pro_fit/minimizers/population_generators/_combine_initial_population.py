from atsim.pro_fit.variables import VariableException

import numpy as np

import itertools


class Combine_Initial_Population(object):
    """Allows two populations to combined"""

    def __init__(self, *populations):
        """Population objects are provided as arguments to this constructor"""
        self.populations = populations
        self._check_population_variables_match()

        self.initialVariables = self.populations[0].initialVariables

    def _check_population_variables_match(self):
        variables = [p.initialVariables for p in self.populations]
        for v_a, v_b in itertools.permutations(variables, 2):
            self._variable_match(v_a, v_b)

    def _variable_match(self, v_a, v_b):
        if not v_a.fitKeys == v_b.fitKeys:
            raise VariableException(
                "Variable fitKeys do not match {} != {}".format(
                    v_a.fitKeys, v_b.fitKeys
                )
            )

        if not v_a.fitBounds == v_b.fitBounds:
            raise VariableException(
                "Variable bounds do not match {} != {}".format(
                    v_a.fitBounds, v_b.fitBounds
                )
            )

    @property
    def population_size(self):
        pop_size = sum([p.population_size for p in self.populations])
        return pop_size

    def generate_candidates(self):
        candidates = [p.generate_candidates() for p in self.populations]
        candidates = np.concatenate(candidates, axis=0)
        return candidates
