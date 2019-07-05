from atsim.pro_fit.variables import BoundedVariableBaseClass


class UniformGenerator(BoundedVariableBaseClass):
    """Inspyred generator that generates bounded candidates from bounds stored in a Variables instance"""

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
