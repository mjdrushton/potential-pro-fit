import operator
from functools import total_ordering

from atsim.pro_fit.exceptions import ConfigException


class ExistingCallbackException(Exception):
    pass


class MinimizerException(Exception):
    pass


class MinimizerConfigException(ConfigException):
    pass


@total_ordering
class MinimizerResults(object):
    """Container for the results obtained from an iteration of a minimizer"""

    def __init__(self, meritValues, candidateJobList):
        """@param meritValues List of merit values in candidate order
    @param candidateJobList List of (Variables,Job list) tuples in candidate order"""
        self.meritValues = meritValues
        self.candidateJobList = candidateJobList

    def _bestIndexValue(self):
        if not self.meritValues:
            raise MinimizerException("List of merit values for population is empty. This may indicate that all candidates produced invalid values.")
        minval = min(enumerate(self.meritValues), key=operator.itemgetter(1))
        return minval

    def bestMeritValue(self):
        return self._bestIndexValue()[1]

    bestMeritValue = property(
        bestMeritValue, doc="Return bestMeritValue for these MinimizerResults"
    )

    def indexOfBest(self):
        return self._bestIndexValue()[0]

    indexOfBest = property(
        indexOfBest,
        doc="Return index of bestMeritValue in meritValues for these MinimizerResults",
    )

    def bestJobList(self):
        return self.candidateJobList[self.indexOfBest][1]

    bestJobList = property(
        bestJobList,
        doc="Return list of Jobs belonging best solution contained in these MinimizerResults",
    )

    def bestVariables(self):
        return self.candidateJobList[self.indexOfBest][0]

    bestVariables = property(
        bestVariables,
        doc="Return Variables belonging to best solution in these MinimizerResults",
    )

    def __eq__(self, other):
        return self.bestMeritValue == other.bestMeritValue

    def __lt__(self, other):
        return self.bestMeritValue < other.bestMeritValue
