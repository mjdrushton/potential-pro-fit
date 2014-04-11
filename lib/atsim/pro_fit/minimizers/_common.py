import operator


class ExistingCallbackException(Exception):
  pass

class MinimizerException(Exception):
  pass

class MinimizerResults(object):
  """Container for the results obtained from an iteration of a minimizer"""

  def __init__(self, meritValues, candidateJobList):
    """@param meritValues List of merit values in candidate order
    @param candidateJobList List of (Variable,Job list) tuples in candidate order"""
    self.meritValues = meritValues
    self.candidateJobList = candidateJobList

  def _bestIndexValue(self):
    minval = min(enumerate(self.meritValues), key =operator.itemgetter(1))
    return minval

  def bestMeritValue(self):
    return self._bestIndexValue()[1]
  bestMeritValue = property(bestMeritValue, doc = "Return bestMeritValue for these MinimizerResults")

  def indexOfBest(self):
    return self._bestIndexValue()[0]
  indexOfBest = property(indexOfBest, doc = "Return index of bestMeritValue in meritValues for these MinimizerResults")

  def bestJobList(self):
    return self.candidateJobList[self.indexOfBest][1]
  bestJobList = property(bestJobList, doc = "Return list of Jobs belonging best solution contained in these MinimizerResults")

  def bestVariables(self):
    return self.candidateJobList[self.indexOfBest][0]
  bestVariables = property(bestVariables, doc = "Return Variables belonging to best solution in these MinimizerResults")

  def __cmp__(self, other ):
    return cmp(self.bestMeritValue, other.bestMeritValue)