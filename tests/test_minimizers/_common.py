import os

import mystic.models

def getResourceDir():
  return os.path.join(
      os.path.dirname(__file__),
      os.path.pardir,
      'resources')


class MockJob(object):

  def __init__(self, variables):
    self.variables = variables

  def __repr__(self):
    return "MockJob("+repr(self.variables)+")"

class MockMeritRosen(object):
  """Mock merit object which evaluates Rosenbrock function"""

  def __init__(self):
    self.afterMerit = None

  def calculate(self, candidates):
    c = candidates[0]
    v = [mystic.models.rosen(c.fitValues)]

    j = MockJob(c)

    if self.afterMerit:
      self.afterMerit(v, [(c, [j])])

    return v

class MockMerit(object):
  """Mock merit object which returns squared sum of variable values."""

  def __init__(self):
    self.afterMerit = None

  def calculate(self, candidates, returnCandidateJobPairs = False):
    retvals = []
    amvals = []
    for c in candidates:
      j = MockJob(c)
      vsum = sum([ v*v for (k,v) in c.variablePairs ])
      retvals.append(vsum)
      amvals.append((c, [j]))

    if self.afterMerit:
      self.afterMerit(retvals, amvals)

    if returnCandidateJobPairs:
      return (retvals, amvals)
    return retvals


class StepCallBack(object):

  def __init__(self):
    self.stepDicts = []
    self.stepNum = 0

  def __call__(self, minimizerResults):
    if self.stepNum > 11:
      return
    self.stepNum += 1

    indexOfBest = minimizerResults.indexOfBest
    mval = minimizerResults.meritValues[indexOfBest]
    variables = minimizerResults.candidateJobList[indexOfBest][0]
    cdict = {}
    cdict['meritval'] = mval
    vdict = dict(variables.variablePairs)
    cdict.update(vdict)
    self.stepDicts.append(cdict)
