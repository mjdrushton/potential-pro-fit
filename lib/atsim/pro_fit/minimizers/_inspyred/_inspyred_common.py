import math

import inspyred

from .._common import MinimizerResults

from atsim.pro_fit._util import MultiCallback

from atsim.pro_fit.fittool import ConfigException


class VariableException(Exception):
  """Exception raised by inspyred related classes when a problem is found with
  input atsim.pro_fit.fittool.Variables instances"""
  pass


class _BoundedVariableBaseClass(object):
  """Abstract base for objects that should throw if a Variables instance passed to constructor
  does not have definite bounds for all fit parameters.

  Stores variables in _variables property. Bounds are stored in inspyred two list form, in _bounds"""

  def __init__(self, variables):
    """@param variables Variables instance whose bounds are used to generate
              bounder and generator. Note: all fitted parameters must
              have definite upper and lower bounds inf/-inf bounds are
              not supported"""
    self._initialVariables = variables
    self._bounds = self._populateBounds()

  def _populateBounds(self):
    lower = []
    upper = []
    for ((n, v, isFit), (lb, ub)) in zip(self._initialVariables.flaggedVariablePairs, self._initialVariables.bounds):
      if not isFit:
        continue
      elif lb == None or lb == float("-inf"):
        raise VariableException("Lower bound for variable: %s cannot be infinite." % n)
      elif ub == None or ub == float("inf"):
        raise VariableException("Upper bound for variable: %s cannot be infinite." % n)
      else:
        lower.append(lb)
        upper.append(ub)
    if not lower:
      raise VariableException("Not parameters enabled for fitting")
    return [lower, upper]


class Bounder(_BoundedVariableBaseClass):
  """Inspyred bounder populated from Variables instance"""

  def __init__(self, variables):
    """@param variables Variables instance defining bounds"""
    _BoundedVariableBaseClass.__init__(self, variables)
    self._bounder = inspyred.ec.Bounder(*self._bounds)

  def __call__(self, candidate, args):
    """@param candidate candidate solution
       @param args Args dictionary
       @return Bounded candidate"""
    return self._bounder(candidate, args)

class Generator(_BoundedVariableBaseClass):
  """Inspyred generator that generates bounded candidates from bounds stored in a Variables instance"""

  def __call__(self, random, args):
    """Inspyred generator.

    @param random random.Random instance passed in by Inspyred
    @param args Args dictionary (not used here)
    @return Candidate with length == adjustable parameters in self.initialVariables sitting within
      limits defined by variable bounds."""
    candidate = []
    for (l,h) in zip(self._bounds[0], self._bounds[1]):
      candidate.append(random.uniform(l,h))
    return candidate

class Evaluator(object):
  """Class that wraps Merit function and adapts it for the inspyred EvolutionaryComputation minimizers.

  Also responsible for calling the stepCallback of the fitting tool minimizers that use this class.
  Also maintains instance of MinimizerResults containing generation with best merit values"""

  __name__ = "atsim.pro_fit.minimizers._inspyred.Evaluator"

  def __init__(self, initialVariables, merit):
    """@param initialVariables Variables instance.
       @param merit atsim.pro_fit.fittool.Merit instance"""

    self._initialVariables = initialVariables
    self._merit = merit

  def __call__(self, candidates, args):
    variables = [ self._initialVariables.createUpdated(c) for c in candidates ]
    meritVals, candidateJobList = self._merit.calculate(variables, True)

    fitnessJobList = []
    for mv, cj in zip(meritVals, candidateJobList):
      if not math.isnan(mv):
        fj = FitnessJob(mv)
        fj.merit = mv
        fj.candidateJobTuple = cj
        fitnessJobList.append(fj)
      else:
        fitnessJobList.append(None)

    return fitnessJobList

class FitnessJob(float):
  """Need way of passing job information, fitness and candidates intact through inspyred.
  Keeping merit value and candidate with the evaluator information required by reporters
  is tricky, therefore, the evaluator overrides float and adds extra properties:
    merit and candidateJobTuple
  to keep everything bundled together.

  Note: I know this is hacky and probably fragile (observer asserts for fitness value to be this
    type as some sort of protection)"""
  pass


class Observer(object):
  """Inspyred observer, responsible for calling the step-callback with current population.

  Extracts values from Individual.fitness which should be instance of FitnessJob and therefore
  contain the candidateJobTuple information injected by the evaluator.

  Also keeps track of the generation containing the best ever solution accessible through the
  bestMinimizerResults property."""


  __name__ = "atsim.pro_fit.minimizers._inspyred.Observer"

  def __init__(self, stepCallback):
    """@param stepCallback Step callback called with single argument: MinimizerResults instance"""
    self.bestMinimizerResults = None
    self.stepCallback = stepCallback

  def __call__(self, population, num_generations, num_evaluations, args):
    meritVals = []
    candidateJobList = []
    for ind in population:
      fj = ind.fitness
      assert(type(fj) == FitnessJob)
      meritVals.append(fj.merit)
      candidateJobList.append(fj.candidateJobTuple)

    minimizerResults = MinimizerResults(meritVals, candidateJobList)

    if self.stepCallback:
      self.stepCallback(minimizerResults)

    if not self.bestMinimizerResults:
      self.bestMinimizerResults = minimizerResults
    elif minimizerResults < self.bestMinimizerResults:
      self.bestMinimizerResults = minimizerResults



class _EvolutionaryComputationMinimizerBaseClass(object):
  """Base class for fittingTool minimizers that wrap minimizers based on inspyred.ec.EvolutionaryComputation."""

  """Maximum number of function evaluations allowed during minimization"""
  maximumEvaluations = 30000

  def __init__(self, initialVariables, evolutionaryComputation, populationSize, **args):
    """@param initialVariables atsim.pro_fit.fittool.Variables instance providing bounds for generator and bounder applied to
                population used within evolutionaryComputation.
       @param evolutionaryComputation Instance of inspyred.ec.EvolutionaryComputation.
                Note: the evaluator, bounder and generator of the evolutionaryComputation are overwritten by this class.
       @param populationSize Size of population used for evolutionary computation.
       @param args Dictionary containing optional keyword arguments that should be passed to the evolutionaryComputation.evolve method"""

    self._initialVariables = initialVariables
    self._ec = evolutionaryComputation
    self._args = args
    self._populationSize = populationSize
    self.stepCallback = None


  def minimize(self, merit):
    """Perform minimization.

    @param merit atsim.pro_fit.fittool.Merit instance used to calculate merit value.
    @return MinimizerResults for candidate solution population containing best merit value."""

    bounder = Bounder(self._initialVariables)
    generator = Generator(self._initialVariables)
    evaluator = Evaluator(self._initialVariables, merit)
    observer = Observer(self.stepCallback)
    origobserver = observer


    # Respect any callbacks already registered on the inspyred minimizer
    if self._ec.bounder:
      bounder = MultiCallback([self._ec.bounder, bounder], retLast = True)

    if self._ec.generator:
      generator = MultiCallback([self._ec.generator, generator], retLast = True)

    if self._ec.evaluator:
      evaluator = MultiCallback([self._ec.evaluator, evaluator], retLast = True)

    if self._ec.observer:
      observer = MultiCallback([self._ec.observer, observer], retLast = True)

    self._ec.observer = observer

    self._ec.evolve(
      generator,
      evaluator,
      bounder = bounder,
      pop_size = self._populationSize,
      maximize = False,
      **self._args)
    return origobserver.bestMinimizerResults


def _convertFactory(clsname, key, convfunc, bounds):
  def f(v):
    try:
      v = convfunc(v)
    except:
      raise ConfigException("Could not parse option '%s' for %s: %s" % (key, clsname, v))

    if bounds:
      if not (v >= bounds[0] and v <= bounds[1]):
        raise ConfigException("Option value does not lie within bounds (%s, %s). Option key '%s' for %s: %s" % (bounds[0], bounds[1], key, clsname, v))
    return v
  return f


def _IntConvert(clsname, key, bounds = None):
  return _convertFactory(clsname, key, int, bounds)

def _FloatConvert(clsname, key, bounds = None):
  return _convertFactory(clsname, key, float, bounds)

def _RandomSeed(clsname, key):
  iconv = _IntConvert(clsname, key, (0, float("inf")))
  import time
  def f(v):
    if v:
      return iconv(v)
    else:
      return int(time.time())
  return f

def _ChoiceConvert(clsname, key, choices):
  choices = set(choices)
  choicestring = sorted(list(choices))
  if len(choicestring) == 2:
    choicestring = " or ".join(choicestring)
  else:
    choicestring = ", ".join(choicestring)


  def f(v):
    v = v.strip()
    if not v in choices:
      raise ConfigException("Could not parse option '%s' for %s. Value '%s' should be one of %s. " % (key, clsname, v, choicestring))
    return v
  return f


