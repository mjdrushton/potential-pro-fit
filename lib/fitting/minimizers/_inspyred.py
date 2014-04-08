import inspyred

import logging

import math

from _common import * # noqa
from atomsscripts.fitting.fittool import ConfigException

class VariableException(Exception):
  """Exception raised by inspyred related classes when a problem is found with
  input atomsscripts.fitting.fittool.Variables instances"""
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

  __name__ = "atomsscripts.fitting.minimizers._inspyred.Evaluator"

  def __init__(self, initialVariables, merit):
    """@param initialVariables Variables instance.
       @param merit atomsscripts.fitting.fittool.Merit instance"""

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


  __name__ = "atomsscripts.fitting.minimizers._inspyred.Observer"

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
    """@param initialVariables atomsscripts.fitting.fittool.Variables instance providing bounds for generator and bounder applied to
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

    @param merit atomsscripts.fitting.fittool.Merit instance used to calculate merit value.
    @return MinimizerResults for candidate solution population containing best merit value."""

    bounder = Bounder(self._initialVariables)
    generator = Generator(self._initialVariables)
    evaluator = Evaluator(self._initialVariables, merit)
    observer = Observer(self.stepCallback)

    self._ec.observer = observer

    self._ec.evolve(
      generator,
      evaluator,
      bounder = bounder,
      pop_size = self._populationSize,
      maximize = False,
      **self._args)
    return observer.bestMinimizerResults



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


class DEAMinimizer(object):
  """Differential Evoluation Algorithm Minimizer.

  This class wraps the DEA minimizer provided by the Inspyred package"""

  logger = logging.getLogger("atomsscripts.fitting.minimizers.DEAMinimizer")

  def __init__(self, initialVariables, **args):

    # Configure the terminator
    terminator = inspyred.ec.terminators.generation_termination

    # Build the DEA object
    import random
    r = random.Random()
    r.seed(args['random_seed'])

    dea = inspyred.ec.DEA(r)
    dea.terminator = terminator

    self._minimizer = _EvolutionaryComputationMinimizerBaseClass(initialVariables, dea,
      args['population_size'],
      num_selected = args['num_selected'],
      tournament_size = args['tournament_size'],
      crossover_rate = args['crossover_rate'],
      mutation_rate = args['mutation_rate'],
      gaussian_mean = args['gaussian_mean'],
      gaussian_stdev = args['gaussian_stdev'],
      max_generations = args['max_iterations'])

  def minimize(self, merit):
    return self._minimizer.minimize(merit)

  def _setStepCallBack(self, callback):
    self._minimizer.stepCallback = callback

  def _getStepCallBack(self):
    return self._minimizer.stepCallback

  stepCallback = property(fget = _getStepCallBack, fset = _setStepCallBack)


  @staticmethod
  def createFromConfig(variables, configitems):
    """Create DEAMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atomsscripts.fitting.fittool.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of DEAMinimizer"""

    # Check bounds are defined
    try:
      _BoundedVariableBaseClass(variables)
    except VariableException,e:
      raise ConfigException("DEA Minimizer:"+e.message)

    cfgdict = dict(configitems)
    del cfgdict['type']

    defaults = dict(
      num_selected = (2, _IntConvert("DEA minimizer", "num_selected", (2, float("inf")))),
      tournament_size = (2, _IntConvert("DEA minimizer", "tournament_size", (2, float("inf")))),
      crossover_rate = (1.0, _FloatConvert("DEA minimizer", "crossover_rate", (0.0, 1.0))),
      mutation_rate = (0.1, _FloatConvert("DEA minimizer", "mutation_rate", (0.0, 1.0))),
      gaussian_mean = (0, _FloatConvert("DEA minimizer", "gaussian_mean")),
      gaussian_stdev = (1, _FloatConvert("DEA minimizer", "gaussian_stdev", (1e-3, float("inf")))),
      max_iterations = (1000, _IntConvert("DEA minimizer", "max_iterations", (1, float("inf")))),
      population_size = (64, _IntConvert("DEA minimizer", "population_size", (2, float("inf")))),
      random_seed = (None, _RandomSeed("DEA minimizer", "random_seed")))

    # Throw if cfgdict has any keys not in defaults
    for k in cfgdict.iterkeys():
      if not defaults.has_key(k):
        raise ConfigException("Unknown configuration option '%s' for DEA minimizer" % (k,))

    # Override any values specified in cfgdict.
    optiondict = {}
    for k, (default, converter) in defaults.iteritems():
      optiondict[k] = converter(cfgdict.get(k, converter(default)))

    # Log the options
    DEAMinimizer.logger.info("Configuring DEAMinimizer with following options:")
    for k, v in optiondict.iteritems():
      DEAMinimizer.logger.info("%s = %s" % (k,v))

    return DEAMinimizer(variables, **optiondict)




