import logging

import math

from _inspyred_common import _BoundedVariableBaseClass
from _inspyred_common import _IntConvert
from _inspyred_common import _FloatConvert
from _inspyred_common import _RandomSeed
from _inspyred_common import _EvolutionaryComputationMinimizerBaseClass

import inspyred

class Simulated_AnnealingMinimizer(object):
  """Simulated Annealing minimizer.

  This class wraps the SA minimizer provided by the Inspyred package"""


  logger = logging.getLogger("atsim.pro_fit.minimizers.Simulated_AnnealingMinimizer")


  def __init__(self, initialVariables, **args):
    # Configure the terminator
    terminator = inspyred.ec.terminators.generation_termination

    # Build the SA object
    import random
    r = random.Random()
    r.seed(args['random_seed'])

    sa = inspyred.ec.SA(r)
    sa.terminator = terminator

    self._minimizer = _EvolutionaryComputationMinimizerBaseClass(initialVariables,
      sa,
      1,
      temperature = args['temperature'],
      cooling_rate = args['cooling_rate'],
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
    """Create Simulated_AnnealingMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atsim.pro_fit.fittool.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of Simulated_AnnealingMinimizer"""

    # Check bounds are defined
    try:
      _BoundedVariableBaseClass(variables)
    except VariableException,e:
      raise ConfigException("Simulated Minimizer:"+e.message)

    cfgdict = dict(configitems)
    del cfgdict['type']

    defaults = dict(
      temperature = (0, _FloatConvert("Simulated_Annealing minimizer", "temperature", (0, float("inf")))),
      cooling_rate = (0.01, _FloatConvert("Simulated_Annealing minimizer", "cooling_rate", (0.0, 1.0))),
      mutation_rate = (0.1, _FloatConvert("Simulated_Annealing minimizer", "mutation_rate", (0.0, 1.0))),
      gaussian_mean = (0, _FloatConvert("Simulated_Annealing minimizer", "gaussian_mean")),
      gaussian_stdev = (1, _FloatConvert("Simulated_Annealing minimizer", "gaussian_stdev", (1e-3, float("inf")))),
      max_iterations = (1000, _IntConvert("Simulated_Annealing minimizer", "max_iterations", (1, float("inf")))),
      random_seed = (None, _RandomSeed("Simulated_Annealing minimizer", "random_seed")))

    # Throw if cfgdict has any keys not in defaults
    for k in cfgdict.iterkeys():
      if not defaults.has_key(k):
        raise ConfigException("Unknown configuration option '%s' for Simulated_Annealing minimizer" % (k,))

    # Override any values specified in cfgdict.
    optiondict = {}
    for k, (default, converter) in defaults.iteritems():
      optiondict[k] = converter(cfgdict.get(k, converter(default)))

    # Log the options
    Simulated_AnnealingMinimizer.logger.info("Configuring Simulated_AnnealingMinimizer with following options:")
    for k, v in optiondict.iteritems():
      Simulated_AnnealingMinimizer.logger.info("%s = %s" % (k,v))

    return Simulated_AnnealingMinimizer(variables, **optiondict)
