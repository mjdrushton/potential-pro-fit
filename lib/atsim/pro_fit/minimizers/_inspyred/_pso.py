import logging


from _inspyred_common import VariableException
from _inspyred_common import _BoundedVariableBaseClass

from _inspyred_common import _IntConvert
from _inspyred_common import _FloatConvert
from _inspyred_common import _RandomSeed
from _inspyred_common import _EvolutionaryComputationMinimizerBaseClass

import inspyred

from atsim.pro_fit.fittool import ConfigException


class Particle_SwarmMinimizer(object):
  """Particle Swarm minimizer.

  This class wraps the PSO minimizer provided by the Inspyred package"""

  logger = logging.getLogger("atsim.pro_fit.minimizers.Particle_SwarmMinimizer")

  def __init__(self, initialVariables, **args):
    # Configure the terminator
    terminator = inspyred.ec.terminators.generation_termination

    # Build the PSO object
    import random
    r = random.Random()
    r.seed(args['random_seed'])

    pso = inspyred.swarm.PSO(r)
    pso.terminator = terminator

    #TODO: allow different topologies to be specified.
    # pso.topology =

    self._minimizer = _EvolutionaryComputationMinimizerBaseClass(
      initialVariables,
      pso,
      args['population_size'],
      inertia = args['inertia'],
      cognitive_rate = args['cognitive_rate'],
      social_rate = args['social_rate'],
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
    """Create Particle_SwarmMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atsim.pro_fit.fittool.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of Particle_SwarmMinimizer"""

    # Check bounds are defined
    try:
      _BoundedVariableBaseClass(variables)
    except VariableException,e:
      raise ConfigException("Particle_Swarm Minimizer:"+e.message)

    cfgdict = dict(configitems)
    del cfgdict['type']



    defaults = dict(
      inertia = (0.5, _FloatConvert("Particle_Swarm minimizer", "inertia", (0, float("inf")))),
      cognitive_rate = (2.1, _FloatConvert("Particle_Swarm minimizer", "cognitive_rate", (0, float("inf")))),
      social_rate = (2.1, _FloatConvert("Particle_Swarm minimizer", "social_rate", (0, float("inf")))),
      max_iterations = (1000, _IntConvert("Particle_Swarm minimizer", "max_iterations", (1, float("inf")))),
      population_size = (64, _IntConvert("DEA minimizer", "population_size", (2, float("inf")))),
      random_seed = (None, _RandomSeed("Particle_Swarm minimizer", "random_seed")))

    # Throw if cfgdict has any keys not in defaults
    for k in cfgdict.iterkeys():
      if not defaults.has_key(k):
        raise ConfigException("Unknown configuration option '%s' for Particle_Swarm minimizer" % (k,))

    # Override any values specified in cfgdict.
    optiondict = {}
    for k, (default, converter) in defaults.iteritems():
      optiondict[k] = converter(cfgdict.get(k, converter(default)))

    # Log the options
    Particle_SwarmMinimizer.logger.info("Configuring Particle_SwarmMinimizer with following options:")
    for k, v in optiondict.iteritems():
      Particle_SwarmMinimizer.logger.info("%s = %s" % (k,v))

    return Particle_SwarmMinimizer(variables, **optiondict)
