import logging


from atsim.pro_fit.variables import VariableException
from atsim.pro_fit.variables import BoundedVariableBaseClass

from ._inspyred_common import (
    _IntConvert,
    _FloatConvert,
    _RandomSeed,
    _ChoiceConvert,
    _EvolutionaryComputationMinimizerBaseClass,
    Population_To_Generator_Adapter,
)

from atsim.pro_fit.minimizers.population_generators import (
    Latin_Hypercube_InitialPopulation,
    Uniform_Random_Initial_Population,
)


import inspyred

from atsim.pro_fit.exceptions import ConfigException


class Particle_SwarmMinimizer(object):
    """Particle Swarm minimizer.

  This class wraps the PSO minimizer provided by the Inspyred package"""

    logger = logging.getLogger(
        "atsim.pro_fit.minimizers.Particle_SwarmMinimizer"
    )

    def __init__(self, initialVariables, topology, **args):
        # Configure the terminator
        terminator = inspyred.ec.terminators.generation_termination

        # Build the PSO object
        import random

        r = random.Random()
        r.seed(args["random_seed"])

        pso = inspyred.swarm.PSO(r)
        pso.terminator = terminator
        pso.topology = topology

        # Create initial population from Latin Hyper Cube
        initial_population = Latin_Hypercube_InitialPopulation(
            initialVariables,
            args["population_size"],
            criterion=Latin_Hypercube_InitialPopulation.Criterion.center,
        )

        generator = Population_To_Generator_Adapter(
            initialVariables,
            Uniform_Random_Initial_Population(initialVariables, 1),
        )

        self._minimizer = _EvolutionaryComputationMinimizerBaseClass(
            generator,
            pso,
            args["population_size"],
            initial_population,
            inertia=args["inertia"],
            cognitive_rate=args["cognitive_rate"],
            social_rate=args["social_rate"],
            max_generations=args["max_iterations"],
        )

    def minimize(self, merit):
        return self._minimizer.minimize(merit)

    def _setStepCallBack(self, callback):
        self._minimizer.stepCallback = callback

    def _getStepCallBack(self):
        return self._minimizer.stepCallback

    stepCallback = property(fget=_getStepCallBack, fset=_setStepCallBack)

    def stopMinimizer(self):
        self._minimizer.stopMinimizer()

    @staticmethod
    def createFromConfig(variables, configitems):
        """Create Particle_SwarmMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atsim.pro_fit.variables.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of Particle_SwarmMinimizer"""

        # Check bounds are defined
        try:
            BoundedVariableBaseClass(variables)
        except VariableException as e:
            raise ConfigException("Particle_Swarm Minimizer:" + str(e))

        cfgdict = dict(configitems)
        del cfgdict["type"]

        clsname = "Particle_Swarm minimizer"
        defaults = dict(
            topology=(
                "star",
                _ChoiceConvert(clsname, "topology", ["star", "ring"]),
            ),
            neighbourhood_size=(3, _IntConvert(clsname, "neighbourhood_size")),
            inertia=(
                0.5,
                _FloatConvert(clsname, "inertia", (0, float("inf"))),
            ),
            cognitive_rate=(
                2.1,
                _FloatConvert(clsname, "cognitive_rate", (0, float("inf"))),
            ),
            social_rate=(
                2.1,
                _FloatConvert(clsname, "social_rate", (0, float("inf"))),
            ),
            max_iterations=(
                1000,
                _IntConvert(clsname, "max_iterations", (1, float("inf"))),
            ),
            population_size=(
                64,
                _IntConvert(clsname, "population_size", (2, float("inf"))),
            ),
            random_seed=(None, _RandomSeed(clsname, "random_seed")),
        )

        # Throw if cfgdict has any keys not in defaults
        for k in cfgdict.keys():
            if k not in defaults:
                raise ConfigException(
                    "Unknown configuration option '%s' for Particle_Swarm minimizer"
                    % (k,)
                )

        # Override any values specified in cfgdict.
        optiondict = {}
        for k, (default, converter) in defaults.items():
            optiondict[k] = converter(cfgdict.get(k, converter(default)))

        # Configure topology
        if optiondict["topology"] == "star":
            if "neighbourhood_size" in cfgdict:
                raise ConfigException(
                    "Particle_Swarm Minimizer, the 'neighbourhood_size' option can only be used with 'topology' = 'ring'"
                )
            del optiondict["neighbourhood_size"]
            topology = inspyred.swarm.topologies.star_topology
        else:
            # Check bounds for ring topology
            nsize = optiondict["neighbourhood_size"]
            if nsize < 2 or nsize > optiondict["population_size"] - 1:
                raise ConfigException(
                    "Particle_Swarm Minimizer, the value of the 'neighbourhood_size' option should be between 2 and %d (population_size-1)"
                    % (optiondict["population_size"] - 1,)
                )
            topology = inspyred.swarm.topologies.ring_topology

        # Log the options
        Particle_SwarmMinimizer.logger.info(
            "Configuring Particle_SwarmMinimizer with following options:"
        )
        for k, v in optiondict.items():
            Particle_SwarmMinimizer.logger.info("%s = %s" % (k, v))

        del optiondict["topology"]

        return Particle_SwarmMinimizer(variables, topology, **optiondict)
