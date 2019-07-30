import logging
import itertools

from atsim.pro_fit.variables import VariableException
from atsim.pro_fit.variables import BoundedVariableBaseClass

from ._inspyred_common import (
    _EvolutionaryComputationMinimizerBaseClass,
    Population_To_Generator_Adapter,
)

from atsim.pro_fit.cfg import (
    int_convert,
    float_convert,
    random_seed_option,
    choice_convert,
)

from ._config import Initial_Population_Config_Helper

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
        initial_population = args["initial_population"]

        generator = args["generator"]

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
                choice_convert(clsname, "topology", ["star", "ring"]),
            ),
            neighbourhood_size=(3, int_convert(clsname, "neighbourhood_size")),
            inertia=(
                0.5,
                float_convert(clsname, "inertia", (0, float("inf"))),
            ),
            cognitive_rate=(
                2.1,
                float_convert(clsname, "cognitive_rate", (0, float("inf"))),
            ),
            social_rate=(
                2.1,
                float_convert(clsname, "social_rate", (0, float("inf"))),
            ),
        )

        cfg_helper = Initial_Population_Config_Helper(clsname)

        # Throw if cfgdict has any keys not in defaults
        relevant_keys = set(
            itertools.chain(defaults.keys(), cfg_helper.default_keys)
        )
        for k in cfgdict.keys():
            if k not in relevant_keys:
                raise ConfigException(
                    "Unknown configuration option '{}' for Particle_Swarm minimizer".format(
                        k
                    )
                )

        # Override any values specified in cfgdict
        optiondict = cfg_helper.parse(variables, cfgdict)
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
