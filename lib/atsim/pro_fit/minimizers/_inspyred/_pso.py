import logging
import itertools
import sys

from atsim.pro_fit.variables import VariableException
from atsim.pro_fit.variables import BoundedVariableBaseClass

from ._inspyred_common import (
    _EvolutionaryComputationMinimizerBaseClass,
    Population_To_Generator_Adapter,
)


from atsim.pro_fit.cfg import Create_From_Config_Parser

import inspyred

from atsim.pro_fit.exceptions import ConfigException


class Particle_SwarmMinimizer(object):
    """Particle Swarm minimizer.

  This class wraps the PSO minimizer provided by the Inspyred package"""

    logger = logging.getLogger(
        "atsim.pro_fit.minimizers.Particle_SwarmMinimizer"
    )

    def __init__(
        self,
        intial_variables,
        topology,
        random_seed,
        initial_population,
        population_size,
        generator,
        inertia,
        cognitive_rate,
        social_rate,
        max_iterations,
    ):
        # Configure the terminator
        terminator = inspyred.ec.terminators.generation_termination

        # Build the PSO object
        import random

        r = random.Random()
        r.seed(random_seed)

        pso = inspyred.swarm.PSO(r)
        pso.terminator = terminator
        pso.topology = topology

        self._minimizer = _EvolutionaryComputationMinimizerBaseClass(
            generator,
            pso,
            population_size,
            initial_population,
            inertia=inertia,
            cognitive_rate=cognitive_rate,
            social_rate=social_rate,
            max_generations=max_iterations,
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

    @classmethod
    def _create_minimizer_instance(
        cls,
        variables,
        population,
        topology,
        random_seed,
        neighbourhood_size,
        inertia,
        social_rate,
        cognitive_rate,
        max_iterations,
    ):
        topology = {
            "star": inspyred.swarm.topologies.star_topology,
            "ring": inspyred.swarm.topologies.ring_topology,
        }[topology]

        population_factory = population(variables)
        population = population_factory.population
        population_size = population.population_size
        generator = population_factory.generator
        minimizer = Particle_SwarmMinimizer(
            variables,
            topology,
            random_seed,
            population,
            population_size,
            generator,
            inertia,
            cognitive_rate,
            social_rate,
            max_iterations,
        )
        return minimizer

    @classmethod
    def _ring_topology_constraint(cls, out_args):
        topology = [
            o.value for o in out_args if o.option.cfg_key == "topology"
        ][0]

        nsize = [
            o.value
            for o in out_args
            if o.option.cfg_key == "neighbourhood_size"
        ][0]

        popsize = [
            o.value for o in out_args if o.option.cfg_key == "population"
        ][0].population_size

        if topology == "star" and nsize is None:
            raise ConfigException(
                "'neghbourhood_size' option can only be used with 'topology'=='ring'"
            )

        if topology == "ring":
            if nsize is None:
                nsize = 3
            if nsize < 2 or nsize > popsize - 1:
                raise ConfigException(
                    "Particle_Swarm Minimizer, the value of the 'neighbourhood_size' option should be between 2 and %d (population_size-1)"
                    % (popsize - 1,)
                )

        return out_args

    @classmethod
    def createFromConfig(cls, variables, config_items):
        """Create Particle_SwarmMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atsim.pro_fit.variables.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of Particle_SwarmMinimizer"""
        import atsim.pro_fit.cfg

        # Check bounds are defined
        try:
            BoundedVariableBaseClass(variables)
        except VariableException as e:
            raise ConfigException("Particle_Swarm Minimizer:" + str(e))

        clsname = "Particle_Swarm minimizer"
        cfgparse = Create_From_Config_Parser(clsname)

        atsim.pro_fit.cfg.add_initial_population_options(cfgparse)

        cfgparse.add_choices_option(
            "topology", "topology", ["star", "ring"], default="star"
        ).add_int_option(
            "neighbourhood_size",
            "neighbourhood_size",
            bounds=(3, sys.maxsize),
            default=3,
        ).add_float_option(
            "inertia", "inertia", bounds=(0, float("inf")), default=0.5
        ).add_float_option(
            "cognitive_rate",
            "cognitive_rate",
            bounds=(0, float("inf")),
            default=2.1,
        ).add_float_option(
            "social_rate", "social_rate", bounds=(0, float("inf")), default=2.1
        ).add_int_option(
            "max_iterations",
            "max_iterations",
            bounds=(1, sys.maxsize),
            default=1000,
        ).add_random_seed_option(
            "random_seed", "random_seed"
        )

        cfgparse.add_constraint(cls._ring_topology_constraint)

        parsed_options = cfgparse.parse(config_items)
        cfgparse.log_options(parsed_options, Particle_SwarmMinimizer.logger)

        args = cfgparse.options_to_function_args(
            parsed_options,
            cls._create_minimizer_instance,
            # drop_self=True,
            extra_args={"variables": variables},
        )
        minimizer = cls._create_minimizer_instance(*args)
        return minimizer
