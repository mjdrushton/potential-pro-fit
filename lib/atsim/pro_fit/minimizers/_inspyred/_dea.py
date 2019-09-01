import logging
import itertools
import math
import sys

import inspyred

from atsim.pro_fit.variables import BoundedVariableBaseClass
from atsim.pro_fit.variables import VariableException
from ._inspyred_common import _EvolutionaryComputationMinimizerBaseClass

import atsim.pro_fit.cfg

from atsim.pro_fit.exceptions import ConfigException


class DEAMinimizer(object):
    """Differential Evoluation Algorithm Minimizer.

  This class wraps the DEA minimizer provided by the Inspyred package"""

    logger = logging.getLogger("atsim.pro_fit.minimizers.DEAMinimizer")

    def __init__(
        self,
        initial_variables,
        initial_population,
        population_size,
        generator,
        random_seed,
        num_selected,
        tournament_size,
        crossover_rate,
        mutation_rate,
        gaussian_mean,
        gaussian_stdev,
        max_iterations,
    ):

        # Configure the terminator
        terminator = inspyred.ec.terminators.generation_termination

        # Build the DEA object
        import random

        r = random.Random()
        r.seed(random_seed)

        dea = inspyred.ec.DEA(r)
        dea.terminator = terminator

        # Create initial population from Latin Hyper Cube
        self._minimizer = _EvolutionaryComputationMinimizerBaseClass(
            generator,
            dea,
            population_size,
            initial_population,
            num_selected=num_selected,
            tournament_size=tournament_size,
            crossover_rate=crossover_rate,
            mutation_rate=mutation_rate,
            gaussian_mean=gaussian_mean,
            gaussian_stdev=gaussian_stdev,
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
        random_seed,
        num_selected,
        tournament_size,
        crossover_rate,
        mutation_rate,
        gaussian_mean,
        gaussian_stdev,
        max_iterations,
    ):
        population_factory = population(variables)
        initial_population = population_factory.population
        population_size = population_factory.population_size
        generator = population_factory.generator

        minimizer = cls(
            variables,
            initial_population,
            population_size,
            generator,
            random_seed,
            num_selected,
            tournament_size,
            crossover_rate,
            mutation_rate,
            gaussian_mean,
            gaussian_stdev,
            max_iterations,
        )

        return minimizer

    @classmethod
    def createFromConfig(cls, variables, configitems):
        """Create DEAMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atsim.pro_fit.variables.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of DEAMinimizer"""

        # Check bounds are defined
        try:
            BoundedVariableBaseClass(variables)
        except VariableException as e:
            raise ConfigException("DEA Minimizer:" + str(e))

        clsname = "DEA minimizer"

        cfgparse = atsim.pro_fit.cfg.Create_From_Config_Parser(clsname)

        cfgparse.add_int_option(
            "num_selected", "num_selected", bounds=(2, sys.maxsize), default=2
        ).add_int_option(
            "tournament_size",
            "tournament_size",
            bounds=(2, sys.maxsize),
            default=2,
        ).add_float_option(
            "crossover_rate", "crossover_rate", bounds=(0.0, 1.0), default=1.0
        ).add_float_option(
            "mutation_rate", "mutation_rate", bounds=(0.0, 1.0), default=0.1
        ).add_float_option(
            "gaussian_mean", "gaussian_mean", default=0
        ).add_float_option(
            "gaussian_stdev",
            "gaussian_stdev",
            bounds=(1e-6, float("inf")),
            default=1.0,
        ).add_random_seed_option(
            "random_seed", "random_seed"
        ).add_int_option(
            "max_iterations",
            "max_iterations",
            bounds=(1, sys.maxsize),
            default=1000,
        )

        atsim.pro_fit.cfg.add_initial_population_options(cfgparse)
        parsed_options = cfgparse.parse(configitems)
        cfgparse.log_options(parsed_options, DEAMinimizer.logger)
        minimizer = cls._create_minimizer_instance(
            *cfgparse.options_to_function_args(
                parsed_options,
                cls._create_minimizer_instance,
                {"variables": variables},
            )
        )

        return minimizer
