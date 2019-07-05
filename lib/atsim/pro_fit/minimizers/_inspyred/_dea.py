import logging

import math

from atsim.pro_fit.minimizers.population_generators import UniformGenerator

from atsim.pro_fit.variables import BoundedVariableBaseClass
from atsim.pro_fit.variables import VariableException
from ._inspyred_common import _IntConvert
from ._inspyred_common import _FloatConvert
from ._inspyred_common import _RandomSeed
from ._inspyred_common import _EvolutionaryComputationMinimizerBaseClass

from atsim.pro_fit.exceptions import ConfigException


import inspyred


class DEAMinimizer(object):
    """Differential Evoluation Algorithm Minimizer.

  This class wraps the DEA minimizer provided by the Inspyred package"""

    logger = logging.getLogger("atsim.pro_fit.minimizers.DEAMinimizer")

    def __init__(self, initialVariables, **args):

        # Configure the terminator
        terminator = inspyred.ec.terminators.generation_termination

        # Build the DEA object
        import random

        r = random.Random()
        r.seed(args["random_seed"])

        dea = inspyred.ec.DEA(r)
        dea.terminator = terminator

        self._minimizer = _EvolutionaryComputationMinimizerBaseClass(
            UniformGenerator(initialVariables),
            dea,
            args["population_size"],
            num_selected=args["num_selected"],
            tournament_size=args["tournament_size"],
            crossover_rate=args["crossover_rate"],
            mutation_rate=args["mutation_rate"],
            gaussian_mean=args["gaussian_mean"],
            gaussian_stdev=args["gaussian_stdev"],
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
        """Create DEAMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atsim.pro_fit.variables.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of DEAMinimizer"""

        # Check bounds are defined
        try:
            BoundedVariableBaseClass(variables)
        except VariableException as e:
            raise ConfigException("DEA Minimizer:" + str(e))

        cfgdict = dict(configitems)
        del cfgdict["type"]

        defaults = dict(
            num_selected=(
                2,
                _IntConvert(
                    "DEA minimizer", "num_selected", (2, float("inf"))
                ),
            ),
            tournament_size=(
                2,
                _IntConvert(
                    "DEA minimizer", "tournament_size", (2, float("inf"))
                ),
            ),
            crossover_rate=(
                1.0,
                _FloatConvert("DEA minimizer", "crossover_rate", (0.0, 1.0)),
            ),
            mutation_rate=(
                0.1,
                _FloatConvert("DEA minimizer", "mutation_rate", (0.0, 1.0)),
            ),
            gaussian_mean=(0, _FloatConvert("DEA minimizer", "gaussian_mean")),
            gaussian_stdev=(
                1,
                _FloatConvert(
                    "DEA minimizer", "gaussian_stdev", (1e-3, float("inf"))
                ),
            ),
            max_iterations=(
                1000,
                _IntConvert(
                    "DEA minimizer", "max_iterations", (1, float("inf"))
                ),
            ),
            population_size=(
                64,
                _IntConvert(
                    "DEA minimizer", "population_size", (2, float("inf"))
                ),
            ),
            random_seed=(None, _RandomSeed("DEA minimizer", "random_seed")),
        )

        # Throw if cfgdict has any keys not in defaults
        for k in cfgdict.keys():
            if k not in defaults:
                raise ConfigException(
                    "Unknown configuration option '%s' for DEA minimizer"
                    % (k,)
                )

        # Override any values specified in cfgdict.
        optiondict = {}
        for k, (default, converter) in defaults.items():
            optiondict[k] = converter(cfgdict.get(k, converter(default)))

        # Log the options
        DEAMinimizer.logger.info(
            "Configuring DEAMinimizer with following options:"
        )
        for k, v in optiondict.items():
            DEAMinimizer.logger.info("%s = %s" % (k, v))

        return DEAMinimizer(variables, **optiondict)
