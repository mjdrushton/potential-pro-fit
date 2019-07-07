import logging
import itertools
import math

import inspyred

from atsim.pro_fit.variables import BoundedVariableBaseClass
from atsim.pro_fit.variables import VariableException
from ._inspyred_common import (
    _IntConvert,
    _FloatConvert,
    _RandomSeed,
    _EvolutionaryComputationMinimizerBaseClass,
)


from atsim.pro_fit.minimizers._inspyred._config import (
    Initial_Population_Config_Helper,
)

from atsim.pro_fit.exceptions import ConfigException


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

        # Create initial population from Latin Hyper Cube
        initial_population = args["initial_population"]

        generator = args["generator"]

        self._minimizer = _EvolutionaryComputationMinimizerBaseClass(
            generator,
            dea,
            args["population_size"],
            initial_population,
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
        clsname = "DEA minimizer"
        defaults = dict(
            num_selected=(
                2,
                _IntConvert(clsname, "num_selected", (2, float("inf"))),
            ),
            tournament_size=(
                2,
                _IntConvert(clsname, "tournament_size", (2, float("inf"))),
            ),
            crossover_rate=(
                1.0,
                _FloatConvert(clsname, "crossover_rate", (0.0, 1.0)),
            ),
            mutation_rate=(
                0.1,
                _FloatConvert(clsname, "mutation_rate", (0.0, 1.0)),
            ),
            gaussian_mean=(0, _FloatConvert(clsname, "gaussian_mean")),
            gaussian_stdev=(
                1,
                _FloatConvert(clsname, "gaussian_stdev", (1e-3, float("inf"))),
            ),
        )

        cfg_helper = Initial_Population_Config_Helper(clsname)

        # Throw if cfgdict has any keys not in defaults
        relevant_keys = set(itertools.chain(defaults.keys(), cfg_helper.default_keys))
        for k in relevant_keys:
            if k not in relevant_keys:
                raise ConfigException(
                    "Unknown configuration option '{}' for DEA minimizer".format(
                        k
                    )
                )

        # Override any values specified in cfgdict.
        optiondict = cfg_helper.parse(variables, cfgdict)
        for k, (default, converter) in defaults.items():
            optiondict[k] = converter(cfgdict.get(k, converter(default)))

        # Log the options
        DEAMinimizer.logger.info(
            "Configuring DEAMinimizer with following options:"
        )
        for k, v in optiondict.items():
            DEAMinimizer.logger.info("%s = %s" % (k, v))

        return DEAMinimizer(variables, **optiondict)
