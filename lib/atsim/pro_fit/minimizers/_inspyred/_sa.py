import logging

import math

from atsim.pro_fit.variables import BoundedVariableBaseClass
from atsim.pro_fit.variables import VariableException
from ._inspyred_common import (
    _EvolutionaryComputationMinimizerBaseClass,
    Population_To_Generator_Adapter,
)

from atsim.pro_fit.cfg import (
    int_convert,
    float_convert,
    random_seed_option,
)

from atsim.pro_fit.minimizers.population_generators import (
    Predefined_Initial_Population,
    Uniform_Random_Initial_Population,
)

import inspyred

from atsim.pro_fit.exceptions import ConfigException
from atsim.pro_fit.variables import Variables

from .._common import MinimizerResults


class _TemperatureVariableReporter(object):
    """Wraps stepcallback, injecting temperature from bounder into minimizer results"""

    def __init__(self, bounder, stepCallback):
        self.bounder = bounder
        self.callback = stepCallback

    def __call__(self, minimizerResults):
        if self.callback == None:
            return

        if self.bounder.variableIndex != None:
            jobLists = minimizerResults.candidateJobList
            idx = self.bounder.variableIndex
            temp = self.bounder.minimizerTemperature

            updatedCandidateJobList = []

            for (variables, joblist) in jobLists:
                fvp = variables.flaggedVariablePairs
                n, _v, flag = fvp[idx]
                fvp[idx] = (n, temp, flag)
                variables = Variables(fvp)
                updatedCandidateJobList.append((variables, joblist))
            minimizerResults = MinimizerResults(
                minimizerResults.meritValues, updatedCandidateJobList
            )

        self.callback(minimizerResults)


class _TemperatureVariableBounder(object):
    """Inspyred Bounder used to inject temperature of Simulated_AnnealingMinimizer
  into a variable, for debugging purposes."""

    _logger = logging.getLogger(
        "atsim.pro_fit.minimizers.Simulated_AnnealingMinimizer._TemperatureVariableBounder"
    )

    def __init__(self, variables, chosenVariable):
        """@param chosenVariable Id of variable into which temperature should be injected (or None if no injection should
          be performed."""
        self.variables = variables
        self.chosenVariable = chosenVariable
        self.variableIndex = None
        self.minimizerTemperature = 0

        if chosenVariable:
            self.variableIndex = [
                i
                for (i, (k, v)) in enumerate(variables.variablePairs)
                if k == chosenVariable
            ][0]
            self._logger.debug(
                "chosenVariable = '%s', index is %d"
                % (self.chosenVariable, self.variableIndex)
            )

    def getStepCallBack(self, callback):
        return _TemperatureVariableReporter(self, callback)

    def __call__(self, candidate, args):
        minimizerTemperature = args["temperature"]
        self._logger.info(
            "Simulated_Annealing Minimizer Temperature = %f"
            % minimizerTemperature
        )
        self.minimizerTemperature = minimizerTemperature

    @classmethod
    def createFromConfig(cls, variables, configitems):
        cfgdict = dict(configitems)

        if "temperature_variable" not in cfgdict:
            cls._logger.debug(
                "'temperature_variable' not found. Returning generic _TemperatureVariableBounder"
            )
            return _TemperatureVariableBounder(variables, None)

        tvar = cfgdict["temperature_variable"].strip()

        # Does target variable exist?
        if not tvar in [k for (k, v) in variables.variablePairs]:
            raise ConfigException(
                "No variable named '%s' as required by 'temperature_variable' configuration option for Simulated_Annealing minimizer."
                % tvar
            )

        # Check that variable isn't a fitting variable
        if tvar in variables.fitKeys:
            raise ConfigException(
                "'temperature_variable' configuration option for Simulated_Annealing minimizer cannot specify variable that is altered during fitting."
            )

        cls._logger.info(
            "Simulated_Annealing minimizer 'temperature_variable' option specified. Minimizer will report its temperature to the '%s' variable."
            % tvar
        )

        return _TemperatureVariableBounder(variables, tvar)


class Simulated_AnnealingMinimizer(object):
    """Simulated Annealing minimizer.

  This class wraps the SA minimizer provided by the Inspyred package"""

    logger = logging.getLogger(
        "atsim.pro_fit.minimizers.Simulated_AnnealingMinimizer"
    )

    def __init__(self, initialVariables, temperatureVariableBounder, **args):
        # Configure the terminator
        terminator = inspyred.ec.terminators.generation_termination

        # Build the SA object
        import random

        r = random.Random()
        r.seed(args["random_seed"])

        sa = inspyred.ec.SA(r)
        sa.terminator = terminator

        self._tbound = temperatureVariableBounder

        if self._tbound:
            sa.bounder = temperatureVariableBounder
            sa.bounder.minimizerTemperature = args["temperature"]

        # Create initial population from variables
        initial_population = Predefined_Initial_Population(
            initialVariables, from_array=[initialVariables.fitValues]
        )

        generator = Population_To_Generator_Adapter(
            initialVariables,
            Uniform_Random_Initial_Population(initialVariables, 1),
        )

        self._minimizer = _EvolutionaryComputationMinimizerBaseClass(
            generator,
            sa,
            1,
            initial_population,
            temperature=args["temperature"],
            cooling_rate=args["cooling_rate"],
            mutation_rate=args["mutation_rate"],
            gaussian_mean=args["gaussian_mean"],
            gaussian_stdev=args["gaussian_stdev"],
            max_generations=args["max_iterations"],
        )

    def minimize(self, merit):
        return self._minimizer.minimize(merit)

    def _setStepCallBack(self, callback):
        if self._tbound:
            callback = self._tbound.getStepCallBack(callback)
        self._minimizer.stepCallback = callback

    def _getStepCallBack(self):
        return self._minimizer.stepCallback

    stepCallback = property(fget=_getStepCallBack, fset=_setStepCallBack)

    def stopMinimizer(self):
        self._minimizer.stopMinimizer()

    @staticmethod
    def createFromConfig(variables, configitems):
        """Create Simulated_AnnealingMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atsim.pro_fit.variables.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of Simulated_AnnealingMinimizer"""

        # Check bounds are defined
        try:
            BoundedVariableBaseClass(variables)
        except VariableException as e:
            raise ConfigException("Simulated_Annealing Minimizer:" + str(e))

        cfgdict = dict(configitems)
        del cfgdict["type"]

        if "temperature_variable" in cfgdict:
            del cfgdict["temperature_variable"]

        defaults = dict(
            temperature=(
                0,
                float_convert(
                    "Simulated_Annealing minimizer",
                    "temperature",
                    (0, float("inf")),
                ),
            ),
            cooling_rate=(
                0.01,
                float_convert(
                    "Simulated_Annealing minimizer", "cooling_rate", (0.0, 1.0)
                ),
            ),
            mutation_rate=(
                0.1,
                float_convert(
                    "Simulated_Annealing minimizer",
                    "mutation_rate",
                    (0.0, 1.0),
                ),
            ),
            gaussian_mean=(
                0,
                float_convert(
                    "Simulated_Annealing minimizer", "gaussian_mean"
                ),
            ),
            gaussian_stdev=(
                1,
                float_convert(
                    "Simulated_Annealing minimizer",
                    "gaussian_stdev",
                    (1e-3, float("inf")),
                ),
            ),
            max_iterations=(
                1000,
                int_convert(
                    "Simulated_Annealing minimizer",
                    "max_iterations",
                    (1, float("inf")),
                ),
            ),
            random_seed=(
                None,
                random_seed_option("Simulated_Annealing minimizer", "random_seed"),
            ),
        )

        # Throw if cfgdict has any keys not in defaults
        for k in cfgdict.keys():
            if k not in defaults:
                raise ConfigException(
                    "Unknown configuration option '%s' for Simulated_Annealing minimizer"
                    % (k,)
                )

        # Override any values specified in cfgdict.
        optiondict = {}
        for k, (default, converter) in defaults.items():
            optiondict[k] = converter(cfgdict.get(k, converter(default)))

        tbound = _TemperatureVariableBounder.createFromConfig(
            variables, configitems
        )

        # Log the options
        Simulated_AnnealingMinimizer.logger.info(
            "Configuring Simulated_AnnealingMinimizer with following options:"
        )
        for k, v in optiondict.items():
            Simulated_AnnealingMinimizer.logger.info("%s = %s" % (k, v))

        return Simulated_AnnealingMinimizer(variables, tbound, **optiondict)
