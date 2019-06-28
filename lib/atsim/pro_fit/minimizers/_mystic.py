import logging

from ._common import *  # noqa
from atsim.pro_fit.exceptions import ConfigException

import gevent
import mystic


class _NelderMeadMeritCallback(object):
    """Callback used by _NelderMeadStepMonitor to capture MinimizerResults for each iteration"""

    def __init__(self):
        self.reset()

    def __call__(self, meritVals, candidateJobList):
        self.currentIterationResults.append(
            MinimizerResults(meritVals, candidateJobList)
        )

    def iterationBest(self):
        return min(self.currentIterationResults)

    def reset(self):
        self.currentIterationResults = []


class _MysticCallbackMonitor(mystic.monitors.Monitor):
    """New versions of mystic change the StepMonitor interface with monitors
  now effectively having to be sub-classes of mystic.monitors.Monitor.

  This class allows a callable to be passed to a mystic solver to act as a StepMonitor"""

    def __init__(self, callback):
        super(_MysticCallbackMonitor, self).__init__()
        self._callback = callback

    def __call__(self, x, y, id=None, **kwds):
        self._callback(x, y)
        return super(_MysticCallbackMonitor, self).__call__(x, y, id, **kwds)


class _NelderMeadStepMonitor(object):
    """Mystic step monitor that stores merit value and variables for each NelderMeadMinimizer step.
  Furthermore, registers afterMerit with merit function allowing merit values and candidate
  job lists to be passed to NelderMeadMinimizer.stepCallback"""

    _logger = logging.getLogger("atsim.pro_fit.minimizers.NelderMeadMinimizer")

    def __init__(self, stepCallback, merit):
        self.bestSolution = None
        self.merit = merit
        self.stepCallback = stepCallback
        self.afterMerit = None

        # Register a memoizing callback with the merit function so we can access Job instances at each step.
        self._logger.debug(
            "Registering _NelderMeadMeritCallback with merit object"
        )
        if merit.afterMerit != None:
            raise ExistingCallbackException(
                "Merit object already has afterMerit registered."
            )
        afterMerit = _NelderMeadMeritCallback()
        self.merit.afterMerit = afterMerit
        self.afterMerit = afterMerit

    def __call__(self, x, fval):
        self._logger.debug(
            "_NelderMeadStepMonitor called with params: %s and fval:%s"
            % (x, fval)
        )
        currentBest = self.afterMerit.iterationBest()
        if self.bestSolution is None:
            self.bestSolution = currentBest
        elif currentBest < self.bestSolution:
            self._logger.debug(
                "Iteration step has improved global best meritValue"
            )
            self.bestSolution = currentBest

        # Call the stepCallback
        if self.stepCallback != None:
            self._logger.debug("Calling NelderMeadMinimizer.stepCallback")
            iterationBest = self.afterMerit.iterationBest()
            self.stepCallback(iterationBest)
        self.afterMerit.reset()

    def cleanUp(self):
        # Unregister callback from the merit function
        self.merit.afterMerit = None


class _NelderMeadInner(object):
    def __init__(self, variables, maxiter, xtol, ftol):
        """Create simplex minimizer

    @param variables Initial parameter set (atomsscript.fitting.variables.Variables)
    @param maxiter Maximum number of minimization max_iterations
    @param xtol Variable value convergence criterion
    @param ftol Merit function convergence criterion"""
        self._logger = logging.getLogger(__name__).getChild(
            "NelderMeadMinimizer"
        )
        self._initialVariables = variables
        self._maxIter = maxiter
        self._xtol = xtol
        self._ftol = ftol
        self._logger.debug(
            "Created NelderMeadMinimizer. initial Variables = %s, maximum iterations = %s, xtol = %s, ftol = %s"
            % (self._initialVariables, self._maxIter, self._xtol, self._ftol)
        )

    def _initialArgs(self):
        return self._initialVariables.fitValues

    def _argsToVariables(self, args):
        return self._initialVariables.createUpdated(args)

    def _meritWrapper(self, merit):
        def f(args):
            candidates = [self._argsToVariables(args)]
            meritvals = merit.calculate(candidates)
            return meritvals[0]

        return f

    def _getBounds(self):
        """If all bounds -inf, inf, return None.
    else return list of bound tuples for each fitted variable"""
        bounds = []
        allUnbounded = True
        for (k, v, f), bound in zip(
            self._initialVariables.flaggedVariablePairs,
            self._initialVariables.bounds,
        ):
            if f:
                if bound != (float("-inf"), float("inf")):
                    allUnbounded = False
                bounds.append(bound)
        if bounds and allUnbounded:
            return None
        return bounds

    def minimize(self, merit, stepevaluator):
        """Perform minimization.

    @param merit atsim.pro_fit.merit.Merit instance used to calculate merit value.
    @param stepevaluator _NelderMeadStepMonitor called every step, used to monitor minimizer progress.
    @return MinimizerResults for candidate solution population containing best merit value."""
        self._logger.info("Starting minimisation.")
        optifunc = self._meritWrapper(merit)
        initargs = self._initialArgs()
        minimizer = mystic.scipy_optimize.NelderMeadSimplexSolver(len(initargs))
        minimizer.SetInitialPoints(initargs)
        minimizer.SetEvaluationLimits(maxiter=self._maxIter)
        bounds = self._getBounds()
        if bounds != None:
            lowbounds = [l for (l, h) in bounds]
            upperbounds = [h for (l, h) in bounds]
            minimizer.SetStrictRanges(lowbounds, upperbounds)

        minimizer.SetGenerationMonitor(_MysticCallbackMonitor(stepevaluator))
        minimizer.Solve(
            optifunc,
            termination=mystic.termination.CandidateRelativeTolerance(
                self._xtol, self._ftol
            ),
        )

        # Extract final optimization merit value and variables from the step monitor.
        return stepevaluator.bestSolution


class NelderMeadMinimizer(object):
    """Minimizer for the Nelder-Mead simplex method.

  This class is simply as wrapper around the mystic.scipy_optimize.NelderMeadSimplexSolver function.

  If you make use of this class you should cite in any resulting work:

    M.M. McKerns, L. Strand, T. Sullivan, A. Fang, M.A.G. Aivazis,
    "Building a framework for predictive science", Proceedings of
    the 10th Python in Science Conference, 2011;
    http://arxiv.org/pdf/1202.1056

    Michael McKerns, Patrick Hung, and Michael Aivazis,
    "mystic: a simple model-independent inversion framework", 2009- ;
    http://dev.danse.us/trac/mystic

  """

    def __init__(self, variables, maxiter, xtol, ftol):
        """Create simplex minimizer

    @param variables Initial parameter set (atomsscript.fitting.variables.Variables)
    @param maxiter Maximum number of minimization max_iterations
    @param xtol Variable value convergence criterion
    @param ftol Merit function convergence criterion"""
        self._inner = _NelderMeadInner(variables, maxiter, xtol, ftol)
        self._greenlet = gevent.Greenlet()
        self.stepCallback = None

    def minimize(self, merit):
        """Perform minimization.

    @param merit atsim.pro_fit.merit.Merit instance used to calculate merit value.
    @return MinimizerResults for candidate solution population containing best merit value."""
        stepevaluator = _NelderMeadStepMonitor(self.stepCallback, merit)
        self._greenlet = gevent.Greenlet(
            self._inner.minimize, merit, stepevaluator
        )

        def clean(grn):
            stepevaluator.cleanUp()

        self._greenlet.link(clean)
        self._greenlet.start()
        return self._greenlet.get()

    @staticmethod
    def createFromConfig(variables, configitems):
        """Create NelderMeadMinimizer from [Minimizer] section of fit.cfg config file.

    @param variables atsim.pro_fit.variables.Variables instance containing starting parameters for minimization.
    @param configitems List of key,value pairs extracted from [Minimizer] section of config file.
    @return Instance of NelderMeadMinimizer"""

        configitems = dict(configitems)
        del configitems["type"]

        allowedfields = set(
            ["value_tolerance", "function_tolerance", "max_iterations"]
        )
        actualfields = set(configitems.keys())

        if actualfields and not actualfields.issubset(allowedfields):
            notallowed = actualfields - (
                allowedfields.intersection(actualfields)
            )
            notallowed = ",".join(sorted(notallowed))
            raise ConfigException(
                "Unknown fields for NelderMead minimizer: %s" % notallowed
            )

        try:
            value_tolerance = float(configitems.get("value_tolerance", 0.0001))
        except ValueError:
            raise ConfigException(
                "Minimizer NelderMead could not convert 'value_tolerance' into a float: %s"
                % configitems["value_tolerance"]
            )

        try:
            function_tolerance = float(
                configitems.get("function_tolerance", 0.0001)
            )
        except ValueError:
            raise ConfigException(
                "Minimizer NelderMead could not convert 'function_tolerance' into a float: %s"
                % configitems["function_tolerance"]
            )

        if "max_iterations" in configitems:
            try:
                max_iterations = int(configitems.get("max_iterations"))
            except ValueError:
                raise ConfigException(
                    "Minimizer NelderMead could not convert 'max_iterations' into an int: %s"
                    % configitems["max_iterations"]
                )
        else:
            max_iterations = None

        try:
            import mystic  # noqa
        except ImportError:
            raise ConfigException(
                "Mystic package not found. NelderMead minimizer relies on mystic, please install"
            )

        return NelderMeadMinimizer(
            variables, max_iterations, value_tolerance, function_tolerance
        )

    def stopMinimizer(self):
        self._greenlet.kill()
