import collections

import cexprtk

from atsim.pro_fit.exceptions import ConfigException


class Variables(object):
    """Class for handling fitting variables"""

    def __init__(self, varValPairs, bounds=None):
        """@param varValPairs List of (variable_name, variable_value, isFitParameter) pairs (name is string and variable values, floats,
        isFitParameter is a boolean indicating that variable should be changed during fitting)
       @param bounds List of (lowbound, highbound) tuples. If None, then (-inf, inf) bounds will be used (i.e. unbounded)"""
        self._processPairs(varValPairs)
        self.id = None

        if bounds == None:
            self._bounds = [
                (float("-inf"), float("inf")) for i in range(len(varValPairs))
            ]
        else:
            assert len(bounds) == len(varValPairs)
            self._bounds = bounds

    def _processPairs(self, pairs):
        d = collections.OrderedDict([(k, v) for (k, v, isP) in pairs])

        fitkeys = [k for (k, v, isP) in pairs if isP]
        self._varDict = d
        self._fitKeys = fitkeys

    def variablePairs(self):
        return list(self._varDict.items())

    variablePairs = property(
        fget=variablePairs,
        doc="""Return list of (variable_name, variable_value) pairs""",
    )

    def fitKeys(self):
        return list(self._fitKeys)

    fitKeys = property(
        fget=fitKeys,
        doc="""Return list of variable names to be adjusted during fitting""",
    )

    def fitValues(self):
        return [self._varDict[k] for k in self.fitKeys]

    fitValues = property(
        fget=fitValues,
        doc="""Return values associated with fitKeys, in the same order""",
    )

    def bounds(self):
        return list(self._bounds)

    bounds = property(
        fget=bounds,
        doc="""Return list of (lowerbound, upperbound) tuples indicating box bounds for each variable returned by variablePairs""",
    )

    def fitBounds(self):
        fk = self.fitKeys
        return [
            b
            for (b, (k, _v)) in zip(self.bounds, self.variablePairs)
            if k in fk
        ]

    fitBounds = property(
        fget=fitBounds,
        doc="""Return list of (lowerbound, upperbound) tuples indicating box bounds for each fitting variable.""",
    )

    @property
    def numFitVariables(self):
        """Return the number of variables flagged for fitting"""
        l = len(self.fitKeys)
        return l

    def inBounds(self, varKey, value):
        """Used to check if ``value`` is within the bounds of the variable named ``varKey``.

    :param str varKey: Variable name.
    :param float value: Check if value is in range.
    :return: ``True`` if ``value`` is in bounds or ``False`` otherwise."""

        boundsDict = dict(
            list(zip([k for (k, v) in self.variablePairs], self.bounds))
        )

        bounds = boundsDict[varKey]

        if not bounds:
            lowBound, highBound = (float("-inf"), float("inf"))
        else:
            lowBound, highBound = bounds

        return lowBound <= value <= highBound

    @staticmethod
    def _parseBounds(s):
        """Parse string of the form (lowbound, upperbound) into a numeric tuple.

    @param s String describing bounds.
    @return Tuple containing two floats of form (lowerbound, upperbound). float("-inf") and float("inf") represent now lower or upper bound respectively.

    @raises ConfigException if bounds can't be parsed."""
        import re

        # Dump spaces
        s = re.sub(r"\s", "", s)
        m = re.match(r"\((.*?),(.*?)\)", s)
        if not m:
            raise ConfigException(
                "Variable bound does not have correct format"
            )

        l, h = m.groups()

        if not l:
            l = "-inf"

        if not h:
            h = "inf"

        try:
            l = float(l)
        except ValueError:
            raise ConfigException("Variable lower bound could not be parsed")

        try:
            h = float(h)
        except ValueError:
            raise ConfigException("Variable upper bound could not be parsed")

        if l > h:
            raise ConfigException(
                "upper bound  > lower bound. Lower bound should be smaller than upper bound"
            )

        return (l, h)

    def flaggedVariablePairs(self):
        fk = set(self.fitKeys)
        return [(k, v, k in fk) for (k, v) in self.variablePairs]

    flaggedVariablePairs = property(
        fget=flaggedVariablePairs,
        doc="""Return list (variable_name, variable_values, isFitParameter) tuples as accepted by Variables constructor""",
    )

    def createUpdated(self, newvals=None):
        """Given a new set of fitValues, create a copy of this Variables instance,
    updated with the new values. The current instance is left unaffected.

    @param newvals Updated list of values for fitting variables in same order as keys returned by fitKeys property.
                   If None, then produce copy of current instance.
    @return Copy of current Variables instance, containing updated values."""
        if newvals is None:
            return Variables(self.flaggedVariablePairs, self.bounds)

        ud = dict(list(zip(self.fitKeys, newvals)))
        ivt = self.flaggedVariablePairs
        updated = []
        for _i, (k, v, isp) in enumerate(ivt):
            if k in ud:
                updated.append((k, ud[k], isp))
            else:
                updated.append((k, v, isp))
        return Variables(updated, self.bounds)

    def __repr__(self):
        s = "Variables("
        tokens = []
        for k, v, ff in self.flaggedVariablePairs:
            ff = {True: "*", False: ""}[ff]
            tokens.append("%s%s=%f" % (ff, k, v))
        tokens = ", ".join(tokens)
        return "%s%s)" % (s, tokens)

    def __str__(self):
        return repr(self)


class CalculatedVariables(object):
    """Class used to support calculated variables ([CalculatedVariables] within fit.cfg).

  CalculatedVariables instances are callable. When called with an instance of Variables,
  arithmetic expressions are evaluated using these variables. A new instance of Variables
  containing the results of this evaluation is then returned. """

    def __init__(self, nameExpressionTuples):
        """Create CalculatedVariables instance from a list of of (variable_name, expression) tuples.
    Where 'expression' is an arithmetic expression that can be parsed by cexprtk.evaluate_expression.

    @param nameExpressionTuples List of (variable_name, expression) tuples."""
        self.nameExpressionTuples = nameExpressionTuples

    def __call__(self, variables):
        """Evaluate expression values using provided variables.

    @param variables Variables instance containing values used for evaluation.
    @return Variables instance containing original variables and additionally evaluated values"""
        if not self.nameExpressionTuples:
            return variables

        variableDict = dict(variables.variablePairs)

        extraVars = []
        for name, expression in self.nameExpressionTuples:
            value = cexprtk.evaluate_expression(expression, variableDict)
            extraVars.append((name, value, False))

        # Create the values we'll need for the Variables constructor.
        vartuples = variables.flaggedVariablePairs
        vartuples.extend(extraVars)

        # Don't forget the bounds
        bounds = variables.bounds
        for _i in range(len(extraVars)):
            bounds.append(None)

        newvariables = Variables(vartuples, bounds)
        return newvariables

    @staticmethod
    def createFromConfig(cfgitems):
        for _name, expression in cfgitems:
            try:
                cexprtk.check_expression(expression)
            except cexprtk.ParseException as e:
                raise ConfigException(
                    "Could not parse expression when processing [CalculatedVariables]: {} : {}".format(
                        expression, e
                    )
                )

        return CalculatedVariables(cfgitems)


class VariableException(Exception):
    """Exception raised by inspyred related classes when a problem is found with
  input atsim.pro_fit.variables.Variables instances"""

    pass


class BoundedVariableBaseClass(object):
    """Abstract base for objects that should throw if a Variables instance passed to constructor
  does not have definite bounds for all fit parameters.

  Stores variables in _variables property. Bounds are stored in inspyred two list form, in _bounds"""

    def __init__(self, variables):
        """@param variables Variables instance whose bounds are used to generate
              bounder and generator. Note: all fitted parameters must
              have definite upper and lower bounds inf/-inf bounds are
              not supported"""
        self.initialVariables = variables
        self.bounds = self._populateBounds()

    def _populateBounds(self):
        lower = []
        upper = []
        for ((n, _v, isFit), (lb, ub)) in zip(
            self.initialVariables.flaggedVariablePairs,
            self.initialVariables.bounds,
        ):
            if not isFit:
                continue
            elif lb == None or lb == float("-inf"):
                raise VariableException(
                    "Lower bound for variable: %s cannot be infinite." % n
                )
            elif ub == None or ub == float("inf"):
                raise VariableException(
                    "Upper bound for variable: %s cannot be infinite." % n
                )
            else:
                lower.append(lb)
                upper.append(ub)
        if not lower:
            raise VariableException("Not parameters enabled for fitting")
        return [lower, upper]
