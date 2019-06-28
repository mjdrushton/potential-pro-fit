import collections

from . import fittool
from . import evaluators

import cexprtk

import logging

FormulaVariable = collections.namedtuple("FormulaVariable", ["name", "key"])
SplitVariableKey = collections.namedtuple(
    "SplitVariableKey", ["job", "evaluator", "record", "attr"]
)
Expression = collections.namedtuple(
    "Expression", ["name", "expression", "weight", "expected_value"]
)


class FormulaMetaEvaluatorVariableException(Exception):
    pass


class FormulaMetaEvaluator(object):
    """MetaEvaluator that takes an arithmetic expression (as a string) and a
  set of variable keys describing which evaluator values should be used within
  this expression.

  Variable definition:
  Evaluator values are described using the following key format:
    JOB_NAME:EVALUATOR_NAME:EVALUATOR_RECORD_NAME[:VALUE]

    The optional VALUE field can take the values:
      extracted_value : value extracted by evaluator,
      expected_value  : value expected by evaluator,
      weight          : evaluator weight,
      merit_value     : evaluator record merit value.

    If VALUE not specified then 'merit_value' is used."""

    _logger = logging.getLogger(
        "atsim.pro_fit.metaevaluators.FormulaMetaEvaluator"
    )

    def __init__(self, name, expressionList, variables):
        """@param name Meta evaluator name.
       @param expressionList List of Expression named tuples giving name and string for arithmetic expression to be evaluated.
                During evaluation, an EvaluatorRecord is generated for each expression in the list. The record will be named
                using each Expression's name property.
                If Expression.expected_value is None then MetaEvaluator will return EvaluatorRecord (i.e. evaluator merit value
                will be the results of evaluating the expression). If Expression.expected_value is None, then an RMSEvaluatorRecord
                is created for the expression meaning that the RMS difference between Expression.expected_value and the result
                of evaluating the expression becomes expression's contribution to the merit value.
       @param variables List of FormulaVariable instances giving placeholder,
        evaluator value keys to be used within expression."""
        self.name = name
        self.expressionList = expressionList
        self.variables = variables

    def _makeVariableDict(self, joblist):
        """@param joblist List of Job instances
       @return Dictionary mapping FormulaVariable.name to value extracted from job.evaluatorRecords"""
        attrToActualAttr = {
            "merit_value": "meritValue",
            "extracted_value": "extractedValue",
            "weight": "weight",
            "expected_value": "expectedValue",
        }

        self._logger.debug("_makeVariableDict")
        for j in joblist:
            self._logger.debug("job name: %s" % j.name)

        retdict = {}
        for name, splitkey in self.variables:
            self._logger.debug("name: %s. splitkey:%s" % (name, str(splitkey)))
            job = [j for j in joblist if splitkey.job == j.name]
            if not job:
                raise FormulaMetaEvaluatorVariableException(
                    "No job found named: %s" % str(splitkey.job)
                )
            job = job[0]
            found = False
            for evalrecords in job.evaluatorRecords:
                for evalrecord in evalrecords:
                    evaluatorName = evalrecord.evaluatorName[
                        len(job.name) + 1 :
                    ]
                    self._logger.debug("evaluatorName: %s " % evaluatorName)
                    if (
                        evaluatorName == splitkey.evaluator
                        and evalrecord.name == splitkey.record
                    ):
                        found = True
                        break
                if found:
                    break
            if not found:
                raise FormulaMetaEvaluatorVariableException(
                    "No evaluator record found for evaluator name: %s and value name: %s"
                    % (splitkey.evaluator, splitkey.record)
                )

            actualAttr = attrToActualAttr[splitkey.attr]
            value = getattr(evalrecord, actualAttr)
            retdict[name] = value
        return retdict

    def __call__(self, joblist):
        """Evaluate expression stored within this MetaEvaluator and return value using variables extracted from the evaluatorRecords
    contained with the Job instances listed in joblist.

    @param joblist List of Job instances
    @return A list containing a single atsim.pro_fit.evaluators.EvaluatorRecord instance with a meritValue equal to the evaluated expression"""
        variableDict = self._makeVariableDict(joblist)
        retlist = []
        for expression in self.expressionList:
            try:
                result = cexprtk.evaluate_expression(
                    expression.expression, variableDict
                )
                weightedresult = result * expression.weight

                if expression.expected_value == None:
                    er = evaluators.EvaluatorRecord(
                        expression.name,
                        0.0,
                        result,
                        expression.weight,
                        weightedresult,
                        self.name,
                    )
                else:
                    er = evaluators.RMSEvaluatorRecord(
                        expression.name,
                        expression.expected_value,
                        result,
                        expression.weight,
                        self.name,
                    )
            except cexprtk.ParseException as e:
                er = evaluators.ErrorEvaluatorRecord(
                    expression.name, 0.0, e, expression.weight, self.name
                )
            retlist.append(er)
        return retlist

    @staticmethod
    def _splitVariables(variables):
        """Convert evaluator value keys into SplitVariableKey instances
    @param variables List of FormulaVariable tuples
    @return List of FormulaVariable tuples in wich string key has been converted to SplitVariableKey"""
        splitvariables = []
        for variable in variables:
            tokens = variable.key.split(":")
            if len(tokens) == 4:
                attr = tokens[3]
            elif len(tokens) != 3:
                raise fittool.ConfigException(
                    "Wrong number of fields when parsing key for Formula meta-evaluator variable '%s': '%s'"
                    % (variable.name, variable.key)
                )
            else:
                attr = "merit_value"

            if attr not in [
                "merit_value",
                "extracted_value",
                "weight",
                "expected_value",
            ]:
                raise fittool.ConfigException(
                    "Unknown field in '%s' variable definition: '%s' for key '%s"
                    % (variable.name, attr, variable.key)
                )

            splitvariable = SplitVariableKey(
                tokens[0], tokens[1], tokens[2], attr
            )

            splitvariables.append(FormulaVariable(variable.name, splitvariable))
        return splitvariables

    @staticmethod
    def _parseExpect(k, expression):
        """Split expression of form "[expect =] formula" into tuple (expect, formula)
    with expect being returned as None if it isn't present"""

        tokens = expression.split("=")

        if len(tokens) > 2:
            raise fittool.ConfigException(
                "Error when splitting Formula meta-evaluator formula into expected_value = expression pair. Got > 2 tokens. (Expression may contain more than one = sign): '%s' : %s"
                % (k, expression)
            )
        elif len(tokens) == 2:
            expect, formula = tokens
            formula = formula.strip()
            try:
                expect = float(expect)
            except ValueError:
                raise fittool.ConfigException(
                    "Error when parsing expected_value for Formula meta-evaluator. Could not convert '%s' into a float. ('%s' : %s)"
                    % (expect, k, expression)
                )
        else:
            expect = None
            formula = tokens[0].strip()

        return expect, formula

    @staticmethod
    def createFromConfig(name, fitRootPath, cfgItems):
        weights = {}
        variables = []
        expressions = []

        for k, v in cfgItems:
            if k.startswith("weight_"):
                wname = k[7:]
                try:
                    v = float(v)
                except ValueError:
                    raise fittool.ConfigException(
                        "Error parsing configuration for meta evaluator '%s'. Variable weight '%s' couldn't be parsed into float: %s"(
                            name, wname, v
                        )
                    )
                weights[wname] = v

        for k, v in cfgItems:
            if k == "type" or k.startswith("weight_"):
                continue
            elif k.startswith("variable_"):
                # Variable
                vname = k[9:]
                variables.append(FormulaVariable(vname, v))
            elif k.startswith("expression_"):
                # Expression
                ename = k[11:]
                expect, formula = FormulaMetaEvaluator._parseExpect(k, v)
                try:
                    cexprtk.check_expression(formula)
                except cexprtk.ParseException as e:
                    raise fittool.ConfigException(
                        "Could not parse formula for Formula meta-evaluator '%s', for expression '%s': %s"
                        % (name, k, e)
                    )
                expressions.append(
                    Expression(ename, formula, weights.get(ename, 1.0), expect)
                )
            else:
                raise fittool.ConfigException(
                    "Error parsing configuration for meta evaluator '%s'. Unknown item: '%s'"
                    % (name, k)
                )
        variables = FormulaMetaEvaluator._splitVariables(variables)

        FormulaMetaEvaluator._logger.info(
            "Creating Formula meta-evaluator: %s" % name
        )
        FormulaMetaEvaluator._logger.debug("MetaEvaluator name=%s" % name)
        FormulaMetaEvaluator._logger.debug(
            "MetaEvaluator expressions=%s" % expressions
        )
        FormulaMetaEvaluator._logger.debug(
            "MetaEvaluator variables=%s" % variables
        )

        # Create the MetaEvaluator
        return FormulaMetaEvaluator(name, expressions, variables)
