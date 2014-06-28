import math

class EvaluatorRecord(object):
  """Object returned for each value extracted by an evaluator"""

  def __init__(self, name, expectedValue, extractedValue, weight = 1.0, meritValue = None, evaluatorName = None ):
    """@param name value label
       @param expectedValue Value expected by evaluator for label
       @param extractedValue Raw value extracted by evaluator
       @param weight Value weight
       @param meritValue
       @param evaluatorName Name of evaluator that created this evluator record"""
    self.name = name
    """Record Name"""

    self.expectedValue = expectedValue
    """Value expected for this evaluator record"""

    self.extractedValue = extractedValue
    """Actual value extracted for this evaluator record"""

    self.weight = weight
    """Weight to be given to evaluator record"""

    self.evaluatorName = evaluatorName
    """Name of evaluator that created this record"""

    self.meritValue = meritValue
    """Value to be used in calculating global merit value"""

    self.errorFlag = False
    """Flag indicating if error was experienced when extracting value"""

  def __repr__(self):
    return "EvaluatorRecord(name=%s, expectedValue=%s, extractedValue=%s, weight=%s, meritValue=%s, evaluatorName=%s)" % (
      self.name,
      self.expectedValue,
      self.extractedValue,
      self.weight,
      self.meritValue,
      self.evaluatorName)


class RMSEvaluatorRecord(EvaluatorRecord):
  """As EvaluatorRecord but with additional rmsDifference (root mean squared difference),
  whose weighted value is used as meritValue"""

  def __init__(self, name, expectedValue, extractedValue, weight = 1.0, evaluatorName = None):
    EvaluatorRecord.__init__(self, name, expectedValue, extractedValue, weight, evaluatorName = evaluatorName)
    self.rmsDifference = self._rmsDifference()
    self.meritValue = self.rmsDifference * self.weight

  def _rmsDifference(self):
    return math.sqrt((self.extractedValue - self.expectedValue)**2.0)

  def __repr__(self):
    return "RMSEvaluatorRecord(name=%s, expectedValue=%f, extractedValue=%f, weight=%f, evaluatorName=%s, rmsDiff=%f)" % (self.name, self.expectedValue, self.extractedValue, self.weight, self.evaluatorName, self.rmsDifference)



class FractionalDifferenceEvaluatorRecord(RMSEvaluatorRecord):
  """EvaluatorRecord that additionally provides fractionalDifference properties"""

  def __init__(self, name, expectedValue, extractedValue, weight = 1.0):
    RMSEvaluatorRecord.__init__(self, name, expectedValue, extractedValue, weight)
    self.fractionalDifference = self._fracDiff()

  def _fracDiff(self):
    fracdiff = float("NaN")
    numerator = (self.expectedValue - self.extractedValue)
    if numerator == 0.0:
      fracdiff = 0.0
    elif self.expectedValue != 0.0:
      fracdiff = math.fabs( numerator / self.expectedValue)
    return fracdiff


class ErrorEvaluatorRecord(EvaluatorRecord):
  """EvaluatorRecord used for indicating an error condition"""

  def __init__(self, name, expectedValue, exception, weight = 1.0, evaluatorName = None ):
      EvaluatorRecord.__init__(self, name, expectedValue, float("nan"), weight = weight, meritValue = float("nan"), evaluatorName = evaluatorName )
      self.errorFlag = True
      self.exception = exception

  def __repr__(self):
      return "ErrorEvaluatorRecord(name=%s, exception=%s, evaluatorName=%s)" % (self.name, self.exception, self.evaluatorName)
