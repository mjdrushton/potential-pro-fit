import unittest
import os
import math

from atsim import pro_fit

def _getResourceDirectory():
  return os.path.join(os.path.abspath(os.path.dirname(__file__)), "resources", "dlpoly_evaluator")

class MockJob(object):
  def __init__(self, path):
    self.path = path

class DLPolySTATISEvaluator(unittest.TestCase):
  """Tests for atsim.pro_fit.evaluators.DLPOLY_STATISEvaluator"""

  def testEvaluator(self):
    startTime = 75.0
    keyExpectPairs = [ ("volume", 84000.0, 2.0)]
    evaluator = pro_fit.evaluators.DLPOLY_STATISEvaluator("STATIS_Eval", startTime, keyExpectPairs)
    volumeExpect = math.sqrt((17849.1273 - 84000.0)**2.0)
    job = MockJob(_getResourceDirectory())

    evaluated = evaluator(job)
    self.assertEquals(1, len(evaluated))
    r = evaluated[0]
    self.assertEquals("volume", r.name)
    self.assertEquals("STATIS_Eval", r.evaluatorName)
    self.assertEquals(84000.0, r.expectedValue)
    self.assertAlmostEquals(17849.1273, r.extractedValue, places = 5)
    self.assertEquals(2.0, r.weight)
    self.assertAlmostEquals(volumeExpect, r.rmsDifference, places = 5)
    self.assertAlmostEquals(volumeExpect*2.0, r.meritValue, places = 4)

  def testCONFIGFields(self):
    """Tests for keys requiring access to a CONFIG file"""
    startTime = 80.0
    keyExpectPairs = [ ("msd_O", 0.41, 2.0)]
    evaluator = pro_fit.evaluators.DLPOLY_STATISEvaluator("STATIS_Eval", startTime, keyExpectPairs)
    job = MockJob(_getResourceDirectory())
    evaluated = evaluator(job)
    r = evaluated[0]
    self.assertAlmostEquals(0.414975, r.extractedValue)

  def testNPTFields(self):
    """Tests for fields requiring an NPT run"""
    startTime = 80.0
    keyExpectPairs = [ ("cella_x", 23.99687, 2.0)]
    evaluator = pro_fit.evaluators.DLPOLY_STATISEvaluator("STATIS_Eval", startTime, keyExpectPairs)
    job = MockJob(_getResourceDirectory())
    evaluated = evaluator(job)
    r = evaluated[0]
    self.assertAlmostEquals(23.99687, r.extractedValue)

  def testErrorRecord(self):
    """Test that ErrorEvaluatorRecord is returned when there is an error during evaluation"""
    startTime = 80.0
    keyExpectPairs = [ ("badkey", 23.0, 2.0)]
    evaluator = pro_fit.evaluators.DLPOLY_STATISEvaluator("STATIS_Eval", startTime, keyExpectPairs)
    job = MockJob(_getResourceDirectory())
    evaluated = evaluator(job)
    r = evaluated[0]

    self.assertEquals(pro_fit.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertEquals(True, r.errorFlag)
    self.assertEquals(KeyError, type(r.exception))
    self.assertEquals("STATIS_Eval", r.evaluatorName)

