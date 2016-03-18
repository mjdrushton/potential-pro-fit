import atsim.pro_fit.db._util as _util

import unittest

class UtilTestCase(unittest.TestCase):
	"""Tests for atsim.pro_fit.webmonitor._util functions"""

	def testPercentDiffBadValues(self):
	  """Test the atsim.pro_fit.webmonitor._columnproviders._EvaluatorColumnProvider._percentDifference can handle bad values"""
	  from atsim.pro_fit.db._columnproviders import _EvaluatorColumnProvider
	  # First one that should pass
	  pdiff = _util.calculatePercentageDifference
	  self.assertEquals(-10, pdiff({'extracted_value': 90, 'expected_value' : 100}))

	  # Zero as expected value
	  self.assertEquals(None, pdiff({'extracted_value': 90, 'expected_value' : 0.0}))

	  self.assertEquals(None, pdiff({'extracted_value': 90, 'expected_value' : None}))
	  self.assertEquals(None, pdiff({'extracted_value': None, 'expected_value' : 90}))
	  self.assertEquals(None, pdiff({'extracted_value': None, 'expected_value' : None}))
