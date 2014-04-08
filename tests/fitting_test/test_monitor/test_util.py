from atomsscripts.fitting.webmonitor import _util

import unittest

class UtilTestCase(unittest.TestCase):
	"""Tests for atomsscripts.fitting.webmonitor._util functions"""

	def testPercentDiffBadValues(self):
	  """Test the atomsscripts.fitting.webmonitor._columnproviders._EvaluatorColumnProvider._percentDifference can handle bad values"""
	  from atomsscripts.fitting.webmonitor._columnproviders import _EvaluatorColumnProvider
	  # First one that should pass
	  pdiff = _util.calculatePercentageDifference
	  self.assertEquals(-10, pdiff({'extracted_value': 90, 'expected_value' : 100}))

	  # Zero as expected value
	  self.assertEquals(None, pdiff({'extracted_value': 90, 'expected_value' : 0.0}))

	  self.assertEquals(None, pdiff({'extracted_value': 90, 'expected_value' : None}))
	  self.assertEquals(None, pdiff({'extracted_value': None, 'expected_value' : 90}))
	  self.assertEquals(None, pdiff({'extracted_value': None, 'expected_value' : None}))
