import unittest

import os

import ConfigParser

from atomsscripts import fitting
from atomsscripts import testutil

def _getResourceDir():
  return os.path.join(os.path.abspath(os.path.dirname(__file__)), 'resources', 'regex_evaluator')

class RegexEvaluatorTestCase(unittest.TestCase):

  def testEvaluator(self):
    """Test atomsscripts.fitting.evaluators.RegexEvaluator"""
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      parser.readfp(infile)

    evaluator = fitting.evaluators.RegexEvaluator.createFromConfig(
      'regex',
      _getResourceDir(),
      parser.items('Evaluator:regex'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)

    extractExpect = {
      'first': 1.234,
      'second' : 5.678, 
      'third' : 9.1011,
      'fourth' : 5.878
     }
    evaluated = evaluator(job)
    actual = dict([ (e.name, e.extractedValue) for e in evaluated])
    testutil.compareCollection(self, extractExpect, actual)
    
    meritExpect = {
      'first': ((1.234-10.0)**2)**0.5,
      'second' : 2.0 * ((5.678-10.0)**2)**0.5, 
      'third' : 2.0*((9.1011-10.0)**2)**0.5,
      'fourth' : ((5.878-10.0)**2)**0.5
    }    

    evaluated = evaluator(job)
    testutil.compareCollection(self, extractExpect, actual)
    actual = dict([ (e.name, e.meritValue) for e in evaluated])
    testutil.compareCollection(self, meritExpect, actual)

