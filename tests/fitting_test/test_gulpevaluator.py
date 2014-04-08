import unittest
import ConfigParser

import os
import math

from atomsscripts import fitting
from atomsscripts import testutil


def _getResourceDir():
  spath = os.path.abspath(os.path.dirname(__file__))
  rpath = os.path.join(spath, 'resources', 'gulp_evaluator')
  return rpath


class GulpDrvParserTestCase(unittest.TestCase):

  def setUp(self):
    self.filename = os.path.join(_getResourceDir(), 'output', 'out.drv')
    infile = open(self.filename, 'rb')
    from atomsscripts.fitting.evaluators._gulp import GulpDrvParser
    self.parser = GulpDrvParser(infile)

  def testGradientsCartesian(self):
    expect = [     
     ( 0.00000001,    -16.94269799,    -14.62767097),
     (-0.00000001,     16.94269799,     14.62767097),
     ( 0.00000001,    -11.61397756,     24.28685966),
     (-0.00000001,     11.61397756,    -24.28685966),
     ( 0.00000002,    -32.31698497,     61.18250054),
     (-0.00000002,     32.31698497,    -61.18250054),
     ( 0.00000001,    -40.08306810,    -20.50021190),
     (-0.00000001,     40.08306810,     20.50021190)]

    actual = self.parser.gradientsCartesian
    actual = testutil.compareCollection(self, expect, actual)

  def testGradientsStrain(self):
    expect = [131.18915259,   -150.44439992,   -112.58477238,
              -13.26823765,     -0.00000001,      0.00000007]
    actual = self.parser.gradientsStrain
    testutil.compareCollection(self, expect, actual)


    symbols = ['xx', 'yy', 'zz', 'yz', 'xz', 'xy']

    expect = zip(symbols, expect)
    actual = []
    for sym in symbols:
      v = getattr(self.parser.gradientsStrain, sym)
      actual.append((sym, v))
    testutil.compareCollection(self, expect, actual)

class GulpDrvEvaluatorTestCase(unittest.TestCase):

  def setUp(self):
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    self.parser = parser

  def testEvaluator(self):
    """Test GulpDrvEvaluator from config to evaluation"""
    with open(os.path.join(_getResourceDir(), 'drv.cfg')) as infile:
      self.parser.readfp(infile)

      evaluator = fitting.evaluators.Gulp_DRVEvaluator.createFromConfig('Gulp:DRV',
        _getResourceDir(),
        self.parser.items('Evaluator:DRV'))

      job = fitting.jobfactories.Job(None, _getResourceDir(), None)
      evalvalues = evaluator(job)

      expect = [ ('atom_gradients', 0.0, 327.0376332, 1.0, 'Gulp:DRV', 327.0376332),
                 ('cell_gradients', 0.0, 407.4865626, 1.0, 'Gulp:DRV', 407.4865626),
                 ('cell_xx', 0.0, 131.18915259, 1.0, 'Gulp:DRV', 131.1891526),
                 ('cell_yy', 0.0, -150.44439992, 1.0, 'Gulp:DRV',150.4443999),
                 ('cell_zz', 0.0, -112.58477238, 1.0, 'Gulp:DRV',112.5847724),
                 ('cell_yz', 0.0, -13.26823765, 1.0, 'Gulp:DRV',13.26823765),
                 ('cell_xz', 0.0, -0.00000001, 1.0, 'Gulp:DRV', 0.00000001),
                 ('cell_xy', 0.0, 0.00000007, 1.0, 'Gulp:DRV', 0.00000007)]
      expect.sort()
      actual = [ (e.name, e.expectedValue, e.extractedValue, e.weight, e.evaluatorName, e.meritValue) for e in evalvalues]
      actual.sort()

      testutil.compareCollection(self, expect, actual)


class GulpEvaluatorTestCase(unittest.TestCase):
  """Tests for GULP Evaluators"""
  def setUp(self):
    parser = ConfigParser.SafeConfigParser()
    parser.optionxform = str
    self.parser = parser

  def testElastic(self):
    """Test elastic constants"""
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      self.parser.readfp(infile)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:Elastic'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)
    evalvalues = evaluator(job)
    extractedvalues = dict([ (v.name, v.extractedValue) for v in evalvalues])
    fractionaldiffs = dict([ (v.name, v.fractionalDifference) for v in evalvalues])
    rmsvalues = dict([(v.name, v.rmsDifference) for v in evalvalues])

    expectrmsvalues = dict(
      elastic_c11=362.0647,
      elastic_c12=152.8691,
      elastic_c13=152.8691,
      elastic_c14=10,
      elastic_c15=10,
      elastic_c16=10,
      elastic_c21=152.8691,
      elastic_c22=372.0647,
      elastic_c23=152.8691,
      elastic_c24=10,
      elastic_c25=10,
      elastic_c26=10,
      elastic_c31=152.8691,
      elastic_c32=152.8691,
      elastic_c33=362.0647,
      elastic_c34=10,
      elastic_c35=10,
      elastic_c36=10,
      elastic_c41=10,
      elastic_c42=10,
      elastic_c43=10,
      elastic_c44=152.8714,
      elastic_c45=10,
      elastic_c46=10,
      elastic_c51=10,
      elastic_c52=10,
      elastic_c53=10,
      elastic_c54=10,
      elastic_c55=152.8714,
      elastic_c56=10,
      elastic_c61=10,
      elastic_c62=10,
      elastic_c63=10,
      elastic_c64=10,
      elastic_c65=10,
      elastic_c66=152.8714)
    testutil.compareCollection(self, expectrmsvalues, rmsvalues)
    
    weightvalues = dict([(v.name, v.weight )for v in evalvalues])

    self.assertAlmostEquals(5.0, weightvalues['elastic_c21'])
    del weightvalues['elastic_c21']
    for v in weightvalues.itervalues():
      self.assertAlmostEquals(1.0, v)

    expectvalues = dict([(v.name, v.expectedValue) for v in evalvalues])
    self.assertAlmostEquals(0.0, expectvalues['elastic_c22'])
    del expectvalues['elastic_c22']
    for v in expectvalues.itervalues():
      self.assertAlmostEquals(10.0, v)

    expectextractedvalues = dict(
      elastic_c11=372.0647 , 
      elastic_c12=162.8691 , 
      elastic_c13=162.8691 , 
      elastic_c14=0        , 
      elastic_c15=0        , 
      elastic_c16=0        , 
      elastic_c21=162.8691 , 
      elastic_c22=372.0647 , 
      elastic_c23=162.8691 , 
      elastic_c24=0        , 
      elastic_c25=0        , 
      elastic_c26=0        , 
      elastic_c31=162.8691 , 
      elastic_c32=162.8691 , 
      elastic_c33=372.0647 , 
      elastic_c34=0        , 
      elastic_c35=0        , 
      elastic_c36=0        , 
      elastic_c41=0        , 
      elastic_c42=0        , 
      elastic_c43=0        , 
      elastic_c44=162.8714 , 
      elastic_c45=0        , 
      elastic_c46=0        , 
      elastic_c51=0        , 
      elastic_c52=0        , 
      elastic_c53=0        , 
      elastic_c54=0        , 
      elastic_c55=162.8714 , 
      elastic_c56=0        , 
      elastic_c61=0        , 
      elastic_c62=0        , 
      elastic_c63=0        , 
      elastic_c64=0        , 
      elastic_c65=0        , 
      elastic_c66=162.8714 ) 
    testutil.compareCollection(self, expectextractedvalues, extractedvalues)

    nan = float('NaN')
    fractionalextractedvalues = dict(
      elastic_c11= 36.20647, 
      elastic_c12= 15.28691, 
      elastic_c13= 15.28691, 
      elastic_c14= 1.0       , 
      elastic_c15= 1.0       , 
      elastic_c16= 1.0       , 
      elastic_c21= 15.28691, 
      elastic_c22= nan       , 
      elastic_c23= 15.28691, 
      elastic_c24= 1.0       , 
      elastic_c25= 1.0       , 
      elastic_c26= 1.0       , 
      elastic_c31= 15.28691, 
      elastic_c32= 15.28691, 
      elastic_c33= 36.20647, 
      elastic_c34= 1.0       , 
      elastic_c35= 1.0       , 
      elastic_c36= 1.0       , 
      elastic_c41= 1.0       , 
      elastic_c42= 1.0       , 
      elastic_c43= 1.0       , 
      elastic_c44= 15.28714, 
      elastic_c45= 1.0       , 
      elastic_c46= 1.0       , 
      elastic_c51= 1.0       , 
      elastic_c52= 1.0       , 
      elastic_c53= 1.0       , 
      elastic_c54= 1.0       , 
      elastic_c55= 15.28714, 
      elastic_c56= 1.0       , 
      elastic_c61= 1.0       , 
      elastic_c62= 1.0       , 
      elastic_c63= 1.0       , 
      elastic_c64= 1.0       , 
      elastic_c65= 1.0       , 
      elastic_c66= 15.28714) 
    testutil.compareCollection(self, fractionalextractedvalues, fractionaldiffs)
    
    expectmeritvalues = dict(
      elastic_c11=362.0647,
      elastic_c12=152.8691,
      elastic_c13=152.8691,
      elastic_c14=10,
      elastic_c15=10,
      elastic_c16=10,
      elastic_c21=152.8691 * 5.0,
      elastic_c22=372.0647,
      elastic_c23=152.8691,
      elastic_c24=10,
      elastic_c25=10,
      elastic_c26=10,
      elastic_c31=152.8691,
      elastic_c32=152.8691,
      elastic_c33=362.0647,
      elastic_c34=10,
      elastic_c35=10,
      elastic_c36=10,
      elastic_c41=10,
      elastic_c42=10,
      elastic_c43=10,
      elastic_c44=152.8714,
      elastic_c45=10,
      elastic_c46=10,
      elastic_c51=10,
      elastic_c52=10,
      elastic_c53=10,
      elastic_c54=10,
      elastic_c55=152.8714,
      elastic_c56=10,
      elastic_c61=10,
      elastic_c62=10,
      elastic_c63=10,
      elastic_c64=10,
      elastic_c65=10,
      elastic_c66=152.8714)
    actualmeritvalues = dict([ (v.name, v.meritValue) for v in evalvalues])
    testutil.compareCollection(self, expectmeritvalues, actualmeritvalues)


  def testBulkModulus(self):
    """Test GulpEvaluator, bulk modulus extraction"""
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      self.parser.readfp(infile)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:BulkModulus'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)
    subevalled = evaluator(job)
    extractedvalues = dict([ (v.name, v.extractedValue) for v in subevalled])
    fractionaldiffs = dict([ (v.name, v.fractionalDifference) for v in subevalled])
    # weighteddiffs = dict([ (v.name, v.weightedDifference) for v in subevalled])

    testutil.compareCollection(self, {'bulkmodulus_reuss' : 232.60093 ,
      'bulkmodulus_voigt' : 233.60093,
      'bulkmodulus_hill'  : 234.60093 }, extractedvalues)

    testutil.compareCollection(self, {'bulkmodulus_reuss' : 0.06959628 ,
      'bulkmodulus_voigt' : 0.01565621739,
      'bulkmodulus_hill'  : 0.07123712329 }, fractionaldiffs)
    
  def testShearModulus(self):
    """Test GulpEvaluator, shear modulus extraction"""
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      self.parser.readfp(infile)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:ShearModulus'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)
    subevalled = evaluator(job)
    extractedvalues = dict([ (v.name, v.extractedValue) for v in subevalled])

    testutil.compareCollection(self, {'shearmodulus_reuss' :  133.19021,
      'shearmodulus_voigt' : 139.56198,
      'shearmodulus_hill'  : 136.37610 }, extractedvalues)

    

  def testUnitCell(self):
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      self.parser.readfp(infile)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:UnitCell'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)
    # import pudb;pudb.set_trace()
    subevalled = evaluator(job)
    extractedvalues = dict([ (v.name, v.extractedValue) for v in subevalled])
    fractionaldiffs = dict([ (v.name, v.fractionalDifference) for v in subevalled])
    # weighteddiffs = dict([ (v.name, v.weightedDifference) for v in subevalled])

    testutil.compareCollection(self,
      {'cell_a' : 4.212,
       'cell_b' : 4.214,
       'cell_c' : 4.215,
       'cell_alpha' : 90.1,
       'cell_beta' : 90.2,
       'cell_gamma' : 90.3}, extractedvalues)


  def testEnergy(self):
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      self.parser.readfp(infile)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:Energy'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)
    actual = evaluator(job)
    actual = dict([(v.name, v.meritValue) for v in actual])
    expect = { 'optimisation_penalty' : 0.0,
               'lattice_energy' : 69.87295}
    testutil.compareCollection(self, expect, actual)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:BadEnergy'))
    job = fitting.jobfactories.Job(None, _getResourceDir(), None)
    actual = evaluator(job)
    actual = dict([(v.name, v.meritValue) for v in actual])
    expect = { 'optimisation_penalty' : 100.0 }
    testutil.compareCollection(self, expect, actual)


  def testPhonon(self):
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      self.parser.readfp(infile)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:Phonon'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)

    actual = evaluator(job)
    actual = dict([(v.name, v.meritValue) for v in actual])
    expect = { 'negative_phonon_penalty' : 100.0}
    testutil.compareCollection(self, expect, actual)  

  def testPhononShrunk(self):
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      self.parser.readfp(infile)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:Phonon_shrunk'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)
    actual = evaluator(job)
    self.assertEqual(False, actual[0].errorFlag)
    actual = dict([(v.name, v.meritValue) for v in actual])
    expect = { 'negative_phonon_penalty' : 0.0}
    testutil.compareCollection(self, expect, actual)  

  def testEvaluationErrors(self):
    """Check that GulpEvaluator returns ErrorEvaluatorRecord for bad values"""
    with open(os.path.join(_getResourceDir(), 'job.cfg')) as infile:
      self.parser.readfp(infile)

    evaluator = fitting.evaluators.GulpEvaluator.createFromConfig(
      'Gulp',
      _getResourceDir(),
      self.parser.items('Evaluator:Error'))

    job = fitting.jobfactories.Job(None, _getResourceDir(), None)
    actual = evaluator(job)
    self.assertEqual(1, len(actual))
    r = actual[0]
    self.assertEqual(fitting.evaluators.ErrorEvaluatorRecord, type(r))
    self.assertTrue(math.isnan(r.meritValue))
    self.assertEqual(100.0, r.expectedValue)
    self.assertTrue(math.isnan(r.extractedValue))
    self.assertEqual(True, r.errorFlag)
    self.assertEqual(ValueError, type(r.exception))
    self.assertEqual("elastic_c11", r.name)
    self.assertEqual("Gulp", r.evaluatorName)
