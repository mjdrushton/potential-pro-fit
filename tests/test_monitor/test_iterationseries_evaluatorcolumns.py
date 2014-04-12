# -*- coding: utf-8 -*-

from _cherrypydbtestcase import CherryPyDBTestCaseBase


from .. import testutil

class IterationSeriesMetaEvaluatorColumnTestCase(CherryPyDBTestCaseBase):
  """Tests for the cherrypy handlers under /fitting/iteration_series"""

  dbname = "meta_eval.db"
  baseurl = "http://localhost:8080/fitting/iteration_series"

  def testMetaEvaluatorColumns(self):
    """Test that evaluator columns targetting meta evaluators can be accessed."""
    baserequest = 'merit_value/all/min?columns=evaluator:meta_evaluator:Gd_Ea:deltaD18_16:extract'
    j = self.fetchJSON(baserequest)
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'evaluator:meta_evaluator:Gd_Ea:deltaD18_16:extract'],
               'values'  : [
                            [0                , 0                , 2416.94923768057,  0.186157195071552],
                            [1                , 0                , 2296.32138117195, 0.101604334555792],
                            [2                , 0                , 2255.55815256615, 0.0319247393189062]
                  ]}

    testutil.compareCollection(self, expect, j)

class IterationSeries_Evaluators_TestCase(CherryPyDBTestCaseBase):
  dbname = "population_fitting_run.db"
  baseurl = 'http://localhost:8080/fitting/iteration_series'

  def testEvaluatorColumnLabelSplit(self):
    """Test _EvaluatorColumnProvider._splitColumnLabel()"""
    from atsim.pro_fit.webmonitor._columnproviders import _EvaluatorColumnProvider
    actual = _EvaluatorColumnProvider._splitColumnLabel("evaluator:CaO:CaO:Gulp:elastic_c12:merit")
    expect = dict(jobName = 'CaO',
      evaluatorName = 'CaO:Gulp',
      valueName = 'elastic_c12',
      valueType = 'merit')
    self.assertEquals(expect,actual)

  def testValueTypeMerit(self):
    """Tests for the evaluator: column types"""
    baserequest = 'merit_value/all/min?columns=evaluator:CaO:CaO:Gulp:elastic_c12:merit'
    j = self.fetchJSON(baserequest)
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'evaluator:CaO:CaO:Gulp:elastic_c12:merit'],
               'values'  : [
                    [0 ,2, 973.78207, 56.2145],
                    [1 ,3, 973.78207, 56.2145],
                    [2 ,3, 973.78207, 56.2145],
                    [3 ,1, 973.78207, 56.2145],
                    [4 ,1, 964.64312, 55.7632],
                    [5 ,3, 964.64312, 55.7632]
                  ]}

    testutil.compareCollection(self, expect, j)

  def testValueTypeExtract(self):
    baserequest = 'merit_value/all/min?columns=evaluator:CaO:CaO:Gulp:elastic_c12:extract'
    j = self.fetchJSON(baserequest)
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'evaluator:CaO:CaO:Gulp:elastic_c12:extract'],
               'values'  : [
                    [0 ,2, 973.78207,1.5955],
                    [1 ,3, 973.78207,1.5955],
                    [2 ,3, 973.78207,1.5955],
                    [3 ,1, 973.78207,1.5955],
                    [4 ,1, 964.64312,2.0468],
                    [5 ,3, 964.64312,2.0468]
                  ]}

    testutil.compareCollection(self, expect, j)

  def testMultipleEvaluatorColumns(self):
    baserequest = 'merit_value/all/min?columns=evaluator:CaO:CaO:Gulp:elastic_c12:merit,evaluator:MgO:MgO:Gulp:cell_a:extract'
    j = self.fetchJSON(baserequest)
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'evaluator:CaO:CaO:Gulp:elastic_c12:merit', 'evaluator:MgO:MgO:Gulp:cell_a:extract'],
               'values'  : [
                    [0 ,2, 973.78207, 56.2145, 5.061349],
                    [1 ,3, 973.78207, 56.2145, 5.061349],
                    [2 ,3, 973.78207, 56.2145, 5.061349],
                    [3 ,1, 973.78207, 56.2145, 5.061349],
                    [4 ,1, 964.64312, 55.7632, 5.061349],
                    [5 ,3, 964.64312, 55.7632, 5.061349]
                  ]}

    testutil.compareCollection(self, expect, j)


  def testValueTypePercentDiff(self):
    """Test calculation of percentage differences for evaluator columns (i.e. test value type of percent)"""
    baserequest = 'merit_value/all/max?columns=evaluator:CaO:CaO:Gulp:cell_a:percent'
    j = self.fetchJSON(baserequest)

    expect =  {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'evaluator:CaO:CaO:Gulp:cell_a:percent'],
               'values'  : [
                              [0 , 1 , 56979.43601 , -3.50646435252546   ] ,
                              [1 , 0 , 5283.62466  , -43.669112450634  ]  ,
                              [2 , 1 , 5096.59874  , -43.8537933901476  ]  ,
                              [3 , 2 , 1546.33659  , 52.7872791519435 ]   ,
                              [4 , 0 , 2300.90601  , 103.999106214924 ]   ,
                              [5 , 1 , 12634.65516 , -6.61635834545832 ]]}
    testutil.compareCollection(self, expect, j)



