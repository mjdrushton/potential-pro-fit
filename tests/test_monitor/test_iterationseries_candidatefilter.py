# -*- coding: utf-8 -*-
from _cherrypydbtestcase import CherryPyDBTestCaseBase
import testutil

class IterationSeries_CandidateFilter_TestCase(CherryPyDBTestCaseBase):
  dbname = "population_fitting_run.db"
  baseurl = 'http://localhost:8080/fitting/iteration_series'

  def testSeriesMin(self):
    """Tests for /fitting/iteration_series/merit_value/all/min"""
    # Test for merit_value
    # stat = min
    # iteration_number  candidate_number  merit_value
    # 0 2 973.78207
    # 1 3 973.78207
    # 2 3 973.78207
    # 3 3 973.78207
    # 4 1 964.64312
    # 5 3 964.64312
    j = self.fetchJSON('merit_value/all/min')
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value'],
               'values'  : [
                    [0 ,2, 973.78207],
                    [1 ,3, 973.78207],
                    [2 ,3, 973.78207],
                    [3 ,1, 973.78207],
                    [4 ,1, 964.64312],
                    [5 ,3, 964.64312]
                  ]}
    testutil.compareCollection(self, expect, j)

  def testSeriesMax(self):
    """Tests for /fitting/iteration_series/merit_value/all/max"""
    # stat = max
    # iteration_number  candidate_number  merit_value
    # 0 1 56979.43601
    # 1 0 5283.62466
    # 2 1 5096.59874
    # 3 2 1546.33659
    # 4 0 2300.90601
    # 5 1 12634.65516
    j = self.fetchJSON('merit_value/all/max')
    expect = {
      'columns' : ['iteration_number', 'candidate_number', 'merit_value'],
      'values'  : [
        [0, 1, 56979.43601],
        [1, 0, 5283.62466],
        [2, 1, 5096.59874],
        [3, 2, 1546.33659],
        [4, 0, 2300.90601],
        [5, 1, 12634.65516],
      ]
    }
    testutil.compareCollection(self, expect, j)
