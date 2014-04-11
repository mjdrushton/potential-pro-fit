# -*- coding: utf-8 -*-
from _cherrypydbtestcase import CherryPyDBTestCaseBase
import testutil

class IterationSeries_IterationFilter_TestCase(CherryPyDBTestCaseBase):
  dbname = "population_fitting_run.db"
  baseurl = 'http://localhost:8080/fitting/iteration_series'

  def testSeriesRunningMin(self):
    """Tests for  /fitting/iteration_series/merit_value/running_min/min"""
    # stat = running_min
    # iteration_number  candidate_number  merit_value
    # 0 0 2 973.78207
    # 4 1 964.64312
    j = self.fetchJSON('merit_value/running_min/min')
    expect = {
      'columns' : ['iteration_number', 'candidate_number', 'merit_value'],
      'values'  : [
         [0, 2, 973.78207],
         [4, 1, 964.64312]
      ]
    }
    testutil.compareCollection(self, expect, j)


  def testSeriesRunningMax(self):
    """Tests for /fitting/iteration_series/merit_value/running_max/max"""
    # stat = running_max
    # iteration_number  candidate_number  merit_value
    # 0 1 56979.43601
    j = self.fetchJSON('merit_value/running_max/max')
    expect = {
      'columns' : ['iteration_number', 'candidate_number', 'merit_value'],
      'values'  : [
        [0, 1, 56979.43601]
      ]
    }
    testutil.compareCollection(self, expect, j)
