# -*- coding: utf-8 -*-
from _cherrypydbtestcase import CherryPyDBTestCaseBase
from .. import testutil


class IterationSeriesTestCase(CherryPyDBTestCaseBase):
  """Tests for the cherrypy handlers under /fitting/iteration_series"""

  dbname = "population_fitting_run.db"
  baseurl = 'http://localhost:8080/fitting/iteration_series'

  def testIsRunningColumns(self):
    """Tests for the it:is_running_min and it:is_running_max column types"""
    baserequest= 'merit_value/all/min?columns=it:is_running_min'
    j = self.fetchJSON(baserequest)
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'it:is_running_min'],
               'values'  : [
                    [0 ,2, 973.78207, True],
                    [1 ,3, 973.78207, False],
                    [2 ,3, 973.78207, False],
                    [3 ,1, 973.78207, False],
                    [4 ,1, 964.64312, True],
                    [5 ,3, 964.64312, False]
                  ]}
    testutil.compareCollection(self, expect, j)

    baserequest= 'merit_value/all/min?columns=it:is_running_max'
    j = self.fetchJSON(baserequest)
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'it:is_running_max'],
               'values'  : [
                    [0 ,2, 973.78207, True],
                    [1 ,3, 973.78207, False],
                    [2 ,3, 973.78207, False],
                    [3 ,1, 973.78207, False],
                    [4 ,1, 964.64312, False],
                    [5 ,3, 964.64312, False]
                  ]}
    testutil.compareCollection(self, expect, j)


    baserequest= 'merit_value/all/min?columns=it:is_running_min,it:is_running_max'
    j = self.fetchJSON(baserequest)
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'it:is_running_min', 'it:is_running_max'],
               'values'  : [
                    [0 ,2, 973.78207, True, True],
                    [1 ,3, 973.78207, False, False],
                    [2 ,3, 973.78207, False, False],
                    [3 ,1, 973.78207, False, False],
                    [4 ,1, 964.64312, True, False],
                    [5 ,3, 964.64312, False, False]
                  ]}
    testutil.compareCollection(self, expect, j)

