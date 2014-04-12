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

  def testMakeTemporary(self):
    """Test that temporary database table is created correctly by webmonitor._temporaryCandidateContextManager()"""

    # Creat session object
    import atsim.pro_fit._sqlalchemy_cherrypy_integration as sacpi
    from atsim.pro_fit.webmonitor import IterationSeries, _formatResults
    import sqlalchemy as sa


    class App(object):
      config = {'/' : {'tools.SATransaction.dburi' : self.dburl}}

    sacpi.configure_session_for_app(App())
    session = sacpi.session
    itseries = IterationSeries()

    iterationFilter = 'running_min'
    candidateFilter = 'min'
    primaryColumnKey = 'merit_value'
    with itseries._temporaryCandidateContextManager(primaryColumnKey, iterationFilter, candidateFilter) as (conn,meta):
      # Query the database
      t = meta.tables['temp_iterationseries']
      query = sa.select([
        t.c.candidate_id,
        t.c.iteration_number,
        t.c.candidate_number,
        t.c.primary_value ])


      results = conn.execute(query)
      actual = _formatResults(results)

      # Check what's in the table
      expect = {
      'columns' : ['candidate_id', 'iteration_number', 'candidate_number', 'primary_value'],
      'values'  : [
         [3, 0, 2, 973.78207],
         [18, 4, 1, 964.64312]
      ]}

      testutil.compareCollection(self, expect,actual)

