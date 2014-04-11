# -*- coding: utf-8 -*-
from _cherrypydbtestcase import CherryPyDBTestCaseBase
from atomsscripts import testutil

class IterationSeries_VariablesColumns_TestCase(CherryPyDBTestCaseBase):
  dbname = "population_fitting_run.db"
  baseurl = 'http://localhost:8080/fitting/iteration_series'


  def testVariablesColumns(self):
    """Tests for the variable: column types"""
    baserequest = 'merit_value/all/min?columns=variable:morse_Ca_O_A'
    j = self.fetchJSON(baserequest)
    expect = {
               'columns' : ['iteration_number', 'candidate_number', 'merit_value', 'variable:morse_Ca_O_A'],
               'values'  : [
                    [0 ,2, 973.78207, 0.473366852725934],
                    [1 ,3, 973.78207, 0.473366852725934],
                    [2 ,3, 973.78207, 0.473366852725934],
                    [3 ,1, 973.78207, 0.473366852725934],
                    [4 ,1, 964.64312, 0.10328268378764],
                    [5 ,3, 964.64312, 0.10328268378764]
                  ]}
    testutil.compareCollection(self, expect, j)

  def testVariablesColumnProvider(self):
    """Tests for _VariablesColumnProvider"""
    import atsim.pro_fit._sqlalchemy_cherrypy_integration as sacpi
    from atsim.pro_fit.webmonitor import IterationSeries, _formatResults
    from atsim.pro_fit.webmonitor._columnproviders import _VariablesColumnProvider
    import sqlalchemy as sa

    class App(object):
      config = {'/' : {'tools.SATransaction.dburi' : self.dburl}}

    sacpi.configure_session_for_app(App())
    session = sacpi.session

    itseries = IterationSeries()

    with itseries._temporaryCandidateContextManager('merit_value', 'running_min', 'min') as (conn,meta):
      col = _VariablesColumnProvider(conn, meta, 'variable:morse_Ca_O_A')
      self.assertEquals('morse_Ca_O_A', col.variableName)

      # Check that internal results set is as expected.
      expect = {
      'columns' : ['candidate_id', 'iteration_number', 'candidate_number','value'],
      'values'  : [
         [3,0,2, 0.473366852725934],
         [18,4,1, 0.10328268378764]
      ]}

      actual = _formatResults(col.results)
      testutil.compareCollection(self, expect,actual)

      # Check that KeyError is raised for a bad variable name
      with self.assertRaises(KeyError):
        col = _VariablesColumnProvider(conn, meta, 'variable:morse_Mg_O_C_bad')
