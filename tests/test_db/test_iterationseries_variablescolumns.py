# -*- coding: utf-8 -*-

import unittest

from .. import testutil

from ._dbtestcase import DBTestCase

from atsim.pro_fit import db

class IterationSeries_VariablesColumns_TestCase(DBTestCase):
  dbname = "population_fitting_run.db"

  def testVariablesColumns(self):
    """Tests for the variable: column types"""
    # baserequest = 'merit_value/all/min?columns=variable:morse_Ca_O_A'

    primary_column = 'merit_value'
    iteration_filter = 'all'
    candidate_filter = 'min'

    columns = ['variable:morse_Ca_O_A']

    tableIterator = db.IterationSeriesTable(self.engine,
      primary_column,
      iteration_filter,
      candidate_filter,
      columns)

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

    columns = next(tableIterator)
    actual = {
      'columns' : columns,
      'values'  : list(tableIterator)
    }


    testutil.compareCollection(self, expect, actual)

  def testVariablesColumnProvider(self):
    """Tests for _VariablesColumnProvider"""
    from atsim.pro_fit.db import IterationSeriesTable
    from atsim.pro_fit.db._columnproviders import _VariablesColumnProvider

    itseries = IterationSeriesTable(self.engine)

    with itseries._temporaryCandidateContextManager('merit_value', 'running_min', 'min') as (conn,meta):
      col = _VariablesColumnProvider(conn, meta, 'variable:morse_Ca_O_A')
      self.assertEqual('morse_Ca_O_A', col.variableName)

      # Check that internal results set is as expected.
      expect = {
      'columns' : ['candidate_id', 'iteration_number', 'candidate_number','value'],
      'values'  : [
         [3,0,2, 0.473366852725934],
         [18,4,1, 0.10328268378764]
      ]}

      actual = {
        'columns' : list(col.results.keys()),
        'values'  : col.results.fetchall()
      }
      testutil.compareCollection(self, expect,actual)

      # Check that KeyError is raised for a bad variable name
      with self.assertRaises(KeyError):
        col = _VariablesColumnProvider(conn, meta, 'variable:morse_Mg_O_C_bad')
