# -*- coding: utf-8 -*-

import unittest

from _dbtestcase import DBTestCase

from .. import testutil

from atsim.pro_fit import db


class IterationSeries_CandidateFilter_TestCase(DBTestCase):
  dbname = "population_fitting_run.db"

  def testSeriesMin(self):
    """Tests for 'min' candidateFilter"""
    # Test for merit_value
    # stat = min
    # iteration_number  candidate_number  merit_value
    # 0 2 973.78207
    # 1 3 973.78207
    # 2 3 973.78207
    # 3 3 973.78207
    # 4 1 964.64312
    # 5 3 964.64312
    t = db.IterationSeriesTable(self.engine, candidateFilter = "min")
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
    actual = {'columns' : t.next(),
              'values'  : list(t)}
    testutil.compareCollection(self, expect, actual)

  def testSeriesMax(self):
    """Tests for candidateFilter = 'max' """
    # stat = max
    # iteration_number  candidate_number  merit_value
    # 0 1 56979.43601
    # 1 0 5283.62466
    # 2 1 5096.59874
    # 3 2 1546.33659
    # 4 0 2300.90601
    # 5 1 12634.65516
    t = db.IterationSeriesTable(self.engine, candidateFilter = "max")
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
    actual = {'columns' : t.next(),
              'values'  : list(t)}
    testutil.compareCollection(self, expect, actual)
