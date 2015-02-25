# -*- coding: utf-8 -*-

import unittest

from .. import testutil

from atsim.pro_fit import db
from _dbtestcase import DBTestCase

class IterationSeries_IterationFilter_TestCase(DBTestCase):
  dbname = "population_fitting_run.db"

  def testSeriesRunningMin(self):
    """Tests for  iterationFilter = running_min, candidateFilter = min"""
    # stat = running_min
    # iteration_number  candidate_number  merit_value
    # 0 0 2 973.78207
    # 4 1 964.64312
    # j = self.fetchJSON('merit_value/running_min/min')

    t = db.IterationSeriesTable(self.engine,
      iterationFilter = "running_min",
      candidateFilter = "min")

    expect = {
      'columns' : ['iteration_number', 'candidate_number', 'merit_value'],
      'values'  : [
         [0, 2, 973.78207],
         [4, 1, 964.64312]
      ]
    }
    actual = {'columns' : t.next(),
              'values'  : list(t)}
    testutil.compareCollection(self, expect, actual)


  def testSeriesRunningMax(self):
    """Tests for iterationFilter = running_max and candidateFilter = max"""
    # stat = running_max
    # iteration_number  candidate_number  merit_value
    # 0 1 56979.43601
    # j = self.fetchJSON('merit_value/running_max/max')

    t = db.IterationSeriesTable(self.engine,
      iterationFilter = "running_max",
      candidateFilter = "max")


    expect = {
      'columns' : ['iteration_number', 'candidate_number', 'merit_value'],
      'values'  : [
        [0, 1, 56979.43601]
      ]
    }
    actual = {'columns' : t.next(),
              'values'  : list(t)}
    testutil.compareCollection(self, expect, actual)
