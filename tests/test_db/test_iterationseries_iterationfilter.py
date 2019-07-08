# -*- coding: utf-8 -*-

import unittest

from .. import testutil

from atsim.pro_fit import db
from ._dbtestcase import DBTestCase


class IterationSeries_IterationFilter_TestCase(DBTestCase):
    dbname = "population_fitting_run.db"

    def testSeriesRunningMin(self):
        """Tests for  iterationFilter = running_min, candidateFilter = min"""
        # stat = running_min
        # iteration_number  candidate_number  merit_value
        # 0 0 2 973.78207
        # 4 1 964.64312
        # j = self.fetchJSON('merit_value/running_min/min')

        t = db.IterationSeriesTable(
            self.engine, iterationFilter="running_min", candidateFilter="min"
        )

        expect = {
            "columns": ["iteration_number", "candidate_number", "merit_value"],
            "values": [[0, 2, 973.78207], [4, 1, 964.64312]],
        }
        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testSeriesRunningMax(self):
        """Tests for iterationFilter = running_max and candidateFilter = max"""
        # stat = running_max
        # iteration_number  candidate_number  merit_value
        # 0 1 56979.43601
        # j = self.fetchJSON('merit_value/running_max/max')

        t = db.IterationSeriesTable(
            self.engine, iterationFilter="running_max", candidateFilter="max"
        )

        expect = {
            "columns": ["iteration_number", "candidate_number", "merit_value"],
            "values": [[0, 1, 56979.43601]],
        }
        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testBest(self):
        """Tests for iterationFilter = contains_best and candidateFilter = all"""
        # stat = running_max
        # iteration_number  candidate_number  merit_value
        # 0 1 56979.43601
        # j = self.fetchJSON('merit_value/running_max/max')

        t = db.IterationSeriesTable(
            self.engine, iterationFilter="global_min", candidateFilter="all"
        )

        expect = {
            "columns": ["iteration_number", "candidate_number", "merit_value"],
            "values": [
                [4, 0, 2300.90601],
                [4, 1, 964.64312],
                [4, 2, 973.78207],
                [4, 3, 973.78207],
            ],
        }

        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testLast(self):
        """Tests for iterationFilter = last and candidateFilter = min"""
        # stat = running_max
        # iteration_number  candidate_number  merit_value
        # 0 1 56979.43601
        # j = self.fetchJSON('merit_value/running_max/max')

        t = db.IterationSeriesTable(
            self.engine, iterationFilter="last", candidateFilter="min"
        )

        expect = {
            "columns": ["iteration_number", "candidate_number", "merit_value"],
            "values": [[5, 3, 964.64312]],
        }

        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testN(self):
        """Tests for iterationFilter = n(2) and candidateFilter = all"""
        # stat = running_max
        # iteration_number  candidate_number  merit_value
        # 0 1 56979.43601
        # j = self.fetchJSON('merit_value/running_max/max')

        t = db.IterationSeriesTable(
            self.engine, iterationFilter="n(2)", candidateFilter="all"
        )

        expect = {
            "columns": ["iteration_number", "candidate_number", "merit_value"],
            "values": [
                [2, 0, 1546.33659],
                [2, 1, 5096.59874],
                [2, 2, 3329.44833],
                [2, 3, 973.78207],
            ],
        }

        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testConstructorArguments(self):
        """Check that IterationSeriesTable throws when incompatible iteration and candidate filters specified"""

        with self.assertRaises(db.BadFilterCombinationException):
            db.IterationSeriesTable(
                self.engine,
                iterationFilter="running_max",
                candidateFilter="all",
            )

        with self.assertRaises(db.BadFilterCombinationException):
            db.IterationSeriesTable(
                self.engine,
                iterationFilter="running_min",
                candidateFilter="all",
            )
