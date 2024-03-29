# -*- coding: utf-8 -*-
from .. import testutil

from ._dbtestcase import DBTestCase

from atsim.pro_fit import db
import atsim.pro_fit.db._columnproviders as cp

import sqlalchemy as sa

import unittest
import itertools


class ColumnKeysTestCase(DBTestCase):
    """Test introspection of datbase for column keys for use with IterationSeriesTable"""

    dbname = "meta_eval.db"

    @classmethod
    def fittingVariableExpect(cls):
        return sorted(["variable:C_Gd_O"])

    @classmethod
    def variableExpect(cls):
        return sorted(
            [
                "variable:Ce_charge",
                "variable:O_charge",
                "variable:M_charge",
                "variable:A_La_O",
                "variable:rho_La_O",
                "variable:A_Sm_O",
                "variable:rho_Sm_O",
                "variable:A_Dy_O",
                "variable:rho_Dy_O",
                "variable:A_Y_O",
                "variable:rho_Y_O",
                "variable:A_Yb_O",
                "variable:rho_Yb_O",
                "variable:C_Dy_O",
                "variable:C_Er_O",
                "variable:C_Gd_O",
                "variable:C_Ho_O",
                "variable:C_La_O",
                "variable:C_Nd_O",
                "variable:C_Sm_O",
                "variable:C_Yb_O",
                "variable:C_Y_O",
                "variable:A_Nd_O",
                "variable:rho_Nd_O",
                "variable:A_Gd_O",
                "variable:rho_Gd_O",
                "variable:A_Ho_O",
                "variable:rho_Ho_O",
                "variable:A_Er_O",
                "variable:rho_Er_O",
            ]
        )

    def testVariables(self):
        """Test that _columnproviders._VariablesColumnProvider() returns correct column keys"""
        expect = self.variableExpect()
        expect.sort()
        actual = cp._VariablesColumnProvider.validKeys(self.engine)
        actual.sort()

        testutil.compareCollection(self, expect, actual)

    @classmethod
    def evaluatorExpect(cls):
        expect = [
            "Gd_x=0.250a:Gd_x=0.250a:Volume:V",
            "T=1400K_Gd_x=0.1:T=1400K_Gd_x=0.1:Volume:V",
            "T=1400K_Gd_x=0.1:T=1400K_Gd_x=0.1:D:D",
            "T=1600K_Gd_x=0.1:T=1600K_Gd_x=0.1:Volume:V",
            "T=1600K_Gd_x=0.1:T=1600K_Gd_x=0.1:D:D",
            "T=1800K_Gd_x=0.1:T=1800K_Gd_x=0.1:Volume:V",
            "T=1800K_Gd_x=0.1:T=1800K_Gd_x=0.1:D:D",
            "meta_evaluator:Gd_Ea:deltaD18_16",
            "meta_evaluator:Gd_Ea:deltaD18_14",
        ]

        expect = ["evaluator:" + e for e in expect]
        allexpect = []

        for suffix in ["extracted_value", "merit_value", "percent_difference"]:
            for e in expect:
                allexpect.append(e + ":" + suffix)

        expect = allexpect
        expect.sort()
        return expect

    def testEvaluators(self):
        """Test that _columnproviders._EvaluatorColumnProvider() returns correct column keys"""
        expect = self.evaluatorExpect()
        expect.sort()
        actual = cp._EvaluatorColumnProvider.validKeys(self.engine)
        actual.sort()
        testutil.compareCollection(self, expect, actual)

    @classmethod
    def statExpect(self):
        expect = [
            "stat:min",
            "stat:max",
            "stat:mean",
            "stat:median",
            "stat:std_dev",
            "stat:quartile1",
            "stat:quartile2",
            "stat:quartile3",
        ]
        expect.sort()
        return expect

    def testStat(self):
        """Test that _columnproviders._StatColumnProvider() returns correct column keys"""
        expect = self.statExpect()
        actual = cp._StatColumnProvider.validKeys(self.engine)
        actual.sort()
        testutil.compareCollection(self, expect, actual)

    @classmethod
    def runningFilterExpect(cls):
        return sorted(["it:is_running_min", "it:is_running_max"])

    def testRunningFilter(self):
        """Test validKeys for db._columnProviders._RunningFilterColumnProvider"""
        expect = self.runningFilterExpect()
        expect.sort()
        actual = cp._RunningFilterColumnProvider.validKeys(self.engine)
        actual.sort()
        testutil.compareCollection(self, expect, actual)

    @classmethod
    def allExpect(cls):
        expect = itertools.chain(
            sorted(cls.runningFilterExpect()),
            sorted(cls.statExpect()),
            sorted(cls.evaluatorExpect()),
            sorted(cls.variableExpect()),
        )
        expect = list(expect)
        return expect

    def testIterationSeriesKeys(self):
        """Test db.IterationSeriesTable.validKeys() method"""
        actual = db.IterationSeriesTable.validKeys(self.engine)
        expect = self.allExpect()
        testutil.compareCollection(self, expect, actual)

    def testIterationSeriesEvaluatorKeys(self):
        expect = sorted(self.evaluatorExpect())
        actual = db.IterationSeriesTable.validEvaluatorKeys(self.engine)
        testutil.compareCollection(self, expect, actual)

    def testIterationSeriesVariableKeys(self):
        expect = sorted(self.variableExpect())
        actual = db.IterationSeriesTable.validVariableKeys(self.engine)
        testutil.compareCollection(self, expect, actual)

    def testIterationSeriesFittingVariableKeys(self):
        expect = sorted(self.fittingVariableExpect())
        actual = db.IterationSeriesTable.validFittingVariableKeys(self.engine)
        testutil.compareCollection(self, expect, actual)

    def testIterationSeriesIterationKeys(self):
        expect = sorted(self.runningFilterExpect())
        actual = db.IterationSeriesTable.validIterationKeys(self.engine)
        testutil.compareCollection(self, expect, actual)

    def testIterationSeriesStatisticsKeys(self):
        expect = sorted(self.statExpect())
        actual = db.IterationSeriesTable.validStatisticsKeys(self.engine)
        testutil.compareCollection(self, expect, actual)


class IterationSeriesTestCase(DBTestCase):
    """Tests for the cherrypy handlers under /fitting/iteration_series"""

    dbname = "population_fitting_run.db"

    def testIsRunningMin(self):
        """Tests for the it:is_running_min and it:is_running_max column types"""
        # baserequest= 'merit_value/all/min?columns=it:is_running_min'

        t = db.IterationSeriesTable(self.engine, columns=["it:is_running_min"])
        expect = {
            "columns": [
                "iteration_number",
                "candidate_number",
                "merit_value",
                "it:is_running_min",
            ],
            "values": [
                [0, 2, 973.78207, True],
                [1, 3, 973.78207, False],
                [2, 3, 973.78207, False],
                [3, 1, 973.78207, False],
                [4, 1, 964.64312, True],
                [5, 3, 964.64312, False],
            ],
        }

        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testIsRunningMax(self):
        # baserequest= 'merit_value/all/min?columns=it:is_running_max'
        t = db.IterationSeriesTable(self.engine, columns=["it:is_running_max"])
        expect = {
            "columns": [
                "iteration_number",
                "candidate_number",
                "merit_value",
                "it:is_running_max",
            ],
            "values": [
                [0, 2, 973.78207, True],
                [1, 3, 973.78207, False],
                [2, 3, 973.78207, False],
                [3, 1, 973.78207, False],
                [4, 1, 964.64312, False],
                [5, 3, 964.64312, False],
            ],
        }

        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testIsRunningMinIsRunningMax(self):
        # baserequest= 'merit_value/all/min?columns=it:is_running_min,it:is_running_max'
        t = db.IterationSeriesTable(
            self.engine, columns=["it:is_running_min", "it:is_running_max"]
        )
        expect = {
            "columns": [
                "iteration_number",
                "candidate_number",
                "merit_value",
                "it:is_running_min",
                "it:is_running_max",
            ],
            "values": [
                [0, 2, 973.78207, True, True],
                [1, 3, 973.78207, False, False],
                [2, 3, 973.78207, False, False],
                [3, 1, 973.78207, False, False],
                [4, 1, 964.64312, True, False],
                [5, 3, 964.64312, False, False],
            ],
        }
        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testMakeTemporary(self):
        """Test that temporary database table is created correctly by webmonitor._temporaryCandidateContextManager()"""
        itseries = db.IterationSeriesTable(self.engine)

        iterationFilter = "running_min"
        candidateFilter = "min"
        primaryColumnKey = "merit_value"
        with itseries._temporaryCandidateContextManager(
            primaryColumnKey, iterationFilter, candidateFilter
        ) as (conn, meta):
            # Query the database
            t = meta.tables["temp_iterationseries"]
            query = sa.select(
                [
                    t.c.candidate_id,
                    t.c.iteration_number,
                    t.c.candidate_number,
                    t.c.primary_value,
                ]
            )

            results = conn.execute(query)
            actual = {
                "columns": list(results.keys()),
                "values": results.fetchall(),
            }

            # Check what's in the table
            expect = {
                "columns": [
                    "candidate_id",
                    "iteration_number",
                    "candidate_number",
                    "primary_value",
                ],
                "values": [[3, 0, 2, 973.78207], [18, 4, 1, 964.64312]],
            }

            testutil.compareCollection(self, expect, actual)
