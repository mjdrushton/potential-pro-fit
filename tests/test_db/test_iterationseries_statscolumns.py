# -*- coding: utf-8 -*-

import unittest

from .. import testutil

from ._dbtestcase import DBTestCase

from atsim.pro_fit import db


class IterationSeries_StatsColumns_TestCase(DBTestCase):
    dbname = "population_fitting_run.db"

    def testSeriesMean(self):
        """Tests for columns=stat:mean"""
        # stat = mean
        # iteration_number merit_value:mean
        # 0 16404.84837
        # 1 3670.86345
        # 2 2736.541433
        # 3 1118.587493
        # 4 1303.278318
        # 5 4142.853898
        # j = self.fetchJSON('merit_value/all/min?columns=stat:mean')
        t = db.IterationSeriesTable(self.engine, columns=["stat:mean"])

        expect = {
            "columns": [
                "iteration_number",
                "candidate_number",
                "merit_value",
                "stat:mean",
            ],
            "values": [
                [0, 2, 973.78207, 16404.84837],
                [1, 3, 973.78207, 3670.86345],
                [2, 3, 973.78207, 2736.541433],
                [3, 1, 973.78207, 1118.587493],
                [4, 1, 964.64312, 1303.278318],
                [5, 3, 964.64312, 4142.853898],
            ],
        }
        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testSeriesMedian(self):
        """Tests for columns=stat:median"""
        # stat = median
        # id,iteration_number,candidate_number,merit_value

        # 3,0,2,973.78207
        # 4,0,3,4336.72706
        # 1,0,0,3329.44833
        # 2,0,1,56979.43601

        # 8,1,3,973.78207
        # 7,1,2,3329.44833
        # 6,1,1,5096.59874
        # 5,1,0,5283.62466

        # 12,2,3,973.78207
        # 9,2,0,1546.33659
        # 11,2,2,3329.44833
        # 10,2,1,5096.59874

        # 14,3,1,973.78207
        # 16,3,3,973.78207
        # 13,3,0,980.44924
        # 15,3,2,1546.33659

        # 18,4,1,964.64312
        # 19,4,2,973.78207
        # 20,4,3,973.78207
        # 17,4,0,2300.90601

        # 24,5,3,964.64312
        # 23,5,2,973.78207
        # 21,5,0,1998.33524
        # 22,5,1,12634.65516

        # j = self.fetchJSON('merit_value/all/min?columns=stat:median')
        t = db.IterationSeriesTable(self.engine, columns=["stat:median"])

        expect = {
            "columns": [
                "iteration_number",
                "candidate_number",
                "merit_value",
                "stat:median",
            ],
            "values": [
                [0, 2, 973.78207, (4336.72706 + 3329.44833) / 2.0],
                [1, 3, 973.78207, (3329.44833 + 5096.59874) / 2.0],
                [2, 3, 973.78207, (1546.33659 + 3329.44833) / 2.0],
                [3, 1, 973.78207, (973.78207 + 980.44924) / 2.0],
                [4, 1, 964.64312, 973.78207],
                [5, 3, 964.64312, (973.78207 + 1998.33524) / 2.0],
            ],
        }
        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testSeriesStdDev(self):
        """Test for columns=stat:std_dev"""
        # stat = std_dev
        # iteration_number merit_value:std_dev
        # 0 23457.51802
        # 1 1733.820355
        # 2 1615.960799
        # 3 246.9760555
        # 4 575.9927005
        # 5 4920.71359

        # TODO: Add primary column, candidate number and stat:std_dev column header
        # j = self.fetchJSON('merit_value/all/min?columns=stat:std_dev')
        t = db.IterationSeriesTable(self.engine, columns=["stat:std_dev"])

        expect = {
            "columns": [
                "iteration_number",
                "candidate_number",
                "merit_value",
                "stat:std_dev",
            ],
            "values": [
                [0, 2, 973.78207, 23457.51802],
                [1, 3, 973.78207, 1733.820355],
                [2, 3, 973.78207, 1615.960799],
                [3, 1, 973.78207, 246.9760555],
                [4, 1, 964.64312, 575.9927005],
                [5, 3, 964.64312, 4920.71359],
            ],
        }
        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)

    def testSeriesQuartiles(self):
        """Tests for columns=quartile 1,2,3"""

        minData = [
            [0, 2, 973.78207],
            [1, 3, 973.78207],
            [2, 3, 973.78207],
            [3, 1, 973.78207],
            [4, 1, 964.64312],
            [5, 3, 964.64312],
        ]

        maxData = [
            56979.43601,
            5283.62466,
            5096.59874,
            1546.33659,
            2300.90601,
            12634.65516,
        ]

        quartile1expect = [
            2151.6152,
            2151.6152,
            1260.05933,
            973.78207,
            964.64312,
            969.212595,
        ]

        quartile2expect = [
            (4336.72706 + 3329.44833) / 2.0,
            (3329.44833 + 5096.59874) / 2.0,
            (1546.33659 + 3329.44833) / 2.0,
            (973.78207 + 980.44924) / 2.0,
            973.78207,
            (973.78207 + 1998.33524) / 2.0,
        ]

        quartile3expect = [
            30658.08154,
            5190.1117,
            4213.023535,
            1263.392915,
            2300.90601,
            7316.4952,
        ]

        # baserequest = 'merit_value/all/min?columns=stat:quartile'
        for q, e in zip(
            ["1", "2", "3"],
            [quartile1expect, quartile2expect, quartile3expect],
        ):
            t = db.IterationSeriesTable(
                self.engine, columns=["stat:quartile" + q]
            )

            values = []
            for m, qv in zip(minData, e):
                vrow = list(m)
                vrow.append(qv)
                values.append(vrow)

            expect = {
                "columns": [
                    "iteration_number",
                    "candidate_number",
                    "merit_value",
                    "stat:quartile" + q,
                ],
                "values": values,
            }
            actual = {"columns": next(t), "values": list(t)}
            testutil.compareCollection(self, expect, actual)

        # j = self.fetchJSON('merit_value/all/min?columns=stat:min,stat:max,stat:quartile1,stat:quartile2,stat:quartile3')
        t = db.IterationSeriesTable(
            self.engine,
            columns=[
                "stat:min",
                "stat:max",
                "stat:quartile1",
                "stat:quartile2",
                "stat:quartile3",
            ],
        )

        expectvalues = []
        for rows in zip(
            minData, maxData, quartile1expect, quartile2expect, quartile3expect
        ):
            newrow = []
            for d in rows:
                if newrow:
                    newrow.append(d)
                else:
                    newrow = list(d)
                    newrow.append(newrow[-1])
            expectvalues.append(newrow)
        # import pudb;pudb.set_trace()
        expect = {
            "columns": [
                "iteration_number",
                "candidate_number",
                "merit_value",
                "stat:min",
                "stat:max",
                "stat:quartile1",
                "stat:quartile2",
                "stat:quartile3",
            ],
            "values": expectvalues,
        }

        actual = {"columns": next(t), "values": list(t)}
        testutil.compareCollection(self, expect, actual)
