import unittest

from atsim import pro_fit

from ._common import *


class MinimizerResultsTestCase(unittest.TestCase):
    """Tests for atsim.pro_fit.minimizers.MinimizerResults"""

    def testMinimizerResults(self):
        meritVals = [2.0, 3.0, 1.0]

        V = pro_fit.fittool.Variables
        c1 = V([("A", 1.0, False), ("B", 2.0, True)])
        c2 = V([("A", 1.0, False), ("B", 2.0, True)])
        c3 = V([("A", 1.0, False), ("B", 2.0, True)])

        j1 = MockJob(c1)
        j2 = MockJob(c2)
        j3 = MockJob(c3)

        candidateJobList = [(c1, [j1]), (c2, [j2]), (c3, [j3])]

        results = pro_fit.minimizers.MinimizerResults(
            meritVals, candidateJobList
        )

        self.assertEqual(meritVals, results.meritValues)
        self.assertEqual(candidateJobList, results.candidateJobList)

        self.assertEqual(1.0, results.bestMeritValue)
        self.assertEqual(2, results.indexOfBest)
        self.assertEqual(candidateJobList[2][1], results.bestJobList)

    def testComparison(self):
        meritVals = [2.0, 3.0, 1.0]
        meritVals2 = [2.0, 0.0, 1.0]

        V = pro_fit.fittool.Variables
        c1 = V([("A", 1.0, False), ("B", 2.0, True)])
        c2 = V([("A", 1.0, False), ("B", 2.0, True)])
        c3 = V([("A", 1.0, False), ("B", 2.0, True)])

        j1 = MockJob(c1)
        j2 = MockJob(c2)
        j3 = MockJob(c3)

        candidateJobList = [(c1, [j1]), (c2, [j2]), (c3, [j3])]

        results1 = pro_fit.minimizers.MinimizerResults(
            meritVals, candidateJobList
        )
        results2 = pro_fit.minimizers.MinimizerResults(
            meritVals2, candidateJobList
        )

        self.assertTrue(results2 < results1)
        self.assertTrue(results1 > results2)
        self.assertTrue(results1 == results1)
        self.assertTrue(results2 == results2)
