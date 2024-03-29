import itertools
import logging
import math
import os
import shutil
import stat
import sys
import unittest

import atsim.pro_fit.evaluators
import atsim.pro_fit.merit
import atsim.pro_fit.variables
import gevent

from . import testutil
from .common import (MockEvaluator, MockJobFactory, MockRunner, e1, e2, e3,
                     logger)


class MeritTestCase(unittest.TestCase):
    """Test for the pro_fit.fitting.merit.Merit class"""

    def setUp(self):
        # Create two initial candidates
        initialVariables = atsim.pro_fit.variables.Variables(
            [
                ("A", 1.0, False),
                ("B", 2.0, True),
                ("C", 3.0, False),
                ("D", 4.0, True),
            ]
        )

        c1 = initialVariables.createUpdated([2.5, 4.5])
        c1.id = 1
        c2 = initialVariables.createUpdated([4.5, 8.5])
        c2.id = 2
        self.candidates = [c1, c2]

        # Now create two runners and four jobs
        r1 = MockRunner("Runner1")
        r2 = MockRunner("Runner2")

        eval1 = MockEvaluator(e1)
        eval2 = MockEvaluator(e2)
        eval3 = MockEvaluator(e3)

        j1 = MockJobFactory("Runner1", "Job1", [eval1])
        j2 = MockJobFactory("Runner2", "Job2", [eval1, eval2])
        j3 = MockJobFactory("Runner1", "Job3", [eval1, eval2, eval3])
        j4 = MockJobFactory("Runner2", "Job4", [eval3])

        class MockMetaEvaluator(object):
            def __init__(self):
                self.name = "MockMetaEvaluator"

            def __call__(self, jobs):
                meritval = 0
                for j in jobs:
                    if j.name == "Job1":
                        meritval += j.evaluatorRecords[0][0].meritValue
                    elif j.name == "Job3":
                        meritval += j.evaluatorRecords[2][0].meritValue
                return [
                    atsim.pro_fit.evaluators.EvaluatorRecord(
                        "value",
                        0.0,
                        meritval,
                        1.0,
                        meritval,
                        "MockMetaEvaluator",
                    )
                ]

        import tempfile

        self.tempd = tempfile.mkdtemp()
        self.mmtempd = tempfile.mkdtemp()
        self.merit = atsim.pro_fit.merit.Merit(
            [r1, r2],
            [j1, j2, j3, j4],
            [],
            atsim.pro_fit.variables.CalculatedVariables([]),
            self.tempd,
        )
        self.metamerit = atsim.pro_fit.merit.Merit(
            [r1, r2],
            [j1, j2, j3, j4],
            [MockMetaEvaluator()],
            atsim.pro_fit.variables.CalculatedVariables([]),
            self.mmtempd,
        )

    def tearDown(self):
        shutil.rmtree(self.tempd, ignore_errors=True)
        shutil.rmtree(self.mmtempd, ignore_errors=True)

    def _jobToDict(self, job):
        d = {}
        infilename = os.path.join(job.path, "job_files", "runjob")
        with open(infilename, "r") as infile:
            next(infile)

            line = next(infile)[:-1]
            self.assertTrue(line.startswith("#Job:"))
            d["Job"] = line.split(":")[1].strip()

            line = next(infile)[:-1]
            self.assertTrue(line.startswith("#Runner:"))
            d["Runner"] = line.split(":")[1].strip()

            line = next(infile)[:-1]
            self.assertTrue(line.startswith("#Candidate:"))
            d["Candidate"] = int(line.split(":")[1].strip())

            variables = {}
            for line in infile:
                if line.startswith("#Variable:"):
                    tokens = line.split(":")
                    k = tokens[1].strip()
                    v = tokens[2]
                    v = float(v)
                    variables[k] = v

            d["variables"] = variables
        return d

    def _outputJobToDict(self, job):
        d = {}
        outputpath = os.path.join(job.path, "job_files", "output")
        outputfilename = os.path.join(outputpath, "output.res")
        with open(outputfilename) as infile:
            for line in infile:
                line = line[:-1]
                k, v = line.split(":")
                k = k.strip()
                logger.debug("Output.res variable: %s:%s" % (k, v))
                v = float(v.strip())
                d[k] = v
        return d

    def testCreateJobs(self):
        """Test Merit._prepareJobs()"""

        _batchpaths, jobs, _candidatejoblists = self.merit._prepareJobs(
            self.candidates
        )

        jobdicts = []
        for jl in jobs:
            jobdicts.append([self._jobToDict(j) for j in jl])

        expect = [
            [  # Runner 1.
                # Candidate 1. J1
                dict(
                    Candidate=1,
                    Job="Job1",
                    Runner="Runner1",
                    variables=dict(A=1.0, B=2.5, C=3.0, D=4.5),
                ),
                # Candidate 1. J3
                dict(
                    Candidate=1,
                    Job="Job3",
                    Runner="Runner1",
                    variables=dict(A=1.0, B=2.5, C=3.0, D=4.5),
                ),
                # Candidate 2. J1
                dict(
                    Candidate=2,
                    Job="Job1",
                    Runner="Runner1",
                    variables=dict(A=1.0, B=4.5, C=3.0, D=8.5),
                ),
                # Candidate 2. J3
                dict(
                    Candidate=2,
                    Job="Job3",
                    Runner="Runner1",
                    variables=dict(A=1.0, B=4.5, C=3.0, D=8.5),
                ),
            ],
            [  # Runner 2.
                # Candidate 1. J2
                dict(
                    Candidate=1,
                    Job="Job2",
                    Runner="Runner2",
                    variables=dict(A=1.0, B=2.5, C=3.0, D=4.5),
                ),
                # Candidate 1. J4
                dict(
                    Candidate=1,
                    Job="Job4",
                    Runner="Runner2",
                    variables=dict(A=1.0, B=2.5, C=3.0, D=4.5),
                ),
                # Candidate 2. J2
                dict(
                    Candidate=2,
                    Job="Job2",
                    Runner="Runner2",
                    variables=dict(A=1.0, B=4.5, C=3.0, D=8.5),
                ),
                # Candidate 2. J4
                dict(
                    Candidate=2,
                    Job="Job4",
                    Runner="Runner2",
                    variables=dict(A=1.0, B=4.5, C=3.0, D=8.5),
                ),
            ],
        ]
        testutil.compareCollection(self, expect, jobdicts)

    def testMeritRunBatches(self):
        """Test Merit._runBatches()"""
        _batchPaths, batchedjobs, _candidatejoblists = self.merit._prepareJobs(
            self.candidates
        )
        finishedEvents = self.merit._runBatches(batchedjobs)
        gevent.wait(finishedEvents)

        # Check that directory structure is as expected
        expected = [
            dict(A=1.0, B=2.5, C=3.0, D=4.5),
            dict(A=1.0, B=2.5, C=3.0, D=4.5),
            dict(A=1.0, B=4.5, C=3.0, D=8.5),
            dict(A=1.0, B=4.5, C=3.0, D=8.5),
            dict(A=1.0, B=2.5, C=3.0, D=4.5),
            dict(A=1.0, B=2.5, C=3.0, D=4.5),
            dict(A=1.0, B=4.5, C=3.0, D=8.5),
            dict(A=1.0, B=4.5, C=3.0, D=8.5),
        ]

        for batch in batchedjobs:
            for job in batch:
                outputpath = os.path.join(job.path, "job_files", "output")
                self.assertTrue(
                    os.path.isfile(os.path.join(outputpath, "STATUS"))
                )
                self.assertTrue(
                    os.path.isfile(os.path.join(outputpath, "runjob"))
                )
                self.assertTrue(
                    os.path.isfile(os.path.join(outputpath, "output.res"))
                )

                d = self._outputJobToDict(job)
                e = expected.pop(0)
                testutil.compareCollection(self, e, d)

    def testApplyEvaluators(self):
        """Test Merit._applyEvaluators()"""
        _batchpaths, batchedjobs, candidatejoblists = self.merit._prepareJobs(
            self.candidates
        )
        finishedEvents = self.merit._runBatches(batchedjobs)
        gevent.wait(finishedEvents)
        self.merit._applyEvaluators(batchedjobs)

        # Convert jobs into a dictionary we can feed to compareCollection
        evaluated = []
        for _v, clist in candidatejoblists:
            cdict = {}
            for job in clist:
                jlist = []
                for evaluator in job.evaluatorRecords:
                    jlist.append(
                        dict([(r.name, r.meritValue) for r in evaluator])
                    )
                cdict[job.name] = jlist
            evaluated.append(cdict)

        expect = [
            # c1
            {
                "Job1": [{"v": -2 - 1.0 / 3.0}],
                "Job2": [{"v": -2 - 1.0 / 3.0}, {"v": 11.0}],
                "Job3": [{"v": -2 - 1.0 / 3.0}, {"v": 11.0}, {"v": -9.0}],
                "Job4": [{"v": -9.0}],
            },
            # c2
            {
                "Job1": [{"v": -1.0}],
                "Job2": [{"v": -1.0}, {"v": 17.0}],
                "Job3": [{"v": -1.0}, {"v": 17.0}, {"v": -15.0}],
                "Job4": [{"v": -15.0}],
            },
        ]

        testutil.compareCollection(self, expect, evaluated)

    def testApplyMetaEvaluators(self):
        """Test Merit._applyMetaevaluators"""

        # Define a MetaEvaluator that sums the results of Job1 eval 1 and Job3 eval 3
        _batchpaths, batchedjobs, candidatejoblists = self.metamerit._prepareJobs(
            self.candidates
        )
        finishedEvents = self.metamerit._runBatches(batchedjobs)

        self.assertEqual(2, len(candidatejoblists))
        batchOneLength = len(candidatejoblists[0][1])
        batchTwoLength = len(candidatejoblists[1][1])

        gevent.wait(finishedEvents)
        self.metamerit._applyEvaluators(batchedjobs)
        self.metamerit._applyMetaEvaluators(
            [joblist for (c, joblist) in candidatejoblists]
        )

        self.assertEqual(batchOneLength + 1, len(candidatejoblists[0][1]))
        self.assertEqual(batchTwoLength + 1, len(candidatejoblists[1][1]))

        self.assertEqual(1, candidatejoblists[0][1][0].variables.id)
        self.assertEqual(2, candidatejoblists[1][1][0].variables.id)

        metajob1 = candidatejoblists[0][1][-1]
        metajob2 = candidatejoblists[1][1][-1]

        self.assertEqual(1, metajob1.variables.id)
        self.assertEqual(2, metajob2.variables.id)

        self.assertTrue(True, metajob1.isMetaEvaluatorJob)
        self.assertTrue(True, metajob2.isMetaEvaluatorJob)

        self.assertEqual(
            (-2 - 1.0 / 3.0) - 9.0, metajob1.evaluatorRecords[0][0].meritValue
        )
        self.assertEqual(
            (-1.0) - 15.0, metajob2.evaluatorRecords[0][0].meritValue
        )

    def testDefaultReductionFunction(self):
        """Test merit._sumValuesReductionFunction()"""

        class ER:
            def __init__(self, name, meritValue):
                self.name = name
                self.meritValue = meritValue

        class J:
            def __init__(self, evalRecords):
                self.evaluatorRecords = evalRecords

        testd = [
            [
                J([[ER("v", 1.0)]]),
                J([[ER("v", 1.0), ER("u", 2.0), ER("l", 1.0)]]),
            ],
            [
                J([[ER("v", 2.0)]]),
                J([[ER("v", 3.0), ER("u", 2.0), ER("l", 1.0)]]),
            ],
        ]
        expect = [1.0 + 1.0 + 2.0 + 1.0, 2.0 + 3.0 + 2.0 + 1.0]
        actual = atsim.pro_fit.merit._sumValuesReductionFunction(testd)
        testutil.compareCollection(self, expect, actual)

    def testCalculateMerit(self):
        """Test Merit.calculate() method using default reduction function"""
        expect = [-3.0, 1.0]
        actual = self.merit.calculate(self.candidates)
        testutil.compareCollection(self, expect, actual)

        # Check that files are cleaned up
        print(os.listdir(self.tempd))
        self.assertTrue(len(os.listdir(self.tempd)) == 0)

    def testCalculateMeritMetaEvaluator(self):
        """Test Merit.calculate() when meta evaluators are specified"""
        expect = [-3.0 + ((-2 - 1.0 / 3.0) - 9.0), 1.0 + ((-1.0) - 15.0)]
        actual = self.metamerit.calculate(self.candidates)
        testutil.compareCollection(self, expect, actual)

    def testCleanBatches(self):
        """Test atsim.pro_fit.merit.Merit.cleanBatches()"""
        batchpaths, _batchedjobs, _candidatejoblists = self.merit._prepareJobs(
            self.candidates
        )
        self.assertTrue(len(os.listdir(self.tempd)) == 2)
        self.merit._cleanBatches(batchpaths)
        self.assertTrue(len(os.listdir(self.tempd)) == 0)

    def testBeforeRunCallback(self):
        """Test pro_fit.merit.Merit.beforeRuncallbacks"""
        beforeRunDict = {}

        def beforeRun(candidateJobPairs):
            for i, (v, jobs) in enumerate(candidateJobPairs):
                beforeRunDict[i] = {}
                beforeRunDict[i]["variables"] = v.variablePairs
                for j in jobs:
                    beforeRunDict[i].setdefault("job_variables", []).append(
                        j.variables.variablePairs
                    )
                    beforeRunDict[i].setdefault("job_names", []).append(j.name)
                    with open(
                        os.path.join(j.path, "job_files", "runjob")
                    ) as infile:
                        for line in infile:
                            if line.startswith("#Candidate:"):
                                candid = line.split(":")[1].strip()
                                beforeRunDict[i].setdefault(
                                    "candidate_ids", []
                                ).append(candid)
                                break

        self.merit.beforeRun.append(beforeRun)
        self.merit.calculate(self.candidates)

        expect = {
            0: {
                "variables": [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                "job_names": ["Job1", "Job2", "Job3", "Job4"],
                "job_variables": [
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                ],
                "candidate_ids": ["1", "1", "1", "1"],
            },
            1: {
                "variables": [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                "job_names": ["Job1", "Job2", "Job3", "Job4"],
                "job_variables": [
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                ],
                "candidate_ids": ["2", "2", "2", "2"],
            },
        }

        testutil.compareCollection(self, expect, beforeRunDict)

    def testAfterRunCallback(self):
        """Test pro_fit.merit.Merit.afterRunCallback"""
        afterRunDict = {}

        outputResProcess = MockEvaluator(lambda v: sorted(v.items()))

        def afterRun(candidateJobPairs):
            for i, (v, jobs) in enumerate(candidateJobPairs):
                afterRunDict[i] = {}
                afterRunDict[i]["variables"] = v.variablePairs
                for j in jobs:
                    afterRunDict[i].setdefault("job_variables", []).append(
                        j.variables.variablePairs
                    )
                    afterRunDict[i].setdefault("job_names", []).append(j.name)
                    with open(
                        os.path.join(j.path, "job_files", "output", "runjob")
                    ) as infile:
                        for line in infile:
                            if line.startswith("#Candidate:"):
                                candid = line.split(":")[1].strip()
                                afterRunDict[i].setdefault(
                                    "candidate_ids", []
                                ).append(candid)
                                break
                        v = outputResProcess(j)
                        vals = dict([(e.name, e.meritValue) for e in v])["v"]
                        afterRunDict[i].setdefault("outputvalues", []).append(
                            vals
                        )

        self.merit.afterRun.append(afterRun)
        self.merit.calculate(self.candidates)
        expect = {
            0: {
                "variables": [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                "job_names": ["Job1", "Job2", "Job3", "Job4"],
                "job_variables": [
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                ],
                "outputvalues": [
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                ],
                "candidate_ids": ["1", "1", "1", "1"],
            },
            1: {
                "variables": [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                "job_names": ["Job1", "Job2", "Job3", "Job4"],
                "job_variables": [
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                ],
                "outputvalues": [
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                ],
                "candidate_ids": ["2", "2", "2", "2"],
            },
        }

        testutil.compareCollection(self, expect, afterRunDict)

    def testAfterEvaluationCallback(self):
        """Test pro_fit.merit.Merit.afterEvaluation callback"""
        afterEvaluationDict = {}

        outputResProcess = MockEvaluator(lambda v: sorted(v.items()))

        def afterEvaluation(candidateJobPairs):
            for i, (v, jobs) in enumerate(candidateJobPairs):
                afterEvaluationDict[i] = {}
                afterEvaluationDict[i]["variables"] = v.variablePairs
                for j in jobs:
                    afterEvaluationDict[i].setdefault(
                        "job_variables", []
                    ).append(j.variables.variablePairs)
                    afterEvaluationDict[i].setdefault("job_names", []).append(
                        j.name
                    )
                    with open(
                        os.path.join(j.path, "job_files", "output", "runjob")
                    ) as infile:
                        for line in infile:
                            if line.startswith("#Candidate:"):
                                candid = line.split(":")[1].strip()
                                afterEvaluationDict[i].setdefault(
                                    "candidate_ids", []
                                ).append(candid)
                                break
                        v = outputResProcess(j)
                        vals = dict([(e.name, e.meritValue) for e in v])["v"]
                        afterEvaluationDict[i].setdefault(
                            "outputvalues", []
                        ).append(vals)

                        elist = []
                        for evaluator in j.evaluatorRecords:
                            evaldict = dict(
                                [(e.name, e.meritValue) for e in evaluator]
                            )
                            elist.append(evaldict)
                        afterEvaluationDict[i].setdefault(
                            "evaluated", []
                        ).append(elist)

        self.merit.afterEvaluation.append(afterEvaluation)
        # import pdb; pdb.set_trace()
        self.merit.calculate(self.candidates)
        expect = {
            0: {
                "variables": [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                "job_names": ["Job1", "Job2", "Job3", "Job4"],
                "job_variables": [
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                ],
                "outputvalues": [
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                    [("A", 1.0), ("B", 2.5), ("C", 3.0), ("D", 4.5)],
                ],
                "candidate_ids": ["1", "1", "1", "1"],
                "evaluated": [
                    [{"v": -2 - 1.0 / 3.0}],
                    [{"v": -2 - 1.0 / 3.0}, {"v": 11.0}],
                    [{"v": -2 - 1.0 / 3.0}, {"v": 11.0}, {"v": -9.0}],
                    [{"v": -9.0}],
                ],
            },
            1: {
                "variables": [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                "job_names": ["Job1", "Job2", "Job3", "Job4"],
                "job_variables": [
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                ],
                "outputvalues": [
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                    [("A", 1.0), ("B", 4.5), ("C", 3.0), ("D", 8.5)],
                ],
                "candidate_ids": ["2", "2", "2", "2"],
                "evaluated": [
                    [{"v": -1.0}],
                    [{"v": -1.0}, {"v": 17.0}],
                    [{"v": -1.0}, {"v": 17.0}, {"v": -15.0}],
                    [{"v": -15.0}],
                ],
            },
        }
        testutil.compareCollection(self, expect, afterEvaluationDict)

    def testAfterEvaluation_substitute_bad_callback(self):
        class NanEvaluator(object):
            def __init__(self):
                self.call_count = 0

            def __call__(self, iitems):
                self.call_count += 1

                if self.call_count % 4 == 0:
                    return float("nan")
                else:
                    return float(self.call_count)

        naneval = NanEvaluator()
        mockeval = MockEvaluator(naneval, name="NaNEval")

        for jf in self.merit._jobfactories:
            jf.evaluators.append(mockeval)

        evalrecords = []

        def afterMerit(meritValues, candidateJobPairs):
            evalrecords[:] = []
            for _v, jobs in candidateJobPairs:
                for j in jobs:
                    for erl in j.evaluatorRecords:
                        for er in erl:
                            evalrecords.append(er)

        self.merit.afterMerit.append(afterMerit)
        self.merit.calculate(self.candidates)

        actual = [er.meritValue for er in evalrecords if er.name == "NaNEval"]
        expect = [1.0, 5.0, 2.0, 6.0, 3.0, 7.0, float("nan"), float("nan")]

        self.assertEqual(len(expect), len(actual))

        self.assertTrue(math.isnan(actual[-1]))
        self.assertTrue(math.isnan(actual[-2]))
        self.assertEqual(expect[:-2], actual[:-2])

        naneval.call_count = 0

        self.merit.afterEvaluation.append(
            atsim.pro_fit.merit.Replace_Merit_After_Evaluation_Callback(
                math.isfinite, 100.0
            )
        )
        self.merit.calculate(self.candidates)

        actual = [er.meritValue for er in evalrecords if er.name == "NaNEval"]
        expect = [1.0, 5.0, 2.0, 6.0, 3.0, 7.0, 100.0, 100.0]

        self.assertEqual(expect, actual)

        # self.fail()
