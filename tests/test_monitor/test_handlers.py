# -*- coding: utf-8 -*-

from ._cherrypydbtestcase import CherryPyDBTestCaseBase

from .. import testutil


class TestHandlers(CherryPyDBTestCaseBase):
    """Tests for the first level cherrypy /fitting handlers"""

    dbname = "population_fitting_run.db"
    baseurl = "http://localhost:8080/fitting/"

    def testCurrentGeneration(self):
        """Test pprofitmon /fitting/current_iteration"""
        j = self.fetchJSON("current_iteration")
        self.assertEqual(5, j["current_iteration"])

    def testBestCandidate(self):
        """Test pprofitmon /fitting/best_candidate"""
        j = self.fetchJSON("best_candidate")
        expect = {
            "id": 18,
            "iteration_number": 4,
            "candidate_number": 1,
            "merit_value": 964.64312,
        }
        testutil.compareCollection(self, expect, j)

    def testIterationOverview(self):
        """Test pprofitmon /fitting/iteration_overview"""
        # id|iteration_number|candidate_number|merit_value
        # 13|3|0|980.44924
        # 14|3|1|973.78207
        # 15|3|2|1546.33659
        # 16|3|3|973.78207
        j = self.fetchJSON("iteration_overview/3")
        expect = {
            "iteration_number": 3,
            "num_candidates": 4,
            "mean": 1118.587493,
            "standard_deviation": 246.9760555,
            "minimum": {
                "id": 14,
                "iteration_number": 3,
                "candidate_number": 1,
                "merit_value": 973.78207,
            },
            "maximum": {
                "id": 15,
                "iteration_number": 3,
                "candidate_number": 2,
                "merit_value": 1546.33659,
            },
        }
        testutil.compareCollection(self, expect, j)

    def testRunStatus(self):
        """Test pprofitmon /fitting/run_status"""
        j = self.fetchJSON("run_status")
        expect = {"runstatus": "Finished", "title": "run title"}
        testutil.compareCollection(self, expect, j)

    def testVariables(self):
        """Test pprofitmon /fitting/variables/ITERATION/CANDIDATE"""
        # ITERATION = 2
        # CANDIDATE = 3
        j = self.fetchJSON("variables/2/3")

        # CANDIDATE_ID = 12
        # id|variable_name|fit_flag|low_bound|upper_bound|calculated_flag|calculation_expression
        # 1|morse_Ca_O_A|1|||0|
        # 2|morse_Ca_O_B|0|0.1|5.0|0|
        # 3|morse_Ca_O_C|1|0.1|5.0|0|
        # 4|lennard_Ca_O_A|1|0.0|50.0|0|
        # 5|morse_Mg_O_A|1|0.001|1.0|0|
        # 6|morse_Mg_O_B|1|0.1|5.0|0|
        # 7|morse_Mg_O_C|1|0.1|5.0|0|
        # 8|lennard_Mg_O_A|1|0.0|50.0|0|
        # 9|morse_O_O_A|1|0.001|1.0|0|
        # 10|morse_O_O_B|1|0.1|5.0|0|
        # 11|morse_O_O_C|1|0.1|5.0|0|
        # 12|lennard_O_O_A|0|0.0|50.0|1|morse_O_O_C * 2

        keys = [
            "variable_name",
            "fit_flag",
            "low_bound",
            "upper_bound",
            "calculated_flag",
            "calculation_expression",
            "value",
        ]

        values = [
            ("morse_Ca_O_A", 1, None, None, 0, None, 0.473366852725934),
            ("morse_Ca_O_B", 0, 0.1, 5.0, 0, None, 1.60801182431367),
            ("morse_Ca_O_C", 1, 0.1, 5.0, 0, None, 4.95798657162849),
            ("lennard_Ca_O_A", 1, 0.0, 50.0, 0, None, 46.7018792099724),
            ("morse_Mg_O_A", 1, 0.001, 1.0, 0, None, 0.595661989577572),
            ("morse_Mg_O_B", 1, 0.1, 5.0, 0, None, 1.12576428175222),
            ("morse_Mg_O_C", 1, 0.1, 5.0, 0, None, 1.21244248936833),
            ("lennard_Mg_O_A", 1, 0.0, 50.0, 0, None, 36.7951599738289),
            ("morse_O_O_A", 1, 0.001, 1.0, 0, None, 0.189047016591048),
            ("morse_O_O_B", 1, 0.1, 5.0, 0, None, 2.64296323891979),
            ("morse_O_O_C", 1, 0.1, 5.0, 0, None, 3.76040282662184),
            (
                "lennard_O_O_A",
                0,
                0.0,
                50.0,
                1,
                "morse_O_O_C * 2",
                11.221208824602,
            ),
        ]

        expect = [dict(list(zip(keys, v))) for v in values]
        testutil.compareCollection(self, expect, j)

    def testEvaluators(self):
        """Test pprofitmon /fitting/evaluated/ITERATION/CANDIDATE"""
        # ITERATION = 2
        # CANDIDATE = 3
        j = self.fetchJSON("evaluated/2/3")

        # CANDIDATE_ID = 12
        # Jobs
        # id|candidate_id|job_name
        # 23|12|CaO
        # 24|12|MgO

        # select * from evaluated, jobs where evaluated.job_id = jobs.id and jobs.candidate_id = 12;
        # id|job_id|evaluator_name|value_name|expected_value|extracted_value|weight|merit_value|evaluatorerror_id|id|candidate_id|job_name
        # 155|23|CaO:Gulp|elastic_c12|57.81|1.5955|1.0|56.2145||23|12|CaO
        # 156|23|CaO:Gulp|elastic_c11|221.89|113.3894|1.0|108.5006||23|12|CaO
        # 157|23|CaO:Gulp|elastic_c44|80.32|1.5955|1.0|78.7245||23|12|CaO
        # 158|23|CaO:Gulp|cell_c|4.811|9.73857|10.0|49.2757||23|12|CaO
        # 159|23|CaO:Gulp|cell_b|4.811|9.73857|10.0|49.2757||23|12|CaO
        # 160|23|CaO:Gulp|cell_a|4.811|9.73857|10.0|49.2757||23|12|CaO
        # 161|23|CaO:Gulp|optimisation_penalty|0.0|0.0|1000.0|0.0||23|12|CaO
        # 162|24|MgO:Gulp|elastic_c12|95.2|329.5643|1.0|234.3643||24|12|MgO
        # 163|24|MgO:Gulp|elastic_c11|297.0|445.8063|1.0|148.8063||24|12|MgO
        # 164|24|MgO:Gulp|elastic_c44|155.7|329.5643|1.0|173.8643||24|12|MgO
        # 165|24|MgO:Gulp|cell_c|4.212|5.061349|10.0|8.49349||24|12|MgO
        # 166|24|MgO:Gulp|cell_b|4.212|5.061349|10.0|8.49349|1|24|12|MgO
        # 167|24|MgO:Gulp|cell_a|4.212|5.061349|10.0|8.49349||24|12|MgO
        # 168|24|MgO:Gulp|optimisation_penalty|0.0|0.0|1000.0|0.0||24|12|MgO
        expect = [
            (
                "CaO:Gulp",
                "elastic_c12",
                57.81,
                1.5955,
                1.0,
                56.2145,
                None,
                "CaO",
                -97.2400968690538,
            ),
            (
                "CaO:Gulp",
                "elastic_c11",
                221.89,
                113.3894,
                1.0,
                108.5006,
                None,
                "CaO",
                -48.8983730677363,
            ),
            (
                "CaO:Gulp",
                "elastic_c44",
                80.32,
                1.5955,
                1.0,
                78.7245,
                None,
                "CaO",
                -98.0135707171315,
            ),
            (
                "CaO:Gulp",
                "cell_c",
                4.811,
                9.73857,
                10.0,
                49.2757,
                None,
                "CaO",
                102.422988983579,
            ),
            (
                "CaO:Gulp",
                "cell_b",
                4.811,
                9.73857,
                10.0,
                49.2757,
                None,
                "CaO",
                102.422988983579,
            ),
            (
                "CaO:Gulp",
                "cell_a",
                4.811,
                9.73857,
                10.0,
                49.2757,
                None,
                "CaO",
                102.422988983579,
            ),
            (
                "CaO:Gulp",
                "optimisation_penalty",
                0.0,
                0.0,
                1000.0,
                0.0,
                None,
                "CaO",
                None,
            ),
            (
                "MgO:Gulp",
                "elastic_c12",
                95.2,
                329.5643,
                1.0,
                234.3643,
                None,
                "MgO",
                246.180987394958,
            ),
            (
                "MgO:Gulp",
                "elastic_c11",
                297.0,
                445.8063,
                1.0,
                148.8063,
                None,
                "MgO",
                50.1031313131313,
            ),
            (
                "MgO:Gulp",
                "elastic_c44",
                155.7,
                329.5643,
                1.0,
                173.8643,
                None,
                "MgO",
                111.666217084136,
            ),
            (
                "MgO:Gulp",
                "cell_c",
                4.212,
                5.061349,
                10.0,
                8.49349,
                None,
                "MgO",
                20.1649810066477,
            ),
            (
                "MgO:Gulp",
                "cell_b",
                4.212,
                5.061349,
                10.0,
                8.49349,
                "Error Message",
                "MgO",
                20.1649810066477,
            ),
            (
                "MgO:Gulp",
                "cell_a",
                4.212,
                5.061349,
                10.0,
                8.49349,
                None,
                "MgO",
                20.1649810066477,
            ),
            (
                "MgO:Gulp",
                "optimisation_penalty",
                0.0,
                0.0,
                1000.0,
                0.0,
                None,
                "MgO",
                None,
            ),
        ]
        keys = [
            "evaluator_name",
            "value_name",
            "expected_value",
            "extracted_value",
            "weight",
            "merit_value",
            "error_message",
            "job_name",
            "percent_difference",
        ]
        expect = [dict(list(zip(keys, r))) for r in expect]
        testutil.compareCollection(self, expect, j)
