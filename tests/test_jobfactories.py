import unittest

import shutil
import tempfile
import os
import configparser

from .common import *
from atsim import pro_fit


def _getResourceDir():
    return os.path.join(os.path.dirname(__file__), "resources")


class TemplateJobFactoryTestCase(unittest.TestCase):
    """Tests for atsim.pro_fit.jobfactories.TemplateJobFactory"""

    def setUp(self):
        self.rootDir = tempfile.mkdtemp()
        rndir = os.path.join(self.rootDir, "runner_files", "runner_name")
        os.makedirs(rndir)
        with open(os.path.join(rndir, "@NAME@.in"), "w") as outfile:
            print("Variable:@A@", file=outfile)

        self.tempd = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempd, ignore_errors=True)
        shutil.rmtree(self.rootDir, ignore_errors=True)

    def testCreateJob(self):
        """Test TemplateJobFactory.createJob()"""
        srcpath = os.path.join(_getResourceDir(), "template_job_factory")

        logger.debug("srcpath: %s" % srcpath)
        logger.debug("destpath: %s" % self.tempd)

        factory = pro_fit.jobfactories.TemplateJobFactory(
            srcpath,
            os.path.join(self.rootDir, "runner_files", "runner_name"),
            "Runner",
            "Job",
            [],
        )

        variables = pro_fit.variables.Variables(
            [("NAME", "Named", True), ("A", 5.0, False)]
        )

        # Create the directory
        job = factory.createJob(self.tempd, variables)

        # Compare the directory and check it contains what it should
        expect = ["job_files", "runner_files"]
        actual = os.listdir(self.tempd)
        self.assertEqual(sorted(expect), sorted(actual))

        actual = os.listdir(os.path.join(self.tempd, "job_files"))
        expect = ["runjob", "Named", "static", "fit.cfg"]
        self.assertEqual(sorted(expect), sorted(actual))

        # Now check that runjob contains what it should.
        expect = """#! /bin/bash

echo 5.0 > output.res
"""
        actual = open(os.path.join(self.tempd, "job_files", "runjob")).read()
        self.assertEqual(expect, actual)

        self.assertEqual(self.tempd, job.path)

        # Test the contents of runner_files
        expect = ["Named"]
        actual = os.listdir(os.path.join(self.tempd, "runner_files"))
        self.assertEqual(expect, actual)

        with open(os.path.join(self.tempd, "runner_files", "Named")) as infile:
            line = next(infile)[:-1]
            self.assertEqual("Variable:5.0", line)

    def testCreateJob_no_runner_files(self):
        """Test TemplateJobFactory.createJob()"""
        srcpath = os.path.join(_getResourceDir(), "template_job_factory")

        logger.debug("srcpath: %s" % srcpath)
        logger.debug("destpath: %s" % self.tempd)

        factory = pro_fit.jobfactories.TemplateJobFactory(
            srcpath, None, "Runner", "Job", []
        )

        variables = pro_fit.variables.Variables(
            [("NAME", "Named", True), ("A", 5.0, False)]
        )

        # Create the directory
        job = factory.createJob(self.tempd, variables)

        # Compare the directory and check it contains what it should
        expect = ["job_files", "runner_files"]
        actual = os.listdir(self.tempd)
        self.assertEqual(sorted(expect), sorted(actual))

        actual = os.listdir(os.path.join(self.tempd, "job_files"))
        expect = ["runjob", "Named", "static", "fit.cfg"]
        self.assertEqual(sorted(expect), sorted(actual))

        # Now check that runjob contains what it should.
        expect = """#! /bin/bash

echo 5.0 > output.res
"""
        actual = open(os.path.join(self.tempd, "job_files", "runjob")).read()
        self.assertEqual(expect, actual)

        self.assertEqual(self.tempd, job.path)

        # Test the contents of runner_files
        expect = []
        actual = os.listdir(os.path.join(self.tempd, "runner_files"))
        self.assertEqual(expect, actual)

    def testCreateFromConfig(self):
        """Test atsim.pro_fit.jobfactories.TemplateJobFactory.createFromConfig"""
        parser = configparser.ConfigParser()
        parser.optionxform = str
        import io

        sio = io.StringIO(
            """[Job]
type : Template
runner : runner_name
"""
        )
        parser.read_file(sio)
        sect = parser.items("Job")

        from . import mockeval1

        eval1 = mockeval1.MockEvaluator1Evaluator()
        jf = pro_fit.jobfactories.TemplateJobFactory.createFromConfig(
            "path/to/sourcedir",
            self.rootDir,
            "runner_name",
            "Blah",
            [eval1],
            sect,
        )
        self.assertEqual(pro_fit.jobfactories.TemplateJobFactory, type(jf))
        self.assertEqual("runner_name", jf.runnerName)
        self.assertEqual("Blah", jf.jobName)
        self.assertEqual([eval1], jf.evaluators)
        self.assertEqual("path/to/sourcedir", jf._templatePath)

        self.assertEqual(
            os.path.abspath(
                os.path.join(self.rootDir, "runner_files", "runner_name")
            ),
            jf.runnerFilesPath,
        )

        shutil.rmtree(os.path.join(self.rootDir, "runner_files", "runner_name"))
        jf = pro_fit.jobfactories.TemplateJobFactory.createFromConfig(
            "path/to/sourcedir",
            self.rootDir,
            "runner_name",
            "Blah",
            [eval1],
            sect,
        )
        self.assertEqual(None, jf.runnerFilesPath)

        shutil.rmtree(os.path.join(self.rootDir, "runner_files"))
        jf = pro_fit.jobfactories.TemplateJobFactory.createFromConfig(
            "path/to/sourcedir",
            self.rootDir,
            "runner_name",
            "Blah",
            [eval1],
            sect,
        )
        self.assertEqual(None, jf.runnerFilesPath)
