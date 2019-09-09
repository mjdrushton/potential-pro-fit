import logging
import os
import shutil

import gevent

from ._common import (
    MinimizerException,
    MinimizerConfigException,
    ExistingCallbackException,
    MinimizerResults,
)

from atsim.pro_fit.exceptions import ConfigException


class SingleStepMinimizer(object):
    """Evaluate merit function a single time and return resulting MinimizerResults.

  Provides option to keep job directories following run and is therefore useful for debugging problems with input files.
  This is used to implement the --single option from the pprofit command line."""

    _logger = logging.getLogger("atsim.pro_fit.minimizers.SingleStepMinimizer")

    def __init__(self, variables, keepFilesDirectory=None):
        """Create SingleStepMinimizer.

    @param variables Variables instance giving run values.
    @param keepFilesDirectory If specified, job files created for variables will be stored in the given directory."""
        self._greenlet = gevent.Greenlet()
        self._initialArgs = variables
        self._keepFilesDirectory = keepFilesDirectory
        self.stepCallback = None
        self._logger.debug(
            "Created SingleStepMinimizer, using variables: %s" % variables
        )
        self._checkDestDirectory()

    def _checkDestDirectory(self):
        """Ensure that the destination directory for copying back both exists and is empty"""
        if not os.path.isdir(self._keepFilesDirectory):
            raise MinimizerConfigException(
                "Destination directory in which run files should be stored after single-step run does not exist: '%s'"
                % self._keepFilesDirectory
            )

        files = [
            f
            for f in os.listdir(self._keepFilesDirectory)
            if not f.startswith(".")
        ]
        if files:
            raise MinimizerConfigException(
                "Destination directory for single-step run files is not empty: '%s'"
                % self._keepFilesDirectory
            )

    def _registerCallback(self, merit):
        """Create a callback that is invoked by merit function after merit evaluation.
    Callback is also responsible for copying back files to self._keepFilesDirectory"""

        logger = logging.getLogger(
            "atsim.pro_fit.minimizers.SingleStepMinimizer"
        )

        def copyback(keepFilesDirectory, candidateJobPairList):
            if not keepFilesDirectory:
                return
            # Clear directory
            for p in os.listdir(keepFilesDirectory):
                shutil.rmtree(
                    os.path.join(keepFilesDirectory, p), ignore_errors=True
                )

            # Only one candidate
            candidate, jobs = candidateJobPairList[0]
            for job in jobs:
                if not hasattr(job, "path"):
                    continue
                destdir = os.path.join(
                    keepFilesDirectory, os.path.basename(job.path)
                )
                logger.info(
                    'Single-step run. Copying files. "%s" --> "%s"'
                    % (job.path, destdir)
                )
                shutil.copytree(job.path, destdir)

        class MeritCallback(object):
            def __init__(self, merit, keepFilesDirectory):
                merit.afterMerit.append(self)
                self.minimizerResults = None
                self._keepFilesDirectory = keepFilesDirectory

            def __call__(self, meritVals, candidateJobList):
                self.minimizerResults = MinimizerResults(
                    meritVals, candidateJobList
                )
                if self._keepFilesDirectory:
                    copyback(self._keepFilesDirectory, candidateJobList)

        # To allow debugging, copy files to the keepFilesDirectory before evaluation
        class BeforeAfterCallback(object):
            def __init__(self, attr, merit, keepFilesDirectory):
                self.attr = attr
                if getattr(merit, self.attr):
                    raise ExistingCallbackException(
                        "Merit.%s not None, not overwriting" % self.attr
                    )
                getattr(merit, self.attr).append(self)
                self._keepFilesDirectory = keepFilesDirectory

            def __call__(self, candidateJobPairList):
                copyback(self._keepFilesDirectory, candidateJobPairList)

        self.afterMerit = MeritCallback(merit, self._keepFilesDirectory)
        self.beforeRun = BeforeAfterCallback(
            "beforeRun", merit, self._keepFilesDirectory
        )  # noqa
        self.afterRun = BeforeAfterCallback(
            "afterRun", merit, self._keepFilesDirectory
        )  # noqa

        def cleanup():
            del merit.afterMerit[merit.afterMerit.index(self.afterMerit)]
            del merit.beforeRun[merit.beforeRun.index(self.beforeRun)]
            del merit.afterRun[merit.afterRun.index(self.afterRun)]

        return self.afterMerit, cleanup

    def _minimize(self, merit):
        """Perform minimization.

    @param merit atsim.pro_fit.merit.Merit instance.
    @return MinimizerResults containing values obtained after merit function evaluation"""
        self._logger.info("Performing single step merit function evaluation.")

        cb, cleanup = self._registerCallback(merit)
        try:
            # Invoke the merit function
            merit.calculate([self._initialArgs])

            if self.stepCallback:
                self.stepCallback(
                    cb.minimizerResults
                )  # pylint: disable=not-callable

            return cb.minimizerResults
        finally:
            cleanup()

    def minimize(self, merit):
        """Perform minimization.

    @param merit atsim.pro_fit.merit.Merit instance.
    @return MinimizerResults containing values obtained after merit function evaluation"""
        self._greenlet = gevent.Greenlet(self._minimize, merit)
        self._greenlet.start()
        return self._greenlet.get()

    def stopMinimizer(self):
        self._greenlet.kill()

    @staticmethod
    def createFromConfig(variables, configitems):
        allowedkeys = set(["type", "keep-files-directory"])
        configitems = dict(configitems)
        for item in list(configitems.keys()):
            if not item in allowedkeys:
                raise ConfigException(
                    "SingleStep minimizer unknown config key: %s" % item
                )

        keepFilesDirectory = configitems.get("keep-files-directory", None)
        return SingleStepMinimizer(variables, keepFilesDirectory)
