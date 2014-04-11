import logging
import os
import shutil

from _common import * # noqa

from atsim.pro_fit.fittool import ConfigException


class SingleStepMinimizer(object):
  """Evaluate merit function a single time and return resulting MinimizerResults.

  Provides option to keep job directories following run and is therefore useful for debugging problems with input files.
  This is used to implement the --single option from the fittingTool.py command line."""

  _logger = logging.getLogger('atsim.pro_fit.minimizers.SingleStepMinimizer')

  def __init__(self, variables, keepFilesDirectory = None):
    """Create SingleStepMinimizer.

    @param variables Variables instance giving run values.
    @param keepFilesDirectory If specified, job files created for variables will be stored in the given directory."""
    self._initialArgs = variables
    self._keepFilesDirectory = keepFilesDirectory
    self.stepCallback = None
    self._logger.debug("Created SingleStepMinimizer, using variables: %s" % variables)
    self._checkDestDirectory()

  def _checkDestDirectory(self):
    """Ensure that the destination directory for copying back both exists and is empty"""
    if not os.path.isdir(self._keepFilesDirectory):
      raise MinimizerException("Destination directory in which run files should be stored after single-step run does not exist: '%s'" % self._keepFilesDirectory)

    files = [ f for f in os.listdir(self._keepFilesDirectory) if not f.startswith('.')]
    if files:
      raise MinimizerException("Destination directory for single-step run files is not empty: '%s'" % self._keepFilesDirectory)

  def _registerCallback(self, merit):
    """Create a callback that is invoked by merit function after merit evaluation.
    Callback is also responsible for copying back files to self._keepFilesDirectory"""

    logger = logging.getLogger('atsim.pro_fit.minimizers.SingleStepMinimizer')

    def copyback(keepFilesDirectory, candidateJobPairList):
        if not keepFilesDirectory:
          return
        # Clear directory
        for p in os.listdir(keepFilesDirectory):
          shutil.rmtree(os.path.join(keepFilesDirectory, p), ignore_errors = True)

        # Only one candidate
        candidate, jobs = candidateJobPairList[0]
        for job in jobs:
          if not hasattr(job, 'path'):
            continue
          destdir = os.path.join(keepFilesDirectory, os.path.basename(job.path))
          logger.info('Single-step run. Copying files. "%s" --> "%s"' % (job.path, destdir))
          shutil.copytree(job.path, destdir)

    class MeritCallback(object):

      def __init__(self, merit, keepFilesDirectory):
        if merit.afterMerit:
          raise ExistingCallbackException("Merit.afterMerit not None, not overwriting")
        merit.afterMerit = self
        self.minimizerResults = None
        self._keepFilesDirectory = keepFilesDirectory

      def __call__(self, meritVals, candidateJobList):
        self.minimizerResults = MinimizerResults(meritVals, candidateJobList)
        if self._keepFilesDirectory:
          copyback(self._keepFilesDirectory, candidateJobList)

    # To allow debugging, copy files to the keepFilesDirectory before evaluation
    class BeforeAfterCallback(object):
      def __init__(self, attr, merit, keepFilesDirectory):
        self.attr = attr
        if getattr(merit, self.attr):
          raise ExistingCallbackException("Merit.%s not None, not overwriting" % self.attr)
        setattr(merit, self.attr,self)
        self._keepFilesDirectory = keepFilesDirectory

      def __call__(self, candidateJobPairList):
        copyback(self._keepFilesDirectory, candidateJobPairList)

    afterMerit = MeritCallback(merit, self._keepFilesDirectory)
    beforeRun = BeforeAfterCallback("beforeRun", merit, self._keepFilesDirectory) # noqa
    afterRun = BeforeAfterCallback("afterRun", merit, self._keepFilesDirectory) # noqa

    def cleanup():
      merit.afterMerit = None
      merit.beforeRun = None
      merit.afterRun = None

    return afterMerit, cleanup

  def minimize(self, merit):
    """Perform minimization.

    @param merit atsim.pro_fit.fittool.Merit instance.
    @return MinimizerResults containing values obtained after merit function evaluation"""
    self._logger.info("Performing single step merit function evaluation.")

    cb,cleanup = self._registerCallback(merit)
    try:
      # Invoke the merit function
      merit.calculate([self._initialArgs])

      if self.stepCallback:
        self.stepCallback(cb.minimizerResults)

      return cb.minimizerResults
    finally:
      cleanup()

  @staticmethod
  def createFromConfig(variables, configitems):
    allowedkeys = set(['type', 'keep-files-directory'])
    configitems = dict(configitems)
    for item in configitems.keys():
      if not item in allowedkeys:
        raise ConfigException("SingleStep minimizer unknown config key: %s" % item)

    keepFilesDirectory = configitems.get('keep-files-directory', None)
    return SingleStepMinimizer(variables, keepFilesDirectory)
