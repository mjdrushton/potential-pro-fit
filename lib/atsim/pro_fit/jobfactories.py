"""Directory containing job factories for fittool.py.

A job factory is responsible for creating a directory representing a job within a fitting run from
an instance of fittool.Variables.

The directory created should contain an executable script or program named 'runjob' that when invoked
will produce output that is later evaluated (see atsim.pro_fit.evaluators) to produce a merit
value for fitting.

The interface for a JobFactory is as follows:


  class JobFactory:
    def __init__(self, runnerName, jobName, evaluators)

      Args:
        runnerName: Name of runner that will invoke 'runjob'
        jobName: Label identifying this job factory
        evaluators (list): List of Evaluators used to extract values from job output
                           (see atsim.pro_fit.evaluators)
      ...

    def createJob(self, destdir, variables):
      Args:
         destdir:   Absolute path in which job factory should create files.
         variables: atsim.pro_fit.fittool.Variables instance from
                    the values of which are used to create job files.

      Returns:
         atsim.pro_fit.jobfactories.Job: Job instance for the created job.
      ...

    @staticmethod
    def createFromConfig(jobpath, runnername, jobname, evaluators, cfgitems):
      ...

JobFactory should also provide name, runnerName and evaluators properties returning
the same values as passed into the constructor.

The static method createFromConfig provides a consistent interface for creating job factories
from the job.cfg files located in fit_files.

"""

import os
import logging

class Job(object):
  def __init__(self, jobFactory, path, variables):
    """

    Args:
        jobFactory: Job factory instance used to create this job.
        path (string): Path in which job's files are located.
        variables (atsim.pro_fit.fittool.Variables): Variables instance from which job files were created.
    """
    self.jobFactory = jobFactory
    self.path = path
    self.variables = variables
    self._evaluatorRecords = None

  @property
  def name(self):
    return self.jobFactory.name

  def outputPath(self):
    return os.path.join(self.path, 'job_files', 'output')
  outputPath = property(fget = outputPath,
    doc =  "Returns the job's output path")

  def evaluate(self):
    """Applies evaluators to Job. This should be called before evaluatorRecords is queried."""
    self._evaluatorRecords =  [ e(self) for e in self.jobFactory.evaluators ]

  def evaluatorRecords(self):
    return self._evaluatorRecords
  evaluatorRecords = property(fget = evaluatorRecords,
    doc = "Return a series of lists (one per evaluator) containing EvaluatorRecords. Note: the evaluate() method needs to be called before accessing this property.")

class MetaEvaluatorJob(object):
  """Job-alike used to pass evaluator records to merit function"""

  def __init__(self, name, evaluatorRecords, variables):
    """

    Args:
        name (string): Evaluator name.
        evaluatorRecords (list): EvaluatorRecords returned by evaluatorRecords property.
        variables (atsim.prof_fit.fittool.Variables): Variables instance.
    """
    self.name = name
    self.evaluatorRecords = evaluatorRecords
    self.variables = variables
    self.isMetaEvaluatorJob = True

from atsim.pro_fit.tools import csvbuild

class TemplateJobFactory(object):
  """Performs csvbuild style template substitution to create job directories from Variables"""

  _logger = logging.getLogger('atsim.pro_fit.jobfactories.TemplateJobFactory')

  def __init__(self, templatePath, runnerFilesPath, runnerName, jobName, evaluators):
    """
    Args:
        templatePath (string): Source directory containing files from which job directory is constructed
        runnerFilesPath (string): Path to runner_files directory used by this job factory or `None` if no runner_files are to be used.
        runnerName (string): Runner used to execute jobs created by this factory.
        jobName (string): Factory name.
        evaluators (list): List of evaluators to be applied to directory after run.
    """
    self.name = jobName
    self.runnerName = runnerName
    self.runnerFilesPath = runnerFilesPath
    self.jobName = jobName
    self.evaluators = evaluators
    self._templatePath = templatePath

  def createJob(self, destdir, variables):
    jfdir = os.path.join(destdir, 'job_files')
    rfdir = os.path.join(destdir, 'runner_files')

    # Create job directory structure
    os.mkdir(jfdir)
    os.mkdir(rfdir)

    self._createJobFiles(jfdir, variables)
    self._createRunnerFiles(rfdir, variables)
    return Job(self, destdir, variables)

  def _createFiles(self, srcdir, destdir, variables):
    import os
    from atsim.pro_fit.fittool import ConfigException
    oldcwd = os.getcwd()
    try:
      rows = [ dict(variables.variablePairs) ]
      os.chdir(os.path.join(srcdir, os.pardir))
      csvbuild.buildDirs(
          rows,
          os.path.basename(srcdir),
          destdir)
    except csvbuild.CSVBuildKeyError,e:
      msg = "Unknown variable name '%s' specified in template: %s" % (e.args[0], os.path.join(srcdir, e.templateFilename))
      raise ConfigException(msg)
    finally:
      os.chdir(oldcwd)

  def _createJobFiles(self, destdir, variables):
    self._createFiles(self._templatePath, destdir, variables)

  def _createRunnerFiles(self, destdir, variables):
    # Populate the job's runner_files directory
    if not self.runnerFilesPath is None:
      self._createFiles(self.runnerFilesPath, destdir, variables)

  @staticmethod
  def createFromConfig(jobpath, fitRootPath, runnername, jobname, evaluators, cfgitems):
    """Create job factory from job.cfg

    Args:
        jobpath (string): Path to job directory.
        fitRootPath (string): Path of fitting run root.
        runnername (string): Name of the runner assigned to this job.
        jobname (string): Name of this job.
        evaluators (list): List of Evaluator instances associated with this job.
        cfgitems (list): List of (key,value) tuples with the configuration items related to this job factory.

    Returns:
        TemplateJobFactory: Job factory instance.
    """

    log = TemplateJobFactory._logger.getChild('createFromConfig')
    log.debug("Configuring TemplateJobFactory using the following options:")
    log.debug("jobpath = '%s'", jobpath)
    log.debug("fitRootPath = '%s'", fitRootPath)
    log.debug("runnername = '%s'", runnername)
    log.debug("jobname = '%s'", jobname)

    # Check for runner_files
    testrunnerfiles = os.path.join(fitRootPath, 'runner_files', runnername)

    log.debug("Checking for presence of 'runner_files' for runner named '%s' in '%s'", runnername, testrunnerfiles)

    if os.path.isdir(testrunnerfiles):
      runnerFilesPath = os.path.abspath(testrunnerfiles)
      log.info("Found 'runner_files' for runner named '%s' in '%s'", runnername, runnerFilesPath)
    else:
      runnerFilesPath = None
      log.debug("No 'runner_files' found for runner named '%s' in '%s'", runnername, runnerFilesPath)

    return TemplateJobFactory(jobpath, runnerFilesPath, runnername, jobname, evaluators)
