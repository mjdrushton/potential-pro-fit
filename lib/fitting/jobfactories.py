"""Directory containing job factories for fittool.py.

A job factory is responsible for creating a directory representing a job within a fitting run from
an instance of fittool.Variables.

The directory created should contain an executable script or program named 'runjob' that when invoked
will produce output that is later evaluated (see atomsscripts.fitting.evaluators) to produce a merit
value for fitting.

The interface for a JobFactory is as follows:


  class JobFactory:
    def __init__(self, runnerName, jobName, evaluators)
      @param runnerName Name of runner that will invoke 'runjob'
      @param jobName Label identifying this job factory
      @param evaluators List of Evaluators used to extract values from job output
             (see atomsscripts.fitting.evaluators)
      ...

    def createJob(self, destdir, variables):
      @param destdir Absolute path in which job factory should create files.
      @param variables atomsscripts.fitting.fittool.Variables instance from
        the values of which are used to create job files.
      @return Job instance for the created job.
      ...

    @staticmethod
    def createFromConfig(jobpath, runnername, jobname, evaluators, cfgitems):

      @param jobpath


JobFactory should also provide name, runnerName and evaluators properties returning
the same values as passed into the constructor.

The static method createFromConfig provides a consistent interface for creating job factories
from the job.cfg files located in fit_files.

"""

class Job(object):
  def __init__(self, jobFactory, path, variables):
    """@param jobFactory Job factory instance used to create this job.
       @param path Path in which job's files are located.
       @param variables Variables instance from which job files were created."""
    self.jobFactory = jobFactory
    self.path = path
    self.variables = variables
    self._evaluatorRecords = None

  @property
  def name(self):
    return self.jobFactory.name

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
    """@param name Evaluator name.
    @param evaluatorRecords EvaluatorRecords returned by evaluatorRecords property
    @param variables Variables instance"""

    self.name = name
    self.evaluatorRecords = evaluatorRecords
    self.variables = variables
    self.isMetaEvaluatorJob = True

from atomsscripts.tools import csvbuild

class TemplateJobFactory(object):
  """Performs csvbuild style template substitution to create job directories from Variables"""

  def __init__(self, templatePath,runnerName, jobName, evaluators):
    """@param templatePath Source directory containing files from which job directory is constructed
    @param runnerName Runner used to execute jobs created by this factory.
    @param jobName Factory name.
    @param evaluators List of evaluators to be applied to directory after run."""
    self.name = jobName
    self.runnerName = runnerName
    self.jobName = jobName
    self.evaluators = evaluators
    self._templatePath = templatePath

  def createJob(self, destdir, variables):
    import os
    oldcwd = os.getcwd()
    try:
      rows = [ dict(variables.variablePairs) ]
      os.chdir(os.path.join(self._templatePath, os.pardir))
      csvbuild.buildDirs(
          rows,
          os.path.basename(self._templatePath),
          destdir)
      return Job(self, destdir, variables)
    finally:
      os.chdir(oldcwd)

  @staticmethod
  def createFromConfig(jobpath, runnername, jobname, evaluators, cfgitems):
    return TemplateJobFactory(jobpath, runnername, jobname, evaluators)
