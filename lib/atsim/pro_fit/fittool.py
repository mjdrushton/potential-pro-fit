import ConfigParser

import collections
import tempfile
import os
import shutil
import logging

import jobfactories

import cexprtk

import gevent

class ConfigException(Exception):
  pass

class MultipleSectionConfigException(ConfigException):
  pass

class FitConfig(object):
  """Object parses fit.cfg at root of a fitTool.py run
  and acts as factory for objects required by the fitting tool"""

  _logger = logging.getLogger('atsim.pro_fit.fittool.FitConfig')

  # The set of section names that can be repeated in a fit.cfg file.
  _repeatedSections = set(['Runner', 'Evaluator'])

  def __init__(self, fitCfgFilename, runnermodules, evaluatormodules, metaevaluatormodules, jobfactorymodules, minimizermodules, jobdir = None):
    """Create FitConfig from file containing configuration information.

    @param fitCfgFilename Filename for fit.cfg.
    @param runnermodules List of python module objects containing Runner objects
    @param evaluatormodules List of python module objects containing Evaluator objects
    @param metaevaluatormodules List of python module objects containing MetaEvaluator objects
    @param jobfactorymodules List of python module objects containing JobFactories objects
    @param minimizermodules List of python module objects contiaining Minimizers
    @param jobdir If specified use as directory for temporary files, if not use rootpath/jobs
    @param minimizer If not None overrides the minimizer specified in fit.cfg"""
    self._fitRootPath = os.path.abspath(os.path.dirname(fitCfgFilename))
    if jobdir:
      self.jobdir = jobdir
    else:
      self.jobdir = os.path.join(self._fitRootPath, 'jobs')
    self._validateConfigStructure(fitCfgFilename)
    cfg = self._parseConfig(fitCfgFilename)
    self._cfg = cfg
    self._variables = self._createVariables()
    self._calculatedVariables = self._createCalculatedVariables()
    self._runners = self._createRunners(runnermodules)
    self._metaevaluators = self._createMetaEvaluators(metaevaluatormodules)
    self._jobfactories = self._createJobFactories(jobfactorymodules, evaluatormodules)

    self._verifyHasJobs()

    # Check that at least some evaluators have been defined
    self._verifyHasEvaluators()

    # Get rid of unused runners
    self._filterRunners()
    if not self._runners:
      raise ConfigException("No Runners have been defined or no jobs assigned to those that have.")

    # Create the runners we actually need.
    self._instantiateRunners()
    self._merit = self._createMerit()
    self._minimizer = self._createMinimizer(minimizermodules)
    self._title = self._parseTitle()

  def title(self):
    return self._title
  title = property(fget=title,
      doc = """Returns name of fitting run representing [FittingRun] title field""")

  def variables(self):
    return self._variables
  variables = property(
      fget = variables,
      doc = """Returns a Variables object representing [Variables] section of fit.cfg""")

  def calculatedVariables(self):
    return self._calculatedVariables
  calculatedVariables = property(
      fget = calculatedVariables,
      doc = """Returns a CalculatedVaraiables instance, this is a callable that can transform variables before job creation. This is created from the [CalculatedVariables] section of the fit.cfg file.""")

  def runners(self):
    return self._runners
  runners = property(
      fget = runners,
      doc = """Returns a dictionary with runner names as keys and runner objects as  values""")

  def merit(self):
    return self._merit
  merit = property(
      fget = merit,
      doc = """Returns Merit instance create from configuration""")

  def minimizer(self):
    return self._minimizer
  minimizer = property(
    fget = minimizer,
    doc = "Returns Minimizer parsed from configuration")

  def metaEvaluators(self):
    return self._metaevaluators
  metaEvaluators = property(
    fget = metaEvaluators,
    doc = "Return list of meta-evaluator objects parsed from fit.cfg")

  def _parseConfig(self, fitCfgFilename):
    """@param fitCfgFilename Filename for fit.cfg.
    @return ConfigParser object"""
    config = ConfigParser.SafeConfigParser()
    config.optionxform = str
    with open(fitCfgFilename, 'rb') as fitCfgFile:
      config.readfp(fitCfgFile)
    return config

  def _validateConfigStructure(self, fitCfgFilename):
    """Check structure of config file. At the moment this checks for multiple configuration sections
    that should only be specified once (e.g. multiple Minimizer and Variables sections).

    @param fitCfgFilename Filename of configuration file

    @raises MultipleSectionConfigException Raised if multiple instances of a configuration section are found
      when only one may be specified."""

    import re

    regex = re.compile('^\s*\[(.*)\]')

    def sectionIterator():
      with open(fitCfgFilename) as infile:
        for line in infile:
          m = regex.match(line)
          if m:
            yield m.groups()[0]

    found = set()
    for sname in sectionIterator():
      stype = sname.split(":")[0]
      stype = stype.strip()

      if not (stype in self._repeatedSections) and stype in found:
        raise MultipleSectionConfigException("Found multiple '%s' sections in fit.cfg file where only one is allowed" % stype)
      found.add(stype)

  def _createVariables(self):
    """Create Variables object from parsed configuration"""
    if not self._cfg.has_section('Variables'):
      raise ConfigException("fit.cfg does not contain [Variables] section")
    import re
    regex = r'^(.*?)(\(.*\))?(\*)?$'
    defaultBound = (float("-inf"), float("inf"))
    regex = re.compile(regex)
    kvpairs = self._cfg.items('Variables')
    newpairs = []
    bounds = []
    for k,v in kvpairs:
      v = re.sub(r'\s', '', v)
      m = regex.match(v)
      if not m:
        raise ConfigException("Variable '%s' has an invalid format" % k)
      groups = m.groups()

      isFitParameter = groups[-1] == '*'
      try:
        v = float(groups[0])
      except ValueError:
        raise ConfigException("Variable '%s' value cannot be converted to float: %s" % (k,v))

      if groups[1] != None:
        try:
          bound = Variables._parseBounds(groups[1])
        except ConfigException as ce:
          raise ConfigException("Variable '%s' has invalid bounds: %s" % (k, ce.msg))
      else:
        bound = defaultBound

      newpairs.append( (k,v, isFitParameter) )
      bounds.append(bound)
    return Variables(newpairs, bounds)

  def _createCalculatedVariables(self):
    """Create a callable from parsed configuration"""
    if not self._cfg.has_section('CalculatedVariables'):
      return CalculatedVariables([])
    else:
      cfgitems = self._cfg.items('CalculatedVariables')
      existingVariables = set([name for (name,expression) in self.variables.variablePairs])
      for name, expression in cfgitems:
        if name in existingVariables:
          raise ConfigException("[CalculatedVariables] defines variable that is already defined in [Variables]: %s" % name)

      for name, expression in cfgitems:
        try:
          cexprtk.check_expression(expression)
        except cexprtk.ParseException, e:
          raise ConfigException("Could not parse formula within [CalculatedVariables] for '%s' with expression '%s' : %s" % (name, expression, e.message))

      return CalculatedVariables(cfgitems)

  def _createRunners(self, runnermodules):
    runnerdict = self._findClasses(runnermodules, 'Runner')

    def runnerMaker(cls, runnerkey, cfgitems):
      def f():
        return cls.createFromConfig(runnerkey, self._fitRootPath, cfgitems)
      return f

    # Extract runner sections from cfg
    runners = {}
    for s in self._cfg.sections():
      if s.startswith('Runner:'):
        runnerkey = self._parseColonKey('Runner', s)
        self._logger.debug('Processing config section: "%s". Runner name: "%s"' % (s, runnerkey))
        # Runner class
        rtype = self._cfg.get(s, 'type')
        try:
          rcls = runnerdict[rtype]
        except KeyError:
          raise ConfigException('Could not find Runner for config section: %s' % s)
        # To allow only instantiating runners we need (i.e. those with jobs)
        # defer creation by placing a no-arg callable in the dictionary responsible
        # for runner creation.
        runners[runnerkey] = runnerMaker(rcls, runnerkey, self._cfg.items(s))
        # self._logger.info('Configured runner: %s' % runnerkey)
    return runners

  def _filterRunners(self):
    """Remove any runner from self._runners that is not used by a job."""

    filtered = {}
    neededRunnerKeys = set([ jf.runnerName for jf in self._jobfactories])
    for k, r in self._runners.iteritems():
      if not k in neededRunnerKeys:
        self._logger.warn("Runner not assigned to any jobs and will therefore not be created: '%s'" % k)
      else:
        filtered[k] = r
    self._runners = filtered

  def _instantiateRunners(self):
    """Create runners"""
    instantiated = {}
    for k, r in self._runners.iteritems():
      self._logger.info("Creating runner: '%s'" % k)
      instantiated[k] = r()
    self._runners = instantiated

  def _createMetaEvaluators(self, metaevaluatorModules):
    evaldict = self._findClasses(metaevaluatorModules, 'MetaEvaluator')

    # Extract MetaEvaluator sections from config
    metaEvaluators = []
    for s in self._cfg.sections():
      if s.startswith('MetaEvaluator:'):
        evalkey = self._parseColonKey('MetaEvaluator', s)
        #Eval class
        evaltype = self._cfg.get(s, 'type')
        try:
          evalcls = evaldict[evaltype]
        except KeyError:
          raise ConfigException('Could not find MetaEvaluator for config section: %s' % s)
        metaEvaluators.append(evalcls.createFromConfig(evalkey, self._fitRootPath, self._cfg.items(s)))
    return metaEvaluators

  def _parseColonKey(self, prefix, s):
    """Strip 'prefix:'' from string s and return result

    @param prefix Prefix to be returned
    @param s String to be stripped
    @return s with 'prefix:' stripped from s"""
    k = s[len(prefix)+1:]
    return k.strip()

  def _createMerit(self):
    # Build the merit object
    runners = [v for (k,v) in sorted(self.runners.items())]
    return Merit(runners, self._jobfactories, self._metaevaluators, self.calculatedVariables, self.jobdir)

  def _createJobFactories(self, jobfactorymodules, evaluatormodules):
    evaldict = self._findClasses(evaluatormodules, 'Evaluator')
    jobfdict = self._findClasses(jobfactorymodules, 'JobFactory')

    # Walk fit_files directory
    fitfilespath = os.path.join(self._fitRootPath, 'fit_files')
    self._logger.debug('Creating jobs from directories in "%s"' % fitfilespath)

    jobfs = []
    for f in sorted(os.listdir(fitfilespath)):
      f = os.path.join(fitfilespath, f)
      if not os.path.isdir(f):
        continue
      self._logger.info('Processing job directory: "%s"' % f)
      jf = self._processJobDirectory(f, evaldict, jobfdict)
      jobfs.append(jf)

    return jobfs

  def _processJobDirectory(self, path, evaldict, jobfdict):
    # Open job.cfg
    cfgpath = os.path.join(path, 'job.cfg')
    try:
      fitcfg = self._parseConfig(cfgpath)
    except IOError:
      raise ConfigException('Could not find "job.cfg" in job directory: "%s"' % path)

    jobname = os.path.basename(path)

    # Process evaluators
    evaluators = self._createEvaluators(jobname, path, fitcfg, evaldict)

    #Process the Job section
    jfclsname = fitcfg.get('Job', 'type')
    try:
      jfcls = jobfdict[jfclsname]
    except KeyError:
      raise ConfigException('Unknown job type: "%s" for job named: "%s"' % (jfclsname, jobname))

    runnername = fitcfg.get('Job', 'runner')
    if not self.runners.has_key(runnername):
      raise ConfigException('Unknown runner: "%s" for job named: "%s"' % (runnername, jobname))
    return jfcls.createFromConfig(path, self._fitRootPath, runnername, jobname, evaluators, fitcfg.items('Job'))

  def _createEvaluators(self, jobname, jobpath, fitcfg, evaldict):
    evaluators = []
    for s in fitcfg.sections():
      if s.startswith('Evaluator:'):
        ename = self._parseColonKey('Evaluator', s)
        self._logger.debug('Processing evaluator "%s"' % ename)
        eclsname = fitcfg.get(s, 'type')
        try:
          ecls = evaldict[eclsname]
        except KeyError:
          raise ConfigException('Unknown evaluator type: "%s" for job "%s"' % (eclsname, jobname))

        evaluator = ecls.createFromConfig(":".join([jobname, ename]), jobpath, fitcfg.items(s))
        evaluators.append(evaluator)
    # Throw configuration exception if job does not define any evaluators
    if not evaluators:
      raise ConfigException("Job does not define any evaluators: '%s'" % jobname)
    return evaluators

  def _createMinimizer(self, minimizermodules):
    minclasses = self._findClasses(minimizermodules, 'Minimizer')
    if not self._cfg.has_section('Minimizer'):
      raise ConfigException('fit.cfg does not contain a [Minimizer] section')

    try:
      clsname = self._cfg.get('Minimizer', 'type')
    except:
      raise ConfigException('fit.cfg [Minimizer] section does not contain "type" option')

    try:
      mincls = minclasses[clsname]
    except KeyError:
      raise ConfigException('Unknown minimizer: "%s"' % clsname)

    self._logger.info("Creating minimizer: %s" % mincls)
    minimizer = mincls.createFromConfig(self.variables, self._cfg.items('Minimizer'))
    return minimizer

  def _findClasses(self, modulelist, nameSuffix):
    """Introspect modulelist to find classes ending in nameSuffix.

    @param List of python module object to be introspected.
    @param nameSuffix Class name suffix (as String) of classes to be returned.
    @return Dictionary relating name used in config 'type' fields to Class objects"""
    # Extract runner classes from runnermodules
    import inspect
    runnerdict = {}
    trimlength = -len(nameSuffix)
    for mod in reversed(modulelist):
      for (name, cls) in inspect.getmembers(mod, inspect.isclass):
        if name.endswith(nameSuffix):
          name = name[:trimlength]
          self._logger.info('Found %s: %s in module %s' % (nameSuffix, name, mod))
          runnerdict[name] = cls
    return runnerdict

  def _parseTitle(self):
    try:
      title =  self._cfg.get('FittingRun', 'title')
      if title == None:
        title = 'fitting_run'
    except ConfigParser.NoSectionError:
      title = 'fitting_run'

    return title

  def _verifyHasJobs(self):
    if not self._jobfactories:
      raise ConfigException("No Jobs defined.")

  def _verifyHasEvaluators(self):
    evaluatorsFound = False
    for jf in self._jobfactories:
      if jf.evaluators:
        evaluatorsFound = True
        break
    if not evaluatorsFound:
      raise ConfigException("No Evaluators have been defined for any Job.")


class Variables(object):
  """Class for handling fitting variables"""

  def __init__(self, varValPairs, bounds = None):
    """@param varValPairs List of (variable_name, variable_value, isFitParameter) pairs (name is string and variable values, floats,
        isFitParameter is a boolean indicating that variable should be changed during fitting)
       @param bounds List of (lowbound, highbound) tuples. If None, then (-inf, inf) bounds will be used (i.e. unbounded)"""
    self._processPairs(varValPairs)
    self.id = None

    if bounds == None:
     self._bounds = [ (float("-inf"), float("inf")) for i in xrange(len(varValPairs))]
    else:
      assert(len(bounds) == len(varValPairs))
      self._bounds = bounds

  def _processPairs(self, pairs):
    d = collections.OrderedDict(
      [ (k,v) for (k,v,isP) in pairs ])

    fitkeys = [ k for (k,v,isP) in pairs if isP ]
    self._varDict = d
    self._fitKeys = fitkeys

  def variablePairs(self):
    return self._varDict.items()
  variablePairs = property(fget = variablePairs,
      doc = """Return list of (variable_name, variable_value) pairs""")

  def fitKeys(self):
    return list(self._fitKeys)
  fitKeys = property(fget = fitKeys,
      doc = """Return list of variable names to be adjusted during fitting""")

  def fitValues(self):
    return [ self._varDict[k] for k in self.fitKeys ]
  fitValues = property(fget = fitValues,
      doc = """Return values associated with fitKeys, in the same order""")

  def bounds(self):
    return list(self._bounds)
  bounds = property(fget =  bounds,
    doc = """Return list of (lowerbound, upperbound) tuples indicating box bounds for each variable returned by variablePairs""")

  def inBounds(self, varKey, value):
    """Used to check if ``value`` is within the bounds of the variable named ``varKey``.

    :param str varKey: Variable name.
    :param float value: Check if value is in range.
    :return: ``True`` if ``value`` is in bounds or ``False`` otherwise."""

    boundsDict = dict(
      zip(
        [k for (k,v) in self.variablePairs],
        self.bounds))

    bounds = boundsDict[varKey]

    if not bounds:
      lowBound, highBound = (float("-inf"), float("inf"))
    else:
      lowBound, highBound = bounds

    return (lowBound <= value <= highBound)

  @staticmethod
  def _parseBounds(s):
    """Parse string of the form (lowbound, upperbound) into a numeric tuple.

    @param s String describing bounds.
    @return Tuple containing two floats of form (lowerbound, upperbound). float("-inf") and float("inf") represent now lower or upper bound respectively.

    @raises ConfigException if bounds can't be parsed."""
    import re
    #Dump spaces
    s = re.sub(r'\s', '', s)
    m = re.match(r'\((.*?),(.*?)\)',s)
    if not m:
      raise ConfigException("Variable bound does not have correct format")

    l,h = m.groups()

    if not l:
      l = "-inf"

    if not h:
      h = "inf"

    try:
      l = float(l)
    except ValueError:
      raise ConfigException("Variable lower bound could not be parsed")

    try:
      h = float(h)
    except ValueError:
      raise ConfigException("Variable upper bound could not be parsed")

    if l > h:
      raise ConfigException("upper bound  > lower bound. Lower bound should be smalle than upper bound")

    return (l,h)

  def flaggedVariablePairs(self):
    fk = set(self.fitKeys)
    return [ (k,v, k in fk) for (k,v) in self.variablePairs ]
  flaggedVariablePairs = property(fget = flaggedVariablePairs,
      doc = """Return list (variable_name, variable_values, isFitParameter) tuples as accepted by Variables constructor""")

  def createUpdated(self, newvals = None):
    """Given a new set of fitValues, create a copy of this Variables instance,
    updated with the new values. The current instance is left unaffected.

    @param newvals Updated list of values for fitting variables in same order as keys returned by fitKeys property.
                   If None, then produce copy of current instance.
    @return Copy of current Variables instance, containing updated values."""
    if newvals is None:
      return Variables(self.flaggedVariablePairs, self.bounds)

    ud = dict(zip(self.fitKeys, newvals))
    ivt = self.flaggedVariablePairs
    updated = []
    for i, (k,v,isp) in enumerate(ivt):
      if k in ud:
        updated.append( (k,ud[k], isp) )
      else:
        updated.append( (k,v,isp))
    return Variables(updated, self.bounds)

  def __repr__(self):
    s = "Variables("
    tokens = []
    for k,v,ff in self.flaggedVariablePairs:
      ff = {True : '*', False : ''}[ff]
      tokens.append("%s%s=%f" % (ff,k,v))
    tokens = ", ".join(tokens)
    return "%s%s)" % (s,tokens)

  def __str__(self):
    return repr(self)


class CalculatedVariables(object):
  """Class used to support calculated variables ([CalculatedVariables] within fit.cfg).

  CalculatedVariables instances are callable. When called with an instance of Variables,
  arithmetic expressions are evaluated using these variables. A new instance of Variables
  containing the results of this evaluation is then returned. """

  def __init__(self, nameExpressionTuples):
    """Create CalculatedVariables instance from a list of of (variable_name, expression) tuples.
    Where 'expression' is an arithmetic expression that can be parsed by cexprtk.evaluate_expression.

    @param nameExpressionTuples List of (variable_name, expression) tuples."""
    self.nameExpressionTuples = nameExpressionTuples

  def __call__(self, variables):
    """Evaluate expression values using provided variables.

    @param variables Variables instance containing values used for evaluation.
    @return Variables instance containing original variables and additionally evaluated values"""
    if not self.nameExpressionTuples:
      return variables

    variableDict = dict(variables.variablePairs)

    extraVars = []
    for name, expression in self.nameExpressionTuples:
      value = cexprtk.evaluate_expression(expression, variableDict)
      extraVars.append( (name, value,False))


    # Create the values we'll need for the Variables constructor.
    vartuples = variables.flaggedVariablePairs
    vartuples.extend(extraVars)

    # Don't forget the bounds
    bounds = variables.bounds
    for i in xrange(len(extraVars)):
      bounds.append(None)

    newvariables = Variables(vartuples, bounds)
    return newvariables

  @staticmethod
  def createFromConfig(cfgitems):
    for name, expression in cfgitems:
      try:
        cexprtk.check_expression(expression)
      except cexprtk.ParseException,e:
        raise ConfigException("Could not parse expression when processing [CalculatedVariables]: %s : %s" % (expression, e.message))

    return CalculatedVariables(cfgitems)

def _sumValuesReductionFunction(evaluatedJobs):
  """Default reduction function, for each list enter sub lists and sum values"""
  ret = []
  for candidates in evaluatedJobs:
    v = 0.0
    for j in candidates:
      for e in j.evaluatorRecords:
        v += sum([ er.meritValue for er in e])
    ret.append(v)
  return ret


class Merit(object):
  """Class defining merit function within fitTool.py.

  The class is responsible for coordinating the job creation, job batching, running and
  result evaluation.

  Merit support a few callbacks which can be set as properties:
    beforeRun : this callable is invoked after job creation but before jobs are run.
                The signature for beforeRun is:
                beforeRun(candidateJobPairs)

                Where candidateJobPairs is a list of (CANDIDATE, JOB_LIST) pairs (one for each
                candidate solution). CANDIDATE is atsim.pro_fit.fittool.Variables instance
                and JOB_LIST is a list of the atsim.pro_fit.jobfactories.Job instances
                representing each job belonging to the candidate.

    afterRun  : this is called after the jobs have been run, but before merit values are
                evaluated. It is passed the same variables as beforeRun.

    afterEvaluation : called after evaluation but before values are reduced to produce
                merit values and also before job directories are removed.

                As for afterRun and beforeRun, list of (CANDIDATE, JOB_LIST) pairs are passed
                to callback and job.evaluate() methods have been called therefore job.evaluatorRecords is
                accessible at this point.

    afterMerit : Called after reduction function (but before job directories are deleted) is applied and before calculate() returns.
                Has signature: afterMerit(meritValues, candidateJobPairs).

                Where candidateJobPairs is as before and meritValues is a list of merit values one
                per candidate. """

  def __init__(self, runners, jobfactories, metaevaluators, calculatedVariables, jobdir):
    """@param runners List of job runners (see atsim.pro_fit.runners module for examples).
       @param jobfactories List of jobfactories (see atsim.pro_fit.jobfactories module for examples).
       @param metaevaluators List of meta-evaluators to apply following job evaluation.
       @param calculatedVariables Callable that accepts Variables and returns Variables that are then used during job creation.
       @param jobdir Directory in which job files should be created"""

    # Initialize callbacks to be None
    self.beforeRun = None
    self.afterRun = None
    self.afterEvaluation = None
    self.afterMerit = None

    self._runners = runners
    self._jobfactories = jobfactories
    self._metaevaluators = metaevaluators
    self.calculatedVariables = calculatedVariables
    self._jobdir = jobdir
    self._reductionFunction = _sumValuesReductionFunction

  def _getjobdir(self):
    return self._jobdir
  jobdir = property(fget=_getjobdir)

  def calculate(self, candidates, returnCandidateJobPairs = False):
    """Calculate Merit value for each Variables object in candidates and return list of merit values.

    @param candidates List of Variables instances
    @return List of merit values if returnCandidateJobPairs is False or if True return tuple (meritValueList, candidateJobPairs),
      where candidateJobPairs is a list of (CANDIDATE, JOB_LIST) pairs (one for each candidate solution).
      CANDIDATE is atomsscripts.fitting.fittool.Variables instance and JOB_LIST is a list of the atsim.pro_fit.jobfactories.Job
      instances representing each job belonging to the candidate. This is equivalent to the values passed to afterMerit callback"""

    batchpaths, batchedJobs, candidate_job_lists = self._prepareJobs(candidates)
    try:
      finishedEvents = self._runBatches(batchedJobs)
      gevent.wait(objects= finishedEvents)

      #Call the afterRun callback
      if self.afterRun:
        self.afterRun(candidate_job_lists)

      # Apply evaluators
      self._applyEvaluators(batchedJobs)
      self._applyMetaEvaluators([ joblist for (v, joblist) in candidate_job_lists])

      # Call the afterEvaluation callback (first zip evaluated lists with their jobs)
      if self.afterEvaluation:
        self.afterEvaluation(candidate_job_lists)

      #Reduce evaluated dictionary into single values
      meritvals = self._reductionFunction([ joblist for (v, joblist) in candidate_job_lists])

      if self.afterMerit:
        self.afterMerit(meritvals, candidate_job_lists)

      if returnCandidateJobPairs:
        return (meritvals, candidate_job_lists)
      else:
        return meritvals
    finally:
      self._cleanBatches(batchpaths)

  def _prepareJobs(self, candidateVariables):
    """Create job directories and populate them with files from jobfactories.
    Returns Job instances and batches them together with their correct JobRunner.

    @param candidateVariables
    @return Tuple (batch_directories, job_lists, candidate_job_lists ).
            Where batch_directories : list of directories each batch is contained within.
            job_lists : list of lists of Job instances. Returned list is parallel to the self._runners list
            with each sub-list containing the jobs for a particular runner.
            candidate_job_lists : list of tuples (one pair per candidate), with pairs of variable instances and list of jobs for those variables."""
    runnerBatches = {}
    candidate_job_lists = []
    batchpaths = []
    for candidate in candidateVariables:
      candidate = self.calculatedVariables(candidate)
      cpath = tempfile.mkdtemp(dir=self._jobdir)
      batchpaths.append(cpath)
      candidate_job_lists.append( (candidate, []))
      for factory in self._jobfactories:
        # Create job path as combination of candidate temporary directory
        # and factory.name.
        jobpath = os.path.join(cpath, factory.name)
        os.mkdir(jobpath)
        job = factory.createJob(jobpath, candidate)
        candidate_job_lists[-1][1].append(job)
        #Assign job to correct batch
        runnerBatches.setdefault(factory.runnerName, []).append(job)

    #Call the beforeRun callback
    if self.beforeRun:
      self.beforeRun(candidate_job_lists)

    # Convert runnerBatches into list of lists
    return (batchpaths,  [ runnerBatches[r.name] for r in self._runners ], candidate_job_lists)

  def _runBatches(self, jobBatches):
    """Execute the runBatch command on the job batches and return threading.Event objects
    indicating when each batch completes

    @param jobBatches List of batched jobs as returned by _prepareJobs.
    @return List of threading.Event objects representing each submitted batch"""
    events = []
    for runner, batch in zip(self._runners, jobBatches):
      f = runner.runBatch(batch)
      events.append(f.finishedEvent)
    return events

  def _cleanBatches(self, batchpaths):
    for p in batchpaths:
      shutil.rmtree(p, ignore_errors = True)

  def _applyEvaluators(self, batchedJobs):
    """Apply job evaluators.

    @param batchedJobs Batched Jobs as returned by _prepareJobs."""
    for batch in batchedJobs:
      for j in batch:
        j.evaluate()

  def _applyMetaEvaluators(self, batchedJobs):
    """Apply meta evaluators. This adds a Job like container to the end of each
    job batch with an evaluatorRecords property populated with EvaluatorRecord like instances
    obtained by passing each job list to the callables stored in self._metaevaluators

    @param batchedJobs Batched Jobs as returned by _prepareJobs."""
    if self._metaevaluators:
      for batch in batchedJobs:
        evaluatorRecords = []
        for metaEvaluator in self._metaevaluators:
          evaluatorRecords.append(metaEvaluator(batch))
        metaEvaluatorJob = jobfactories.MetaEvaluatorJob("meta_evaluator", evaluatorRecords,batch[0].variables)
        batch.append(metaEvaluatorJob)

  def close(self):
    for runner in self._runners:
      runner.close()


