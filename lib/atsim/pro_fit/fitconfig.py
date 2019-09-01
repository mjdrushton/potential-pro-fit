from atsim.pro_fit.exceptions import (
    ConfigException,
    MultipleSectionConfigException,
)
from atsim.pro_fit.variables import Variables, CalculatedVariables
from atsim.pro_fit.merit import Merit, Replace_Merit_After_Evaluation_Callback
from atsim.pro_fit.cfg import float_convert

import configparser

import os
import logging
import math

from atsim.pro_fit import jobfactories

import cexprtk

import gevent


class FitConfig(object):
    """Object parses fit.cfg at root of a pprofit run
  and acts as factory for objects required by the fitting tool"""

    # The set of section names that can be repeated in a fit.cfg file.
    _repeatedSections = set(["Runner", "Evaluator"])

    def __init__(
        self,
        fitCfgFilename,
        runnermodules,
        evaluatormodules,
        metaevaluatormodules,
        jobfactorymodules,
        minimizermodules,
        jobdir=None,
    ):
        """Create FitConfig from file containing configuration information.

    @param fitCfgFilename Filename for fit.cfg.
    @param runnermodules List of python module objects containing Runner objects
    @param evaluatormodules List of python module objects containing Evaluator objects
    @param metaevaluatormodules List of python module objects containing MetaEvaluator objects
    @param jobfactorymodules List of python module objects containing JobFactories objects
    @param minimizermodules List of python module objects contiaining Minimizers
    @param jobdir If specified use as directory for temporary files, if not use rootpath/jobs
    @param minimizer If not None overrides the minimizer specified in fit.cfg"""
        self._logger = logging.getLogger(__name__).getChild("FitConfig")
        self._fitRootPath = os.path.abspath(os.path.dirname(fitCfgFilename))
        if jobdir:
            self.jobdir = jobdir
        else:
            self.jobdir = os.path.join(self._fitRootPath, "jobs")
        self._validateConfigStructure(fitCfgFilename)
        cfg = self._parseConfig(fitCfgFilename)
        self._cfg = cfg
        self._variables = self._createVariables()
        self._calculatedVariables = self._createCalculatedVariables()
        self._runners = self._createRunners(runnermodules)
        self._metaevaluators = self._createMetaEvaluators(metaevaluatormodules)
        self._jobfactories = self._createJobFactories(
            jobfactorymodules, evaluatormodules
        )

        self._verifyHasJobs()

        # Check that at least some evaluators have been defined
        self._verifyHasEvaluators()

        # Get rid of unused runners
        self._filterRunners()
        if not self._runners:
            raise ConfigException(
                "No Runners have been defined or no jobs assigned to those that have."
            )

        # Create the runners we actually need.
        self._instantiateRunners()
        self._title = self._parseTitle()
        self._bad_merit_substitute = self._parse_bad_merit_substitute()

        self._merit = self._createMerit()
        self._minimizer = self._createMinimizer(minimizermodules)

        self._endEvent = gevent.event.Event()

        self._closeGreenlet = gevent.spawn(self._close)
        self._closeGreenlet.name = "FitConfig_closedGreenlet-{}".format(
            self._closeGreenlet.name
        )
        self._closedEvent = gevent.event.Event()

        def closedevtset(grn):
            self._closedEvent.set()

        self._closeGreenlet.link(closedevtset)

    def _close(self):
        """Shuts-down the fitting system"""
        self._endEvent.wait()
        logger = logging.getLogger("console.shutdown")
        logger.info("Shutting down 'pprofit'")
        self._minimizer.stopMinimizer()
        evts = self._closeRunners()
        gevent.wait(evts)

    def _closeRunners(self):
        evts = []
        logger = logging.getLogger("console.shutdown")
        for name, r in self._runners.items():
            logger.info("Closing runner: '%s'", name)
            e = r.close()

            def repclose(evt, name):
                evt.wait()
                logger.info("Runner '%s' now closed.", name)

            grn = gevent.spawn(repclose, e, name)
            grn.name = "FitConfig__closeRunners-{}-{}".format(name, grn.name)

            evts.append(e)
        return evts

    def close(self):
        """Used to terminate pprofit. This is equivalent to self.endEvent.set()"""
        self.endEvent.set()
        self._closedEvent.wait()

    def title(self):
        return self._title

    title = property(
        fget=title,
        doc="""Returns name of fitting run representing [FittingRun] title field""",
    )

    def variables(self):
        return self._variables

    variables = property(
        fget=variables,
        doc="""Returns a Variables object representing [Variables] section of fit.cfg""",
    )

    def calculatedVariables(self):
        return self._calculatedVariables

    calculatedVariables = property(
        fget=calculatedVariables,
        doc="""Returns a CalculatedVaraiables instance, this is a callable that can transform variables before job creation. This is created from the [CalculatedVariables] section of the fit.cfg file.""",
    )

    def runners(self):
        return self._runners

    runners = property(
        fget=runners,
        doc="""Returns a dictionary with runner names as keys and runner objects as  values""",
    )

    def merit(self):
        return self._merit

    merit = property(
        fget=merit, doc="""Returns Merit instance create from configuration"""
    )

    def minimizer(self):
        return self._minimizer

    minimizer = property(
        fget=minimizer, doc="Returns Minimizer parsed from configuration"
    )

    def metaEvaluators(self):
        return self._metaevaluators

    metaEvaluators = property(
        fget=metaEvaluators,
        doc="Return list of meta-evaluator objects parsed from fit.cfg",
    )

    def endEvent(self):
        return self._endEvent

    endEvent = property(
        fget=endEvent,
        doc="Return event set when SIGINT is called indicating that system should be terminated.",
    )

    def closedEvent(self):
        return self._closedEvent

    closedEvent = property(
        closedEvent,
        doc="Return event that is set when the system clean-up has taken place.",
    )

    def bad_merit_substitute(self):
        return self._bad_merit_substitute

    bad_merit_substitute = property(
        bad_merit_substitute,
        doc="If this is not None, evaluator merit values that are non finite (e.g. nan or inf) will be replaced with this value",
    )

    def _parseConfig(self, fitCfgFilename):
        """@param fitCfgFilename Filename for fit.cfg.
    @return ConfigParser object"""
        config = configparser.SafeConfigParser()
        config.optionxform = str
        with open(fitCfgFilename, "r") as fitCfgFile:
            config.read_file(fitCfgFile)
        return config

    def _validateConfigStructure(self, fitCfgFilename):
        """Check structure of config file. At the moment this checks for multiple configuration sections
    that should only be specified once (e.g. multiple Minimizer and Variables sections).

    @param fitCfgFilename Filename of configuration file

    @raises MultipleSectionConfigException Raised if multiple instances of a configuration section are found
      when only one may be specified."""

        import re

        regex = re.compile(r"^\s*\[(.*)\]")

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
                raise MultipleSectionConfigException(
                    "Found multiple '%s' sections in fit.cfg file where only one is allowed"
                    % stype
                )
            found.add(stype)

    def _createVariables(self):
        """Create Variables object from parsed configuration"""
        if not self._cfg.has_section("Variables"):
            raise ConfigException(
                "fit.cfg does not contain [Variables] section"
            )
        import re

        regex = r"^(.*?)(\(.*\))?(\*)?$"
        defaultBound = (float("-inf"), float("inf"))
        regex = re.compile(regex)
        kvpairs = self._cfg.items("Variables")
        newpairs = []
        bounds = []
        for k, v in kvpairs:
            v = re.sub(r"\s", "", v)
            m = regex.match(v)
            if not m:
                raise ConfigException(
                    "Variable '%s' has an invalid format" % k
                )
            groups = m.groups()

            isFitParameter = groups[-1] == "*"
            try:
                v = float(groups[0])
            except ValueError:
                raise ConfigException(
                    "Variable '%s' value cannot be converted to float: %s"
                    % (k, v)
                )

            if groups[1] != None:
                try:
                    bound = Variables._parseBounds(groups[1])
                except ConfigException as ce:
                    raise ConfigException(
                        "Variable '%s' has invalid bounds: %s" % (k, ce)
                    )
            else:
                bound = defaultBound

            newpairs.append((k, v, isFitParameter))
            bounds.append(bound)
        return Variables(newpairs, bounds)

    def _createCalculatedVariables(self):
        """Create a callable from parsed configuration"""
        if not self._cfg.has_section("CalculatedVariables"):
            return CalculatedVariables([])
        else:
            cfgitems = self._cfg.items("CalculatedVariables")
            existingVariables = set(
                [name for (name, expression) in self.variables.variablePairs]
            )
            for name, expression in cfgitems:
                if name in existingVariables:
                    raise ConfigException(
                        "[CalculatedVariables] defines variable that is already defined in [Variables]: %s"
                        % name
                    )

            for name, expression in cfgitems:
                try:
                    cexprtk.check_expression(expression)
                except cexprtk.ParseException as e:
                    raise ConfigException(
                        "Could not parse formula within [CalculatedVariables] for '%s' with expression '%s' : %s"
                        % (name, expression, e)
                    )

            return CalculatedVariables(cfgitems)

    def _createRunners(self, runnermodules):
        runnerdict = self._findClasses(runnermodules, "Runner")

        def runnerMaker(cls, runnerkey, cfgitems):
            def f():
                return cls.createFromConfig(
                    runnerkey, self._fitRootPath, cfgitems
                )

            return f

        # Extract runner sections from cfg
        runners = {}
        for s in self._cfg.sections():
            if s.startswith("Runner:"):
                runnerkey = self._parseColonKey("Runner", s)
                self._logger.debug(
                    'Processing config section: "%s". Runner name: "%s"'
                    % (s, runnerkey)
                )
                # Runner class
                rtype = self._cfg.get(s, "type")
                try:
                    rcls = runnerdict[rtype]
                except KeyError:
                    raise ConfigException(
                        "Could not find Runner for config section: %s" % s
                    )
                # To allow only instantiating runners we need (i.e. those with jobs)
                # defer creation by placing a no-arg callable in the dictionary responsible
                # for runner creation.
                runners[runnerkey] = runnerMaker(
                    rcls, runnerkey, self._cfg.items(s)
                )
                # self._logger.info('Configured runner: %s' % runnerkey)
        return runners

    def _filterRunners(self):
        """Remove any runner from self._runners that is not used by a job."""

        filtered = {}
        neededRunnerKeys = set([jf.runnerName for jf in self._jobfactories])
        for k, r in self._runners.items():
            if not k in neededRunnerKeys:
                self._logger.warn(
                    "Runner not assigned to any jobs and will therefore not be created: '%s'"
                    % k
                )
            else:
                filtered[k] = r
        self._runners = filtered

    def _instantiateRunners(self):
        """Create runners"""
        instantiated = {}
        for k, r in self._runners.items():
            self._logger.info("Creating runner: '%s'" % k)
            instantiated[k] = r()
        self._runners = instantiated

    def _createMetaEvaluators(self, metaevaluatorModules):
        evaldict = self._findClasses(metaevaluatorModules, "MetaEvaluator")

        # Extract MetaEvaluator sections from config
        metaEvaluators = []
        for s in self._cfg.sections():
            if s.startswith("MetaEvaluator:"):
                evalkey = self._parseColonKey("MetaEvaluator", s)
                # Eval class
                evaltype = self._cfg.get(s, "type")
                try:
                    evalcls = evaldict[evaltype]
                except KeyError:
                    raise ConfigException(
                        "Could not find MetaEvaluator for config section: %s"
                        % s
                    )
                metaEvaluators.append(
                    evalcls.createFromConfig(
                        evalkey, self._fitRootPath, self._cfg.items(s)
                    )
                )
        return metaEvaluators

    def _parseColonKey(self, prefix, s):
        """Strip 'prefix:'' from string s and return result

    @param prefix Prefix to be returned
    @param s String to be stripped
    @return s with 'prefix:' stripped from s"""
        k = s[len(prefix) + 1 :]
        return k.strip()

    def _createMerit(self):
        # Build the merit object
        runners = [v for (k, v) in sorted(self.runners.items())]
        merit = Merit(
            runners,
            self._jobfactories,
            self._metaevaluators,
            self.calculatedVariables,
            self.jobdir,
        )

        if self.bad_merit_substitute is not None:
            # Register afterEvaluation callback
            after_evaluation = Replace_Merit_After_Evaluation_Callback(
                math.isfinite, self.bad_merit_substitute
            )
            merit.afterEvaluation.append(after_evaluation)
            self._logger.info(
                "bad_merit_substitute specified. Invalid merit values will be repalced with %f".format(
                    self.bad_merit_substitute
                )
            )

        return merit

    def _createJobFactories(self, jobfactorymodules, evaluatormodules):
        evaldict = self._findClasses(evaluatormodules, "Evaluator")
        jobfdict = self._findClasses(jobfactorymodules, "JobFactory")

        # Walk fit_files directory
        fitfilespath = os.path.join(self._fitRootPath, "fit_files")
        self._logger.debug(
            'Creating jobs from directories in "%s"' % fitfilespath
        )

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
        cfgpath = os.path.join(path, "job.cfg")
        try:
            fitcfg = self._parseConfig(cfgpath)
        except IOError:
            raise ConfigException(
                'Could not find "job.cfg" in job directory: "%s"' % path
            )

        jobname = os.path.basename(path)

        # Process evaluators
        evaluators = self._createEvaluators(jobname, path, fitcfg, evaldict)

        # Process the Job section
        jfclsname = fitcfg.get("Job", "type")
        try:
            jfcls = jobfdict[jfclsname]
        except KeyError:
            raise ConfigException(
                'Unknown job type: "%s" for job named: "%s"'
                % (jfclsname, jobname)
            )

        runnername = fitcfg.get("Job", "runner")
        if runnername not in self.runners:
            raise ConfigException(
                'Unknown runner: "%s" for job named: "%s"'
                % (runnername, jobname)
            )
        return jfcls.createFromConfig(
            path,
            self._fitRootPath,
            runnername,
            jobname,
            evaluators,
            fitcfg.items("Job"),
        )

    def _createEvaluators(self, jobname, jobpath, fitcfg, evaldict):
        evaluators = []
        for s in fitcfg.sections():
            if s.startswith("Evaluator:"):
                ename = self._parseColonKey("Evaluator", s)
                self._logger.debug('Processing evaluator "%s"' % ename)
                eclsname = fitcfg.get(s, "type")
                try:
                    ecls = evaldict[eclsname]
                except KeyError:
                    raise ConfigException(
                        'Unknown evaluator type: "%s" for job "%s"'
                        % (eclsname, jobname)
                    )

                evaluator = ecls.createFromConfig(
                    ":".join([jobname, ename]), jobpath, fitcfg.items(s)
                )
                evaluators.append(evaluator)
        # Throw configuration exception if job does not define any evaluators
        if not evaluators:
            raise ConfigException(
                "Job does not define any evaluators: '%s'" % jobname
            )
        return evaluators

    def _createMinimizer(self, minimizermodules):
        minclasses = self._findClasses(minimizermodules, "Minimizer")
        if not self._cfg.has_section("Minimizer"):
            raise ConfigException(
                "fit.cfg does not contain a [Minimizer] section"
            )

        try:
            clsname = self._cfg.get("Minimizer", "type")
        except:
            raise ConfigException(
                'fit.cfg [Minimizer] section does not contain "type" option'
            )

        try:
            mincls = minclasses[clsname]
        except KeyError:
            raise ConfigException('Unknown minimizer: "%s"' % clsname)

        self._logger.info("Creating minimizer: %s" % mincls)
        minimizer = mincls.createFromConfig(
            self.variables, self._cfg.items("Minimizer")
        )
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
                    self._logger.info(
                        "Found %s: %s in module %s" % (nameSuffix, name, mod)
                    )
                    runnerdict[name] = cls
        return runnerdict

    def _parseTitle(self):
        try:
            title = self._cfg.get("FittingRun", "title")
            if title is None:
                title = "fitting_run"
        except configparser.NoSectionError:
            title = "fitting_run"

        return title

    def _parse_bad_merit_substitute(self):
        value = None
        try:
            converter = float_convert(
                "fit.cfg", "bad_merit_substitute", bounds=(0.0, float("inf"))
            )
            value = self._cfg.get("FittingRun", "bad_merit_substitute")
            if value:
                value = converter(value)
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            pass

        return value

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
            raise ConfigException(
                "No Evaluators have been defined for any Job."
            )
