#! /usr/bin/env python
from gevent import monkey

monkey.patch_all()
monkey.patch_thread()

import atsim.pro_fit.evaluators
import atsim.pro_fit.jobfactories
import atsim.pro_fit.metaevaluators
import atsim.pro_fit.minimizers
import atsim.pro_fit.reporters
import atsim.pro_fit.runners
import atsim.pro_fit.fitconfig
import atsim.pro_fit.exceptions

from atsim.pro_fit._util import MultiCallback, iter_namespace
from atsim.pro_fit.console import Console
from atsim.pro_fit.exceptions import ConfigException

import argparse
import sys
import os
import logging
import logging.config
import shutil
import tempfile
import contextlib
import pkgutil
import importlib.resources
import curses

import jinja2
import gevent


def _monkeyPatchExecnetSIGINT():
    """The ssh instances launched by execnet were receiving SIGINT before cleanup had completed.
  Monkeypath execnet.gateway_base.Execmodel.get_execmodel.ExecModel.PopenPiped method to ignore SIGINT in the
  ssh subprocess"""

    def PopenPiped(self, args):
        def prefn():
            import signal

            signal.signal(signal.SIGINT, signal.SIG_IGN)

        PIPE = self.subprocess.PIPE
        return self.subprocess.Popen(
            args, stdout=PIPE, stdin=PIPE, preexec_fn=prefn
        )

    from execnet.gateway_base import get_execmodel as get_execmodel_orig

    def get_execmodel(backend):
        retobj = get_execmodel_orig(backend)
        import types

        retobj.PopenPiped = types.MethodType(PopenPiped, retobj)
        return retobj

    # ... do the monkeypatch
    import execnet.gateway_base

    execnet.gateway_base.get_execmodel = get_execmodel


_monkeyPatchExecnetSIGINT()


def _registerSignalHandlers(intEvent):
    import signal

    def setevt(signum, stackframe):
        intEvent.set()

    signal.signal(signal.SIGINT, setevt)
    signal.signal(signal.SIGTERM, setevt)


class _FittingToolException(Exception):
    pass


def _isValidDirectory():
    # Check that current working directory has required files and directories.
    # return True if directory is valid, False otherwise.

    if not os.path.isfile("fit.cfg"):
        raise ConfigException(
            "Not a valid pprofit directory. Required file 'fit.cfg' not found."
        )

    if not os.path.isdir("fit_files"):
        raise ConfigException(
            "Not a valid pprofit directory. Required directory 'fit_files' not found."
        )

    # Check for existence of a lockfile
    if os.path.isfile("lockfile"):
        with open("lockfile") as infile:
            pidline = next(infile)[:-1]
            raise ConfigException(
                "Found 'lockfile', this indicates that pprofit is already running with PID=%s"
                % pidline
            )

    return True


def _makeLockFile():
    # Put PID in lockfile
    with open("lockfile", "w") as outfile:
        print(os.getpid(), file=outfile)


def _removeLockFile():
    # Remove lockfile
    os.remove("lockfile")


class _DirectoryInitializationException(Exception):
    pass


def _initializeRun(directoryName):
    """Initialize fitting run directory. Create directoryName and populate with
  skeleton version of files required for a run.

  Args:
      directoryName (string): Path where run should be initialized.

  Raises:
      _DirectoryInitializationException: Raised if there is problem initializing run.
  """

    if os.path.exists(directoryName):
        raise _DirectoryInitializationException(
            "Directory already exists: '%s'" % directoryName
        )

    try:
        os.mkdir(directoryName)
        logging.getLogger("console").info(
            "Created directory: %s" % directoryName
        )
    except Exception as e:
        raise _DirectoryInitializationException(str(e))

    # Create fit_files directory
    dirname = os.path.join(directoryName, "fit_files")
    try:
        os.mkdir(dirname)
        logging.getLogger("console").info("Created directory: %s" % dirname)
    except Exception as e:
        raise _DirectoryInitializationException(
            "Could not create 'fit_files' directory: %s" % str(e)
        )

    templateLoader = jinja2.PackageLoader("atsim.pro_fit", "resources/dirinit")
    env = jinja2.Environment(loader=templateLoader)
    template = env.get_template("fit.cfg.jinja")

    runner_name = "Local"

    # Get variables required by the template
    import multiprocessing

    templateVariables = {
        "runner_name": runner_name,
        "title": os.path.basename(directoryName),
        "ncpus": multiprocessing.cpu_count(),
    }

    # Write the stream into a file
    fitCfgName = os.path.join(directoryName, "fit.cfg")
    template.stream(**templateVariables).dump(fitCfgName)
    logging.getLogger("console").info("Created: %s" % fitCfgName)

    # Create runner_files directory
    dirname = os.path.join(directoryName, "runner_files", runner_name)
    try:
        os.makedirs(dirname)
        logging.getLogger("console").info(
            "Created runner_files directory for the default runner '%s': %s"
            % (runner_name, dirname)
        )
    except Exception as e:
        raise _DirectoryInitializationException(
            "Could not create 'runner_files' directory: %s" % str(e)
        )


def _getValidRunners():
    """Open 'fit.cfg' and produce list of runner names.
  If fit.cfg does not exst or does not contain any runners,
  return []

  @return List of runner names"""

    if not os.path.exists("fit.cfg"):
        return []

    import configparser

    config = configparser.SafeConfigParser()
    config.optionxform = str
    with open("fit.cfg", "r") as fitCfgFile:
        config.read_file(fitCfgFile)

    runners = []
    for s in config.sections():
        if s.startswith("Runner:"):
            runnerkey = s[7:]
            runners.append(runnerkey)

    return runners


def _initializeJob(jobDescription):
    """Parses jobDescription into JOB_NAME:RUNNER_NAME pair. Then
  creates skeleton of a template job"""
    if not _isValidDirectory():
        raise _DirectoryInitializationException(
            "jobs should be created from within a valid fitting-run directory"
        )

    tokens = jobDescription.split(":")
    if len(tokens) == 1:
        jobname = tokens[0]
        runnerName = None
    elif len(tokens) > 1:
        jobname = tokens[0]
        runnerName = tokens[1]
    else:
        raise _DirectoryInitializationException(
            "Unable to parse job name whilst initializing job: '%s'"
            % jobDescription
        )

    # Check or get the name of the runner
    validRunners = _getValidRunners()
    if not validRunners:
        raise _DirectoryInitializationException(
            "No runners specified in 'fit.cfg'"
        )
    elif runnerName:
        if not runnerName in validRunners:
            raise _DirectoryInitializationException(
                "Runner name not found in 'fit.cfg': %s" % runnerName
            )
    else:
        runnerName = validRunners[0]

    # Create directory
    jobDirname = os.path.join("fit_files", jobname)

    if os.path.exists(jobDirname):
        raise _DirectoryInitializationException(
            "Job directory already exists: '%s'" % jobDirname
        )

    try:
        os.mkdir(jobDirname)
    except Exception as e:
        raise _DirectoryInitializationException(
            "Could not create job directory '%s': %s" % (jobDirname, str(e))
        )

    # Create 'job.cfg'
    templateLoader = jinja2.PackageLoader("atsim.pro_fit", "resources/jobinit")
    env = jinja2.Environment(loader=templateLoader)

    template = env.get_template("job.cfg.jinja")
    outname = os.path.join(jobDirname, "job.cfg")
    template.stream(runnerName=runnerName).dump(outname)
    logging.getLogger("console").info(
        "Created: '%s' using runner named: '%s'" % (outname, runnerName)
    )

    # Create 'runjob'
    template = env.get_template("runjob.jinja")
    outname = os.path.join(jobDirname, "runjob")
    template.stream().dump(outname)
    logging.getLogger("console").info("Created: %s" % outname)


def _setupLogging(verbose):
    """Set-up python logging"""
    # Read logging information from logging.cfg in the resources package
    # cfg = pkgutil.get_data('atsim.pro_fit', 'resources/logging.cfg')
    cfg = importlib.resources.read_text(
        "atsim.pro_fit.resources", "logging.cfg"
    )
    import io

    cfg = io.StringIO(cfg)

    logging.config.fileConfig(cfg)

    # Messages from warnings.warn shoud go into the logging system.
    logging.captureWarnings(True)

    if verbose:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
    else:
        logger = logging.getLogger("atsim.pro_fit.fittingTool")
    return logger


def _setupConsoleLogging(console):
    """If it is enabled, route messages to curses GUI"""
    console_handler = logging.handlers.QueueHandler(
        console.model.messages_queue
    )

    # Add this handler to the console logger
    console_logger = logging.getLogger("console")
    console_logger.addHandler(console_handler)


def _parseCommandLine():
    usage = """%(prog)s performs an automated potential fitting run in the current directory. See documentation \
for description of required directory structure and files."""

    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        default=False,
        help="use verbose logging",
    )

    parser.add_argument(
        "--disable-console",
        dest="console",
        action="store_false",
        default=True,
        help="disable the curses based console",
    )

    optgroup = parser.add_argument_group("Run options")
    optgroup.add_argument(
        "-s",
        "--single-step",
        dest="single_step",
        action="store",
        metavar="JOB_DIR",
        help="using the values from fit.cfg evaluate the merit function and store job files in JOB_DIR",
    )

    optgroup.add_argument(
        "-c",
        "--create-files",
        dest="create_files",
        action="store",
        metavar="JOB_DIR",
        help="create job files but do not run or perform evaluation. Jobs are created in JOB_DIR",
    )

    optgroup.add_argument(
        "-p",
        "--plugin",
        dest="plugins",
        action="append",
        metavar="PYTHON_FILE",
        help="Search python .py with filename PYTHON_FILE for additional meta-evalutaors, evaluators, runners, minimizers and job-factories. Specify -p/--plugin once for each plugin file.",
    )

    optgroup = parser.add_argument_group(
        "Initialisation", "Options for creating fitting runs and jobs"
    )
    optgroup.add_argument(
        "-i",
        "--init",
        dest="init",
        action="store",
        metavar="RUN_DIR",
        help="create directory named RUN_DIR and initialize directory structure for new fitting run.",
    )

    optgroup.add_argument(
        "-j",
        "--init-job",
        dest="initjob",
        action="store",
        metavar="JOB_NAME[:RUNNER]",
        help="create skeleton of a new job using 'Template' job factory, within fit_files directory. Job directory will be named JOB_DIRECTORY. Optionally, RUNNER can be specified. \
This gives name of runner (defined in fit.cfg) to be associated with created JOB. If not specified, first runner within 'fit.cfg' will be used.",
    )

    args = parser.parse_args()

    # Validate option choice.
    if args.single_step and (
        args.init or args.initjob or args.create_files
    ):
        parser.error("-s/--single-step cannot be specified with other options")

    if args.create_files and (
        args.init or args.initjob or args.single_step
    ):
        parser.error(
            "-c/--create-files cannot be specified with other options"
        )

    if args.init and args.initjob:
        parser.error("-i/--init cannot be specified with -j/--init-job")

    return args


def _getfitcfg(
    jobdir, cls=atsim.pro_fit.fitconfig.FitConfig, pluginmodules=[]
):
    """Creates atsim.pro_fit.fitconfig.FitConfig from configuration files.
     @param jobdir Directory in which temporary job files should be created.
     @param cls FitConfig class
     @param pluginmodules Additional modules to be scanned by FitConfig for evaluators, runners etc.
     @return atsim.pro_fit.fitconfig.FitConfig instance"""
    import atsim.pro_fit.runners

    runners = [atsim.pro_fit.runners]
    evaluators = [atsim.pro_fit.evaluators]
    metaevaluators = [atsim.pro_fit.metaevaluators]
    jobfactories = [atsim.pro_fit.jobfactories]
    minimizers = [atsim.pro_fit.minimizers]

    # Introspect the the atsim.pro_fit.plugins namespace and add these to pluginmodules too.
    ns_pluginsmodules = []
    try:
        import atsim.pro_fit.plugins
        import importlib

        for _finder, name, _ispkg in iter_namespace(atsim.pro_fit.plugins):
            m = importlib.import_module(name)
            ns_pluginsmodules.append(m)
    except ImportError:
        pass

    for l in [runners, evaluators, metaevaluators, jobfactories, minimizers]:
        l.extend(pluginmodules)
        l.extend(ns_pluginsmodules)

    return cls(
        "fit.cfg",
        runners,
        evaluators,
        metaevaluators,
        jobfactories,
        minimizers,
        jobdir=jobdir,
    )


def _getSingleStepCfg(jobdir, keepDirectory, pluginmodules):
    """Creates atsim.pro_fit.fitconfig.FitConfig like object from configuration files.
  Object is customised such that its minimizer property return SingleStepMinimizer that will
  copy files to keepDirectory following run.

  @param jobdir Directory in which temporary job files should be created.
  @param keepDirectory Path into which job files are copied following run.
  @param pluginmodules List of module objects to be scanned by FitConfig
  @return atsim.pro_fit.fitconfig.FitConfig like object configured to use SingleStepMinimizer"""

    class CustomConfig(atsim.pro_fit.fitconfig.FitConfig):
        def _createMinimizer(self, minimizermodules):
            return atsim.pro_fit.minimizers.SingleStepMinimizer(
                self.variables, keepDirectory
            )

    return _getfitcfg(jobdir, cls=CustomConfig, pluginmodules=pluginmodules)


def _getCreateFilesCfg(jobdir, keepDirectory, pluginmodules):
    """Creates atsim.pro_fit.fitconfig.FitConfig like object from configuration files.
  Object is customised such that its minimizer property return SingleStepMinimizer that will
  copy files to keepDirectory following run. In addition evaluators and meta-evaluators are disabled.
  Further, all runners are replaced by NullRunner instances.

  @param jobdir Directory in which temporary job files should be created.
  @param keepDirectory Path into which job files are copied following run.
  @param pluginmodules List of modules to be scanned by FitConfig for runners, evaluators etc.
  @return atsim.pro_fit.fitconfig.FitConfig like object configured to use SingleStepMinimizer"""
    import collections

    def defaultfactory():
        return atsim.pro_fit.runners.NullRunner

    runnerdict = collections.defaultdict(defaultfactory)

    class CustomConfig(atsim.pro_fit.fitconfig.FitConfig):
        def _createMinimizer(self, minimizermodules):
            return atsim.pro_fit.minimizers.SingleStepMinimizer(
                self.variables, keepDirectory
            )

        def _findClasses(self, modulelist, nameSuffix):
            if nameSuffix == "Runner":
                return runnerdict
            elif nameSuffix == "Evaluator":
                return {}
            return super(CustomConfig, self)._findClasses(
                modulelist, nameSuffix
            )

        def _createMetaEvaluators(self, metaevaluatorModules):
            return []

        def _createEvaluators(self, jobname, jobpath, fitcfg, evaldict):
            return []

        def _verifyHasEvaluators(self):
            pass

    return _getfitcfg(jobdir, cls=CustomConfig, pluginmodules=pluginmodules)


def _invokeMinimizer(cfg, logsql, console):
    logger = logging.getLogger(__name__).getChild("_invokeMinimizer")
    console_logger = logging.getLogger("console")

    console_logger.info(
        "Temporary job files will be created in '%s'", cfg.jobdir
    )

    # Perform minimization run
    if not os.path.isdir(cfg.jobdir):
        raise _FittingToolException(
            'Minimization run. Job temporary directory does not exist: "%s"'
            % cfg.jobdir
        )

    if len(cfg.variables.fitKeys) == 0:
        raise atsim.pro_fit.exceptions.ConfigException(
            'No variables selected to change during minimization within "fit.cfg" [Variables] section.'
        )

    # Set-up reporters
    stepCallback = MultiCallback()
    # ... create the console log reporter
    stepCallback.append(atsim.pro_fit.reporters.LogReporter())
    # ... create SQLiteReporter
    if logsql:
        if os.path.exists("fitting_run.db"):
            console_logger.info("Removing existing 'fitting_run.db'")
            os.remove("fitting_run.db")
        sqlreporter = atsim.pro_fit.reporters.SQLiteReporter(
            "fitting_run.db",
            cfg.variables,
            cfg.merit.calculatedVariables,
            cfg.title,
        )
        stepCallback.append(sqlreporter)

    minimizer = cfg.minimizer
    minimizer.stepCallback = stepCallback

    if console:
        console.registerConfig(cfg)
        stepCallback.append(console.stepCallback)
    try:
        # with contextlib.closing(cfg.merit):
        _registerSignalHandlers(cfg.endEvent)

        minimizer.minimize(cfg.merit)
        if logsql:
            sqlreporter.finished()
    except:
        logger.exception("Exception raised")
        raise
    finally:
        if logsql:
            sqlreporter.finished(True)


def _welcome_message():
    console_logger = logging.getLogger("console.starting")
    console_logger.info(
        "Potential Pro-Fit v%s\n\n", atsim.pro_fit._version.__version__
    )
    console_logger.info("Starting...")
    gevent.sleep(0)


class _PluginException(Exception):
    def __init__(self, filename, childexception):
        self.filename = filename
        self.childexception = childexception


def _processPlugins(pathList):
    """Load python files listed in pathList and return list of python module objects.

  @param pathList List of paths to python files.
  @return List of python module objects"""
    import imp

    def loadmod(path):
        modname, _ext = os.path.splitext(path)
        return imp.load_source(modname, path)

    retlist = []
    for p in pathList:
        try:
            retlist.append(loadmod(p))
        except Exception as e:
            raise _PluginException(p, e)

    return retlist


def main():
    # Get command line options
    options = _parseCommandLine()
    _setupLogging(options.verbose)
    console = None
    exit_code = 0

    try:
        # Support for the --disable-console option.
        if options.console:
            console = Console()
            _setupConsoleLogging(console)
            console.start()
            gevent.sleep(0)
        else:
            # ... if enabled, the data logged to pprofit.log will also appear in the terminal.
            root_logger = logging.getLogger()
            stream_handler = logging.StreamHandler(sys.stderr)
            stream_handler.setLevel(logging.DEBUG)
            stream_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s:%(name)s:%(levelname)s %(module)s:%(lineno)d:  %(message)s"
                )
            )
            root_logger.addHandler(stream_handler)

        _welcome_message()

        pluginmodules = []

        if options.plugins:
            try:
                pluginmodules.extend(_processPlugins(options.plugins))
            except _PluginException as pe:
                filename = pe.filename
                try:
                    raise pe.childexception
                except IOError as e:
                    raise ConfigException(
                        "Error processing --plugin option for file '%s': '%s'"
                        % (filename, e.strerror)
                    )
                except Exception:
                    raise ConfigException(
                        "Error processing --plugin option for file '%s'"
                        % (filename,)
                    )

        if options.init:
            # Initialize directory
            try:
                _initializeRun(options.init)
                logging.getLogger("console").info(
                    "Now cd into directory and use --init-job to create jobs."
                )
                sys.exit(0)
            except _DirectoryInitializationException as e:
                raise ConfigException(
                    "Error initializing directory: {}".format(str(e))
                )
        elif options.initjob:
            # Initialize job
            try:
                _initializeJob(options.initjob)
                sys.exit(0)
            except _DirectoryInitializationException as e:
                raise ConfigException(
                    "Error initializing job: {}".format(str(e))
                )

        # Check that directory is valid
        _isValidDirectory()
        _makeLockFile()

        # Start the minimizer
        perform_minimization(options, pluginmodules, console)
    except Exception as e:
        logger = logging.getLogger(__name__).getChild("main")
        try:
            raise e
        except ConfigException:
            logger.error(e)
        except Exception:
            logger.exception(e)

        if console and console.started:
            evt = console.terminalError(str(e))
            evt.wait()
        exit_code = 1
    finally:
        if console and console.started:
            console_close_event = console.close()
            gevent.wait([console_close_event])
        sys.exit(exit_code)


def perform_minimization(options, pluginmodules, console):
    tempdir = tempfile.mkdtemp()

    console_logger = logging.getLogger("console")

    try:
        cfg = None
        logsql = True
        if options.single_step:
            # Do not run a minimization run
            console_logger.info("Performing Single-step run")
            cfg = _getSingleStepCfg(
                tempdir, options.single_step, pluginmodules
            )
        elif options.create_files:
            console_logger.info(
                "Job files will be created in: %s", options.create_files
            )
            cfg = _getCreateFilesCfg(
                tempdir, options.create_files, pluginmodules
            )
            logsql = False
        else:
            # Perform a minimization run
            console_logger.info("Performing Minimization run")
            cfg = _getfitcfg(tempdir, pluginmodules=pluginmodules)

        _invokeMinimizer(cfg, logsql, console)

    finally:
        if cfg:
            cfg.endEvent.set()
            cfg_closed_event = cfg.closedEvent
        else:
            cfg_closed_event = gevent.event.Event()
            cfg_closed_event.set()

        gevent.wait([cfg_closed_event])

        _removeLockFile()
        shutil.rmtree(tempdir, ignore_errors=True)
