"""Module containing fittool job runners.

The basic interface for a runner is as follows:

  class Runner:
    def runBatch(self, jobs):
      ...

    @staticmethod
    def createFromConfig(runnerName, fitRootPath, cfgitems):
      ...

The runBatch method accepts a list of job instances as created by a job factory (see atsim.pro_fit.jobfactories) and
returns an object that will block until the jobs in the batch complete when its .join() method is called.

The runner should go into each job directory (given by Job.path property) and execute the 'runjob' command found there.
Any output from the command should then be placed in a sub-directory called output. The exit status of runjob should
be put in output/STATUS.

The static createFromConfig method provides a consistent interface for creating runnners from config information stored
in the fit.cfg file.
    def createFromConfig(runnerName, fitRootPath, cfgitems):
      @param runnerName String containing the label by which this runner is known
      @param fitRootPath Root directory for the fitting run containing the fit.cfg file
      @param cfgitems List of (option, value) pairs for this runner taken from [Runner] section in fit.cfg
      @return Runner instance

"""

from _localrunner import LocalRunner # noqa
from _remoterunner import RemoteRunner # noqa
from _base_remoterunner import BaseRemoteRunnerObserverAdapter
from _runner_job import RunnerJobObserverAdapter
from _exceptions import RunnerClosedException #noqa
from _pbsrunner import PBSRunner
from _slurmrunner import SlurmRunner
from _sgerunner import SGERunner

from gevent.event import Event

class NullFuture:

  def __init__(self):
    evt = Event()
    evt.set()
    self.finishedEvent = evt


class NullRunner(object):
  """Runner that does not run jobs. Used in conjunction with the SingleStepMinimizer
     to support the --create-files command line option"""

  def __init__(self, name):
    self.name = name
    self.observers = []

  def runBatch(self, jobs):
    return NullFuture()

  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    return NullRunner(runnerName)

  def close(self):
    evt = Event()
    evt.set()
    return evt