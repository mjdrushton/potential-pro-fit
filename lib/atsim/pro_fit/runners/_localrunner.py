from atsim.pro_fit.fittool import ConfigException

import threading
import Queue


class LocalRunner(object):
  """Runner that coordinates parallel job submission to local machine"""

  def __init__(self, name, nprocesses):
    """@param name Name of this runner.
    @param nprocesses Number of processes that can be run in parallel by this runner"""
    self.name = name
    self._batchinputqueue = Queue.Queue()
    self._i = 0
    self._runner = _InnerRunner(self._batchinputqueue, nprocesses)
    self._runner.start()


  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    @param jobs List of job instances as created by a JobFactory
    @return LocalRunnerFuture a job future that supports .join() to block until completion"""
    event = threading.Event()
    self._i += 1
    future = LocalRunnerFuture('Batch %s' % self._i, event, jobs)
    return future

  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    allowedkeywords = set(['nprocesses', 'type'])
    cfgdict = dict(cfgitems)

    for k in cfgdict.iterkeys():
      if not k in allowedkeywords:
        raise ConfigException("Unknown keyword for Local runner '%s'" % k)

    try:
      nprocesses = cfgdict['nprocesses']
    except KeyError:
      raise ConfigException("nprocesses configuration item not found")

    try:
      nprocesses = int(nprocesses)
    except ValueError:
      raise ConfigException("Could not convert nprocesses configuration item into an integer")

    return LocalRunner(runnerName, nprocesses)
