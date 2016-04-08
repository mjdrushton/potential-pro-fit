
from atsim.pro_fit.fittool import ConfigException

import execnet
import StringIO
import tarfile
import os
import shutil
import threading
import tempfile
import posixpath
import Queue
import logging

from _localrunner import LocalRunnerFuture
from _inner import _InnerRunner

from _remote_common import RemoteRunnerBase, _copyFilesFromRemote
import _execnet

EXECNET_TERM_TIMEOUT=10


class RemoteRunnerFuture(LocalRunnerFuture):
  """Joinable future object as returned by RemoteRunnerFuture.runBatch()"""

  def __init__(self, name, e, jobs, execnetURL, remotePath, batchDir, localToRemotePathTuples):
    """@param name Name future
    @param e threading.Event used to communicate when batch finished
    @param jobs List of Job instances belonging to batch
    @param execnetURL URL used to create execnet gateway.
    @param remotePath Base directory for batches on remote server
    @param batchDir Name of batch directory for batch belonging to this future
    @param localToRemotePathTuples Pairs of localPath, remotePath tuples for each job"""
    LocalRunnerFuture.__init__(self, name, e, jobs)
    self._remotePath = remotePath
    self._batchDir = batchDir
    self._gwurl = execnetURL
    self._localToRemotePathTuples = localToRemotePathTuples

  def run(self):
    self._e.wait()
    group = execnet.Group()
    group.defaultspec = self._gwurl
    gw = group.makegateway()
    try:
      _copyFilesFromRemote(gw, self._remotePath, self._batchDir, self._localToRemotePathTuples)
    finally:
      group.terminate(EXECNET_TERM_TIMEOUT)

class RemoteRunner(RemoteRunnerBase):
  """Runner that uses SSH to run jobs in parallel on a remote machine"""

  logger = logging.getLogger("atsim.pro_fit.runners.RemoteRunner")

  def __init__(self, name, url, nprocesses, identityfile=None, extra_ssh_options = []):
    """@param name Name of this runner.
       @param url Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
       @param nprocesses Number of processes that can be run in parallel by this runner
       @param identityfile Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                         the platform's ssh command are used.
       @param extra_ssh_options List of (key,value) tuples that are added to the ssh_config file used when making ssh connections."""
    super(RemoteRunner, self).__init__(name, url, identityfile, extra_ssh_options)

    self._batchinputqueue = Queue.Queue()
    self._i = 0
    self._runner = _InnerRunner(self._batchinputqueue, nprocesses, self.gwurl)
    self._runner.start()

  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    @param jobs List of job instances as created by a JobFactory
    @return RemoteRunnerFuture a job future that supports .join() to block until completion"""

    self._gwgroup = execnet.Group()
    self._gwgroup.defaultspec = self.gwurl
    try:
      gw = self._gwgroup.makegateway()
      batchdir, localToRemotePathTuples,minimalJobs = self._prepareJobs(gw, jobs)
    finally:
      self._gwgroup.terminate(EXECNET_TERM_TIMEOUT)

    # ... finally create the batch job future.
    event = threading.Event()
    self._i += 1
    future = RemoteRunnerFuture('Batch %s' % self._i, event, minimalJobs,
      self.gwurl, self.remotePath, batchdir, localToRemotePathTuples)
    future.start()
    self._batchinputqueue.put(future)
    return future

  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    allowedkeywords = set(['nprocesses', 'type', 'remotehost'])
    cfgdict = dict(cfgitems)

    for k in cfgdict.iterkeys():
      if not k in allowedkeywords:
        raise ConfigException("Unknown keyword for Remote runner '%s'" % k)

    try:
      nprocesses = cfgdict['nprocesses']
    except KeyError:
      raise ConfigException("nprocesses configuration item not found")

    try:
      nprocesses = int(nprocesses)
    except ValueError:
      raise ConfigException("Could not convert nprocesses configuration item into an integer")

    try:
      remotehost = cfgdict['remotehost']
    except KeyError:
      raise ConfigException("remotehost configuration item not found")

    if not remotehost.startswith("ssh://"):
      raise ConfigException("remotehost configuration item must start with ssh://")

    username, host, port,  path = RemoteRunner._urlParse(remotehost)
    if not host:
      raise ConfigException("remotehost configuration item should be of form ssh://[username@]hostname/remote_path")

    # Attempt connection and check remote directory exists
    group = execnet.Group()
    try:
      gwurl, sshcfg = RemoteRunner.makeExecnetConnectionSpec(username, host, port)
      gw = group.makegateway(gwurl)

      # Check existence of remote directory
      channel = gw.remote_exec(_execnet._remoteCheck)
      channel.send(path)
      status = channel.receive()
      if not status:
        raise ConfigException("Remote directory does not exist or is not read/writable:'%s'" % path)

      channel.waitclose()
      if sshcfg:
        sshcfg.close()
    except execnet.gateway_bootstrap.HostNotFound:
      raise ConfigException("Couldn't connect to host: %s" % gwurl)
    finally:
        group.terminate(EXECNET_TERM_TIMEOUT)

    return RemoteRunner(runnerName, remotehost,nprocesses)
