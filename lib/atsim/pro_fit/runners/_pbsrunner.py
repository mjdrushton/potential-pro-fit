import execnet
import os
import collections
import threading
import posixpath
import logging
import uuid

import jinja2

from _remote_common import RemoteRunnerBase, _copyFilesFromRemote
import atsim.pro_fit._execnet as _execnet
from atsim.pro_fit._execnet import EXECNET_TERM_TIMEOUT

class PBSRunnerFuture(threading.Thread):
  """Joinable future object as returned by PBSRunner.runBatch()"""

  def __init__(self, name, execnetURL, remotePath, batchDir, localToRemotePathTuples, pbsJobId, pbsIdentify):
    """@param name Name future
    @param execnetURL URL used to create execnet gateway.
    @param remotePath Base directory for batches on remote server
    @param batchDir Name of batch directory for batch belonging to this future
    @param localToRemotePathTuples Pairs of localPath, remotePath tuples for each job
    @param pbsIdentify PBSIdentifyRecord giving the flavour of PBS we're working with"""
    threading.Thread.__init__(self)
    self._remotePath = remotePath
    self._batchDir = batchDir
    self._gwurl = execnetURL
    self._localToRemotePathTuples = localToRemotePathTuples
    self._pollWait = 5.0
    self._pbsJobId = pbsJobId
    self._pbsIdentify = pbsIdentify

  def run(self):
    # Monitor file completion
    import time
    group = _execnet.Group()
    group.defaultspec = self._gwurl
    gw = group.makegateway()

    everSeen = False
    try:
      channel = gw.remote_exec(_execnet._monitorRun)
      while True:
        channel.send(self._pbsJobId)
        jobStatus = channel.receive()
        if not jobStatus and everSeen:
          # Job not running anymore return.
          break

        if jobStatus and not everSeen:
          # Release the hold on the job
          everSeen = True


          qrls_channel = gw.remote_exec(self._qrls)
          qrls_channel.send(self._pbsJobId)
          retcode = qrls_channel.receive()
          qrls_channel.waitclose()

        time.sleep(self._pollWait)
      # Close the channel
      channel.send(None)
      channel.waitclose()

      # Copy files back
      _copyFilesFromRemote(gw, self._remotePath, self._batchDir, self._localToRemotePathTuples)
    finally:
      group.terminate(EXECNET_TERM_TIMEOUT)

  @property
  def _qrls(self):
    if self._pbsIdentify.flavour == "TORQUE":
      return _execnet._qrls_torque
    else:
      return _execnet._qrls_pbs

class PBSRunnerException(Exception):
  pass

class PBSRunner(object):
  """Runner that allows a remote PBS queuing system to be used to run jobs.

  SSH is used to communicate with server to submit jobs and copy files."""

  logger = logging.getLogger("atsim.pro_fit.runners.PBSRunner")

  def __init__(self, name, url, pbsinclude, identifyRecord, identityfile=None, extra_ssh_options = []):
    """Create PBSRunnner instance.

    @param name Name of runner
    @param url Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
    @param pbsinclude String that will be inserted at top of PBS submission script, this can be used to customise job requirements.
    @param identifyRecord PBSIdentifyRecord customising runner to different flavours of PBS.
    @param identityfile Path of a private key to be used with this runner's SSH transport. If None, the default's used by
                      the platform's ssh command are used.
    @param extra_ssh_options List of (key,value) tuples that are added to the ssh_config file used when making ssh connections."""
    super(PBSRunner, self).__init__(name, url, identityfile, extra_ssh_options)
    self.name = name
    self._i = 0
    self.pbsinclude = pbsinclude
    self._identifyRecord = identifyRecord

  def _createExtraFiles(self, tempdir, batchdir, localToRemotePathTuples):
    """Add submit.sh file to the temporary directory that will be used to create a
    PBS array job."""
    loader = jinja2.PackageLoader("atsim.pro_fit.runners", "templates")
    env = jinja2.Environment(loader = loader)
    submit_template = env.get_template("submit.sh")
    uuid_template = env.get_template("uuid")
    uuid_val = uuid.uuid4()

    templatevars = dict(
      submissionpath = posixpath.join(self.remotePath, batchdir),
      arrayFlag = self._identifyRecord.arrayFlag,
      arrayStart = 1,
      arrayEnd = len(localToRemotePathTuples),
      localToRemotePathTuples = localToRemotePathTuples,
      arrayIDVariable = self._identifyRecord.arrayIDVariable,
      uuid = uuid_val)

    wd = os.path.join(tempdir, batchdir)

    with open(os.path.join(wd, "submit.sh"), "wb") as submission:
      submission.write(submit_template.render(templatevars))

    with open(os.path.join(wd, "uuid"), "wb") as uuidfile:
      uuidfile.write(uuid_template.render(templatevars))

  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    @param jobs List of job instances as created by a JobFactory
    @return RemoteRunnerFuture a job future that supports .join() to block until completion"""
    # Copy job files and minimal jobs
    self.logger.debug("runBatch() called.")
    self._gwgroup = _execnet.Group()
    self._gwgroup.defaultspec = self.gwurl
    try:
      gw = self._gwgroup.makegateway()
      batchdir, localToRemotePathTuples,minimalJobs = self._prepareJobs(gw, jobs)
      # ... finally create the batch job future.
      self._i += 1
      channel = gw.remote_exec(_execnet._submitRun)
      channel.send(('submit', {'batchdir': posixpath.join(self.remotePath, batchdir)}))
      msg, msgdata = channel.receive()
      channel.waitclose()
      if msg != "submit_okay":
        raise PBSRunnerException("Job submission failed: %s" % msgdata)

      self.logger.debug("runBatch() submission okay. Received: (%s,%s)" % (msg,msgdata))

      pbsJobId = msgdata['pbsJobId']

      #Take leading number from job
      import re
      pbsJobId = re.match(r'^([0-9]+).*$', pbsJobId).groups()[0]
      self.logger.debug("runBatch() pbsJobId: %s" % pbsJobId)

      #Create future
      future = PBSRunnerFuture(self.name+str(self._i),
          self.gwurl,
          self.remotePath,
          batchdir,
          localToRemotePathTuples,
          pbsJobId,
          self._identifyRecord)
      future.start()
      return future
    finally:
      self._gwgroup.terminate(EXECNET_TERM_TIMEOUT)


  @staticmethod
  def createFromConfig(runnerName, fitRootPath, cfgitems):
    allowedkeywords = set(['type', 'remotehost', 'pbsinclude'])
    cfgdict = dict(cfgitems)

    for k in cfgdict.iterkeys():
      if not k in allowedkeywords:
        raise ConfigException("Unknown keyword for Remote runner '%s'" % k)

    try:
      remotehost = cfgdict['remotehost']
    except KeyError:
      raise ConfigException("remotehost configuration item not found")

    if not remotehost.startswith("ssh://"):
      raise ConfigException("remotehost configuration item must start with ssh://")

    username, host, port, path = PBSRunner._urlParse(remotehost)
    if not host:
      raise ConfigException("remotehost configuration item should be of form ssh://[username@]hostname/remote_path")

    # Attempt connection and check remote directory exists
    group = _execnet.Group()
    try:
      if username:
        gwurl = "ssh=%s@%s" % (username, host)
      else:
        gwurl = "ssh=%s" % host
      gw = group.makegateway(gwurl)
      channel = gw.remote_exec(_execnet._remoteCheckPBS)
      channel.send(path)
      status = channel.receive()

      if not status:
        raise ConfigException("Remote directory does not exist or is not read/writable:'%s'" % path)

      # Check that qselect can be run
      status = channel.receive()
      if not status.startswith('qselect okay'):
        raise ConfigException("Cannot run 'qselect' on remote host.")

      # Check that qsub can be run
      status = channel.receive()
      if not status.startswith('qsub okay'):
        raise ConfigException("Cannot run 'qsub' on remote host.")

      # Configure runner from the version string
      identifyRecord = pbsIdentify(status[9:])

      channel.waitclose()

    except execnet.gateway_bootstrap.HostNotFound:
      raise ConfigException("Couldn't connect to host: %s" % gwurl)
    finally:
        group.terminate(EXECNET_TERM_TIMEOUT)

    pbsinclude = cfgdict.get('pbsinclude', None)
    if pbsinclude:
      try:
        pbsinclude = open(pbsinclude, 'rb').read()
      except IOError:
        raise ConfigException("Could not open file specified by 'pbsinclude' directive: %s" % pbsinclude)

    return PBSRunner(runnerName, remotehost, pbsinclude, identifyRecord)
