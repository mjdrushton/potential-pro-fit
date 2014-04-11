
from atomsscripts.fitting.fittool import ConfigException

import execnet
import StringIO
import tarfile
import os
import shutil
import collections
import threading
import tempfile
import posixpath
import Queue
import logging

from _localrunner import LocalRunnerFuture
from _inner import _InnerRunner

EXECNET_TERM_TIMEOUT=10

def _copyFilesFromRemote(gw, remotePath, batchDir, localToRemotePathTuples):
  tempdir = tempfile.mkdtemp()
  try:
    channel = gw.remote_exec(_tarGet)
    # Tell channel where the batch directory is
    channel.send(posixpath.join(remotePath, batchDir))
    content = channel.receive()
    channel.waitclose()
    sio = StringIO.StringIO(content)
    tar = tarfile.open(fileobj=sio)
    tar.extractall(path=tempdir)
    tar.close()
    tar = None
    sio = None
    content = None
    _redistributeFiles(tempdir, localToRemotePathTuples)
  finally:
    shutil.rmtree(tempdir, ignore_errors = True)

def _redistributeFiles(tempdir, localToRemotePathTuples):
  """Move job/output directories to correct locations"""
  for t in localToRemotePathTuples:
    outputDir = os.path.join(tempdir, t.remotePath, "output")
    localDir = os.path.join(t.localPath, "output")
    shutil.copytree(outputDir, localDir)

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


class PBSRunnerFuture(threading.Thread):
  """Joinable future object as returned by PBSRunner.runBatch()"""

  def __init__(self, name, execnetURL, remotePath, batchDir, localToRemotePathTuples, pbsJobId):
    """@param name Name future
    @param execnetURL URL used to create execnet gateway.
    @param remotePath Base directory for batches on remote server
    @param batchDir Name of batch directory for batch belonging to this future
    @param localToRemotePathTuples Pairs of localPath, remotePath tuples for each job"""
    threading.Thread.__init__(self)
    self._remotePath = remotePath
    self._batchDir = batchDir
    self._gwurl = execnetURL
    self._localToRemotePathTuples = localToRemotePathTuples
    self._pollWait = 5.0
    self._pbsJobId = pbsJobId

  def run(self):
    # Monitor file completion
    import time
    group = execnet.Group()
    group.defaultspec = self._gwurl
    gw = group.makegateway()

    everSeen = False
    everSeenCount = 0
    try:
      channel = gw.remote_exec(_monitorRun)
      while True:
        channel.send(self._pbsJobId)
        jobStatus = channel.receive()
        if not jobStatus and everSeen:
          # Job not running anymore return.
          break
        elif not jobStatus and not everSeen:
          # This is an attempt to fix a problem with certain PBS set-ups,
          # although qsub returns a job ID, there seems to be a small delay
          # before qselect will show the job to be running.
          # In these cases _monitorRun would indicate that the job has finished
          # before it had actually started. Now, a everSeen is set to True
          # if the job is seen. The job then terminates only when everSeen
          # is set and jobStatus is False. For times when the job actually
          # has completed quickly and everSeen never gets set, we wait for
          # ten polling intervals and then just assume it has finished.
          everSeenCount += 1
          if everSeenCount >= 10:
            break
        else:
          everSeen = True
          time.sleep(self._pollWait)
      # Close the channel
      channel.send(None)
      channel.waitclose()

      # Copy files back
      _copyFilesFromRemote(gw, self._remotePath, self._batchDir, self._localToRemotePathTuples)
    finally:
      group.terminate(EXECNET_TERM_TIMEOUT)


# Execnet tar runner for uploading files
def _tarPut(channel):
  import tarfile
  import os
  import StringIO
  destdir = channel.receive()
  os.chdir(destdir)
  sio = StringIO.StringIO(channel.receive())
  sio.seek(0)
  tar = tarfile.open(fileobj = sio)
  tar.extractall()

# Execnet tar runner for downloading files
def _tarGet(channel):
  import tarfile
  import os
  import shutil
  # Change directory
  destdir = channel.receive()
  os.chdir(destdir)
  # Create tar
  filelist = os.listdir(".")
  tar = tarfile.open("batch.tar", "w")
  for f in filelist:
    tar.add(f)
  tar.close()

  # Send tar contents back along channel
  with open("batch.tar", "rb") as tfile:
    channel.send(tfile.read())

  # Perform file cleanup
  shutil.rmtree(destdir, ignore_errors = True)

# Check connection to remote server and existence of remote path
def _remoteCheck(channel):
  import os
  remoteDir = channel.receive()
  status = os.path.isdir(remoteDir)
  permissions = os.access(remoteDir, os.W_OK | os.X_OK | os.R_OK)
  channel.send(status and permissions)

def _remoteCheckPBS(channel):
  import os
  remoteDir = channel.receive()
  status = os.path.isdir(remoteDir)
  permissions = os.access(remoteDir, os.W_OK | os.X_OK | os.R_OK)
  channel.send(status and permissions)

  # Attempt to run qstat
  import subprocess
  try:
    p = subprocess.Popen(["qselect"], stdout = subprocess.PIPE, close_fds=True)
    output, err = p.communicate()
    channel.send("qselect okay:")
  except OSError:
    channel.send("qselect bad")

  try:
    p = subprocess.Popen(["qsub", "--version"],
      stdout = subprocess.PIPE,
      stderr = subprocess.PIPE,
      close_fds=True)
    output, err = p.communicate()
    output = output.strip()
    err = err.strip()
    if err:
      sstring = err
    else:
      sstring = output
    channel.send("qsub okay:"+sstring)
  except OSError:
    channel.send("qsub bad")

# Check file existence on remote server
def _monitorRun(channel):
  import subprocess
  import re
  import os
  for jobId in channel:
    if jobId == None:
      return

    regex = re.compile('^(%s)[^0-9].*$' % jobId)

    p = subprocess.Popen(["qselect"], stdout = subprocess.PIPE, close_fds=True)
    output, err = p.communicate()

    found = False
    for line in output.split(os.linesep):
      if regex.match(line):
        found = True
        break
    channel.send(found)

class RemoteRunnerBase(object):

  _LocalToRemotePathTuple = collections.namedtuple('_LocalToRemotePathTuple', ['localPath', 'remotePath'])
  _MinimalJob = collections.namedtuple('_MinimalJob', ['path'])

  def _makeLocalToRemotePathTuples(self, jobs):
    import tempfile
    batchdir = tempfile.mktemp(dir='')
    tuples = []
    for i,job in enumerate(jobs):
      remoteJobPath = str(i)
      localJobPath = job.path
      tuples.append(self._LocalToRemotePathTuple(localPath = localJobPath, remotePath = remoteJobPath))
    return batchdir, tuples

  def _makeMinimalJobs(self, batchdir, localToRemotePathTuples):
    minjobs = []
    for pathTuple in localToRemotePathTuples:
      minjobs.append(self._MinimalJob(posixpath.join(self.remotePath, batchdir, pathTuple.remotePath)))
    return minjobs

  def _createExtraFiles(self, tempdir, batchdir, localToRemotePathTuples):
    """Method that is called just before file copying,
    override to add extra files to batch directory before they are uplaoaded."""
    pass

  def _prepareJobs(self, gw, jobs):
    # Copy files to remote directory
    # ... first create a local job to remote job mapping
    batchdir, localToRemotePathTuples = self._makeLocalToRemotePathTuples(jobs)
    # ... now perform file copying
    self._copyFilesToRemote(gw, batchdir, localToRemotePathTuples)
    # ... create Job records with the remote directory to be passed to _InnerRunner queue.
    minimalJobs = self._makeMinimalJobs( batchdir, localToRemotePathTuples)
    return (batchdir, localToRemotePathTuples, minimalJobs)

  def _copyFilesToRemote(self, gw, batchdir, localToRemotePathTuples):
    """Copies batch to remote machine"""
    tempdir = tempfile.mkdtemp()
    try:
      os.mkdir(os.path.join(tempdir, batchdir))
      for t in localToRemotePathTuples:
        shutil.copytree(t.localPath,
          os.path.join(tempdir, batchdir, t.remotePath))

      # Create any additional files before upload
      self._createExtraFiles(tempdir, batchdir, localToRemotePathTuples)

      # Pipe tar file to execnet
      channel = gw.remote_exec(_tarPut)

      # Get tar receive to change directory
      channel.send(self.remotePath)
      import tarfile
      tfilename = os.path.join(tempdir,'batch.tar')
      tar = tarfile.open(tfilename, 'w')
      tar.add(os.path.join(tempdir, batchdir), arcname = batchdir, recursive = True)
      tar.close()

      with open(tfilename, 'rb') as tfile:
        channel.send(tfile.read())
      channel.waitclose()
    finally:
      shutil.rmtree(tempdir, ignore_errors = True)

  @staticmethod
  def _urlParse(url):
    """Parse url into username, host, path components

    @param url  URL of form [username@]host/remote_path
    @return Tuple (username, host, path). If any component not specified then it will be None."""
    import urlparse
    parsed = urlparse.urlparse(url)
    username = parsed.username
    if username == None:
      username = ''
    host = parsed.hostname
    path = parsed.path
    if path.startswith("//"):
      path = path[1:]

    return username, host, path

class RemoteRunner(RemoteRunnerBase):
  """Runner that uses SSH to run jobs in parallel on a remote machine"""

  def __init__(self, name, url, nprocesses):
    """@param name Name of this runner.
       @param url Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
       @param nprocesses Number of processes that can be run in parallel by this runner"""
    super(RemoteRunner, self).__init__()
    self.name = name
    self.username, self.hostname, self.remotePath = self._urlParse(url)
    # Create a common url used to create execnet gateways
    if self.username:
      gwurl = "ssh=%s@%s" % (self.username, self.hostname)
    else:
      gwurl = "ssh=%s" % (self.hostname,)

    self.gwurl = gwurl

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

    username, host, path = RemoteRunner._urlParse(remotehost)
    if not host:
      raise ConfigException("remotehost configuration item should be of form ssh://[username@]hostname/remote_path")

    # Attempt connection and check remote directory exists
    group = execnet.Group()
    try:
      if username:
        gwurl = "ssh=%s@%s" % (username, host)
      else:
        gwurl = "ssh=%s" % host
      gw = group.makegateway(gwurl)

      # Check existence of remote directory
      channel = gw.remote_exec(_remoteCheck)
      channel.send(path)
      status = channel.receive()
      if not status:
        raise ConfigException("Remote directory does not exist or is not read/writable:'%s'" % path)


      channel.waitclose()
    except execnet.gateway_bootstrap.HostNotFound:
      raise ConfigException("Couldn't connect to host: %s" % gwurl)
    finally:
        group.terminate(EXECNET_TERM_TIMEOUT)

    return RemoteRunner(runnerName, remotehost,nprocesses)

class PBSRunnerException(Exception):
  pass

def _submitRun(channel):
  import subprocess
  import os
  msgtype, msgdata = channel.receive()
  batchdir = msgdata['batchdir']
  oldchdir = os.getcwd()
  try:
    os.chdir(batchdir)
    p = subprocess.Popen(['qsub', 'submit.sh'], stdout = subprocess.PIPE, close_fds = True)
    stdout, stderr = p.communicate()
    pbsJobId = stdout.strip()
    channel.send( ('submit_okay', dict(pbsJobId  = pbsJobId)) )
    channel.send(None)
  finally:
    os.chdir(oldchdir)


PBSIdentifyRecord = collections.namedtuple("PBSIdentifyRecord", ["arrayFlag", "arrayIDVariable"])

def pbsIdentify(versionString):
  """Given output of qstat --version, return a record containing fields used to configure
  PBSRunner for the version of PBS being used.

  Record has following fields:
    arrayFlag - The qsub flag used to specify array job rangs.
    arrayIDVariable - Environment variable name provided to submission script to identify ID of current array sub-job.

  @param versionString String as returned by qstat --versionString
  @return Field of the form described above"""
  logger = logging.getLogger("atomsscripts.fitting.runners.PBSRunner.pbsIdentify")
  import re
  if re.search("PBSPro", versionString):
    #PBS Pro
    logger.info("Identified PBS as: PBSPro")
    record =  PBSIdentifyRecord(
      arrayFlag = "-J",
      arrayIDVariable = "PBS_ARRAY_INDEX")
  else:
    #TORQUE
    logger.info("Identified PBS as: TORQUE")
    record = PBSIdentifyRecord(
      arrayFlag = "-t",
      arrayIDVariable = "PBS_ARRAYID")

  logger.debug("pbsIdentify record: %s" % str(record))
  return record


class PBSRunner(RemoteRunnerBase):
  """Runner that allows a remote PBS queuing system to be used to run jobs.

  SSH is used to communicate with server to submit jobs and copy files."""

  _logger = logging.getLogger("atomsscripts.fitting.runners.PBSRunner")

  def __init__(self, name, url, pbsinclude, identifyRecord):
    """Create PBSRunnner instance.

    @param name Name of runner
    @param url Host and remote directory of remote host where jobs should be run in the form ssh://[username@]host/remote_path
    @param pbsinclude String that will be inserted at top of PBS submission script, this can be used to customise job requirements.
    @param identifyRecord PBSIdentifyRecord customising runner to different flavours of PBS"""
    super(PBSRunner, self).__init__()

    self.name = name
    self._i = 0
    self.username, self.hostname, self.remotePath = self._urlParse(url)
    # Create a common url used to create execnet gateways
    if self.username:
      gwurl = "ssh=%s@%s" % (self.username, self.hostname)
    else:
      gwurl = "ssh=%s" % (self.hostname,)
    self.gwurl = gwurl
    self.pbsinclude = pbsinclude
    self._identifyRecord = identifyRecord

  def _createExtraFiles(self, tempdir, batchdir, localToRemotePathTuples):
    """Add submit.sh file to the temporary directory that will be used to create a
    PBS array job."""

    with open(os.path.join(tempdir, batchdir, "submit.sh"), "wb") as submission:
      submissionpath = posixpath.join(self.remotePath, batchdir)
      print >>submission, "#! /bin/bash"
      print >>submission, "#PBS -o %s/job.out" % submissionpath
      print >>submission, "#PBS -e %s/job.err" % submissionpath
      print >>submission, "#PBS -S /bin/bash"

      # Write the number of array jobs
      if len(localToRemotePathTuples) > 1:
        print >>submission, "#PBS %s %d-%d" % (self._identifyRecord.arrayFlag, 1, len(localToRemotePathTuples))

      if self.pbsinclude:
        print >>submission, self.pbsinclude

      if len(localToRemotePathTuples) == 1:
        #PBS Pro takes issue with array jobs containing a single job, therefore...
        print >>submission, "%s=1" % self._identifyRecord.arrayIDVariable

      for (i,lrt) in enumerate(localToRemotePathTuples):
        i = i+1
        r = posixpath.join(submissionpath, lrt.remotePath)
        print >>submission, 'JOBS[%d]="%s"' % (i,r)
        print >>submission, ""

      print >>submission, 'export JOB_DIR="${JOBS[%s]}"' % (self._identifyRecord.arrayIDVariable,)
      print >>submission, 'cd "$TMPDIR"'
      print >>submission, 'cp -r "$JOB_DIR/"* "$PWD"'
      print >>submission, 'chmod u+x runjob'
      print >>submission, './runjob'
      print >>submission, 'echo $? > STATUS'
      print >>submission, 'mkdir "$JOB_DIR/output"'
      print >>submission, 'cp -r * "$JOB_DIR/output"'

  def runBatch(self, jobs):
    """Run job batch and return a job future that can be joined.

    @param jobs List of job instances as created by a JobFactory
    @return RemoteRunnerFuture a job future that supports .join() to block until completion"""
    # Copy job files and minimal jobs
    self._logger.debug("runBatch() called.")
    self._gwgroup = execnet.Group()
    self._gwgroup.defaultspec = self.gwurl
    try:
      gw = self._gwgroup.makegateway()
      batchdir, localToRemotePathTuples,minimalJobs = self._prepareJobs(gw, jobs)
      # ... finally create the batch job future.
      self._i += 1
      channel = gw.remote_exec(_submitRun)
      channel.send(('submit', {'batchdir': posixpath.join(self.remotePath, batchdir)}))
      msg, msgdata = channel.receive()
      channel.waitclose()
      if msg != "submit_okay":
        raise PBSRunnerException("Job submission failed: %s" % msgdata)

      self._logger.debug("runBatch() submission okay. Received: (%s,%s)" % (msg,msgdata))

      pbsJobId = msgdata['pbsJobId']

      #Take leading number from job
      import re
      pbsJobId = re.match(r'^([0-9]+).*$', pbsJobId).groups()[0]
      self._logger.debug("runBatch() pbsJobId: %s" % pbsJobId)

      #Create future
      future = PBSRunnerFuture(self.name+str(self._i), self.gwurl, self.remotePath, batchdir, localToRemotePathTuples, pbsJobId)
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

    username, host, path = PBSRunner._urlParse(remotehost)
    if not host:
      raise ConfigException("remotehost configuration item should be of form ssh://[username@]hostname/remote_path")

    # Attempt connection and check remote directory exists
    group = execnet.Group()
    try:
      if username:
        gwurl = "ssh=%s@%s" % (username, host)
      else:
        gwurl = "ssh=%s" % host
      gw = group.makegateway(gwurl)
      channel = gw.remote_exec(_remoteCheckPBS)
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
