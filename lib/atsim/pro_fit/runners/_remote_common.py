import tarfile
import StringIO
import collections
import tempfile
import posixpath
import os
import shutil
import logging

import _execnet
import execnet

def _redistributeFiles(tempdir, localToRemotePathTuples):
  """Move job/output directories to correct locations"""
  for t in localToRemotePathTuples:
    outputDir = os.path.join(tempdir, t.remotePath, "job_files", "output")
    localDir = os.path.join(t.localPath, "job_files", "output")
    shutil.copytree(outputDir, localDir)


def _copyFilesFromRemote(gw, remotePath, batchDir, localToRemotePathTuples):
  tempdir = tempfile.mkdtemp()
  try:
    channel = gw.remote_exec(_execnet._tarGet)
    # Tell channel where the batch directory is
    rpath = posixpath.join(remotePath, batchDir)
    channel.send(rpath)
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


class RemoteRunnerBase(object):

  _LocalToRemotePathTuple = collections.namedtuple('_LocalToRemotePathTuple', ['localPath', 'remotePath'])
  _MinimalJob = collections.namedtuple('_MinimalJob', ['path'])

  def __init__(self, name, url, identityfile, extra_ssh_options):
    """Args:
          name (string): name of this runner.
          url  (string): ssh:// url.
          identifyfile (string): Path to ssh identity file to be used for connection or None
          extra_ssh_options (list): List of (key,value) tuples that are added to the ssh_config file used when making ssh connections. """
    self.name = name
    self.username, self.hostname, self.port, self.remotePath = self._urlParse(url)

    # Create a common url used to create execnet gateways
    self.gwurl, sshcfgfile = RemoteRunnerBase.makeExecnetConnectionSpec(self.username, self.hostname, self.port, identityfile, extra_ssh_options)

    if not self.remotePath:
      # Create an appropriate remote path.
      self._createTemporaryRemoteDirectory()

    if sshcfgfile:
      self._sshcfgfile = sshcfgfile

      # If _remoted_is_temp is True the runner needs to clean it up on termination.
      self._remoted_is_temp = True
    else:
      #... if it's False then don't delete the remote directory.
      self._remoted_is_temp = False


  @staticmethod
  def makeExecnetConnectionSpec(username, host, port, identityfile = None, extraoptions = []):
    gwurl = "ssh="
    sshcfg = tempfile.NamedTemporaryFile(mode="w+")

    if port:
      sshcfg.write("Port %s\n" % port)

    if username:
      gwurl += "%s@%s" % (username, host)
    else:
      gwurl += host

    if identityfile:
      identifyfile = os.path.abspath(identityfile)

      if not os.path.isfile(identityfile):
        # Check that the identityfile can be opened
        with open(identityfile, 'r') as infile:
          pass

      sshcfg.write("IdentityFile %s\n" % identityfile)

    for k,v in extraoptions:
      sshcfg.write("%s %s \n" % (k,v))

    logger = logging.getLogger("atsim.pro_fit.runners.RemoteRunnerBase.makeExecnetConnectionSpec")
    logger.debug("Execnet url: '%s' for username = '%s', host = '%s', port= '%s', identityfile = '%s'" % (gwurl, username, host, port, identityfile))

    sshcfg.flush()
    sshcfg.seek(0)
    for line in sshcfg:
      logger.debug("ssh_config contents: %s" % line)

    xspec = execnet.XSpec(gwurl)
    xspec.ssh_config = sshcfg.name

    return xspec, sshcfg

  def _createTemporaryRemoteDirectory(self):
    """Makes a remote call to tempfile.mkdtemp() and sets
      * self.remoted to the name of the created directory.
      * self._remoted_is_temp is set to True to support correct cleanup behaviour.
    """
    from _execnet import _makeTemporaryDirectory

    gw = execnet.makegateway(self.gwurl)
    channel = gw.remote_exec(_makeTemporaryDirectory)
    tmpdir = channel.receive()
    self.logger.getChild("_createTemporaryRemoteDirectory").debug("Remote temporary directory: %s", tmpdir)
    self.remotePath = tmpdir


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
    override to add extra files to batch directory before they are uploaded."""
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
      channel = gw.remote_exec(_execnet._tarPut)

      # Get tar receive to change directory
      channel.send(self.remotePath)
      import tarfile
      tfilename = os.path.join(tempdir,'batch.tar')
      tar = tarfile.open(tfilename, 'w')
      tar.add(os.path.join(tempdir, batchdir), arcname = batchdir, recursive = True)
      tar.close()

      with open(tfilename, 'rb') as tfile:
        channel.send(tfile.read())

      status = channel.receive()
      if  status != "Done":
        raise Exception("Transfer of files to remote unsuccessful")

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
    port = parsed.port
    if path.startswith("//"):
      path = path[1:]

    return username, host, port, path

