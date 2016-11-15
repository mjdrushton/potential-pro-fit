
EXECNET_TERM_TIMEOUT=10

import logging
import tempfile
import execnet
import os


logger = logging.getLogger("atsim.pro_fit._execnet")

def Group(*args, **kwargs):
  group = execnet.Group(*args, **kwargs)
  group.set_execmodel("gevent", "thread")
  return group

def urlParse(url):
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

  local_log = logger.getChild("makeExecnetConnectionSpec")
  local_log.debug("Execnet url: '%s' for username = '%s', host = '%s', port= '%s', identityfile = '%s'" % (gwurl, username, host, port, identityfile))

  sshcfg.flush()
  sshcfg.seek(0)
  for line in sshcfg:
    local_log.debug("ssh_config contents: %s" % line)

  xspec = execnet.XSpec(gwurl)
  xspec.ssh_config = sshcfg.name

  return xspec, sshcfg


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

def _makeTemporaryDirectory(channel):
  """Function to be executed remotely and create temporary directory"""
  import tempfile
  tmpdir = tempfile.mkdtemp()
  channel.send(tmpdir)

# def _submitRun(channel):
#   import subprocess
#   import os
#   msgtype, msgdata = channel.receive()
#   batchdir = msgdata['batchdir']
#   oldchdir = os.getcwd()
#   try:
#     os.chdir(batchdir)
#     p = subprocess.Popen(['qsub', 'submit.sh'], stdout = subprocess.PIPE, close_fds = True)
#     stdout, stderr = p.communicate()
#     pbsJobId = stdout.strip()
#     channel.send( ('submit_okay', dict(pbsJobId  = pbsJobId)) )
#     channel.send(None)
#   finally:
#     os.chdir(oldchdir)

# def _qrls_pbs(channel):
#   import subprocess
#   import re
#   import os
#   jobId = channel.receive()
#   if jobId == None:
#     return

#   p = subprocess.Popen(["qrls", "%s[]" % jobId], close_fds=True)
#   p.wait()
#   channel.send(p.returncode)

# def _qrls_torque(channel):
#   import subprocess
#   import os
#   import re
#   jobId = channel.receive()
#   if jobId == None:
#     return

#   arrayJobIDs = []

#   p = subprocess.Popen(["qselect", "-h", "u"], stdout = subprocess.PIPE, close_fds=True)
#   output, err = p.communicate()

#   regex = re.compile('^(%s)[^0-9].*$' % jobId)

#   for line in [l for l in output.split(os.linesep) if l]:
#     if regex.match(line):
#       arrayJobIDs.append(line)

#   args = ["qrls"]
#   args.extend(arrayJobIDs)

#   p = subprocess.Popen(args, close_fds=True)
#   p.wait()
#   channel.send(p.returncode)

