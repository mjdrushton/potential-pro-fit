
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
    local_log.debug("ssh_config contents: %s" % line[:-1])

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


def _makeTemporaryDirectory(channel):
  """Function to be executed remotely and create temporary directory"""
  import tempfile
  tmpdir = tempfile.mkdtemp()
  channel.send(tmpdir)
