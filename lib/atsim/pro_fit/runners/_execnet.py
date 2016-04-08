"""Functions designed to be run remotely through execnet"""

EXECNET_TERM_TIMEOUT=10


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
  channel.send("Done")

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

def _makeTemporaryDirectory(channel):
  """Function to be executed remotely and create temporary directory"""
  import tempfile
  tmpdir = tempfile.mkdtemp()
  channel.send(tmpdir)

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


def _qrls_pbs(channel):
  import subprocess
  import re
  import os
  jobId = channel.receive()
  if jobId == None:
    return

  p = subprocess.Popen(["qrls", "%s[]" % jobId], close_fds=True)
  p.wait()
  channel.send(p.returncode)

def _qrls_torque(channel):
  import subprocess
  import os
  import re
  jobId = channel.receive()
  if jobId == None:
    return

  arrayJobIDs = []

  p = subprocess.Popen(["qselect", "-h", "u"], stdout = subprocess.PIPE, close_fds=True)
  output, err = p.communicate()

  regex = re.compile('^(%s)[^0-9].*$' % jobId)

  for line in [l for l in output.split(os.linesep) if l]:
    if regex.match(line):
      arrayJobIDs.append(line)

  args = ["qrls"]
  args.extend(arrayJobIDs)

  p = subprocess.Popen(args, close_fds=True)
  p.wait()
  channel.send(p.returncode)

