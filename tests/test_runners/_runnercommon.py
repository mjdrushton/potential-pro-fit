import os
import shutil
import stat
import tempfile
import uuid
import time

import atsim.pro_fit._execnet as _execnet

from .. import common
from atsim import pro_fit

from pytest import fixture

import execnet

DIR = 0
FILE = 1

def _compareDir(path):
  actual = []
  for f in os.listdir(path):
    fullpath = os.path.join(path, f)
    mode = os.stat(fullpath).st_mode
    if stat.S_ISDIR(mode):
      m = DIR
    elif stat.S_ISREG(mode):
      m = FILE
    else:
      m = None
    actual.append((f, m))
  return actual

def runnertestjob(runfixture, jobid, expectstderr_stdout = False):
  """Test running a single job"""

  job_path = runfixture.jobs[jobid].path
  jfdir = os.path.join(job_path, 'job_files')
  rfdir = os.path.join(job_path, 'runner_files')

  expect = [ ('job_files', DIR),
             ('runner_files', DIR)]
  actual = _compareDir(job_path)

  # Check job_files directory contents
  expect = [ ('runjob', FILE),
             ('output', DIR)]
  actual = _compareDir(jfdir)
  assert sorted(expect) == sorted(actual)

  # Check output directory contents
  expect = [ ('runjob', FILE),
           ('STATUS', FILE),
          #  ('STDERR', FILE),
          #  ('STDOUT', FILE),
           ('runner_files_contents', FILE),
           ('output.res', FILE)]
  if expectstderr_stdout:
    expect.extend([('STDOUT', FILE), ('STDERR', FILE)])

  output_dir = os.path.join(jfdir, 'output')
  actual = _compareDir(output_dir)
  assert sorted(expect) == sorted(actual)

  # Check file contents
  status = open(os.path.join(output_dir, 'STATUS')).readline()[:-1]
  assert status == '0'

  common.logger.debug("output.res: %s" % open(os.path.join(output_dir, 'output.res')).read())
  assert runfixture.jobs[jobid].variables.id == jobid

  d = runfixture.jobfactory.evaluators[0](runfixture.jobs[jobid])
  assert jobid == d[0].meritValue

class FixtureObj(object):
  def __init__(self, tempd, remoted, jobfactory, jobs):
    self.tempd = tempd
    self.remoted = remoted
    self.jobfactory = jobfactory
    self.jobs = jobs

@fixture(scope="function")
def runfixture(tmpdir, request):
  remoted = None
  evaluator = common.MockEvaluator(lambda d: d['A'])

  # Create some jobs
  jobfactory = common.MockJobFactory('Runner', 'Test', [evaluator])
  jobs = []

  for i in range(12):
    variables = pro_fit.fittool.Variables([('A', i, True)])
    variables.id = i
    jd = tmpdir.join(str(i))
    jd.mkdir()
    jobs.append(
        jobfactory.createJob( jd.strpath, variables))
  jobs = jobs

  fixture = FixtureObj(tmpdir.strpath, remoted, jobfactory, jobs)
  return fixture

@fixture
def execnet_gw(request):
  # execnet.set_execmodel("gevent", "gevent")
  group = _execnet.Group()
  gw = group.makegateway()

  def finalizer():
    group.terminate(timeout=1.0)

  request.addfinalizer(finalizer)
  return gw

@fixture
def channel_id():
  return str(uuid.uuid4())

def create_dir_structure(tmpdir):
  # Create directory structure to download
  rpath = tmpdir.join("remote")
  names = ["One", "Two", "Three"]

  p = rpath
  for i,name in enumerate(names):
    p = p.join(str(i))
    for name in names:
      p.join(name).write(name, ensure = True)

  dpath =  os.path.join(rpath.strpath, "0", "1", "2", "Three")
  assert os.path.isfile(dpath)

  dpath = tmpdir.join('dest')
  dpath.mkdir()

from filecmp import dircmp
def cmpdirs(left, right):
  dcmp = dircmp(left, right)
  def docmp(dcmp):
    try:
      assert [] == dcmp.diff_files
      assert [] == dcmp.left_only
      assert [] == dcmp.right_only
    except AssertionError:
      print(dcmp.report())
      raise
    for subcmp in list(dcmp.subdirs.values()):
      docmp(subcmp)
  docmp(dcmp)


def _check_pid(channel):
  import subprocess
  for pid in channel:
    if pid is None:
      return
    returncode = subprocess.call(["/bin/ps", "-p", str(pid)])
    channel.send(returncode == 0)

class CheckPIDS(object):

  def __init__(self, gw):
    self.channel = gw.remote_exec(_check_pid)

  def checkpids(self, pids, status):
    for pid in pids:
      self.channel.send(pid)
      found = self.channel.receive()

      if status:
        if found == False:
          self.channel.send(None)
          self.channel.waitclose()
          assert False, "PID not found on remote host: %s" % pid
      else:
        if found == True:
          self.channel.send(None)
          self.channel.waitclose()
          assert False, "PID was found on remote host when it should no longer exist: %s" % pid

  def close(self):
    self.channel.send(None)
    self.channel.waitclose()


def isdir_remote(channel):
  import os
  for msg in channel:
    if msg is None:
      return

    channel.send(os.path.isdir(msg))


def mkrunjobs(gw, num, numSuffix = False, sleep = None):

  def mkrunjob(channel, num, numSuffix, sleep):
    import tempfile
    import os
    tmpdir = tempfile.mkdtemp()
    try:
      outpaths = []
      for i in range(num):
        nd = os.path.join(tmpdir, str(i))
        os.mkdir(nd)
        filename = os.path.join(nd, 'runjob')
        with open(filename, 'wb') as outfile:
          if not sleep is None:
            print("sleep %d" % sleep, file=outfile)

          if numSuffix:
            print("echo Hello%d > outfile" % i, file=outfile)
          else:
            print("echo Hello > outfile", file=outfile)
        outpaths.append(filename)
      channel.send(outpaths)
      rcv = channel.receive()
    finally:
      import shutil
      shutil.rmtree(tmpdir, ignore_errors = True)
  ch = gw.remote_exec(mkrunjob, num = num, numSuffix = numSuffix, sleep = sleep)
  runjobs = ch.receive()
  return ch, runjobs

def send_and_compare(ch, sendmsg, expect):
  pause = 0.5
  for i in range(10):
    ch.send(sendmsg)
    msg = ch.receive(2)
    try:
      assert msg == expect
      return
    except AssertionError:
      pass
    time.sleep(pause)
    pause *= 2.0
  assert msg == expect