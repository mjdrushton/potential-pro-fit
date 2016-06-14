"""Tests for atsim.pro_fit.runners.RemoteRunner"""

import logging
import os
import pytest
import shutil
import stat
import tempfile
import time
import posixpath

import py.path

import execnet
from assertpy import assert_that,fail

from atsim import pro_fit

from _runnercommon import runfixture, DIR, FILE, runnertestjob
from .. import common
from ..testutil import vagrant_basic

def _createRunner(runfixture, vagrantbox, ncpu=1):
  username = vagrantbox.user()
  hostname = vagrantbox.hostname()
  port = vagrantbox.port()
  keyfilename = vagrantbox.keyfile()

  extraoptions = [("StrictHostKeyChecking","no")]

  if runfixture.remoted:
    runner = pro_fit.runners.RemoteRunner('RemoteRunner', "ssh://%s@%s:%s/%s" % (username, hostname, port, runfixture.remoted),
      ncpu,
      identityfile = keyfilename,
      extra_ssh_options = extraoptions)
  else:
    runner = pro_fit.runners.RemoteRunner('RemoteRunner', "ssh://%s@%s:%s" % (username, hostname, port),
      ncpu,
      identityfile = keyfilename,
      extra_ssh_options = extraoptions)

  return runner

def _runBatch(runner, jobs):
  return runner.runBatch(jobs)

def testUrlParse():
  """Test parsing of host directory string"""
  from atsim.pro_fit.runners._execnet import urlParse
  username, host, port, path = urlParse("ssh://username@localhost/remote/path")
  assert username == 'username'
  assert host == 'localhost'
  assert path == "/remote/path"
  assert port is None

  username, host, port, path = urlParse("ssh://localhost/remote/path")
  assert username == ''
  assert host == 'localhost'
  assert path == "/remote/path"
  assert port is None

  username, host, port, path = urlParse("ssh://localhost:2222/remote/path")
  assert username == ''
  assert host == 'localhost'
  assert path == "/remote/path"
  assert port == 2222

  username, host, port, path = urlParse("ssh://username@localhost:2222")
  assert username == 'username'
  assert host == 'localhost'
  assert path == ''
  assert port == 2222

def testSingle(runfixture, vagrant_basic):
  runner = _createRunner(runfixture,vagrant_basic, 1)
  _runBatch(runner, [runfixture.jobs[0]]).join()
  runnertestjob(runfixture, 0)

def testAllInSingleBatch(runfixture, vagrant_basic):
  runner = _createRunner(runfixture, vagrant_basic, 3)
  _runBatch(runner, runfixture.jobs).join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id)

def testAllInMultipleBatch(runfixture, vagrant_basic):
  runner = _createRunner(runfixture, vagrant_basic, 3)

  f1 = _runBatch(runner, runfixture.jobs[:6])
  f2 = _runBatch(runner, runfixture.jobs[6:])
  f2.join()
  f1.join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id)

def testTempDirectoryCleanup(runfixture, vagrant_basic):
  runfixture.remoted = ""
  runner = _createRunner(runfixture,vagrant_basic, 1)
  assert_that(runner.remotePath).is_not_none()
  assert_that(runner._remoted_is_temp).is_true()

  # Check that a temporary remote path has been created
  # on the execution host.
  from atsim.pro_fit.runners._execnet import _remoteCheck

  group = execnet.Group()
  gw = group.makegateway(runner.gwurl)
  try:
    channel = gw.remote_exec(_remoteCheck)
    path = runner.remotePath
    channel.send(runner.remotePath)
    status = channel.receive()
    assert_that(status).is_true()
    # msg="Remote directory does not exist or is not read/writable:'%s'" % path)
    channel.waitclose()

  finally:
    group.terminate(10)

  # Need to add the test for cleanup.
  assert(False)

from atsim.pro_fit.jobfactories import Job

def _mklongjob(tmpdir):
  import itertools
  logger = logging.getLogger("test_remoterunner._mklongjob")

  jf = common.MockJobFactory("Remote", "Job", [])

  for i in itertools.count():
    tempd = tmpdir.mkdir(str(i))
    logger.debug("Created longjob directory: %s", tempd)
    jfdir = tempd.mkdir("job_files")
    rfdir = tempd.mkdir("runner_files")
    runjob = jfdir.join('runjob').open('w')

    runjob.write("""#! /bin/bash
echo $$ > JOB_PID
sleep 1200
""")

    # Create Job instance
    job = Job(jf, str(tempd), [])

    yield job

def _mkexecnetgw(vagrant_basic):
  with py.path.local(vagrant_basic.root).as_cwd():
    gw = execnet.makegateway("vagrant_ssh=default")
  return gw

def _remote_is_file(channel):
  import os

  for path in channel:
    if path is None:
      return
    channel.send(os.path.exists(path))

def _read_pids(channel):
  for path in channel:
    if path is None:
      return
    channel.send(open(path).read())

def _check_pid(channel):
  import subprocess
  for pid in channel:
    if pid is None:
      return
    returncode = subprocess.call(["/bin/ps", "-p", str(pid)])
    channel.send(returncode == 0)

def testTerminate(tmpdir, runfixture, vagrant_basic):
  """Test runner future's .terminate() method."""

  # TODO: Write a test for when jobs run serially (i.e. one job at a time)

  logger = logging.getLogger("test_remoterunner.testTerminate")
  # import pdb;pdb.set_trace()
  # Create a long running job.
  jobiter = _mklongjob(tmpdir)
  j1 = jobiter.next()
  assert(os.path.exists(str(tmpdir.join('0', 'job_files', 'runjob'))))
  assert(os.path.exists(str(tmpdir.join('0', 'runner_files'))))

  batch1 = [jobiter.next() for i in xrange(4)]
  batch2 = [jobiter.next() for i in xrange(4)]

  # Run the jobs
  runner = _createRunner(runfixture, vagrant_basic, 4)

  b1_future = _runBatch(runner, batch1)

  # Check that the remote job has created the directories it needs.
  gw = _mkexecnetgw(vagrant_basic)


  # Check for the PID_FILES in
  def checkpaths(channel, f):
    batchpath = posixpath.join(f._remotePath, f._batchDir)
    # Check five times with a sleep of 1s between attempts
    # if after that remote files don't appear, then they never will
    for i in xrange(5):
      # Check paths
      count = 0
      pids = []
      for localPath, remotePath in f._localToRemotePathTuples:
        fullpath = posixpath.join(batchpath, remotePath, "rundir", "JOB_PID")
        channel.send(fullpath)
        found = channel.receive()
        logger.debug("checkpaths: '%s' found = '%s'" % (fullpath, found))
        count += found

      logger.debug("checkpaths count on %d attempt: %d (expecting %d)", i, count, len(f._localToRemotePathTuples))
      time.sleep(1)
      if count == len(f._localToRemotePathTuples):
        return
    fail("Remote paths not found")

  channel = gw.remote_exec(_remote_is_file)
  checkpaths(channel, b1_future)
  channel.send(None)
  channel.waitclose()

  # ... job should write PID to file.
  def getpids(channel, f):
    paths = []
    batchpath = posixpath.join(f._remotePath, f._batchDir)
    for localPath, remotePath in f._localToRemotePathTuples:
      fullpath = posixpath.join(batchpath, remotePath, "rundir", "JOB_PID")
      channel.send(fullpath)
      paths.append(channel.receive()[:-1])
    return paths

  channel = gw.remote_exec(_read_pids)
  b1_pids = getpids(channel, b1_future)
  channel.send(None)
  channel.waitclose()

  def checkpids(channel, pids, status):
    for pid in pids:
      channel.send(pid)
      found = channel.receive()
      if found != status:
        channel.send(None)
        channel.waitclose()
        fail("PID not found on remote host: %d" % pid)

  channel = gw.remote_exec(_check_pid)
  checkpids(channel, b1_pids, True)

  # Call terminate - make sure that temporary directories are cleaned up.
  # Ensure job PID has gone.
  b1_term = b1_future.terminate()
  b1_term.join()

  checkpids(channel, b1_pids, False)
  channel.send(None)
  channel.waitclose()


  #TODO: Check file cleanup here.
  b2_future = _runBatch(runner, batch2)
  channel = gw.remote_exec(_remote_is_file)
  checkpaths(channel, b2_future)
  channel.send(None)
  channel.waitclose()

  channel = gw.remote_exec(_read_pids)
  b2_pids = getpids(channel, b2_future)
  channel.send(None)
  channel.waitclose()

  channel = gw.remote_exec(_check_pid)
  checkpids(channel, b2_pids, True)

  b2_term = b2_future.terminate()
  b2_term.join()
  checkpids(channel, b2_pids, False)


  # TODO: Check file cleanup here.

  # Ensure that future's terminated flag is set.

  fail("Not implemented")

def testClose():
  """Test runner's .close() method."""
  assert(False)
