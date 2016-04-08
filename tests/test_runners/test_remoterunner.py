"""Tests for atsim.pro_fit.runners.RemoteRunner"""

import execnet
import os
import pytest
import shutil
import tempfile
import stat
from assertpy import assert_that

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


def testUrlParse(runfixture):
  """Test parsing of host directory string"""
  username, host, port, path = pro_fit.runners.RemoteRunner._urlParse("ssh://username@localhost/remote/path")
  assert_that(username).is_equal_to('username')
  assert_that(host).is_equal_to('localhost')
  assert_that(path).is_equal_to("/remote/path")
  assert_that(port).is_none()

  username, host, port, path = pro_fit.runners.RemoteRunner._urlParse("ssh://localhost/remote/path")
  assert_that(username).is_equal_to('')
  assert_that(host).is_equal_to('localhost')
  assert_that(path).is_equal_to("/remote/path")
  assert_that(port).is_none()

  username, host, port, path = pro_fit.runners.RemoteRunner._urlParse("ssh://localhost:2222/remote/path")
  assert_that(username).is_equal_to('')
  assert_that(host).is_equal_to('localhost')
  assert_that(path).is_equal_to("/remote/path")
  assert_that(port).is_equal_to(2222)

  username, host, port, path = pro_fit.runners.RemoteRunner._urlParse("ssh://username@localhost:2222")
  assert_that(username).is_equal_to('username')
  assert_that(host).is_equal_to('localhost')
  assert_that(path).is_equal_to('')
  assert_that(port).is_equal_to(2222)

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


def testTerminate():
  """Test runner's .terminate() method."""
  assert(False)

def testClose():
  """Test runner's .close() method."""
  assert(False)
