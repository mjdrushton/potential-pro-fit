"""Tests for atsim.pro_fit.runners.RemoteRunner"""

import logging
import os
import pytest
import shutil
import stat
import tempfile
import time
import posixpath
import itertools

import py.path

import atsim.pro_fit._execnet as _execnet
import execnet

from atsim import pro_fit

from atsim.pro_fit.runners import RunnerClosedException

from _runnercommon import runfixture, DIR, FILE, runnertestjob, CheckPIDS, isdir_remote
from .. import common
from ..testutil import vagrant_basic

import gevent
import gevent.event

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
  from atsim.pro_fit._execnet import urlParse
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
  import logging
  import sys

  runner = _createRunner(runfixture,vagrant_basic, 1)

  batch = _runBatch(runner, [runfixture.jobs[0]])
  assert batch.join(10)
  runnertestjob(runfixture, 0)

def testAllInSingleBatch(runfixture, vagrant_basic):
  runner = _createRunner(runfixture, vagrant_basic, 3)
  _runBatch(runner, runfixture.jobs).join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id)

  assert runner.close().wait(10)

def testAllInMultipleBatch(runfixture, vagrant_basic):
  runner = _createRunner(runfixture, vagrant_basic, 3)

  f1 = _runBatch(runner, runfixture.jobs[:6])
  f2 = _runBatch(runner, runfixture.jobs[6:])
  f2.join()
  f1.join()
  for job in runfixture.jobs:
    runnertestjob(runfixture, job.variables.id)

  assert runner.close().wait(10)

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

    with runjob:
      runjob.write("""#! /bin/bash
echo $$ > JOB_PID
sleep 1200
""")
      runjob.flush()

    # Create Job instance
    job = Job(jf, str(tempd), [])

    yield job

def _mkexecnetgw(vagrant_basic):
  with py.path.local(vagrant_basic.root).as_cwd():
    group = _execnet.Group()
    gw = group.makegateway("vagrant_ssh=default")
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

def testTerminate(tmpdir, runfixture, vagrant_basic):
  """Test runner future's .terminate() method."""
  logger = logging.getLogger(__name__).getChild("testTerminate")
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
    # batchpath = posixpath.join(f._remotePath, f._batchDir)
    # Check five times with a sleep of 1s between attempts
    # if after that remote files don't appear, then they never will
    for i in xrange(5):
      # Check paths
      count = 0
      pids = []
      for job in f.jobs:
        fullpath = posixpath.join(job.remotePath, 'job_files', "JOB_PID")
        channel.send(fullpath)
        found = channel.receive()
        logger.debug("checkpaths: '%s' found = '%s'" % (fullpath, found))
        count += found

      logger.debug("checkpaths count on %d attempt: %d (expecting %d)", i, count, len(f.jobs))
      time.sleep(1)
      if count == len(f.jobs):
        return
    assert count == len(f.jobs), "Remote paths not found"

  channel = gw.remote_exec(_remote_is_file)
  checkpaths(channel, b1_future)
  channel.send(None)
  channel.waitclose()

  # ... job should write PID to file.
  def getpids(channel, f):
    paths = []
    # batchpath = posixpath.join(f.remotePath, f._batchDir)
    for job in f.jobs:
      fullpath = posixpath.join(job.remotePath, "job_files", "JOB_PID")
      channel.send(fullpath)
      paths.append(channel.receive()[:-1])
    return paths

  channel = gw.remote_exec(_read_pids)
  b1_pids = getpids(channel, b1_future)
  channel.send(None)
  channel.waitclose()

  cp = CheckPIDS(gw)
  cp.checkpids(b1_pids, True)

  # Call terminate - make sure that temporary directories are cleaned up.
  # Ensure job PID has gone.
  jobs1 = list(b1_future.jobs)

  b1_term = b1_future.terminate()
  assert b1_term.wait(5)

  cp.checkpids(b1_pids, False)
  cp.close()

  #Check file cleanup here.
  cleanupEvent = runner._inner.cleanupFlush()
  assert cleanupEvent.wait(5)

  isdirchannel = gw.remote_exec(isdir_remote)
  for job in jobs1:
    isdirchannel.send(job.remotePath)
    assert not isdirchannel.receive(), "Remote path should not exist but does: %s" % job.remotePath

  # Check that the batch directory has been deleted.
  isdirchannel.send(b1_future.remoteBatchDir)
  assert not isdirchannel.receive(), "Remote path should not exist but does: %s" % b1_future.remoteBatchDir

  b2_future = _runBatch(runner, batch2)
  jobs2 = list(b2_future.jobs)
  channel = gw.remote_exec(_remote_is_file)
  checkpaths(channel, b2_future)
  channel.send(None)
  channel.waitclose()

  channel = gw.remote_exec(_read_pids)
  b2_pids = getpids(channel, b2_future)
  channel.send(None)
  channel.waitclose()

  cp = CheckPIDS(gw)
  cp.checkpids(b2_pids, True)

  b2_term = b2_future.terminate()
  assert b2_term.wait(20)
  cp.checkpids(b2_pids, False)

  #Check file cleanup here.
  cleanupEvent = runner._inner.cleanupFlush()
  assert cleanupEvent.wait(5)

  isdirchannel = gw.remote_exec(isdir_remote)
  for job in jobs2:
    isdirchannel.send(job.remotePath)
    assert not isdirchannel.receive(), "Remote path should not exist but does: %s" % job.remotePath

  # Check that the batch directory has been deleted.
  isdirchannel.send(b2_future.remoteBatchDir)
  assert not isdirchannel.receive(), "Remote path should not exist but does: %s" % b1_future.remoteBatchDir

  # Ensure that future's terminated flag is set.
  isdirchannel.send(None)
  isdirchannel.close()
  isdirchannel.waitclose()

def testClose(runfixture, vagrant_basic, tmpdir):
  """Test runner's .close() method."""
  ncpu = 3
  runner = _createRunner(runfixture, vagrant_basic, ncpu)
  gw = _mkexecnetgw(vagrant_basic)

  rstatus = runner._inner._gw.remote_status()
  assert  1 + ncpu +  runner._inner._numDownload + runner._inner._numUpload == rstatus.numchannels

  jobiter = _mklongjob(tmpdir)

  b1_jobs = []
  for i in xrange(5):
    b1_jobs.append(jobiter.next())

  b2_jobs = []
  for i in xrange(5):
    b2_jobs.append(jobiter.next())

  b1 = runner.runBatch(b1_jobs)
  b2 = runner.runBatch(b2_jobs)


  def waitForRun():
    running = []
    while len(running) < ncpu:
      running =  [ j for j in itertools.chain(b1.jobs,b2.jobs) if not j.pidSetEvent is None and j.pidSetEvent.is_set() ]
      gevent.sleep(0.1)
    return running

  grn = gevent.Greenlet.spawn(waitForRun)
  grn.join()
  running = grn.value

  assert len(running) == ncpu

  isdirchannel = gw.remote_exec(isdir_remote)
  for job in running:
    isdirchannel.send(job.remotePath)
    assert isdirchannel.receive(), "Remote path should not exist but does: %s" % job.remotePath

  closeevent = runner.close()
  assert closeevent.wait(30)

  for job in itertools.chain(b1.jobs, b2.jobs):
    isdirchannel.send(job.remotePath)
    assert not isdirchannel.receive(), "Remote path should not exist but does: %s" % job.remotePath

  isdirchannel.send(b1.remoteBatchDir)
  assert not isdirchannel.receive(), "Batch 1 remote path should not exist but does: %s" % b1_future.remoteBatchDir

  isdirchannel.send(b2.remoteBatchDir)
  assert not isdirchannel.receive(), "Batch 2 remote path should not exist but does: %s" % b1_future.remoteBatchDir

  isdirchannel.send(runner._inner._remotePath)
  assert not isdirchannel.receive(), "Runner remote path should not exist but does: %s" % b1_future.remoteBatchDir

  # Check that the runner indicates it has been terminated
  try:
    runner.runBatch(b1_jobs)
    assert False, "RunnerClosed Exception not raised"
  except RunnerClosedException:
    pass

  # Check that the download channel have been terminated.
  # Check that the upload channels have been closed.
  # Check that the cleanup channel has been terminated
  rstatus = runner._inner._gw.remote_status()
  assert rstatus.numchannels == 0

def testBatchTerminate2(runfixture, vagrant_basic, tmpdir):
  """Test runner's .close() method."""

  try:
    ncpu = 3
    runner = _createRunner(runfixture, vagrant_basic, ncpu)
    gw = _mkexecnetgw(vagrant_basic)

    rstatus = runner._inner._gw.remote_status()
    assert  1 + ncpu +  runner._inner._numDownload + runner._inner._numUpload == rstatus.numchannels

    jobiter = _mklongjob(tmpdir)

    b1_jobs = []
    for i in xrange(5):
      b1_jobs.append(jobiter.next())

    b2_jobs = []
    for i in xrange(5):
      b2_jobs.append(jobiter.next())

    b1 = runner.runBatch(b1_jobs)

    def waitForRun():
      running = []
      while len(running) < ncpu:
        running =  [ j for j in b1.jobs if not j.pidSetEvent is None and j.pidSetEvent.is_set() ]
        gevent.sleep(0.1)
      return running

    grn = gevent.Greenlet.spawn(waitForRun)
    grn.join()
    running = grn.value
    assert len(running) == ncpu

    b2 = runner.runBatch(b2_jobs)

    def waitForUpload():
      uploaded = []
      while len(uploaded) < len(b1_jobs)+len(b2_jobs):
        uploaded = [ j for j in itertools.chain(b1.jobs, b2.jobs) if "finish upload" in j.status]
        gevent.sleep(0.1)
      return uploaded

    grn = gevent.Greenlet.spawn(waitForUpload)
    grn.join()

    b1CloseEvent = b1.terminate()
    b2CloseEvent = b2.terminate()

    resb1 =  b1CloseEvent.wait(5)
    assert resb1
    resb2 = b2CloseEvent.wait(5)
    assert resb2

    jkl = ["start upload", "finish upload", "start job run", "finish job run killed", "finish job killed"]

    # All jobs will have got to the point at which a job has been submitted to the run client...
    status_expect = [jkl, jkl, jkl, jkl, jkl,
                     jkl, jkl, jkl, jkl, jkl]

    statuses = [ j.status for j in itertools.chain(b1.jobs, b2.jobs) ]
    assert status_expect == statuses
  finally:
    runner.close()
