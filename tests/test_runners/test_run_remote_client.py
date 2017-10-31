from atsim.pro_fit.runners._run_remote_client import RunChannel, RunClient, RunJobKilledException, JobAlreadyFinishedException

from _runnercommon import execnet_gw, channel_id, CheckPIDS

import gevent
import gevent.event

import logging

import pytest
pytestmark = pytest.mark.skip()


class TstCallback(object):

  logger = logging.getLogger("test_run_remote_client.TstCallback")

  def __init__(self, jobid, jobdir):
    self.jobdir = jobdir
    self.jobid = jobid
    self.event = gevent.event.Event()
    self.called = False
    self.exception = None

  def __call__(self, exception):
    self.logger.getChild("__call__").info("TstCallback called with '%s'", exception)
    self.called = True
    self.exception = exception
    self.event.set()


def test_run_remote_client_single(tmpdir, execnet_gw, channel_id):
  channel = RunChannel(execnet_gw, channel_id, nprocesses = 1)

  with tmpdir.join("runjob").open("w") as outfile:
    print >>outfile, "echo Hello World > job.out"

  runclient = RunClient(channel)
  runclient.runCommand(tmpdir.strpath)

  assert tmpdir.join("job.out").isfile()
  assert tmpdir.join("job.out").read()[:-1] == "Hello World"

  channel.send(None)
  channel.waitclose()

def test_run_remote_client_multiple(tmpdir, execnet_gw, channel_id):
  channel = RunChannel(execnet_gw, channel_id, nprocesses = 3)
  callbacks = []

  for jobid in xrange(5):
    jobdir = tmpdir.join(str(jobid))
    jobdir.ensure_dir()
    with jobdir.join("runjob").open("w") as outfile:
      print >>outfile, "echo %d > job.out" % jobid
      print >>outfile, "sleep 1"

    callback = TstCallback(jobid, jobdir)
    callbacks.append(callback)

  runclient = RunClient(channel)

  import time
  for callback in callbacks:
    runclient.runCommand(callback.jobdir.strpath, callback)
    # time.sleep(2)

  for callback in callbacks:
    callback.event.wait(5)

  for callback in callbacks:
    jobdir = callback.jobdir
    if not callback.exception is None:
      raise callback.exception
    assert jobdir.join("job.out").isfile()
    assert jobdir.join("job.out").read()[:-1] == str(callback.jobid)

  channel.send(None)
  channel.waitclose()


def test_run_remote_client_kill_job(tmpdir, execnet_gw, channel_id):
  channel = RunChannel(execnet_gw, channel_id, nprocesses = 3)
  runclient = RunClient(channel)

  with tmpdir.join('runjob').open('w') as outfile:
    print >>outfile,  "#! /bin/bash"
    print >>outfile,  "sleep 1200"

  callback = TstCallback(None, None)
  runjob = runclient.runCommand(tmpdir.strpath, callback)

  gevent.wait([runjob.pidSetEvent])

  checkpids = CheckPIDS(execnet_gw)
  checkpids.checkpids([runjob.pid], True)

  killevent = runjob.kill()
  assert killevent.wait(5)

  checkpids.checkpids([runjob.pid], False)

  assert callback.called

  try:
    raise callback.exception
  except RunJobKilledException:
    pass

  # channel.send(None)

  # Now attempt to kill the job again.
  # An exception should be thrown.
  try:
    runjob.kill()
    assert False, 'JobAlreadyFinishedException not raised'
  except JobAlreadyFinishedException:
    pass

  channel.send(None)
  channel.waitclose()


def test_run_remote_client_kill_not_started_job(tmpdir, execnet_gw, channel_id):
  channel = RunChannel(execnet_gw, channel_id, nprocesses = 1)
  runclient = RunClient(channel)

  runjob_path1 = tmpdir.join("0")
  runjob_path2 = tmpdir.join("1")

  runjob_path1.ensure_dir()
  runjob_path2.ensure_dir()

  with runjob_path1.join('runjob').open('w') as outfile:
    print >>outfile,  "#! /bin/bash"
    print >>outfile,  "sleep 1200"

  with runjob_path2.join('runjob').open('w') as outfile:
    print >>outfile,  "#! /bin/bash"
    print >>outfile,  "sleep 1200"

  callback1 = TstCallback(None, None)
  callback2 = TstCallback(None, None)

  runjob1 = runclient.runCommand(runjob_path1.strpath, callback1)
  runjob2 = runclient.runCommand(runjob_path2.strpath, callback2)

  runjob1.pidSetEvent.wait()

  checkpids = CheckPIDS(execnet_gw)
  checkpids.checkpids([runjob1.pid], True)

  assert runjob2.pid is None

  killevent = runjob2.kill()
  assert killevent.wait(5)
  assert callback2.called

  try:
    raise callback2.exception
  except RunJobKilledException:
    pass

  killevent = runjob1.kill()
  assert killevent.wait(5)
  checkpids.checkpids([runjob1.pid], False)
  assert callback1.called

  try:
    raise callback1.exception
  except RunJobKilledException:
    pass

  channel.send(None)
  channel.waitclose()
