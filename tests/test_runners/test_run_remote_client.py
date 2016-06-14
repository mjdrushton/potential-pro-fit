from atsim.pro_fit.runners._run_remote_client import RunChannels, RunClient

from _runnercommon import execnet_gw, channel_id

import threading

def test_run_remote_client_single(tmpdir, execnet_gw, channel_id):
  channel = RunChannels(execnet_gw, channel_id, num_channels = 1)

  with tmpdir.join("runjob").open("w") as outfile:
    print >>outfile, "echo Hello World > job.out"

  runclient = RunClient(channel)
  runclient.runCommand(tmpdir.strpath)

  assert tmpdir.join("job.out").isfile()
  assert tmpdir.join("job.out").read()[:-1] == "Hello World"

  channel.broadcast(None)

def test_run_remote_client_multiple(tmpdir, execnet_gw, channel_id):
  channel = RunChannels(execnet_gw, channel_id, num_channels = 3)
  assert len(channel) == 3
  callbacks = []

  class TestCallback(object):

    def __init__(self, jobid, jobdir):
      self.jobdir = jobdir
      self.jobid = jobid
      self.event = threading.Event()
      self.exception = None

    def __call__(self, exception):
      self.exception = exception
      self.event.set()

  for jobid in xrange(5):
    jobdir = tmpdir.join(str(jobid))
    jobdir.ensure_dir()
    with jobdir.join("runjob").open("w") as outfile:
      print >>outfile, "echo %d > job.out" % jobid
      print >>outfile, "sleep 1"

    callback = TestCallback(jobid, jobdir)
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

  channel.broadcast(None)
