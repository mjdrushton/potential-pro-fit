

from atsim.pro_fit.runners import _remote_exec

from pytest import fixture

from assertpy import assert_that, fail, contents_of

import execnet
import collections

import uuid
import time

@fixture
def execnet_gw(request):
  group = execnet.Group()
  gw = group.makegateway()

  def finalizer():
    group.terminate(timeout=1.0)

  request.addfinalizer(finalizer)
  return gw

@fixture
def channel_id():
  return str(uuid.uuid4())

def testBadStart(execnet_gw):
  ch1 = execnet_gw.remote_exec(_remote_exec)

  ch1.send({'msg' : 'START_CHANNEL'})
  msg = ch1.receive(1.0)

  assert_that(msg["msg"]).is_equal_to("ERROR")

def testAlreadyRunning(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id })
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))
  jobpath = tmpdir.join("runjob")
  with jobpath.open("w") as outfile:
    print >>outfile, "sleep 10"
  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to({'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)})

  ch1.send({'msg' : 'READY_QUERY'})
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "BUSY", channel_id = channel_id))

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to({'msg': 'JOB_START_ERROR', 'channel_id' : channel_id, 'job_id' : (1,2,3), 'reason' : 'BUSY'})

def testJobPathDoesntExist(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id })
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))
  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to({'msg': 'JOB_START_ERROR', 'channel_id' : channel_id, 'job_id' : (1,2,3), 'reason' : 'PATH_ERROR'})
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

def testStart(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id })

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

  ch1.send({'msg' : 'READY_QUERY'})

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

  # Create a short job to run.
  jobpath = tmpdir.join("runjob")
  joboutput = tmpdir.join('joboutput')
  with jobpath.open("w") as outfile:
    print >>outfile, "echo \"I'm good\" > joboutput"

  assert_that(joboutput.exists()).is_false()

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to({'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)})

  msg = ch1.receive(20.0)
  assert_that(joboutput.strpath).is_file()
  assert_that(joboutput.readlines()[0][:-1]).is_equal_to("I'm good")
  assert_that(msg).is_equal_to({'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : 0, 'job_id' : (1,2,3)})

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

  ch1.send(None)

def testEasyKill(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id })

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

  ch1.send({'msg' : 'READY_QUERY'})

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

  # Create a short job to run.
  jobpath = tmpdir.join("runjob")
  joboutput = tmpdir.join('joboutput')
  with jobpath.open("w") as outfile:
    print >>outfile, "echo \"I'm good\" > joboutput"
    print >>outfile, "sleep 10"

  assert_that(joboutput.exists()).is_false()

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to({'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)})

  ch1.send({'msg' : 'JOB_KILL', 'job_id' : (1,2,3)})

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to({'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : -15, 'job_id' : (1,2,3), 'killed' : True})

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

def testHardKill(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'hardkill_timeout' : 2})

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

  ch1.send({'msg' : 'READY_QUERY'})

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

  # Create a short job to run.
  jobpath = tmpdir.join("runjob")
  joboutput = tmpdir.join('joboutput')
  with jobpath.open("w") as outfile:
    print >>outfile, 'trap "" SIGINT SIGTERM'
    print >>outfile, "echo \"I'm good\" > joboutput"
    print >>outfile, "sleep 20"

  assert_that(joboutput.exists()).is_false()

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  time.sleep(1.0)
  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to({'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)})

  ch1.send({'msg' : 'JOB_KILL', 'job_id' : (1,2,3)})

  msg = ch1.receive(10.0)
  assert_that(msg).is_equal_to({'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : -9, 'job_id' : (1,2,3), 'killed' : True})

  msg = ch1.receive(1.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id))

