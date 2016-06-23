from atsim.pro_fit.runners import _run_remote_exec

# from assertpy import assert_that, fail, contents_of

import collections
import time

from _runnercommon import execnet_gw, channel_id

def testAlreadyRunning(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'heartbeat_enabled' : False })
  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)
  jobpath = tmpdir.join("runjob")
  with jobpath.open("w") as outfile:
    print >>outfile, "sleep 10"
  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)}

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert msg == {'msg': 'JOB_START_ERROR', 'channel_id' : channel_id, 'job_id' : (1,2,3), 'reason' : 'BUSY'}

def testJobPathDoesntExist(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'heartbeat_enabled' : False})
  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)
  ch1.send(msg)
  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert msg['reason'].startswith('PATH_ERROR')
  del msg['reason']
  assert msg == {'msg': 'JOB_START_ERROR', 'channel_id' : channel_id, 'job_id' : (1,2,3)}
  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)

def testShellDoesntExist(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'shell' : '/this/shell/doesnt/exist' , 'heartbeat_enabled' : False})
  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "ERROR", channel_id = channel_id, reason = "shell cannot be executed: '%s'" % '/this/shell/doesnt/exist')

def testStart(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'heartbeat_enabled' : False})

  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)
  ch1.send(msg)
  # Create a short job to run.
  jobpath = tmpdir.join("runjob")
  joboutput = tmpdir.join('joboutput')
  with jobpath.open("w") as outfile:
    print >>outfile, "echo \"I'm good\" > joboutput"

  assert not joboutput.exists()

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)}

  msg = ch1.receive(20.0)
  assert joboutput.isfile()
  assert joboutput.readlines()[0][:-1] == "I'm good"
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : 0, 'job_id' : (1,2,3), 'killed' : False}

  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)

  ch1.send(None)

def testEasyKill(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'heartbeat_enabled' : False })

  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)
  ch1.send(msg)

  # Create a short job to run.
  jobpath = tmpdir.join("runjob")
  joboutput = tmpdir.join('joboutput')
  with jobpath.open("w") as outfile:
    print >>outfile, "echo \"I'm good\" > joboutput"
    print >>outfile, "sleep 10"

  assert not joboutput.exists()

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(1.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)}

  ch1.send({'msg' : 'JOB_KILL', 'job_id' : (1,2,3)})

  msg = ch1.receive(1.0)
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : -15, 'job_id' : (1,2,3), 'killed' : True}

  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)

def testHardKill(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'hardkill_timeout' : 2, 'heartbeat_enabled' : False})

  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)
  ch1.send(msg)

  # Create a short job to run.
  jobpath = tmpdir.join("runjob")
  joboutput = tmpdir.join('joboutput')
  with jobpath.open("w") as outfile:
    print >>outfile, 'trap "" SIGINT SIGTERM'
    print >>outfile, "echo \"I'm good\" > joboutput"
    print >>outfile, "sleep 20"

  assert not joboutput.exists()

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  time.sleep(1.0)
  msg = ch1.receive(1.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)}

  ch1.send({'msg' : 'JOB_KILL', 'job_id' : (1,2,3)})

  msg = ch1.receive(10.0)
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : -9, 'job_id' : (1,2,3), 'killed' : True}

  msg = ch1.receive(1.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)

