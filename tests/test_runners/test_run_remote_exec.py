from atsim.pro_fit.runners import _run_remote_exec

# from assertpy import assert_that, fail, contents_of

import collections
import time

from _runnercommon import execnet_gw, channel_id

# class MockChannel(object):

#   def __init__(self, receive_q, send_q):
#     self.receive_q = receive_q
#     self.send_q = send_q

#   def receive(self, timeout = None):
#     return self.receive_q.get(True,timeout)

#   def send(self, msg):
#     self.send_q.put(msg)


# def mkchpair():
#   import Queue
#   a_to_b = Queue.Queue()
#   b_to_a = Queue.Queue()

#   ch1 = MockChannel(b_to_a, a_to_b)
#   ch2 = MockChannel(a_to_b, b_to_a)

#   return ch1, ch2

# def testJobPathDoesntExist_asthread(tmpdir, channel_id):
#   ch1, ch2 = mkchpair()
#   eloop = _run_remote_exec.EventLoop(ch2)
#   eloop.start()

#   ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
#   msg = ch1.receive(10.0)
#   assert msg == dict(msg =  "READY", channel_id = channel_id)
#   ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
#   msg = ch1.receive()
#   assert msg['reason'].startswith('PATH_ERROR')
#   del msg['reason']
#   assert msg == {'msg': 'JOB_START_ERROR', 'channel_id' : channel_id, 'job_id' : (1,2,3)}


def testJobPathDoesntExist(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
  msg = ch1.receive(10.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)
  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(10.0)
  assert msg['reason'].startswith('PATH_ERROR')
  del msg['reason']
  assert msg == {'msg': 'JOB_START_ERROR', 'channel_id' : channel_id, 'job_id' : (1,2,3)}
  ch1.send(None)

def testShellDoesntExist(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'shell' : '/this/shell/doesnt/exist'})
  msg = ch1.receive(10.0)
  assert msg == dict(msg =  "ERROR", channel_id = channel_id, reason = "shell cannot be executed: '%s'" % '/this/shell/doesnt/exist')

def testStart(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})

  msg = ch1.receive(10.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)
  # Create a short job to run.
  jobpath = tmpdir.join("runjob")
  joboutput = tmpdir.join('joboutput')
  with jobpath.open("w") as outfile:
    print >>outfile, "echo \"I'm good\" > joboutput"

  assert not joboutput.exists()

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(10.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)}

  msg = ch1.receive(20.0)
  assert joboutput.isfile()
  assert joboutput.readlines()[0][:-1] == "I'm good"
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : 0, 'job_id' : (1,2,3), 'killed' : False}

  ch1.send(None)

def testEasyKill(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id })

  msg = ch1.receive(10.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)

  # Create a short job to run.
  jobpath = tmpdir.join("runjob")
  joboutput = tmpdir.join('joboutput')
  with jobpath.open("w") as outfile:
    print >>outfile, "echo \"I'm good\" > joboutput"
    print >>outfile, "sleep 10"

  assert not joboutput.exists()

  ch1.send({'msg' : 'JOB_START', 'job_path' : tmpdir.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(10.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)}

  ch1.send({'msg' : 'JOB_KILL', 'job_id' : (1,2,3)})

  msg = ch1.receive(10.0)
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : -15, 'job_id' : (1,2,3), 'killed' : True}

  ch1.send(None)


def testHardKill(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'hardkill_timeout' : 2})

  msg = ch1.receive(10.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)

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
  msg = ch1.receive(10.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)}

  ch1.send({'msg' : 'JOB_KILL', 'job_id' : (1,2,3)})

  msg = ch1.receive(10.0)
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : -9, 'job_id' : (1,2,3), 'killed' : True}

  ch1.send(None)


def testMultipleJobs(execnet_gw, tmpdir, channel_id):
  ch1 = execnet_gw.remote_exec(_run_remote_exec)
  ch1.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id, 'nprocesses' : 2 })

  msg = ch1.receive(10.0)
  assert msg == dict(msg =  "READY", channel_id = channel_id)

  # Create a short job to run.
  jobdir1 = tmpdir.join("job1")
  jobdir1.ensure_dir()
  jobpath1 = jobdir1.join("runjob")
  joboutput1 = jobdir1.join('joboutput')
  with jobpath1.open("w") as outfile:
    print >>outfile, "echo \"I'm good1\" > joboutput"
    print >>outfile, "sleep 10"
  assert not joboutput1.exists()

  jobdir2 = tmpdir.join("job2")
  jobdir2.ensure_dir()
  jobpath2 = jobdir2.join("runjob")
  joboutput2 = jobdir2.join('joboutput')
  with jobpath2.open("w") as outfile:
    print >>outfile, "echo \"I'm good2\" > joboutput"
    print >>outfile, "sleep 5"
  assert not joboutput2.exists()

  jobdir3 = tmpdir.join("job3")
  jobdir3.ensure_dir()
  jobpath3 = jobdir3.join("runjob")
  joboutput3 = jobdir3.join('joboutput')
  with jobpath3.open("w") as outfile:
    print >>outfile, "echo \"I'm good3\" > joboutput"
  assert not joboutput3.exists()


  ch1.send({'msg' : 'JOB_START', 'job_path' : jobdir1.strpath, 'job_id' : (1,2,3)})
  msg = ch1.receive(10.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (1,2,3)}

  ch1.send({'msg' : 'JOB_START', 'job_path' : jobdir2.strpath, 'job_id' : (2,3,4)})
  msg = ch1.receive(10.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg': 'JOB_START', 'channel_id' : channel_id, 'job_id' : (2,3,4)}

  ch1.send({'msg' : 'JOB_START', 'job_path' : jobdir3.strpath, 'job_id' : (4,5,8)})

  msg = ch1.receive(8.0)
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : 0, 'job_id' : (2,3,4), 'killed' : False}

  msg = ch1.receive(2.0)
  assert msg.has_key('pid')
  del msg['pid']
  assert msg == {'msg' : 'JOB_START', 'channel_id' : channel_id, 'job_id' : (4,5,8)}

  msg = ch1.receive(12.0)
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : 0, 'job_id' : (4,5,8), 'killed' : False}

  msg = ch1.receive(12.0)
  assert msg == {'msg': 'JOB_END', 'channel_id' : channel_id, 'returncode' : 0, 'job_id' : (1,2,3), 'killed' : False}

  assert joboutput1.exists()
  assert joboutput1.read()[:-1] == "I'm good1"
  assert joboutput2.exists()
  assert joboutput2.read()[:-1] == "I'm good2"
  assert joboutput3.exists()
  assert joboutput3.read()[:-1] == "I'm good3"

  ch1.send(None)
