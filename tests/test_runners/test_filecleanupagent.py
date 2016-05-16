
from atsim.pro_fit.runners import _file_cleanup_remote_exec
from _runnercommon import execnet_gw, channel_id

import posixpath
import os
import time

import py.path

def test_file_cleanup_end_to_end(tmpdir, execnet_gw, channel_id):
  # Create directory structure
  tmpdir.ensure('one', 'two', 'three', 'four', dir = True)
  root = tmpdir.join('one')
  p = tmpdir

  for i, subp in enumerate(['one', 'two', 'three', 'four']):
    p = p.join(subp)
    for j in xrange(i+1):
      j += 1
      p.ensure("%d.txt" % j)

  two = root.join("two")
  two.ensure("a", "b", dir = True)
  two.ensure("a", "c", dir = True)

  def currfiles():
    files = root.visit()
    files = [ f.relto(tmpdir) for f in files]
    return set(files)

  allfiles = set(['one/two/a/c',
                  'one/two',
                  'one/two/three/1.txt',
                  'one/two/three/four/1.txt',
                  'one/two/2.txt',
                  'one/two/three/four/2.txt',
                  'one/two/three/3.txt',
                  'one/two/three/2.txt',
                  'one/two/three/four/3.txt',
                  'one/two/a',
                  'one/two/three',
                  'one/two/1.txt',
                  'one/two/three/four',
                  'one/1.txt',
                  'one/two/three/four/4.txt',
                  'one/two/a/b'])

  assert currfiles() == allfiles

  ch1 = execnet_gw.remote_exec(_file_cleanup_remote_exec)
  ch1.send({'msg' : 'START_CLEANUP_CHANNEL', 'channel_id' : channel_id, 'remote_path' : root.strpath})

  msg = ch1.receive(10)
  assert msg['msg'] == 'READY'

  transid = 'transid'

  ch1.send({'msg' : 'LOCK', 'id' : transid, 'remote_path' : 'two/three/four'})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'LOCKED', 'channel_id' : channel_id, 'id' : transid}

  ch1.send({'msg' : 'LOCK', 'id' : transid, 'remote_path' : ['two', 'two/three']})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'LOCKED', 'channel_id' : channel_id, 'id' : transid}

  ch1.send({'msg' : 'LOCK', 'id' : transid, 'remote_path' : [
    'two', 'two/three',
    'two/a/b', 'two/a/c'
  ]})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'LOCKED', 'channel_id' : channel_id, 'id' : transid}

  # Unlock 'one/two/three/four' - four deleted
  ch1.send({'msg' : 'UNLOCK', 'id' : transid, 'remote_path' : ['two/three/four']})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'UNLOCKED', 'channel_id' : channel_id, 'id' : transid}

  ch1.send({'msg' : 'FLUSH', 'id' : transid })
  msg = ch1.receive(10)
  assert msg == {'msg' : 'FLUSHED', 'channel_id' : channel_id, 'id' : transid}

  allfiles = set(['one/two/a/c',
                  'one/two',
                  'one/two/three/1.txt',
                  'one/two/2.txt',
                  'one/two/three/3.txt',
                  'one/two/three/2.txt',
                  'one/two/a',
                  'one/two/three',
                  'one/two/1.txt',
                  'one/1.txt',
                  'one/two/a/b'])

  assert currfiles() == allfiles

  ch1.send({'msg' : 'UNLOCK', 'id' : transid, 'remote_path' : ['two', 'two/three']})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'UNLOCKED', 'channel_id' : channel_id, 'id' : transid}


  allfiles = set(['one/two/a/c',
                  'one/two',
                  'one/two/2.txt',
                  'one/two/a',
                  'one/two/1.txt',
                  'one/1.txt',
                  'one/two/a/b'])

  assert currfiles() == allfiles

 # Unlock 'one/a' - a deleted
  ch1.send({'msg' : 'UNLOCK', 'id' : transid, 'remote_path' : ['two/a', 'two/a/b']})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'UNLOCKED', 'channel_id' : channel_id, 'id' : transid}

  ch1.send({'msg' : 'FLUSH', 'id' : transid })
  msg = ch1.receive(10)
  assert msg == {'msg' : 'FLUSHED', 'channel_id' : channel_id, 'id' : transid}

  allfiles = set([
                  'one/two/a/c',
                  'one/two',
                  'one/two/2.txt',
                  'one/two/a',
                  'one/two/1.txt',
                  'one/1.txt'
                  ])

  assert currfiles() == allfiles

  ch1.close()
  ch1.waitclose()
  time.sleep(1)
  assert not root.exists()

def test_file_cleanup_start(tmpdir, execnet_gw, channel_id):
  root = tmpdir.ensure('root', dir = True)
  assert os.path.isdir(root.strpath)
  ch1 = execnet_gw.remote_exec(_file_cleanup_remote_exec)
  ch1.send({'msg' : 'START_CLEANUP_CHANNEL', 'channel_id' : channel_id, 'remote_path' : root.strpath })
  msg = ch1.receive(10.0)
  assert dict(msg =  "READY", channel_id = channel_id, remote_path = root.strpath) == msg
  ch1.close()
  ch1.waitclose()
  time.sleep(1)
  assert not os.path.isdir(root.strpath), "Root directory still present after cleanup channel closed."

def test_file_cleanup_BadStart_nonexistent_directory(execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_cleanup_remote_exec)

  badpath = "/this/is/not/a/path"
  assert not py.path.local(badpath).exists()

  ch1.send({'msg' : 'START_CLEANUP_CHANNEL', 'channel_id' : channel_id, 'remote_path' : badpath })
  msg = ch1.receive(10.0)

  assert msg.has_key('reason')
  assert msg['reason'].startswith('path does not exist')
  del msg['reason']
  msg == dict(msg =  "ERROR", channel_id = channel_id, remote_path = badpath)

def test_file_cleanup_lock_bad(tmpdir, execnet_gw, channel_id):
  root = tmpdir.ensure('root', dir = True)
  assert os.path.isdir(root.strpath)
  ch1 = execnet_gw.remote_exec(_file_cleanup_remote_exec)
  ch1.send({'msg' : 'START_CLEANUP_CHANNEL', 'channel_id' : channel_id, 'remote_path' : root.strpath })
  msg = ch1.receive(10.0)
  assert dict(msg =  "READY", channel_id = channel_id, remote_path = root.strpath) == msg

  ch1.send({'msg': 'LOCK', 'id' : 'transid', 'remote_path' : '../'})
  msg = ch1.receive(10.0)
  expect = {'msg' : 'ERROR',
    'id' : 'transid',
    'reason' : 'path does not lie within directory structure',
    'remote_path' : '../',
    'error_code' : ("PATHERROR", "NOTCHILD") }

  ch1.send({'msg': 'LOCK', 'id' : 'transid', 'remote_path' : '/'})
  msg = ch1.receive(10.0)
  expect = {'msg' : 'ERROR',
    'id' : 'transid',
    'reason' : 'path does not lie within directory structure',
    'remote_path' : '/',
    'error_code' : ("PATHERROR", "NOTCHILD")}

  ch1.close()
  ch1.waitclose()
  time.sleep(1)
  assert not os.path.isdir(root.strpath)

def test_file_cleanup_unlock_bad(tmpdir, execnet_gw, channel_id):
  root = tmpdir.ensure('root', dir = True)
  assert os.path.isdir(root.strpath)
  ch1 = execnet_gw.remote_exec(_file_cleanup_remote_exec)
  ch1.send({'msg' : 'START_CLEANUP_CHANNEL', 'channel_id' : channel_id, 'remote_path' : root.strpath })
  msg = ch1.receive(10.0)
  assert dict(msg =  "READY", channel_id = channel_id, remote_path = root.strpath) == msg

  ch1.send({'msg': 'LOCK', 'id' : 'transid', 'remote_path' : 'blah'})
  msg = ch1.receive(10.0)
  assert msg['msg'] == "LOCKED"


  ch1.send({'msg': 'UNLOCK', 'id' : 'transid', 'remote_path' : '../../'})
  msg = ch1.receive(10.0)
  expect = {'msg' : 'ERROR',
    'id' : 'transid',
    'reason' : 'path does not lie within cleanup directory structure',
    'remote_path' : '../../',
    'error_code' : ("PATHERROR", "NOTCHILD")}


  ch1.send({'msg': 'UNLOCK', 'id' : 'transid', 'remote_path' : 'boop'})
  msg = ch1.receive(10.0)
  expect = {'msg' : 'ERROR',
    'id' : 'transid',
    'reason' : 'path is not registered with cleanup agent',
    'remote_path' : 'boop',
    'error_code' : ("PATHERROR", "NOT_REGISTERED")}


  ch1.close()
  ch1.waitclose()

def test_file_cleanup_unlock_path_normalize(tmpdir, execnet_gw, channel_id):
  # Create directory structure
  tmpdir.ensure('one', dir = True)
  root = tmpdir.join('one')
  p = tmpdir

  root.ensure('a', 'b', 'c', 'd', dir = True)

  def currfiles():
    files = root.visit()
    files = [ f.relto(tmpdir) for f in files]
    return set(files)

  allfiles = set(['one/a','one/a/b','one/a/b/c', 'one/a/b/c/d'])
  assert currfiles() == allfiles

  ch1 = execnet_gw.remote_exec(_file_cleanup_remote_exec)
  ch1.send({'msg' : 'START_CLEANUP_CHANNEL', 'channel_id' : channel_id, 'remote_path' : root.strpath})

  msg = ch1.receive(10)
  assert msg['msg'] == 'READY'

  transid = 'transid'

  ch1.send({'msg' : 'LOCK', 'id' : transid, 'remote_path' : ['a/../a/b/../b', 'a/b/c/d']})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'LOCKED', 'channel_id' : channel_id, 'id' : transid}

  ch1.send({'msg' : 'UNLOCK', 'id' : transid, 'remote_path' : ['a/b/c/d/../../']})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'UNLOCKED', 'channel_id' : channel_id, 'id' : transid}

  assert currfiles() == allfiles

  ch1.send({'msg' : 'UNLOCK', 'id' : transid, 'remote_path' : ['a/../a/b/c/d']})
  msg = ch1.receive(10)
  assert msg == {'msg' : 'UNLOCKED', 'channel_id' : channel_id, 'id' : transid}

  allfiles = set(['one/a'])
  assert currfiles() == allfiles

  ch1.close()
  ch1.waitclose()
