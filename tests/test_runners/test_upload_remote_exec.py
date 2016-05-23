from assertpy import assert_that, fail

from atsim.pro_fit.runners import _file_transfer_remote_exec

from _runnercommon import execnet_gw, channel_id

import uuid
import os

def testGoodStart_explicit_dir(tmpdir, execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)

  path = tmpdir.join("0")
  assert_that(path.isdir()).is_false()

  ch1.send({'msg' : 'START_UPLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : path.strpath })
  msg = ch1.receive(10.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id, remote_path = path.strpath))

  assert_that(path.strpath).is_directory()

def testGoodStart_tmpdir(execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_UPLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : None })

  msg = ch1.receive(10.0)
  assert_that(msg).contains_key('remote_path')
  rpt = msg['remote_path']
  assert_that(rpt).is_directory()
  import shutil
  shutil.rmtree(rpt, ignore_errors = True)

  del msg['remote_path']
  assert_that(msg).is_equal_to(dict(msg = "READY", channel_id = channel_id))

def testBadStart_destination_unwriteable(tmpdir, execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)

  tmpdir.chmod(0o500)
  try:
    ch1.send({'msg' : 'START_UPLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.strpath })
    msg = ch1.receive(10.0)
    assert msg == dict(msg = "ERROR",
                       channel_id = channel_id,
                       remote_path = tmpdir.strpath,
                       reason = 'Directory is not writeable.',
                       error_code =  ('IOERROR', 'PERMISSION_DENIED'))
  finally:
    tmpdir.chmod(0o700)

def testSendFile(tmpdir, execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)

  sourcedir = tmpdir.join("source")
  sourcedir.mkdir()
  ofilename = sourcedir.join("runjob")

  with ofilename.open('w') as ofile:
    ofile.write("#! /bin/bash\n")
    ofile.write("echo Hello \n")
  ofilename.chmod(0o744)

  destdir = tmpdir.join("dest")
  assert_that(destdir.isdir()).is_false()

  ch1.send(dict(
    msg = "START_UPLOAD_CHANNEL",
    remote_path = destdir.strpath))

  msg = ch1.receive()
  assert_that(msg['msg']).is_equal_to("READY")
  channel_id = msg['channel_id']
  assert_that(msg['remote_path']).is_equal_to(destdir.strpath)
  assert_that(destdir.strpath).is_directory()

  destpath = destdir.join("runjob")

  assert_that(destpath.exists()).is_false()

  filecontents = ofilename.read()
  mode = os.stat(ofilename.strpath).st_mode

  fileid = str(uuid.uuid4())
  ch1.send(dict(msg = 'UPLOAD',
    remote_path = 'runjob',
    file_data = filecontents,
    mode = mode,
    id = fileid))
  msg = ch1.receive(10.0)

  assert_that(msg).is_equal_to(dict(msg = 'UPLOADED', id = fileid, remote_path = destpath.strpath, channel_id = channel_id))

  assert_that(destpath.strpath).is_file()
  assert_that(mode).is_equal_to(os.stat(destpath.strpath).st_mode)

  scontent = ofilename.open('rb').read()
  dcontent = destpath.open('rb').read()

  assert_that(dcontent).is_equal_to(scontent)

  # Now upload the same file to a child directory that doesn't exist
  destpath = destdir.join("child1").join("child2").join("runjob")
  assert_that(destpath.exists()).is_false()

  ch1.send(dict(msg = 'UPLOAD',
    remote_path = destpath.strpath,
    file_data = filecontents,
    mode = mode,
    id = fileid))
  msg = ch1.receive(10.0)
  assert_that(msg).is_equal_to(dict(msg = 'UPLOADED', id = fileid, remote_path = destpath.strpath, channel_id = channel_id))

  assert_that(destpath.strpath).is_file()
  assert_that(mode).is_equal_to(os.stat(destpath.strpath).st_mode)

  dcontent = destpath.open('rb').read()
  assert_that(dcontent).is_equal_to(scontent)

  assert_that(mode).is_equal_to(os.stat(destpath.strpath).st_mode)

def testNormalizePath():
  root_path = "/this/is/the/root"
  sub = "1/2/3"

  expect = "/this/is/the/root/1/2/3"
  actual =  _file_transfer_remote_exec.normalize_path(root_path, sub)

  assert_that(actual).is_equal_to(expect)

  sub = "/1/2/3"
  actual =  _file_transfer_remote_exec.normalize_path(root_path, sub)
  assert_that(actual).is_equal_to(expect)

  sub = "/this/is/the/root/1/2/3"
  actual =  _file_transfer_remote_exec.normalize_path(root_path, sub)
  assert_that(actual).is_equal_to(expect)

  sub = "../1/2/3"
  actual =  _file_transfer_remote_exec.normalize_path(root_path, sub)
  assert_that(actual).is_none()

  sub = "/1/2/../../1/2/3"
  actual = _file_transfer_remote_exec.normalize_path(root_path, sub)
  assert_that(actual).is_equal_to("/this/is/the/root/1/2/3")

def testMkfile(tmpdir, execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)

  destdir = tmpdir.join("dest")
  assert_that(destdir.isdir()).is_false()

  ch1.send(dict(
    msg = "START_UPLOAD_CHANNEL",
    remote_path = destdir.strpath))

  msg = ch1.receive()
  assert_that(msg['msg']).is_equal_to("READY")
  channel_id = msg['channel_id']
  assert_that(msg['remote_path']).is_equal_to(destdir.strpath)
  assert_that(destdir.strpath).is_directory()


  fileid = str(uuid.uuid4())
  path1 = destdir.join("path1")
  ch1.send(dict(msg = 'MKDIR',
    remote_path = 'path1',
    id = fileid))
  msg = ch1.receive(10.0)
  assert msg == dict(msg = 'MKDIR', id = fileid, channel_id = channel_id, remote_path = path1.strpath)

  assert path1.isdir()

  fileid = str(uuid.uuid4())
  path2 = destdir.join("path2")
  ch1.send(dict(msg = 'MKDIR',
    remote_path = path2.strpath,
    mode = 0o600,
    id = fileid))
  msg = ch1.receive(10.0)
  assert msg == dict(msg = 'MKDIR', id = fileid, channel_id = channel_id, remote_path = path2.strpath)

  assert path2.isdir()
  import stat
  assert stat.S_IMODE(os.stat(path2.strpath).st_mode) == 0o600

