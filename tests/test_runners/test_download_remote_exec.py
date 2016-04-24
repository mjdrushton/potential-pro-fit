from assertpy import assert_that, fail

from atsim.pro_fit.runners import _file_transfer_remote_exec
from atsim.pro_fit.runners._file_transfer_remote_exec import FILE, DIR
from atsim.pro_fit.runners._file_transfer_client import DownloadDirectory, DownloadHandler

from _runnercommon import execnet_gw, channel_id

import uuid
import os

import py.path

def testGoodStart(tmpdir, execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.strpath })
  msg = ch1.receive(10.0)
  assert dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.strpath) == msg

def testBadStart_nonexistent_directory(execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)

  badpath = "/this/is/not/a/path"
  assert not py.path.local(badpath).exists()

  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : badpath })
  msg = ch1.receive(10.0)

  assert_that(msg).contains_key('reason')
  assert_that(msg['reason']).starts_with('path does not exist')
  del msg['reason']
  msg == dict(msg =  "ERROR", channel_id = channel_id, remote_path = badpath)

def testListDir(tmpdir, execnet_gw, channel_id):
  files =[]
  # Create a fake directory structure
  p = tmpdir.join("0")
  p.mkdir()
  p.chmod(0o700)
  f = dict(remote_path = p.strpath, mode = os.stat(p.strpath).st_mode, type = DIR)
  files.append(f)

  p = tmpdir.join("runjob")
  p.write("")
  p.chmod(0o700)
  f = dict(remote_path = p.strpath, mode = os.stat(p.strpath).st_mode, type = FILE)
  files.append(f)

  p = tmpdir.join("file2")
  p.write("")
  p.chmod(0o600)
  f = dict(remote_path = p.strpath, mode = os.stat(p.strpath).st_mode, type = FILE)
  files.append(f)

  transid = 0

  expect = {'msg' : 'LIST',
   'id' : transid,
   'channel_id' : channel_id,
   'files' : sorted(files)}

  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.strpath })
  msg = ch1.receive(10.0)
  assert_that(msg).is_equal_to(dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.strpath))

  ch1.send({'msg' : 'LIST', 'id': transid, 'remote_path' : tmpdir.strpath})
  msg = ch1.receive(10.0)

  assert_that(msg).contains_key("files")
  msg['files'] = sorted(msg['files'])
  assert expect == msg

def testDownloadFile_bad(tmpdir, execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.strpath })
  msg = ch1.receive(10.0)

  # Ask for a non existent file
  rpath = tmpdir.join('not_there').strpath
  ch1.send({'msg' : 'DOWNLOAD_FILE', 'id' : 1, 'remote_path' : rpath})
  rcv = ch1.receive(10.0)
  assert {'msg' : 'ERROR', 'channel_id' : channel_id, 'reason' : 'file does not exist', 'remote_path' :  rpath, 'id' : 1} == rcv

  rpath = tmpdir.join('directory')
  rpath.mkdir()
  assert rpath.isdir()
  ch1.send({'msg' : 'DOWNLOAD_FILE', 'id' : 1, 'remote_path' : rpath.strpath})
  rcv = ch1.receive(10.0)
  assert {'msg' : 'ERROR', 'channel_id' : channel_id, 'reason' : 'path refers to a directory and cannot be downloaded', 'remote_path' :  rpath, 'id' : 1} == rcv

  rpath = tmpdir.join('unreadable')
  rpath.write("")
  rpath.chmod(0o0)
  ch1.send({'msg' : 'DOWNLOAD_FILE', 'id' : 1, 'remote_path' : rpath.strpath})
  rcv = ch1.receive(10.0)
  assert {'msg' : 'ERROR', 'channel_id' : channel_id, 'reason' : 'permission denied', 'remote_path' :  rpath.strpath, 'id' : 1} == rcv
  rpath.chmod(0o600)

def testDownloadFile(tmpdir, execnet_gw, channel_id):
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.strpath })
  msg = ch1.receive(10.0)

  chpath = tmpdir.join("0").join("1").join("file")
  contents = "one two three four"
  chpath.write(contents, ensure = True)

  ch1.send({'msg' : 'DOWNLOAD_FILE', 'id' : 1, 'remote_path' : chpath.strpath})
  rcv = ch1.receive(10.0)
  assert {
    'msg' : 'DOWNLOAD_FILE',
    'channel_id' : channel_id,
    'id' : 1,
    'remote_path' : chpath.strpath,
    'file_data' : contents,
    'mode' : os.stat(chpath.strpath).st_mode,
  } == rcv

def create_dir_structure(tmpdir):
  # Create directory structure to download
  rpath = tmpdir.join("remote")
  names = ["One", "Two", "Three"]

  p = rpath
  for i,name in enumerate(names):
    p = p.join(str(i))
    for name in names:
      p.join(name).write(name, ensure = True)

  dpath =  os.path.join(rpath.strpath, "0", "1", "2", "Three")
  assert os.path.isfile(dpath)

  dpath = tmpdir.join('dest')
  dpath.mkdir()

def do_dl(tmpdir, channels):
  rpath = tmpdir.join("remote")
  dpath = tmpdir.join("dest")
  dl = DownloadDirectory(channels, rpath.strpath, dpath.strpath)
  dl.download()

  # Compare the remote and dest directories
  from filecmp import dircmp
  def docmp(dcmp):
    try:
      assert [] == dcmp.diff_files
      assert [] == dcmp.left_only
      assert [] == dcmp.right_only
    except AssertionError:
      print dcmp.report()
      raise
    for subcmp in dcmp.subdirs.values():
      docmp(subcmp)
  dcmp = dircmp(rpath.strpath, dpath.strpath)
  docmp(dcmp)

def testDirectoryDownload_single_channel(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  # Create a download channel.
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.join("remote").strpath })
  msg = ch1.receive(10.0)
  assert dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.join("remote").strpath) == msg
  do_dl(tmpdir, [ch1])

def testDirectoryDownload_multiple_channels(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  channels = []
  for i in range(4):
    ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
    ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.join("remote").strpath })
    msg = ch1.receive(10.0)
    assert dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.join("remote").strpath) == msg
    channels.append(ch1)
  do_dl(tmpdir, channels)

def testDirectoryDownload_emptysrc(tmpdir, execnet_gw, channel_id):
  tmpdir.join("remote").mkdir()
  tmpdir.join("dest").mkdir()
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.join("remote").strpath })
  msg = ch1.receive(10.0)
  assert dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.join("remote").strpath) == msg
  do_dl(tmpdir, [ch1])

def testDirectoryDownload_emptytree(tmpdir, execnet_gw, channel_id):
  tmpdir.join("remote").mkdir()
  os.makedirs(tmpdir.join("one", "two", "three", "four").strpath)
  tmpdir.join("dest").mkdir()
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.join("remote").strpath })
  msg = ch1.receive(10.0)
  assert dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.join("remote").strpath) == msg
  do_dl(tmpdir, [ch1])

def testDirectoryDownload_errors(tmpdir, execnet_gw, channel_id):
  fail("Not implemented")


def testDownloadHandler_rewrite_path():
  p1 = py.path.local("/One/Two/Three/source")
  p2 = py.path.local("/Five/Six/Seven/dest")

  dlh = DownloadHandler(p1.strpath, p2.strpath)
  expect = "/Five/Six/Seven/dest/file"
  actual = dlh.rewrite_path({'remote_path' : p1.join('file').strpath})
  assert actual == expect
