
from atsim.pro_fit.runners import _file_transfer_remote_exec
from atsim.pro_fit.runners._file_transfer_remote_exec import FILE, DIR
from atsim.pro_fit.runners._file_transfer_client import DownloadDirectory, DownloadHandler
from atsim.pro_fit.runners._file_transfer_client import DirectoryDownloadException
from _runnercommon import execnet_gw, channel_id

import uuid
import os
import shutil
import stat

import py.path
from assertpy import assert_that, fail

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
  assert {'msg' : 'ERROR', 'channel_id' : channel_id,
          'reason' : 'file does not exist',
          'error_code' : ('IOERROR', 'FILEDOESNOTEXIST'),
          'remote_path' :  rpath, 'id' : 1} == rcv

  rpath = tmpdir.join('directory')
  rpath.mkdir()
  assert rpath.isdir()
  ch1.send({'msg' : 'DOWNLOAD_FILE', 'id' : 1, 'remote_path' : rpath.strpath})
  rcv = ch1.receive(10.0)
  assert {'msg' : 'ERROR', 'channel_id' : channel_id,
          'reason' : 'path refers to a directory and cannot be downloaded',
          'remote_path' :  rpath, 'id' : 1,
          'error_code' : ('IOERROR', 'ISDIR')} == rcv

  rpath = tmpdir.join('unreadable')
  rpath.write("")
  rpath.chmod(0o0)
  ch1.send({'msg' : 'DOWNLOAD_FILE', 'id' : 1, 'remote_path' : rpath.strpath})
  rcv = ch1.receive(10.0)
  assert rcv.has_key('exc_msg')
  del rcv['exc_msg']
  assert {'msg' : 'ERROR',
          'channel_id' : channel_id,
          'reason' : 'permission denied',
          'remote_path' :  rpath.strpath,
          'id' : 1,
          'error_code': ('IOERROR', 'FILEOPEN')} == rcv
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

from filecmp import dircmp
def cmpdirs(left, right):
  dcmp = dircmp(left, right)
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
  docmp(dcmp)

def do_dl(tmpdir, channels, dl = None):
  rpath = tmpdir.join("remote")
  dpath = tmpdir.join("dest")
  if dl is None:
    dl = DownloadDirectory(channels, rpath.strpath, dpath.strpath)
  dl.download()

  # Compare the remote and dest directories
  cmpdirs(rpath.strpath, dpath.strpath)

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

def testDirectoryDownload_missing_dest(tmpdir, execnet_gw, channel_id):
  # Errors to test
  # Destination doesn't exist
  rpath = tmpdir.join("remote")
  rpath.mkdir()
  dpath = tmpdir.join("dest")
  ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
  ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.join("remote").strpath })
  msg = ch1.receive(10.0)
  assert dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.join("remote").strpath) == msg

  try:
    dl = DownloadDirectory([ch1], rpath.strpath, dpath.strpath)
    fail("DownloadDirectory.download() should have raised IOError")
  except IOError,e:
    pass

def testDirectoryDownload_access_denied(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  # Deny access to the 0/1/2 directory
  import shutil
  shutil.copytree(tmpdir.join("remote").strpath, tmpdir.join("source").strpath)
  shutil.rmtree(tmpdir.join("remote", "0", "1", "2").strpath)

  p = tmpdir.join("remote", "0", "1", "2")
  try:
    p.mkdir()
    p.chmod(0o0)

    tmpdir.join("source", "0", "1", "2").chmod(0o0)

    # Create a download channel.
    ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
    ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.join("source").strpath })
    msg = ch1.receive(10.0)
    assert dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.join("source").strpath) == msg

    dl = DownloadDirectory([ch1], tmpdir.join("source").strpath, tmpdir.join("dest").strpath)
    dl.download()

    # Check that directory 2 exists and has the correct mode
    dpath = tmpdir.join("dest","0","1","2")
    assert dpath.isdir()
    assert os.stat(dpath.strpath).st_mode == stat.S_IFDIR

    # Now remove directory 2 from remote and dest to allow their comparison
    dpath.chmod(0o700)
    shutil.rmtree(dpath.strpath)
    p.chmod(0o700)
    shutil.rmtree(p.strpath)

    cmpdirs(tmpdir.join("remote").strpath, tmpdir.join("dest").strpath)
  finally:
    if p.exists():
      p.chmod(0o700)
    d = tmpdir.join("dest", "0", "1", "2")
    if d.exists():
      d.chmod(0o700)
    d = tmpdir.join("source", "0", "1", "2")
    if d.exists():
      d.chmod(0o700)

def testDirectoryDownload_file_access_denied(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)

  shutil.copytree(tmpdir.join("remote").strpath, tmpdir.join("source").strpath)

  tmpdir.join("remote", "0", "One").remove()
  try:
    tmpdir.join("source", "0", "One").chmod(0o0)

    # Create a download channel.
    ch1 = execnet_gw.remote_exec(_file_transfer_remote_exec)
    ch1.send({'msg' : 'START_DOWNLOAD_CHANNEL', 'channel_id' : channel_id, 'remote_path' : tmpdir.join("source").strpath })
    msg = ch1.receive(10.0)
    assert dict(msg =  "READY", channel_id = channel_id, remote_path = tmpdir.join("source").strpath) == msg

    dl = DownloadDirectory([ch1], tmpdir.join("source").strpath, tmpdir.join("dest").strpath)
    dl.download()

    cmpdirs(tmpdir.join("remote").strpath, tmpdir.join("dest").strpath)
  finally:
    d = tmpdir.join("source", "0", "One")
    if d.exists():
      d.chmod(0o600)

def testDownloadHandler_rewrite_path():
  p1 = py.path.local("/One/Two/Three/source")
  p2 = py.path.local("/Five/Six/Seven/dest")

  dlh = DownloadHandler(p1.strpath, p2.strpath)
  expect = "/Five/Six/Seven/dest/file"
  actual = dlh.rewrite_path({'remote_path' : p1.join('file').strpath})
  assert actual == expect
