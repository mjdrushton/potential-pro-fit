from _common import execnet_gw, channel_id,  cmpdirs
from _common import create_dir_structure as _create_dir_structure

from atsim.pro_fit.filetransfer import UploadChannel, UploadDirectory, UploadHandler
from atsim.pro_fit.filetransfer import ChannelException

import py.path

def create_dir_structure(tmpdir):
  _create_dir_structure(tmpdir)
  rpath = tmpdir.join("remote")
  lpath = tmpdir.join("local")
  rpath.rename(lpath)
  dpath = tmpdir.join("dest")
  dpath.rename(rpath)

  with lpath.join("0", "1", "hello_world.txt").open("w") as outfile:
    print >>outfile, "Hello World!"


def do_ul(tmpdir, channels, dl = None, do_cmp = True):
  spath = tmpdir.join("local")
  dpath = tmpdir.join("remote")
  if dl is None:
    dl = UploadDirectory(channels, spath.strpath, dpath.strpath)
  dl.upload()

  # Compare the remote and dest directories
  if do_cmp:
    cmpdirs(spath.strpath, dpath.strpath)


def testUploadChannel_BadStart_nonexistent_directory(execnet_gw, channel_id):
  badpath = "/this/is/not/a/path"
  assert not py.path.local(badpath).exists()

  try:
    ch = UploadChannel(execnet_gw, badpath, channel_id = channel_id)
    assert False,  "ChannelException should have been raised."
  except ChannelException,e:
    pass

  assert e.message.endswith('are existing directories.')

def testDirectoryUpload_single_channel(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  # Create a upload channel.
  ch1 = UploadChannel(execnet_gw, tmpdir.join("remote").strpath, channel_id = channel_id)
  do_ul(tmpdir, ch1)

def testUploadHandler_rewrite_remote_path():
  ulh = UploadHandler('/var/private/source')

  msg = {'remote_path' : '/var/private/source/hello.txt'}
  assert { 'remote_path' : "hello.txt" } == ulh.rewrite_file_path(msg)

  msg = {'remote_path' : '/var/private/source/one/two'}
  assert { 'remote_path' : "one/two" } == ulh.rewrite_directory_path(msg)

def testDirectoryUpload_empty_directory(tmpdir, execnet_gw, channel_id):
  assert False, "Not Implemented"

