from _common import execnet_gw, channel_id,  cmpdirs
from _common import create_dir_structure as _create_dir_structure

from atsim.pro_fit.filetransfer import UploadChannel, UploadDirectory, UploadHandler
from atsim.pro_fit.filetransfer import ChannelException, DirectoryUploadException

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

def testDirectoryUpload_local_nonexistent(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)

  spath = tmpdir.join("local")
  dpath = tmpdir.join("remote")
  spath.remove(rec = True)

  assert not spath.exists()

  # Create a upload channel.
  ch1 = UploadChannel(execnet_gw, tmpdir.join("remote").strpath, channel_id = channel_id)
  try:
    dl = UploadDirectory(ch1, spath.strpath, dpath.strpath)
    assert False, "OSError should have been raised, it wasn't."
  except OSError:
    pass


def testUploadHandler_rewrite_remote_path():
  ulh = UploadHandler('/var/private/source')

  msg = {'remote_path' : '/var/private/source/hello.txt'}
  assert { 'remote_path' : "hello.txt" } == ulh.rewrite_file_path(msg)

  msg = {'remote_path' : '/var/private/source/one/two'}
  assert { 'remote_path' : "one/two" } == ulh.rewrite_directory_path(msg)

def testUploadHandler_complete_callback(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  # Create a download channel.
  ch1 = UploadChannel(execnet_gw, tmpdir.join("remote").strpath, channel_id = channel_id)

  class ULH(UploadHandler):
    def __init__(self, local_path):
      self.complete_called = False
      self.complete_exception = None
      super(ULH,self).__init__(local_path)

    def finish(self, error = None):
      self.complete_called = True
      self.completion_exception = error

  remote_path = tmpdir.join("remote")
  local_path = tmpdir.join("local")

  ulh = ULH(local_path.strpath)
  ul = UploadDirectory(
      ch1,
      local_path.strpath,
      remote_path.strpath,
      ulh)

  do_ul(tmpdir, ch1, ul)
  assert ulh.complete_called == True
  assert ulh.completion_exception is None

  # Now test what happens if no exception is passed to DownloadHandler.finish() but finish itself raises.
  class ThrowMe(Exception):
    pass

  class ThrowHandler(ULH):
    def finish(self, exception = None):
      super(ThrowHandler, self).finish(exception)
      raise ThrowMe()
      return False

  remote_path.remove(rec=True)
  remote_path.ensure_dir()
  ulh = ThrowHandler(local_path.strpath)
  ul = UploadDirectory(
      ch1,
      local_path.strpath,
      remote_path.strpath,
      ulh)

  try:
    do_ul(tmpdir, ch1, ul, False)
    assert False, "ThrowMe exception should have been raised but wasn't"
  except ThrowMe:
    pass
  assert ulh.complete_called == True
  assert ulh.completion_exception is None

  # Now try again when an error-occurs - destination isn't writable should throw an error.
  remote_path.chmod(0o0)
  try:
    ulh = ULH(local_path.strpath)
    ul = UploadDirectory(
        ch1,
        local_path.strpath,
        remote_path.strpath,
        ulh)

    try:
      do_ul(tmpdir, ch1, ul, False)
      assert False, "DirectoryUploadException should have been raised."
    except DirectoryUploadException:
      pass

    assert ulh.complete_called == True
    assert type(ulh.completion_exception) == DirectoryUploadException

    # Now check supresssion of exception raising.
    class NoRaise(ULH):
      def finish(self, error = None):
        super(NoRaise, self).finish(error)
        return False

    ulh = NoRaise(local_path.strpath)
    ul = UploadDirectory(
        ch1,
        local_path.strpath,
        remote_path.strpath,
        ulh)

    do_ul(tmpdir, [ch1], ul, False)
    assert ulh.complete_called == True
    assert type(ulh.completion_exception) == DirectoryUploadException

    # Test that DownloadHandler.complete can raise exception and this will be correctly propagated.
    ulh = ThrowHandler(local_path.strpath)
    ul = UploadDirectory(
        ch1,
        local_path.strpath,
        remote_path.strpath,
        ulh)
  finally:
    local_path.chmod(0o700)


