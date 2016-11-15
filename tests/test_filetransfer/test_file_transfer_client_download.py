
from atsim.pro_fit.filetransfer import DownloadDirectory, DownloadHandler, DownloadCancelledException
from atsim.pro_fit.filetransfer import DownloadChannel, DownloadChannels, ChannelException

import os
import shutil
import stat
import threading

from _common import execnet_gw, channel_id, create_dir_structure, cmpdirs

import py.path

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

def do_dl(tmpdir, channels, dl = None, do_cmp = True):
  rpath = tmpdir.join("remote")
  dpath = tmpdir.join("dest")
  if dl is None:
    dl = DownloadDirectory(channels, rpath.strpath, dpath.strpath)
  dl.download()

  # Compare the remote and dest directories
  if do_cmp:
    cmpdirs(rpath.strpath, dpath.strpath)

def testDownloadChannel_BadStart_nonexistent_directory(execnet_gw, channel_id):
  badpath = "/this/is/not/a/path"
  assert not py.path.local(badpath).exists()
  try:
    ch = DownloadChannel(execnet_gw, badpath, channel_id = channel_id)
    assert False,  "ChannelException should have been raised."
  except ChannelException,e:
    pass

  assert e.message.endswith('path does not exist or is not a directory')

def testDirectoryDownload_single_channel(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  # Create a download channel.
  ch1 = DownloadChannel(execnet_gw, tmpdir.join("remote").strpath, channel_id = channel_id)
  do_dl(tmpdir, ch1)

def testDirectoryDownload_emptysrc(tmpdir, execnet_gw, channel_id):
  tmpdir.join("remote").mkdir()
  tmpdir.join("dest").mkdir()
  ch1 = DownloadChannel(execnet_gw, tmpdir.join("remote").strpath, channel_id = channel_id)
  do_dl(tmpdir, ch1)

def testDirectoryDownload_emptytree(tmpdir, execnet_gw, channel_id):
  tmpdir.join("remote").mkdir()
  os.makedirs(tmpdir.join("one", "two", "three", "four").strpath)
  tmpdir.join("dest").mkdir()
  ch1 = DownloadChannel(execnet_gw, tmpdir.join("remote").strpath, channel_id = channel_id)
  do_dl(tmpdir, ch1)

def testDirectoryDownload_missing_dest(tmpdir, execnet_gw, channel_id):
  # Errors to test
  # Destination doesn't exist
  rpath = tmpdir.join("remote")
  rpath.mkdir()
  dpath = tmpdir.join("dest")
  ch1 = DownloadChannel(execnet_gw, tmpdir.join("remote").strpath, channel_id = channel_id)

  try:
    dl = DownloadDirectory(ch1, rpath.strpath, dpath.strpath)
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
    ch1 = DownloadChannel(execnet_gw, tmpdir.join("source").strpath, channel_id = channel_id)

    dl = DownloadDirectory(ch1, tmpdir.join("source").strpath, tmpdir.join("dest").strpath)
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
    ch1 = DownloadChannel(execnet_gw, tmpdir.join("source").strpath, channel_id = channel_id)
    dl = DownloadDirectory(ch1, tmpdir.join("source").strpath, tmpdir.join("dest").strpath)
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

def testDownloadHandler_complete_callback(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  # Create a download channel.
  ch1 = DownloadChannel(execnet_gw, tmpdir.join("remote").strpath, channel_id = channel_id)

  class DLH(DownloadHandler):
    def __init__(self, remote_path, dest_path):
      self.complete_called = False
      self.complete_exception = None
      super(DLH,self).__init__(remote_path, dest_path)

    def finish(self, error = None):
      self.complete_called = True
      self.completion_exception = error

  remote_path = tmpdir.join("remote")
  dest_path = tmpdir.join("dest")

  dlh = DLH(remote_path.strpath, dest_path.strpath)
  dl = DownloadDirectory(
      ch1,
      remote_path.strpath,
      dest_path.strpath,
      dlh)

  do_dl(tmpdir, ch1, dl)
  assert dlh.complete_called == True
  assert dlh.completion_exception is None

  # Now test what happens if no exception is passed to DownloadHandler.finish() but finish itself raises.
  class ThrowMe(Exception):
    pass

  class ThrowHandler(DLH):
    def finish(self, exception = None):
      super(ThrowHandler, self).finish(exception)
      raise ThrowMe()
      return False

  dest_path.remove(rec=True)
  dest_path.ensure_dir()
  dlh = ThrowHandler(remote_path.strpath, dest_path.strpath)
  dl = DownloadDirectory(
      ch1,
      remote_path.strpath,
      dest_path.strpath,
      dlh)

  try:
    do_dl(tmpdir, ch1, dl, False)
    assert False, "ThrowMe exception should have been raised but wasn't"
  except ThrowMe:
    pass
  assert dlh.complete_called == True
  assert dlh.completion_exception is None

  # Now try again when an error-occurs - destination isn't writable should throw an error.
  dest_path.chmod(0o0)
  try:
    dlh = DLH(remote_path.strpath, dest_path.strpath)
    dl = DownloadDirectory(
        ch1,
        remote_path.strpath,
        dest_path.strpath,
        dlh)

    try:
      do_dl(tmpdir, ch1, dl, False)
      assert False, "OSError should have been raised."
    except OSError:
      pass

    assert dlh.complete_called == True
    assert type(dlh.completion_exception) == OSError

    # Now check supresssion of exception raising.
    class NoRaise(DLH):
      def finish(self, error = None):
        super(NoRaise, self).finish(error)
        return False

    dlh = NoRaise(remote_path.strpath, dest_path.strpath)
    dl = DownloadDirectory(
        ch1,
        remote_path.strpath,
        dest_path.strpath,
        dlh)

    do_dl(tmpdir, [ch1], dl, False)
    assert dlh.complete_called == True
    assert type(dlh.completion_exception) == OSError

    # Test that DownloadHandler.complete can raise exception and this will be correctly propagated.
    dlh = ThrowHandler(remote_path.strpath, dest_path.strpath)
    dl = DownloadDirectory(
        ch1,
        remote_path.strpath,
        dest_path.strpath,
        dlh)

    try:
      do_dl(tmpdir, [ch1], dl, False)
      assert False, "ThrowMe exception should have been raised but wasn't"
    except ThrowMe:
      pass
    assert dlh.complete_called == True
    assert type(dlh.completion_exception) == OSError

  finally:
    dest_path.chmod(0o700)

def testDirectoryDownload_multiple_channels(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  channels = []
  multichannel = DownloadChannels(execnet_gw, tmpdir.join("remote").strpath, 4, channel_id = channel_id)
  do_dl(tmpdir, multichannel)

def testDirectoryDownload_channel_reuse(tmpdir, execnet_gw, channel_id):
  create_dir_structure(tmpdir)
  ch1 = DownloadChannel(execnet_gw, tmpdir.join("remote").strpath)
  do_dl(tmpdir, ch1)
  dest_path = tmpdir.join("dest")
  dest_path.remove(rec = True)
  dest_path.ensure_dir()
  do_dl(tmpdir, ch1)

def testDirectoryDownload_create_multiple_downloads(tmpdir, execnet_gw, channel_id):
  # import logging
  # import sys
  source1 = tmpdir.join("source_1")
  source2 = tmpdir.join("source_2")

  source1.ensure_dir()
  source2.ensure_dir()

  with source1.join("file.txt").open("w") as outfile:
    print >>outfile, "Hello"

  with source2.join("file.txt").open("w") as outfile:
    print >>outfile, "Goodbye"

  dest1 = tmpdir.join("dest_1")
  dest2 = tmpdir.join("dest_2")

  dest1.ensure_dir()
  dest2.ensure_dir()

  ch1 = DownloadChannel(execnet_gw, tmpdir.strpath)
  dl1 = DownloadDirectory(ch1, source1.strpath, dest1.strpath)
  dl2 = DownloadDirectory(ch1, source2.strpath, dest2.strpath)

  dl1.download()
  assert dest1.join("file.txt").isfile()
  line = dest1.join("file.txt").open().next()[:-1]
  assert line == "Hello"

  dl2.download()
  assert dest2.join("file.txt").isfile()
  line = dest2.join("file.txt").open().next()[:-1]
  assert line == "Goodbye"

def testDirectoryDownload_test_nonblocking(tmpdir, execnet_gw, channel_id):
  import threading
  import logging
  import sys

  source1 = tmpdir.join("source_1")
  source1.ensure_dir()

  with source1.join("file.txt").open("w") as outfile:
    print >>outfile, "Hello"

  dest1 = tmpdir.join("dest_1")

  dest1.ensure_dir()

  pause_event = threading.Event()
  download_event = threading.Event()
  class PauseDownloadHandler(DownloadHandler):

    def writefile(self, msg):
      download_event.set()
      pause_event.wait()
      return super(PauseDownloadHandler, self).writefile(msg)

  class CallEventThread(threading.Thread):

    def __init__(self, dest1, paus_event):
      self.dest1 = dest1
      self.pause_event = pause_event
      super(CallEventThread, self).__init__()

    def run(self):
      download_event.wait()
      import time
      time.sleep(1)
      self.dest1_state = self.dest1.join("file.txt").exists()
      self.pause_event.set()

  ct = CallEventThread(dest1, pause_event)
  ct.start()

  ch1 = DownloadChannel(execnet_gw, tmpdir.strpath)
  dl1 = DownloadDirectory(ch1, source1.strpath, dest1.strpath, PauseDownloadHandler(source1.strpath, dest1.strpath))
  finished_event = dl1.download(non_blocking = True)
  import time
  time.sleep(2)
  finished_event.wait(10)
  assert dest1.join("file.txt").isfile()
  line = dest1.join("file.txt").open().next()[:-1]
  assert line == "Hello"

  assert not ct.dest1_state

def testDirectoryDownload_cancel(tmpdir, execnet_gw, channel_id):
  import threading
  import logging
  import sys

  source1 = tmpdir.join("source_1")
  source1.ensure_dir()

  with source1.join("file.txt").open("w") as outfile:
    print >>outfile, "Hello"

  with source1.join("file2.txt").open("w") as outfile:
    print >>outfile, "Hello"

  dest1 = tmpdir.join("dest_1")

  dest1.ensure_dir()

  pause_event = threading.Event()
  download_event = threading.Event()
  class PauseDownloadHandler(DownloadHandler):

    def __init__(self, *args, **kwargs):
      super(PauseDownloadHandler, self).__init__(*args, **kwargs)
      self.finishCalled = False
      self.exception = None

    def writefile(self, msg):
      pause_event.wait(3)
      return super(PauseDownloadHandler, self).writefile(msg)

    def finish(self, exception = None):
      self.finishCalled =True
      self.exception = exception
      return None

  ch1 = DownloadChannel(execnet_gw, tmpdir.strpath)
  dlh = PauseDownloadHandler(source1.strpath, dest1.strpath)
  dl1 = DownloadDirectory(ch1, source1.strpath, dest1.strpath, dlh)
  finished_event = dl1.download(non_blocking = True)
  cancel_event = dl1.cancel()
  pause_event.set()
  assert cancel_event.wait(4)
  assert finished_event.wait(4)
  assert dlh.finishCalled

  try:
    raise dlh.exception
  except DownloadCancelledException:
    pass
