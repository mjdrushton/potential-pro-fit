import os
import shutil
import stat
import tempfile
import uuid

from pytest import fixture

import execnet

DIR = 0
FILE = 1

def _compareDir(path):
  actual = []
  for f in os.listdir(path):
    fullpath = os.path.join(path, f)
    mode = os.stat(fullpath).st_mode
    if stat.S_ISDIR(mode):
      m = DIR
    elif stat.S_ISREG(mode):
      m = FILE
    else:
      m = None
    actual.append((f, m))
  return actual

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
