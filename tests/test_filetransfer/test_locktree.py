
from atsim.pro_fit.filetransfer.remote_exec import file_cleanup_remote_exec
from ._common import execnet_gw, channel_id

import posixpath
import os

import py.path

def testLockTree_2():
  root = "/one"
  lt = file_cleanup_remote_exec.LockTree(root)
  lt.add('two/three/four')
  lt.add('two')
  lt.add('two/three')
  lt.add('two')
  lt.add('two/a/b')
  lt.add('two/a/c')
  assert set(['two', 'two/three', 'two/three/four',   'two/a/b', 'two/a/c']) == set(lt.locked())
  assert set([]) == set(lt.unlocked())

  lt.unlock('two/three/four')
  assert set([ 'two', 'two/three', 'two/a/b', 'two/a/c']) == set(lt.locked())
  assert set(['two/three/four']) == set(lt.unlocked())

  lt.unlock('two')
  lt.unlock('two/three')
  assert set(['two', 'two/a/b', 'two/a/c']) == set(lt.locked())
  assert set(['two/three', 'two/three/four']) == set(lt.unlocked())

def testLockTree_3():
  root = "/one"
  lt = file_cleanup_remote_exec.LockTree(root)
  lt.add('two')
  lt.unlock('two')
  lt.add('two/a')
  lt.add('two/three')
  lt.unlock('two/a')

  assert set(['two', 'two/three']) == set(lt.locked())
  assert set(['two/a']) == set(lt.unlocked())


def testLockTree():
  lt = file_cleanup_remote_exec.LockTree("/root/directory")

  lt.add("one")

  assert lt.rootnode.path == '/root/directory'
  assert len(lt.rootnode.children) == 1
  child = lt.rootnode.children['one']
  assert len(child.children) == 0
  assert child.path == "one"
  assert child.islocked

  lt.add("two")

  assert set() == set(lt.unlocked())
  assert set(["one", "two"]) == set(lt.locked())

  lt.unlock("two")
  assert set(["two"]) == set(lt.unlocked())
  assert set(["one"]) == set(lt.locked())

  lt.remove("two")
  assert set() == set(lt.unlocked())
  assert set(["one"]) == set(lt.locked())

  lt.add("one/two/three/four")
  lt.add("one/two/file.txt")
  assert lt.rootnode.children["one"].islocked
  assert lt.rootnode.children["one"].children["two"].islocked is None
  assert lt.rootnode.children["one"].children["two"].children["three"].islocked is None
  assert lt.rootnode.children["one"].children["two"].children["three"].children["four"].islocked
  assert lt.rootnode.children["one"].children["two"].children["file.txt"].islocked

  lt.unlock('/root/directory/one/two')
  assert set([]) == set(lt.unlocked())
  assert set(["one/two/three/four", "one", "one/two/file.txt"]) == set(lt.locked())

  lt.unlock_tree("one/two/three")
  assert lt.rootnode.children["one"].islocked
  assert lt.rootnode.children["one"].children["two"].islocked is None
  assert lt.rootnode.children["one"].children["two"].children["three"].islocked is None
  assert lt.rootnode.children["one"].children["two"].children["three"].children["four"].islocked == False
  assert lt.rootnode.children["one"].children["two"].children["file.txt"].islocked

  assert set(["one/two/three/four"]) == set(lt.unlocked())
  assert set(["one", "one/two/file.txt"]) == set(lt.locked())

  lt.unlock("one")
  assert set(["one/two/three/four"]) == set(lt.unlocked())
  assert set(["one", "one/two/file.txt"]) == set(lt.locked())
  assert not lt.islocked("one/two/three")

  assert set(["/root/directory/one/two/three/four"]) == set(lt.unlocked(include_root = True))
  assert set(["/root/directory/one", "/root/directory/one/two/file.txt"]) == set(lt.locked(include_root = True))

  lt.unlock("one/two/file.txt")
  assert set(["one", "one/two/file.txt", "one/two/three/four"]) == set(lt.unlocked())
  assert set([ ]) == set(lt.locked())

def testLockTree_get():
  rootdir = "/this/is/the/root"
  lt = file_cleanup_remote_exec.LockTree(rootdir)

  lt.add("one/two/three")
  assert id(lt.rootnode.children["one"]) == id(lt["one"])
  assert id(lt.rootnode.children["one"].children["two"]) == id(lt["one/two"])
  assert id(lt.rootnode.children["one"].children["two"].children["three"]) == id(lt["one/two/three"])

  assert id(lt.rootnode.children["one"]) == id(lt[posixpath.join(rootdir, "one")])
  assert id(lt.rootnode.children["one"].children["two"]) == id(lt[posixpath.join(rootdir, "one/two")])
  assert id(lt.rootnode.children["one"].children["two"].children["three"]) == id(lt[posixpath.join(rootdir, "one/two/three")])

  try:
    lt["one/two/three/four"]
    assert False, "KeyError not raised"
  except KeyError:
    pass

def testLockTree_splitpath():
  lt = file_cleanup_remote_exec.LockTree("/this/is/the/root")
  assert ["one"] == lt._splitpath("one")
  assert ["one"] == lt._splitpath("/this/is/the/root/one")
  assert ["one", "two", "three"] == lt._splitpath("one/two/three")
