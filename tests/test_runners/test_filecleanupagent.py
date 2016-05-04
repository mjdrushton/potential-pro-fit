
from atsim.pro_fit.runners import _file_cleanup_remote_exec

import posixpath

def testLockTree():
  lt = _file_cleanup_remote_exec.LockTree("/root/directory")

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

  assert set([]) == set(lt.unlocked())
  assert set(["one", "one/two/file.txt", "one/two/three/four"]) == set(lt.locked())

  lt.unlock("one")
  assert set(["one", "one/two/three/four"]) == set(lt.unlocked())
  assert set(["one/two/file.txt"]) == set(lt.locked())
  assert not lt.islocked("one/two/three")

  # lt.unlock("one/two")
  # assert set(["one/two/three/four"]) == set(lt.unlocked())
  # assert set([ "one/two", "one", "one/two/file.txt"]) == set(lt.locked())

  lt.unlock("one/two/file.txt")
  assert set(["one", "one/two/file.txt", "one/two/three/four"]) == set(lt.unlocked())
  assert set([ ]) == set(lt.locked())


def testLockTree_get():
  rootdir = "/this/is/the/root"
  lt = _file_cleanup_remote_exec.LockTree(rootdir)

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
  lt = _file_cleanup_remote_exec.LockTree("/this/is/the/root")
  assert ["one"] == lt._splitpath("one")
  assert ["one"] == lt._splitpath("/this/is/the/root/one")
  assert ["one", "two", "three"] == lt._splitpath("one/two/three")
