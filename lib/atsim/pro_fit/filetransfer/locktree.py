import collections
import os

CONTINUE = 1
STOP = 2
YIELD = 3

def nullVisitor(node, state):
  return None

class Node(object):
  def __init__(self, path, islocked):
    self.path = path
    self.children= {}
    self.islocked = islocked

  def getChild(self, child):
    return self.children.get(child, None)

  def addChild(self, child_path, lockstatus):
    if not self.getChild(child_path) is None:
      raise KeyError("Child already exists: '%s'" % child_path)
    child = Node(child_path, lockstatus)
    self.children[child_path] = child
    return child

  def treeIterator(self, visitor =nullVisitor, state = None):
    state = visitor(self, state)
    yield (self,state)
    for child in self.children.itervalues():
      for (node,childstate) in child.treeIterator(visitor, state):
        yield (node,childstate)

  def __repr__(self):
    return "Node(path='%s', islocked=%s)"  % (self.path, self.islocked )

  def __str__(self):
    return repr(self)


class LockTreeException(Exception):
  pass

class LockTree(object):
  """Tree of file paths allowing locking and unlocking of sub-trees."""

  def __init__(self, root):
    """Create LockTree

    Args:
        root (str): Root path for tree.
    """
    self.root = root
    self.rootnode = Node(root, None)

  def add(self, path):
    """Add path to tree and lock it.

    Args:
        path (str): Path to add.
    """
    tokens = self._splitpath(path)
    currnode = self.rootnode

    for token in tokens:
      child = currnode.getChild(token)
      if not child:
        currnode = currnode.addChild(token, None)
      else:
        currnode = child
    currnode.islocked = True

  def remove(self, path):
    parent, child = os.path.split(path)
    node = self[parent]
    del node.children[child]


  def _registered(self):
    VisitorState = collections.namedtuple("VisitorTuple", ("path_components", "is_locked"))

    def visitor(node, state):
      path_components = list(state.path_components)
      path_components.append(node.path)
      return VisitorState(path_components, node.islocked)

    for node,state in self.rootnode.treeIterator(visitor, VisitorState(tuple(), None)):
      if state.is_locked != None:
        yield node, state.path_components

  def unlocked(self, include_root = False):
    """Returns the paths previously added to the tree that have been unlocked.

    Args:
        include_root (bool) : If True, returned paths are joined with the tree's root-path, otherwise paths are relative to the root.

    Returns:
        iterator: Iterator returning unlocked paths.
    """
    return self._locked_or_unlocked(False, include_root)

  def locked(self, include_root = False):
    """Returns the paths previously added to the tree that are locked.

    Args:
        include_root (bool) : If True, returned paths are joined with the tree's root-path, otherwise paths are relative to the root.

    Returns:
        iterator: Iterator returning unlocked paths.
    """
    return self._locked_or_unlocked(True, include_root)

  def _locked_or_unlocked(self, teststate, include_root):
    for (node,path_components) in self._registered():
      if self.islocked_node(node) == teststate:
        if not include_root:
          path_components = path_components[1:]
        if not path_components:
          yield ''
        else:
          yield os.path.join(*path_components)

  def unlock_tree(self, path):
    """Unlock path and anything below it by setting nodes with `islocked` == `True`
    to False and leaving any that are None as None"""
    startnode = self[path]
    for node, state in startnode.treeIterator():
      if not node.islocked is None:
        node.islocked = False

  def islocked_node(self, startnode):
    def visitor(node, state):
      return node.islocked

    for node, state in startnode.treeIterator(visitor, None):
      if state == True:
        return True
    return False

  def islocked(self, path):
    startnode = self[path]
    return self.islocked_node(startnode)

  def unlock(self, path):
    node = self[path]
    if not node.islocked is None:
      node.islocked = False
    return node

  def __getitem__(self, path):
    tokens = self._splitpath(path)
    currnode = self.rootnode
    for token in tokens:
      currnode = currnode.children[token]
    return currnode

  def _splitpath(self, pathstring):
    if os.path.isabs(pathstring):
      pathstring = os.path.normpath(pathstring)
      if not pathstring.startswith(self.root):
        raise LockTreeException("Absolute path does not lie within root ('%s'): '%s'" % (self.root, pathstring))
      else:
        pathstring = pathstring.replace(self.root, "", 1)
        if len(pathstring) > 0 and pathstring[0] == "/":
          pathstring = pathstring[1:]

    tokens = []
    while pathstring != '':
      pathstring, leaf = os.path.split(pathstring)
      tokens.insert(0,leaf)
    return tokens
