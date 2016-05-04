import collections
import os

CONTINUE = 1
STOP = 2
YIELD = 3

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

  def __iter__(self):
    return self.children.itervalues()

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
    self.rootnode = Node(root, False)

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

  def unlocked(self):
    """Returns the paths previously added to the tree that have been unlocked.

    Returns:
        iterator: Iterator returning unlocked paths.
    """

    def predicate(node, currpath, islocked):
      if islocked:
        return (STOP, None)

      if node.islocked == False:
        retpath = os.path.join(*currpath)
        if retpath == '':
          return (CONTINUE, None)
        else:
          return (YIELD, os.path.join(*currpath))
      return (CONTINUE, None)

    for path in self._walktree(predicate):
      yield path

  def locked(self):
    """Returns the paths previously added to the tree that are locked.

    Returns:
        iterator: Iterator returning unlocked paths.
    """

    def predicate(node, currpath, islocked):
      if islocked and not node.islocked is None:
        retpath = os.path.join(*currpath)
        if retpath == '':
          return (CONTINUE, None)
        else:
          return (YIELD, os.path.join(*currpath))
      return (CONTINUE, None)

    for path in self._walktree(predicate):
      yield path

  def unlock_tree(self, path):
    """Unlock path and anything below it by setting nodes with `islocked` == `True`
    to False and leaving any that are None as None"""

    node = self[path]

    def predicate(node, currpath, islocked):
      if node.islocked == True:
        node.islocked = False
      return (CONTINUE, None)

    for st in self._walktree(predicate, node):
      pass

  def islocked(self, path):
    tokens = self._splitpath(path)
    currnode = self.rootnode
    islocked = currnode.islocked
    for token in tokens:
      currnode = currnode.getChild(token)
      islocked = islocked or currnode.islocked
      if islocked:
        return True
    return False


  def _walktree(self, yieldpredicate, startnode = None):
    """Walk the tree from the root node.

    Accepts a function which determines behaviour at each point in the tree.
    This has the following signature:

    def predicate(node, currpath, islocked):
      ...

    Where `node` is `Node` instance at current tree level.
    `currpath` is list of strings giving the current tree path.
    `islocked` is the lock value logically ANDed with lock values visted until this point.

    Predicate should return a tuple (status, yield_value):
      `status` can be one of:

        * CONTINUE (indicating that tree traversal should continue without yielding `yield_value`).
        * STOP  (tree traversal should terminate and return `yield_value`, `yield_value` is None then no value will be yielded)
        * YIELD (tree traversal should continue after yielding `yield_value`)


    Args:
        yieldpredicate (TYPE): Description

    Returns:
        iterator: Tree iterator
    """
    def subtree(node, currpath, islocked):
      islocked = islocked  or node.islocked

      if currpath is None:
        currpath = []
      else:
        currpath = list(currpath)

      if node.path:
        currpath.append(node.path)

      (status,yield_value) = yieldpredicate(node, currpath, islocked)

      if status == STOP:
        if not yield_value is None:
          yield yield_value
        return
      elif status == YIELD:
        yield yield_value

      for child in node:
        for st in subtree(child, currpath, islocked):
          yield st

    rn = Node(None, None)
    if startnode is None:
      rn.children = self.rootnode.children
    else:
      rn.children = { startnode.path : startnode }


    for st in subtree(rn, None, False):
      yield st

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
        if pathstring[0] == "/":
          pathstring = pathstring[1:]

    tokens = []
    while pathstring != '':
      pathstring, leaf = os.path.split(pathstring)
      tokens.insert(0,leaf)
    return tokens

  # def __repr__(self):
  #   import StringIO
  #   paths = []

  #   def pred(node, currpath, islocked):
  #     paths.append("%s(%s)" % (node.path, node.islocked))
  #     return CONTINUE, None

  #   for st in self._walktree(pred):
  #     pass

  #   return "/".join(paths)+")"

  # def __str__(self):
  #   return repr(self)


