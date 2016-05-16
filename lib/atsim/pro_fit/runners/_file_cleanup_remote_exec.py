import os
import uuid

import threading
import Queue
import shutil
import types

# lock = threading.RLock()

#INCLUDE "_locktree.py"

#INCLUDE "_remote_exec_funcs.py.inc"

log =  open("/Users/mr498/Desktop/log.log", "w")

class FileDeleter(object):

  class _DeletionThread(threading.Thread):

    def __init__(self, root_path):
      self.queue = Queue.Queue()
      self.root_path = root_path
      super(FileDeleter._DeletionThread, self).__init__()

    def run(self):
      item = self.queue.get(True)
      while item:
        if os.path.exists(item):
          shutil.rmtree(item, ignore_errors=True)
        self.queue.task_done()
        item = self.queue.get(True)
      self.queue.task_done()
      shutil.rmtree(self.root_path, ignore_errors=True)

    def put(self, path):
      self.queue.put(path)

  def __init__(self, root_path):
    self._deletion_thread = FileDeleter._DeletionThread(root_path)
    self._deletion_thread.daemon = False
    self._locktree = LockTree(root_path)
    self._deletion_thread.start()

  def lock(self, remote_path):
    """Register `remote_path` with `FileDeleter`"""
    print >>log, "Lock", remote_path
    self._locktree.add(remote_path)
    print >>log, "Locked", list(self._locktree.locked())
    print >>log, "Unlocked", list(self._locktree.unlocked())

  def unlock(self, remote_path):
    """Unlock and delete everything beneath `remote_path`"""
    print >>log, "Unlock", remote_path
    self._locktree.unlock(remote_path)
    print >>log, "Locked", list(self._locktree.locked())
    print >>log, "Unlocked", list(self._locktree.unlocked())

    unlocked = list(self._locktree.unlocked(include_root = True))
    for p in unlocked:
      try:
        self._locktree.remove(p)
      except KeyError:
        pass
      # TODO: Filter out paths that have common ancestors (i.e. only delete the parents)
      self._deletion_thread.put(p)

  def finish(self):
    """Terminate deletion thread and delete `root_path`"""
    self._deletion_thread.put(None)
    self._deletion_thread.join()

  def flush(self):
    """Blocks until items currently being processed by the `Deleter`
    have been completed"""
    self._deletion_thread.queue.join()


def _getpath(msg, channel, channel_id):
  path = msg.get("remote_path", None)
  if path is None:
    error(channel, channel_id,
    "Could not find 'remote_path' argument in 'DOWNLOAD_FILE' request'",
    ("MSGERROR", "KEYERROR"),
    key = 'remote_path')
    return
  return path

def _getmsgid(msg, channel, channel_id):
  transid = msg.get("id", None)
  if transid is None:
    error(channel, channel_id,
    "Could not find 'id' argument in 'DOWNLOAD_FILE' request'",
    ("MSGERROR", "KEYERROR"),
    key = 'id')
    return
  return transid

def _deleter_action(msg, channel, channel_id, remote_root, action, confirm_msg):

  def error_action(p):
    try:
      action(p)
      return True
    except KeyError, e:
      error(channel, channel_id,
        "path not registerd with cleanup agent",
        ("PATHERROR", "UNKNOWN_PATH"),
        remote_path = p,
        keyerror = str(e))
    return None

  path = _getpath(msg, channel, channel_id)
  if path is None:
    return
  transid = _getmsgid(msg, channel, channel_id)
  if transid is None:
    return

  if type(path) is types.ListType or type(path) is types.TupleType:
    for p in path:
      p  = normalize_path_with_error(channel, channel_id, remote_root, p)
      if p is None:
        return
      if error_action(p) is None:
        return
  else:
    path  = normalize_path_with_error(channel, channel_id, remote_root, path)
    if path is None:
      return
    if error_action(path) is None:
      return

  channel.send({'msg' : confirm_msg, 'channel_id' : channel_id, 'id' : transid})


def lock(msg, channel, channel_id, remote_root, deleter):
  def action(p):
    deleter.lock(p)
  _deleter_action(msg, channel, channel_id, remote_root, action, "LOCKED")

def unlock(msg, channel, channel_id, remote_root, deleter):
  def action(p):
    deleter.unlock(p)
  _deleter_action(msg, channel, channel_id, remote_root, action, "UNLOCKED")

def flush(msg, channel, channel_id, deleter):
  transid = _getmsgid(msg, channel, channel_id)
  if transid is None:
    return
  deleter.flush()
  channel.send({'msg' : 'FLUSHED', 'channel_id' : channel_id, 'id' : transid})

def cleanup_remote_exec(channel, channel_id, remote_root):
  ready(channel, channel_id, remote_root)
  deleter = FileDeleter(remote_root)
  try:
    for msg in channel:
      if msg is None:
        break
      mtype = extract_mtype(msg, channel, channel_id)
      if mtype is None:
        continue
      elif mtype == 'LOCK':
        lock(msg, channel, channel_id, remote_root, deleter)
      elif mtype == 'UNLOCK':
        unlock(msg, channel, channel_id, remote_root, deleter)
      elif mtype == 'FLUSH':
        flush(msg, channel, channel_id, deleter)
      else:
        error(channel, channel_id,
          "Unknown 'msg' type: '%s'" % (mtype,),
          ("MSGERROR", "UNKNOWN_MSGTYPE"),
          mtype = mtype)
  finally:
    deleter.finish()

def process_path(channel, channel_id, remote_path):
  # If the final element of remote_path doesn't exist, but the rest of it does, then make the directory
  remote_path = os.path.realpath(remote_path)

  if not os.path.exists(remote_path):
    error(channel, channel_id, "path does not exist: '%s'" % remote_path,
      ("IOERROR", "FILEDOESNOTEXIST"),
      remote_path = remote_path)
    return False, remote_path


  if os.path.isdir(remote_path):
    return True, remote_path

  if os.path.exists(remote_path):
    error(channel,
      channel_id,
      "'remote_path' exists but is not a directory '%s'" % remote_path,
      ("IOERROR","REMOTE_IS_NOT_DIRECTORY"))
    return False, remote_path

  error(channel,
  channel_id,
  "'remote_path' is not valid '%s'" % remote_path,
  ("IOERROR","REMOTE_IS_NOT_VALID"))
  return False, remote_path


def start_channel(channel):
  msg = channel.receive()
  mtype = extract_mtype(msg, channel, None)

  if not mtype == 'START_CLEANUP_CHANNEL':
    error(channel,
          None,
          'was expecting "START_CLEANUP_CHANNEL" got "%s" instead' % (mtype,),
          ("MSGERROR", "UNEXPECTED_MSG"))
    return

  channel_id = msg.get('channel_id', str(uuid.uuid4()))
  remote_path = msg.get('remote_path', None)
  if not msg.has_key('remote_path'):
    error(channel,
      channel_id,
      "UPLOAD message does not contain 'remote_path' argument for msg id = '%s'" % msg.get('id', None)
      ("MSGERROR", "KEYERROR"),
      key = 'remote_path')
    return False

  status, remote_path = process_path(channel, channel_id, remote_path)
  if not status:
    return

  cleanup_remote_exec(channel, channel_id, remote_path)


if __name__ == '__channelexec__':
  start_channel(channel)
