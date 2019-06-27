def ready(channel, channel_id, remote_path):
  channel.send(dict(
    msg = 'READY',
    channel_id = channel_id,
    remote_path = remote_path))

def error(channel, channel_id, reason, error_code, **extra_args):
  msg = dict(msg = "ERROR",
             reason = reason,
             error_code = error_code)
  if channel_id:
    msg['channel_id'] = channel_id

  msg.update(extra_args)
  channel.send(msg)

def normalize_path(remote_root, dest_path):
  dest_path = os.path.normpath(dest_path)
  if os.path.isabs(dest_path):
    cp = os.path.commonprefix([remote_root, dest_path])
    dest_path = dest_path.replace(cp, "", 1)
    if os.path.isabs(dest_path):
      dest_path = dest_path[1:]

  dest_path = os.path.join(remote_root, dest_path)
  dest_path = os.path.realpath(dest_path)
  # Check that the destination path hasn't escaped from the file root
  cp = os.path.commonprefix([remote_root, dest_path])
  if cp != remote_root:
    return None
  return dest_path

def normalize_path_with_error(channel, channel_id, remote_root, remote_path, trans_id = None):
  remote_path = normalize_path(remote_root, remote_path)
  if remote_path is None:
    kwargs = {'remote_path' : remote_path}
    if not trans_id is None:
      kwargs['id'] = trans_id
    error(channel,
      channel_id,
      "path does not lie within directory structure.",
      ("PATHERROR", "NOTCHILD"),
      **kwargs)
    return None
  return remote_path

def child_path(channel, channel_id, remote_root, msg):
  """Extract 'remote_path' from msg and normalize path.
  Create error message and return None if path not found to be under `remote_root` """
  if not 'remote_path' in msg:
    error(channel,
      channel_id,
      "message does not contain 'remote_path' argument for msg id = '%s'" % msg.get('id', None),
      ("MSGERROR", "KEYERROR"),
      key = 'remote_path')
    return None
  remote_path = msg['remote_path']
  return normalize_path_with_error(channel, channel_id, remote_root, remote_path)

def extract_mtype(msg, channel, channel_id):
  try:
    mtype = msg.get('msg', None)
  except Exception:
    error(channel,
      channel_id,
      "Malformed message",
      ("MSGERROR", "MALFORMED"),
      request = msg)
    return None

  if mtype is None:
    error(channel,
      channel_id,
      "'msg' not found in message",
      ("MSGERROR", "KEYERROR"),
      key = 'msg')
    return None

  return mtype
