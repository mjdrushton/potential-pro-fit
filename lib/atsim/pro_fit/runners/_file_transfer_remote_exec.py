import uuid
import tempfile
import os

def ready(channel, channel_id, remote_path):
  channel.send(dict(
    msg = 'READY',
    channel_id = channel_id,
    remote_path = remote_path))

def error(channel, channel_id, reason, **extra_args):
  msg = dict(msg = "ERROR",
             reason = reason)
  if channel_id:
    msg['channel_id'] = channel_id

  msg.update(extra_args)
  channel.send(msg)

def mktempdir(channel, channel_id):
  try:
    tmpdir = tempfile.mkdtemp()
    return tmpdir
  except Exception, e:
    error(channel, channel_id, "Couldn't create temporary directory. '%s' " % e)
    return False

def process_path(channel, channel_id, remote_path):
  # If the final element of remote_path doesn't exist, but the rest of it does, then make the directory
  remote_path = os.path.realpath(remote_path)

  if os.path.isdir(remote_path):
    return True, remote_path

  if os.path.exists(remote_path):
    error(channel, channel_id, "'remote_path' exists but is not a directory '%s'" % remote_path)
    return False, remote_path

  rootpath = os.path.dirname(remote_path)
  if not os.path.isdir(rootpath):
    error(channel, channel_id, "'remote_path' is invalid, neither '%s' or '%s' are existing directories." % (remote_path, rootpath))
    return False, remote_path

  try:
    os.mkdir(remote_path)
    return True, remote_path
  except Exception, e:
    error(channel, channel_id, "Couldn't create directory. '%s' reason '%s'" % (remote_path, e))
    return False, remote_path

def chkpath(channel, channel_id, remote_path):
  # Ensure that the destination directory is writeable
  if not os.access(remote_path, os.W_OK):
    error(channel, channel_id, "Directory is not writeable.", remote_path = remote_path)
    return False, remote_path
  return True, remote_path

def normalize_path(remote_root, dest_path):
  # import pdb;pdb.set_trace()
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

def upload(channel, channel_id, remote_root, msg):

  if not msg.has_key('id'):
    error(channel, channel_id, "UPLOAD message does not contain 'id' argument")
    return False

  if not msg.has_key('remote_path'):
    error(channel, channel_id, "UPLOAD message does not contain 'remote_path' argument for msg id = '%s'" % msg.get('id', None))
    return False

  file_data = msg.get("file_data", "")
  mode = msg.get("mode", None)
  fileid = msg['id']
  remote_path = msg['remote_path']
  rp = normalize_path(remote_root, remote_path)

  if rp is None:
    error(channel, channel_id, "Error 'remote_path' cannot be converted to path under upload directory.",
    remote_path = remote_path,
    id = fileid)
    return

  remote_path = rp

  try:
    # Create directory if necessary.
    dname = os.path.dirname(remote_path)
    if not os.path.exists(dname):
      os.makedirs(dname)

    with open(remote_path, 'wb') as outfile:
      outfile.write(file_data)
  except Exception,e:
    error(channel, channel_id, "Error writing file: '%s'" % str(e),
      remote_path = remote_path,
      id = fileid)
    return False

  if mode:
    os.chmod(remote_path, mode)

  uploaded_msg = dict(msg = 'UPLOADED', channel_id = channel_id,
    id = fileid,
    remote_path = remote_path)

  channel.send(uploaded_msg)
  return True

def upload_remote_exec(channel, channel_id, remote_path):
  if remote_path is None:
    remote_path = mktempdir(channel, channel_id)
    if not remote_path:
      return
  else:
    rc, remote_path = process_path(channel, channel_id, remote_path)
    if not remote_path:
      return

  rc,remote_path = chkpath(channel, channel_id, remote_path)
  if not rc:
    return

  ready(channel, channel_id, remote_path)

  for msg in channel:
    if msg is None:
      return

    try:
      mtype = msg.get('msg', None)
    except Exception:
      error(channel, channel_id, "Malformed message")
      continue

    if mtype is None:
      error(channel, channel_id, "'msg' not found in message")
      continue

    if mtype == 'UPLOAD':
      upload(channel, channel_id, remote_path, msg)

def start_channel(channel):
  msg = channel.receive()

  mtype = msg.get('msg', None)
  channeltypes = {'START_UPLOAD_CHANNEL' : upload_remote_exec,
                  'START_DOWNLOAD_CHANNEL' : None}

  if not mtype in channeltypes:
    error(channel,
          None,
          'was expecting msg that is one of %s, got "%s" instead' % (
            ",".join(['"%s"' % ctype for ctype in channeltypes.keys()],
             mtype))
          )
    return

  channel_id = msg.get('channel_id', str(uuid.uuid4()))
  remote_path = msg.get('remote_path', None)

  channeltypes[mtype](channel, channel_id, remote_path)


if __name__ == '__channelexec__':
  start_channel(channel)
