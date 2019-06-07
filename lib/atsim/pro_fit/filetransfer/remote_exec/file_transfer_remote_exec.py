import uuid
import tempfile
import os
import traceback

FILE = 1
DIR = 2

#INCLUDE "_remote_exec_funcs.py.inc"

def mktempdir(channel, channel_id):
  try:
    tmpdir = tempfile.mkdtemp()
    return tmpdir
  except Exception as e:
    error(channel,
      channel_id,
      "Couldn't create temporary directory. '%s' " % e,
      ("IOERROR","MKTEMPDIR"))
    return False

def process_path(channel, channel_id, remote_path):
  # If the final element of remote_path doesn't exist, but the rest of it does, then make the directory
  remote_path = os.path.realpath(remote_path)

  if os.path.isdir(remote_path):
    return True, remote_path

  if os.path.exists(remote_path):
    error(channel,
      channel_id,
      "'remote_path' exists but is not a directory '%s'" % remote_path,
      ("IOERROR","REMOTE_IS_NOT_DIRECTORY"))
    return False, remote_path

  rootpath = os.path.dirname(remote_path)
  if not os.path.isdir(rootpath):
    error(channel,
      channel_id,
      "'remote_path' is invalid, neither '%s' or '%s' are existing directories." % (remote_path, rootpath),
      ("IOERROR","REMOTE_DOES_NOT_EXIST"))
    return False, remote_path

  try:
    os.mkdir(remote_path)
    return True, remote_path
  except Exception as e:
    error(channel,
      channel_id,
      "Couldn't create directory. '%s' reason '%s'" % (remote_path, e),
      ("IOERROR", "MKDIR_FAILED"))
    return False, remote_path

def keepalive(channel, channel_id, msg):
  channel.send(msg)

def chkpath(channel, channel_id, remote_path):
  # Ensure that the destination directory is writeable
  if not os.access(remote_path, os.W_OK):
    error(channel,
      channel_id,
      "Directory is not writeable.",
      ("IOERROR", "PERMISSION_DENIED"),
      remote_path = remote_path)
    return False, remote_path
  return True, remote_path

def upload(channel, channel_id, remote_root, msg):
  if 'id' not in msg:
    error(channel,
      channel_id,
      "UPLOAD message does not contain 'id' argument",
      ("MSGERROR", "KEYERROR"),
      key = 'id')
    return False

  file_data = msg.get("file_data", "")
  mode = msg.get("mode", None)
  fileid = msg['id']
  rp = child_path(channel, channel_id, remote_root, msg)

  if rp is None:
    return

  remote_path = rp

  try:
    # Create directory if necessary.
    dname = os.path.dirname(remote_path)
    if not os.path.exists(dname):
      os.makedirs(dname)

    with open(remote_path, 'wb') as outfile:
      outfile.write(file_data)
  except Exception as e:
    error(channel, channel_id, "Error writing file: '%s'" % str(e),
      ("IOERROR", "WRITE"),
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

def mkdir(channel, channel_id, remote_root, msg):
  if 'id' not in msg:
    error(channel,
      channel_id,
      "MKDIR message does not contain 'id' argument",
      ("MSGERROR", "KEYERROR"),
      key = 'id')
    return False

  mode = msg.get("mode", 0o777)
  fileid = msg['id']
  rp = child_path(channel, channel_id, remote_root, msg)

  if rp is None:
    return

  remote_path = rp

  try:
    os.mkdir(remote_path, mode)
  except OSError as e:
    error(channel, channel_id, "Error making directory: '%s'" % str(e),
      ("IOERROR", "OSERROR"),
      remote_path = remote_path,
      id = fileid)
    return False

  dirmade_msg = dict(msg = 'MKDIR', channel_id = channel_id,
    id = fileid,
    remote_path = remote_path)

  channel.send(dirmade_msg)
  return True

def mkdirs(channel, channel_id, remote_root, msg):
  if 'id' not in msg:
    error(channel,
      channel_id,
      "MKDIRS message does not contain 'id' argument",
      ("MSGERROR", "KEYERROR"),
      key = 'id')
    return False

  mode = msg.get("mode", 0o777)
  fileid = msg['id']
  rp = child_path(channel, channel_id, remote_root, msg)

  if rp is None:
    return

  remote_path = rp

  dirmade_msg = dict(msg = 'MKDIRS', channel_id = channel_id,
    id = fileid,
    remote_path = remote_path)

  if os.path.exists(remote_path):
    dirmade_msg['path_already_exists'] = True
  else:
    try:
      os.makedirs(remote_path, mode)
    except OSError as e:
      error(channel, channel_id, "Error making directory: '%s'" % str(e),
        ("IOERROR", "OSERROR"),
        remote_path = remote_path,
        id = fileid)
      return False
  channel.send(dirmade_msg)
  return True

def list_dir(channel, channel_id, remote_root, msg):
  # Extract required arguments
  path = msg.get("remote_path", None)
  if path is None:
    error(channel, channel_id,
      "Could not find 'remote_path' argument in 'LIST' request'",
      ("MSGERROR", "KEYERROR"),
      key = "remote_path")
    return

  fileid = msg.get("id", None)
  if fileid is None:
    error(channel, channel_id,
      "Could not find 'id' argument in 'LIST' request'",
      ("MSGERROR", "KEYERROR"),
      key = 'id')
    return

  rpath = child_path(channel, channel_id, remote_root, msg)
  if rpath is None:
    return
  path = rpath

  files = []
  retmsg = dict(msg = "LIST", id =  fileid, channel_id = channel_id, files = files)

  try:
    file_list = os.listdir(path)
  except OSError as e:
    error(channel, channel_id,
      "Could not list directory",
      ("OSERROR", "LISTDIR"),
      remote_path = path,
      exc_msg = str(e),
      id = fileid)
    return

  for f in file_list:
    p = normalize_path(remote_root, os.path.join(path,f))
    mode = os.stat(p).st_mode

    if os.path.isdir(p):
      filetype = DIR
    else:
      filetype = FILE

    files.append(dict(remote_path = p, type = filetype, mode = mode))
  channel.send(retmsg)

def download_file(channel,channel_id, remote_root, msg):
  # Extract required arguments
  path = msg.get("remote_path", None)
  if path is None:
    error(channel, channel_id,
    "Could not find 'remote_path' argument in 'DOWNLOAD_FILE' request'",
    ("MSGERROR", "KEYERROR"),
    key = 'remote_path')
    return

  fileid = msg.get("id", None)
  if fileid is None:
    error(channel, channel_id,
    "Could not find 'id' argument in 'DOWNLOAD_FILE' request'",
    ("MSGERROR", "KEYERROR"),
    key = 'id')
    return

  rpath = normalize_path(remote_root, path)
  if rpath is None:
    error(channel, channel_id,
    "'path' argument in 'DOWNLOAD_FILE' request references location outside channel root.",
    ("PATHERROR", "NOTCHILD"),
    remote_path = path, remote_root = remote_root, id = fileid)
    return
  path = rpath

  if not os.path.exists(path):
    error(channel, channel_id,
    "file does not exist",
    ("IOERROR", "FILEDOESNOTEXIST"),
    remote_path = path, id = fileid)
    return

  if os.path.isdir(path) or not os.path.isfile(path):
    error(channel, channel_id, "path refers to a directory and cannot be downloaded",
    ("IOERROR", "ISDIR"),
    remote_path = path, id = fileid)
    return

  try:
    with open(path,'rb') as infile:
      filecontents = infile.read()
  except IOError as e:
    error(channel,
      channel_id,
      "permission denied",
      ("IOERROR", "FILEOPEN"),
      id = fileid,
      exc_msg = str(e),
      remote_path = path)
    return

  mode = os.stat(path).st_mode
  retmsg = dict(msg = 'DOWNLOAD_FILE', id = fileid, channel_id = channel_id, remote_path = path, file_data = filecontents, mode = mode)
  channel.send(retmsg)

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
      mtype = extract_mtype(msg, channel, channel_id)
      if mtype is None:
        continue

      if mtype == 'UPLOAD':
        upload(channel, channel_id, remote_path, msg)
      elif mtype == 'MKDIR':
        mkdir(channel, channel_id, remote_path, msg)
      elif mtype == 'MKDIRS':
        mkdirs(channel, channel_id, remote_path, msg)
      elif mtype == "KEEP_ALIVE":
        keepalive(channel, channel_id, msg)
      else:
        error(channel, channel_id,
          "Unknown 'msg' type: '%s'" % (mtype,),
          ("MSGERROR", "UNKNOWN_MSGTYPE"),
          mtype = mtype)
    except Exception as e:
      error(channel, channel_id,
        "Exception: %s" % str(e),
        ("EXCEPTION", str(type(e))),
        traceback = traceback.format_exc())

def download_remote_exec(channel, channel_id, remote_path):
  if remote_path is None:
    error(channel, channel_id, "'remote_path' argument not found in START_DOWNLOAD channel request.")
    return

  remote_path = os.path.normpath(remote_path)
  if not os.path.isdir(remote_path):
    error(channel, channel_id, "path does not exist or is not a directory",
    ("IOERROR", "FILEDOESNOTEXIST"),
    remote_path = remote_path)
    return

  ready(channel, channel_id, remote_path)

  for msg in channel:
    if msg is None:
      return
    try:
      mtype = extract_mtype(msg, channel, channel_id)
      if mtype is None:
        continue

      if mtype == 'LIST':
        list_dir(channel, channel_id, remote_path, msg)
      elif mtype == 'DOWNLOAD_FILE':
        download_file(channel, channel_id, remote_path, msg)
      elif mtype == "KEEP_ALIVE":
        keepalive(channel, channel_id, msg)
      else:
        error(channel, channel_id,
          "Unknown 'msg' type: '%s'" % (mtype,),
          ("MSGERROR", "UNKNOWN_MSGTYPE"),
          mtype = mtype)
    except Exception as e:
      error(channel, channel_id,
      "Exception: %s" % str(e),
      ("EXCEPTION", str(type(e))),
      traceback = traceback.format_exc())

def start_channel(channel):
  msg = channel.receive()

  mtype = msg.get('msg', None)
  channeltypes = {'START_UPLOAD_CHANNEL' : upload_remote_exec,
                  'START_DOWNLOAD_CHANNEL' : download_remote_exec}

  if not mtype in channeltypes:
    error(channel,
          None,
          'was expecting msg that is one of %s, got "%s" instead' % (
            ",".join(['"%s"' % ctype for ctype in list(channeltypes.keys())],
             ("MSGERROR", "UNKNOWN_MSGTYPE"),
             mtype))
          )
    return

  channel_id = msg.get('channel_id', str(uuid.uuid4()))
  remote_path = msg.get('remote_path', None)

  channeltypes[mtype](channel, channel_id, remote_path)

if __name__ == '__channelexec__':
  start_channel(channel)
