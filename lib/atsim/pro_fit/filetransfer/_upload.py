import logging
import os
import uuid
import threading


from _channel import BaseChannel
from remote_exec.file_transfer_remote_exec import FILE, DIR


class DirectoryUploadException(Exception):
  pass

class UploadChannel(BaseChannel):

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.UploadChannel")

  def __init__(self, execnet_gw, remote_path, channel_id = None):
    """Create a channel object for use with `DownloadDirectory`

    Args:
        execnet_gw (execnet.Gateway): Gateway used to create channel objects.
        remote_path (str): Path defining root of remote upload destination tree.
        channel_id (None, optional): ID of this channel (auto generated if not specified)
    """
    super(UploadChannel, self,).__init__(
      execnet_gw,
      'START_UPLOAD_CHANNEL',
      remote_path,
      channel_id)


class UploadHandler(object):
  """Class used by UploadDirectory for rewriting local paths to remote paths and also acting as a callback to monitor completion of file upload"""

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.UploadHandler")

  def __init__(self, local_root):
    """

    Args:
        local_root (str): Root of the directory structure from which files are being uploaded.
    """
    self.local_root = local_root


  def mkdir(self, msg):
    """Called by UploadDirectory with MKDIR request dictionary before it is sent via an UploadChannel. When `msg` is passed into this method,
    its `remote_path` is actually the local path that needs to be created on the remote. This method is typically used to rewrite `remote_path`
    so that it refers to a suitable location in the destination directory tree.

    In this implementation remote_path rewriting is performed by `rewrite_file_path` so this can be overidden if different bahviour is required.

    Args:
        msg (dict): MKDIR message request.

    Returns:
        dict : Rewritten message.
    """
    return self.rewrite_file_path(msg)

  def upload(self, msg):
    """Called by UploadDirectory with UPLOAD request dictionary before it is sent via an UploadChannel. When `msg` is passed into this method,
    its `remote_path` is actually the local path that needs to be created on the remote. This method is typically used to rewrite `remote_path`
    so that it refers to a suitable location in the destination directory tree.

    In this implementation remote_path rewriting is performed by `rewrite_file_path` so this can be overidden if different bahviour is required.

    Args:
        msg (dict): UPLOAD message request.

    Returns:
        dict : Rewritten message.
    """
    return self.rewrite_file_path(msg)


  def rewrite_file_path(self, msg):
    return self.rewrite_path(msg)

  def rewrite_directory_path(self, msg):
    return self.rewrite_path(msg)

  def transform_path(self, path):
    assert path.startswith(self.local_root)
    path = path.replace(self.local_root, '', 1)
    if path.startswith('/'):
      path = path[1:]
    return path

  def rewrite_path(self, msg):
    path = msg['remote_path']
    self._logger.debug("rewrite_path: original path: '%s'", path)
    path = self.transform_path(path)
    self._logger.debug("rewrite_path: transformed path: '%s'", path)
    msg['remote_path'] = path
    return msg


class UploadDirectory(object):
  """Class that coordinates an UploadChannel to allowing directory hierarchies to be uploaded."""

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.UploadDirectory")

  def __init__(self, ulchannels, local_path, remote_path, upload_handler = None):
    """Specify execnet channels and the source (remote) and destination (local)
    paths for the directory download.

    Args:
        ulchannels (UploadChannel): DownloadChannel instance.
        local_path (str): Path on local drive from which files will be copied.
        remote_path (str): Path of remote directory into which files should be copied (must be within the root path used when creating the UploadChannel)
    """
    self.remote_path = remote_path
    self.local_path = os.path.abspath(local_path)
    self.transaction_id = str(uuid.uuid4())

    self._callback = _UploadCallback(self)
    self._callback.channel_iter = self._init_channels(ulchannels)

    if upload_handler is None:
      self.upload_handler = UploadHandler(self.local_path )
    else:
      self.upload_handler = upload_handler


    log = self._logger.getChild("__init__")
    log.debug("local_path: '%s'" % self.local_path)
    log.debug("remote_path: '%s'" % self.remote_path)
    log.debug("transaction_id: '%s'" % self.transaction_id)

  def _init_channels(self, ulchannels):
    ulchannels.setcallback(self._callback)
    return iter(ulchannels)

  def upload(self):
    log = self._logger.getChild("upload")
    log.info("Starting upload id='%s' from local ('%s') to remote ('%s').", self.transaction_id, self.local_path, self.remote_path)

    # Start the process
    self._callback.start()
    log.info("Finished upload id='%s'", self.transaction_id)


class _UploadCallback(object):
  _logger = UploadDirectory._logger.getChild("_UploadCallback")

  def __init__(self, parent):
    self.parent = parent
    self.event = threading.Event()
    self._lock = threading.RLock()
    self._upload_wait = None
    self._walk_iterator = None

    self.enabled = False
    self._exc = None


  def __call__(self, msg):
    try:
      if not self.enabled:
        return
      self._logger.debug("Received message: '%s'", msg)

      try:
        mtype = msg.get('msg', None)
      except AttributeError:
        self._error("Received malformed message: %s" %  msg)
        return
      if mtype == None:
        self._error("Couldn't extract 'msg' field from message: '%s'" % msg)
        return

      if mtype == 'ERROR':
        self._error("Received 'ERROR' message from remote: %s" % msg['reason'])
        return

      if mtype == 'UPLOADED':
        self._donext(msg)
      elif mtype == 'MKDIR':
        self._donext(msg)

    except Exception,e:
      with self._lock:
        self.enabled = False
        # try:
        #   if self.parent.upload_handler.finish(e) != False:
        #     self._exc = sys.exc_info()
        # except Exception, e:
        #   self._exc = sys.exc_info()
        self._exc = sys.exc_info()
        traceback.print_exc()
        self._finish()

  def _error(self, msg):
    with self._lock:
      try:
        raise DirectoryUploadException(msg)
      except:
        self._exc = sys.exc_info()
        self._finish()

  def start(self):
    self.event.clear()
    self.enabled = True
    self._upload_wait = set()
    self._walk_iterator = os.walk(self.parent.local_path)
    self._next_iteration()

    self.event.wait()
    if self._exc:
      raise self._exc

  def _donext(self, msg):
    """Called when a confirmation message received.

    Performs next iteration if no messages are outstanding"""
    transid = msg['id']
    with self._lock:
      self._upload_wait.remove(transid)

      if not self._upload_wait:
        self._next_iteration()

  def _next_iteration(self):
    """Gets the next set of directories and files to be uploaded and makes requests to channels"""
    try:
      root_path, directories, files = self._walk_iterator.next()
      self._makedirectories(root_path, directories)
      self._uploadfiles(root_path, files)
    except StopIteration:
      self._finish()

  def _finish(self):
    with self._lock:
      self.enabled = False
      self.event.set()

  def _makedirectories(self, root_path, directories):
    """Makes a series of UploadChannel 'MKDIR' requests for a given parent
    directory (root_path) child directories (directories). These come from
    the values obtained using the os.walk iterator stored in self.walk_iterator.

    Args:
        root_path (str): Path of parent directory to the sub-directories stored in `directories`
        directories (list): List of sub-directory names to be created.

    Returns:
        TYPE: Description
    """
    for d in directories:
      msgdict = self._makedirectory_request(root_path, d)
      transid = msgdict['id']
      with self._lock:
        # Send the message
        self._channel_send(msgdict)
        # Add the msg id to the upload wait set
        self._upload_wait.add(transid)

  def _uploadfiles(self, root_path, files):
    """Makes a series of 'UPLOAD' requests through UploadChannel for the files
    contained in the directory given by `root_path`.

    Args:
        root_path (str): Path to the local directory containing the files to be uploaded (given in the `files` argument)
        files (list): List of the files in `root_path` that should be uploaded.
    """
    for f in files:
      msgdict = self._makeupload_request(root_path,f)
      transid = msgdict['id']
      with self._lock:
        self._channel_send(msgdict)
        # Add the msg id to the upload wait set
        self._upload_wait.add(transid)

  def _makedirectory_request(self, root_path, directory):
    """Constructs MKDIR request to be sent through UploadChannel.

    Message is rewritten by self.upload_handler.mkdir() to translater local paths to
    remote paths.

    Args:
        root_path (str): Path giving parent of directory.
        directory (str): Sub-directory that should be created.
    Returns:
        dict : Dictionary suitable for making an UploadChannel MKDIR request.
    """
    local_path = os.path.join(root_path, directory)
    transid = self._transid(local_path, DIR)

    # Get the directory mode.
    mode = os.stat(local_path).st_mode

    msgdict = self._build_msg("MKDIR", transid,
                   mode = mode,
                   remote_path = local_path)

    msgdict = self.parent.upload_handler.mkdir(msgdict)
    return msgdict

  def _makeupload_request(self, root_path, f):
    """Constructs UPLOAD request to be sent through UploadChannel.

    Message is rewritten by self.upload_handler.upload() to translate local paths to
    remote paths.

    Args:
        root_path (str): Path giving parent of directory.
        directory (str): File within `root_path` that should be uploaded
    Returns:
        dict : Dictionary suitable for making an UploadChannel UPLOAD request.
    """
    local_path = os.path.join(root_path, f)
    transid = self._transid(local_path, FILE)

    # Get the file mode.
    mode = os.stat(local_path).st_mode

    with open(local_path, 'rb') as infile:
      file_data = infile.read()


    msgdict = self._build_msg("UPLOAD", transid,
                   mode = mode,
                   remote_path = local_path,
                   file_data = file_data)

    msgdict = self.parent.upload_handler.upload(msgdict)
    return msgdict

  def _transid(self, path, file_type):
    """Make a unique request id from the given file path and file_type (PATH or DIR).

    path and file_type are combined with the parent UploadDirectory transaction_id.

    Args:
        path (str): Local file path
        file_type (int): One of the constants PATH or DIR.

    Returns:
        tuple : Tuple (parent.transaction_id, filet_type, path)
    """
    return (self.parent.transaction_id, file_type, path)

  def _build_msg(self, msg, transid, **kwargs):
    """Method providing convenient way to build an UploadChannel message dictionary.

    Basic message format is:
      { 'msg' : msg,
        'id' : transid,
      }

    This message is the updated with contents of the kwargs dictionary.


    Args:
        msg (str): Contents of 'msg' field
        transid  : Contents of the transaction 'id' field.
        **kwargs (dict): Additional fields.

    Returns:
        dict: Message dictionary.
    """
    msgdict = dict(msg = msg, id = transid)
    msgdict.update(kwargs)
    return msgdict

  def _channel_send(self, msgdict):
    self._logger.debug("Sending request: '%s'", msgdict)
    ch = self._get_channel()
    ch.send(msgdict)

  def _get_channel(self):
    ch = self.channel_iter.next()
    return ch
