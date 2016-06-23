import logging
import os
import uuid
import threading
import sys
import traceback

from atsim.pro_fit._channel import MultiChannel
from _basechannel import BaseChannel, ChannelFactory
from remote_exec.file_transfer_remote_exec import FILE, DIR
from atsim.pro_fit._util import MultiCallback


class DirectoryUploadException(Exception):
  pass


class UploadCancelledException(DirectoryUploadException):
  pass

class UploadHandlerException(DirectoryUploadException):
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

class UploadChannels(MultiChannel):
  """MultiChannel instance for managing UploadChannel instances"""

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.UploadChannels")

  def __init__(self, execnet_gw, remote_path, num_channels = 1, channel_id = None):
    factory = ChannelFactory(UploadChannel, remote_path)
    super(UploadChannels,self).__init__(execnet_gw, factory, num_channels, channel_id)

class UploadHandler(object):
  """Class used by UploadDirectory for rewriting local paths to remote paths and also acting as a callback to monitor completion of file upload"""

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.UploadHandler")

  def __init__(self, source_path, dest_path):
    """
    Args:
        source_path (str): Root of the directory structure from which files are being uploaded.
        dest_path (str): Destination directory
    """
    self.source_path = source_path
    self.dest_path = dest_path


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

  def finish(self, exception = None):
    """Called when `DirectoryUpload` completes. If an error occurred during download
    the exception is passed into this method as `exception`. This exception will be raised once `finish` returns unless  finish returns `False`
    or `finish` raises an exception.

    Args:
        exception (None, DirectoryUploadException): Exception describing any error that occurred during upload
          or None if no error occurred.

    Returns:
        bool : If `False` is returned the exception passed into finish will not be raised.
    """
    return None


  def rewrite_file_path(self, msg):
    return self.rewrite_path(msg)

  def rewrite_directory_path(self, msg):
    return self.rewrite_path(msg)

  def transform_path(self, path):
    if not path.startswith(self.source_path):
      raise UploadHandlerException("Path to be transformed did not start with source_path. Path: '%s', source_path: '%s'" % (path, self.source_path))

    path = path.replace(self.source_path, '', 1)
    if path.startswith('/'):
      path = path[1:]

    path = os.path.join(self.dest_path, path)
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

    self.exception = None

    if not os.path.isdir(self.local_path):
      raise OSError("Path does not exist or is not a directory: '%s'" % self.local_path)

    self.transaction_id = str(uuid.uuid4())

    self._callback = _UploadCallback(self)
    self._callback.channel_iter = self._init_channels(ulchannels)

    if upload_handler is None:
      self.upload_handler = UploadHandler(self.local_path, self.remote_path)
    else:
      self.upload_handler = upload_handler


    log = self._logger.getChild("__init__")
    log.debug("local_path: '%s'" % self.local_path)
    log.debug("remote_path: '%s'" % self.remote_path)
    log.debug("transaction_id: '%s'" % self.transaction_id)

  def _init_channels(self, ulchannels):
    if ulchannels.callback == None:
      ulchannels.callback = MultiCallback()

    if not isinstance(ulchannels.callback, MultiCallback):
      raise DirectoryDownloadException("Callback already registered with DownloadChannel is not an instance of MultiCallback")

    ulchannels.callback.append(self._callback)
    return ulchannels

  def upload(self, non_blocking = False):
    """Start the upload.

    Uploads can be blocking or non-blocking.

    If blocking, any exceptions encountered during upload will be thrown in the calling
    thread.

    In the non-blocking case the exception will be caught in callback thread.

    If you need to access the exception information this can be accessed through this object's
    `exception` property.

    Args:
        non_blocking (bool, optional): If `True`, upload will return immediately after call,
          if False, then block until upload completes.

    Returns:
        threading.Event: If non_blocking is `True` then upload returns an event object that is signalled when upload finishes.
    """
    log = self._logger.getChild("upload")
    log.info("Starting upload id='%s' from local ('%s') to remote ('%s').", self.transaction_id, self.local_path, self.remote_path)

    # Start the process
    rv = self._callback.start(non_blocking = non_blocking)
    if non_blocking:
      return rv
    else:
      log.info("Finished upload id='%s'", self.transaction_id)

  def cancel(self):
    """Cancel the upload.

    Returns:
        threading.Event: Event that will be set() once cancellation completes.
    """
    return self._callback.cancel()


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

  def _is_msg_relevant(self, msg):
    msgid = msg['id']

    try:
       transid = msgid[0]
    except (TypeError,IndexError):
      self._logger.debug("Callback ID: '%s'. Unrecognized 'id' field, message will not be processed: '%s'" , self.parent.transaction_id, str(msgid))
      return False

    retval = transid == self.parent.transaction_id

    if not retval:
      self._logger.debug("Callback ID: '%s'. Message ID doesn't match transaction_id and will not be processed. %s != %s ", self.parent.transaction_id, transid, self.parent.transaction_id)

    return retval

  def __call__(self, msg):
    try:
      self._logger.debug("Callback ID: '%s'. Received message: '%s'", self.parent.transaction_id, msg)
      if not self.enabled:
        self._logger.debug("Callback ID: '%s'. Callback is not enabled, ignoring message.", self.parent.transaction_id)
        return

      try:
        mtype = msg.get('msg', None)
      except AttributeError:
        self._error("Received malformed message: %s" %  msg)
        return
      if mtype == None:
        self._error("Couldn't extract 'msg' field from message: '%s'" % msg)
        return

      if not self._is_msg_relevant(msg):
        return

      if mtype == 'ERROR':
        self._error("Received 'ERROR' message from remote: %s" % msg['reason'])
        return

      if mtype == 'UPLOADED':
        self._donext(msg)
      elif mtype == 'MKDIR':
        self._donext(msg)
      elif mtype == 'MKDIRS':
        self._donext(msg)

    except Exception,e:
      with self._lock:
        self.enabled = False
        try:
          if self.parent.upload_handler.finish(e) != False:
            self._exc = sys.exc_info()
        except Exception, e:
          self._exc = sys.exc_info()
        traceback.print_exc()
        self._finish()

  def _error(self, msg):
    self._logger.warning("Callback ID: '%s'. Error: %s", self.parent.transaction_id, msg)
    raise DirectoryUploadException(msg)

  def start(self, non_blocking = False):
    self.event.clear()
    self.enabled = True
    self._upload_wait = set()
    self._walk_iterator = os.walk(self.parent.local_path)
    self._make_dest_path()

    if not non_blocking:
      self.event.wait()
      if self._exc:
        raise self._exc
    else:
      return self.event

  def _make_dest_path(self):
    transid = self._transid(self.parent.local_path, DIR)
    self._upload_wait.add(transid)
    request = self._build_msg("MKDIRS", transid, remote_path = self.parent.remote_path)
    self._channel_send(request)

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

      # Move on if directory is empty
      if not directories and not files:
        self._next_iteration()

      self._makedirectories(root_path, directories)
      self._uploadfiles(root_path, files)
    except StopIteration:
      self._logger.debug("StopIteration")
      try:
        self.parent.upload_handler.finish(None)
      except Exception as e:
        with self._lock:
          self.enabled = False
          self._exc = sys.exc_info()
          traceback.print_exc()
      finally:
        self._finish()

  def _finish(self):
    with self._lock:
      self.enabled = False
      self._unregister_callback()
      self.parent.exception = self._exc
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

  def _unregister_callback(self):
    cb = self.channel_iter.callback
    cb.remove(self)

  def cancel(self):
    exc = UploadCancelledException()
    with self._lock:
      self.enabled = False
      try:
        self.parent.upload_handler.finish(exc)
      except Exception, e:
        self._exc = sys.exc_info()
      self._finish()
    return self.event
