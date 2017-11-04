import collections
import logging
import uuid
import os
# import threading
import gevent
import gevent.queue
import sys
import traceback
import itertools

from atsim.pro_fit._channel import MultiChannel
from _basechannel import BaseChannel, ChannelFactory
from remote_exec.file_transfer_remote_exec import FILE, DIR
from atsim.pro_fit._util import MultiCallback, NamedEvent

_DirectoryRecord = collections.namedtuple("_DirectoryRecord", ['transid', 'path'])

class DirectoryDownloadException(Exception):
  pass

class DownloadCancelledException(DirectoryDownloadException):
  pass

class DownloadChannel(BaseChannel):

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.DownloadChannel")

  def __init__(self, execnet_gw, remote_path, channel_id = None, keepAlive = 10):
    """Create a channel object for use with `DownloadDirectory`

    Args:
        execnet_gw (execnet.Gateway): Gateway used to create channel objects.
        remote_path (str): Path defining root of remote download tree.
        channel_id (None, optional): ID of this channel (auto generated if not specified)
        keepAlive (int, optional): Send a `KEEP_ALIVE` message to the server every `keepAlive` seconds. If `None` do not send `KEEP_ALIVE` messages.
    """
    super(DownloadChannel, self).__init__(
      execnet_gw,
      'START_DOWNLOAD_CHANNEL',
      remote_path,
      channel_id,
      keepAlive)

class DownloadChannels(MultiChannel):
  """MultiChannel instance for managing DownloadChannel instances"""

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.DownloadChannels")

  def __init__(self, execnet_gw, remote_path, num_channels = 1, channel_id = None, keepAlive = 10):
    factory = ChannelFactory(DownloadChannel, remote_path, keepAlive)
    super(DownloadChannels,self).__init__(execnet_gw, factory, num_channels, channel_id)

class DownloadHandler(object):
  """Class used by DownloadDirectory to handle mapping of remote paths to local
  filesystem. DownloadHandler is also responsible for converting DownloadHandler
  wire messages into directories and files on disc"""

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.DownloadHandler")

  def __init__(self, remote_path, dest_path):
    """Create DownloadHandler instance for copying files from `remote_path` into
    the directory rooted at `dest_path`.

    Args:
        remote_path (str): Remote path.
        dest_path (str): Destination.
    """
    self.remote_path = remote_path
    self.dest_path = dest_path

  def mkdir(self, msg):
    local_path = self.rewrite_directory_path(msg)
    mode = msg['mode']
    self._logger.debug("mkdir path: '%s' with mode %o", local_path, mode)
    os.mkdir(local_path, mode)

  def writefile(self, msg):
    local_path = self.rewrite_file_path(msg)
    mode = msg['mode']
    self._logger.debug("writing file with path: '%s' and mode %o", local_path, mode)
    filedata = msg['file_data']
    with open(local_path,'wb') as outfile:
      outfile.write(filedata)
    os.chmod(local_path, mode)

  def rewrite_directory_path(self, msg):
    """Called by mkdir to translage msg['remote_path'] into a local filesystem path.
    In this implementation this calls self.rewrite_path()"""
    return self.rewrite_path(msg)

  def rewrite_file_path(self, msg):
    """Called by writefile to translage msg['remote_path'] into a local filesystem path.
    In this implementation this calls self.rewrite_path()"""
    return self.rewrite_path(msg)

  def transform_path(self, path):
    assert path.startswith(self.remote_path)
    path = path.replace(self.remote_path, '', 1)

    if path.startswith('/'):
      path = path[1:]

    path = os.path.join(self.dest_path, path)
    return path

  def rewrite_path(self, msg):
    """Extracts 'remote_path' from msg, `self.remote_path` is then stripped from this path
    and replaced with `self.dest_path`."""
    path = msg['remote_path']
    self._logger.debug("rewrite_path: original path: '%s'", path)
    path = self.transform_path(path)
    self._logger.debug("rewrite_path: transformed path: '%s'", path)
    return path

  def finish(self, exception = None):
    """Called when `DirectoryDownload` completes. If an error occurred during download
    the exception is passed into this method as `exception`. This exception will be raised once `finish` returns unless  finish returns `False`
    or `finish` raises an exception.

    Args:
        exception (None, DirectoryDownloadException): Exception describing any error that occurred during download
          or None if no error occurred.

    Returns:
        bool : If `False` is returned the exception passed into finish will not be raised.
    """
    return None

class DownloadDirectory(object):
  """Class that coordinates an execnet channel started with the _file_transfer_remote_exec to
  allow directory hierarchies to be downloaded."""

  _logger = logging.getLogger("atsim.pro_fit.runners._file_transfer_client.DownloadDirectory")

  def __init__(self, dlchannels, remote_path, dest_path, download_handler = None):
    """Specify execnet channels and the source (remote) and destination (local)
    paths for the directory download.

    Args:
        dlchannels (DownloadChannel): DownloadChannel instance.
        remote_path (str): Path to directory to be copied (must be within the root path used when creating the execnet channels)
        dest_path (str): Path on local drive into which files will be copied.
    """
    self.remote_path = remote_path
    self.dest_path = dest_path
    self.transaction_id = str(uuid.uuid4())

    self.exception = None

    if not os.path.exists(self.dest_path):
      raise IOError("Download destination path doesn't exist: '%s'" % self.dest_path)

    if download_handler is None:
      download_handler = DownloadHandler(self.remote_path, self.dest_path)

    self.download_handler = download_handler

    self._callback = _DownloadCallback(self)
    self._callback.channel_iter = self._init_channels(dlchannels)

    log = self._logger.getChild("__init__")
    log.debug("remote_path: '%s'" % self.remote_path)
    log.debug("dest_path: '%s'" % self.dest_path)
    log.debug("transaction_id: '%s'" % self.transaction_id)

  def _init_channels(self, dlchannels):
    if dlchannels.callback == None:
      dlchannels.callback = MultiCallback()

    if not isinstance(dlchannels.callback, MultiCallback):
      raise DirectoryDownloadException("Callback already registered with DownloadChannel is not an instance of MultiCallback")

    dlchannels.callback.append(self._callback)
    return dlchannels

  def download(self, non_blocking = False):
    """Start the download.

    Downloads can be blocking or non-blocking.

    If blocking, any exceptions encountered during upload will be thrown in the calling
    thread.

    In the non-blocking case the exception will be caught in callback thread.

    If you need to access the exception information this can be accessed through this object's
    `exception` property.

    Args:
        non_blocking (bool, optional): If `True`, download will return immediately after call,
          if False, then block until download completes.

    Returns:
        gevent.event.Event: If non_blocking is `True` then upload returns an event object that is signalled when upload finishes.
    """
    log = self._logger.getChild("download")
    log.info("Starting download id='%s' from remote ('%s') to local ('%s').", self.transaction_id, self.remote_path, self.dest_path)

    # Start the process
    grl = gevent.Greenlet.spawn(self._callback.queue_loop)
    rv = self._callback.event
    if non_blocking:
      return rv
    else:
      grl.join()
      if self.exception and self.exception != (None,None,None):
        raise self.exception
      log.info("Finished download id='%s'", self.transaction_id)

  def cancel(self):
    """Cancel the download

    Returns:
        threading.Event: Event that is set once cancellation is complete.
    """
    return self._callback.cancel()


class _DownloadCallback(object):
  _logger = DownloadDirectory._logger.getChild("DownloadCallback")

  def __init__(self, parent):
    self.parent = parent
    self.event = NamedEvent("_DownloadCallback")

    self.msg_q = gevent.queue.Queue()

    self.channel_iter = None
    self.file_q_wait = None
    self.dir_q_wait = None
    self.dir_q = None
    self.enabled = False
    self._exc = None

  def _is_msg_relevant(self, msg):
    msgid = msg['id']

    try:
      (transid, path) = msgid
    except (TypeError,ValueError):
      self._logger.debug("Callback ID: '%s'. Unrecognized 'id' field, message will not be processed: '%s'" , self.parent.transaction_id, str(msgid))
      return False

    retval = transid == self.parent.transaction_id

    if not retval:
      self._logger.debug("Callback ID: '%s'. Message ID doesn't match transaction_id and will not be processed. %s != %s ", self.parent.transaction_id, transid, self.parent.transaction_id)
    return retval

  def __call__(self, msg):
    self.msg_q.put(msg)

  def _process_queue_item(self, msg):
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
      if msg.get('error_code', None) == ('OSERROR', 'LISTDIR'):
        self._logger.warning("Callback ID: '%s'. Could not list directory (skipping contents): '%s'", self.parent.transaction_id, msg.get('exc_msg', None))
        dirid = msg.get('id')
        self._skip_dir(dirid)
      elif msg.get('error_code', None) == ('IOERROR', 'FILEOPEN'):
        self._logger.warning("Callback ID: '%s'. Could not open file for reading (skipping): '%s'", self.parent.transaction_id, msg.get('exc_msg', None))
        fileid = msg.get('id')
        self._skip_file(fileid)
      else:
        self._error("Received 'ERROR' message from remote: %s" % msg['reason'])
        return

    if mtype == 'LIST':
      self._process_list_dir_response(msg)
    elif mtype == 'DOWNLOAD_FILE':
      self._process_download_file_response(msg)

  def _error(self, msg):
    try:
      self._logger.warning("Callback ID: '%s'. Error: %s", self.parent.transaction_id, msg)
      raise DirectoryDownloadException(msg)
    except:
      self._exc = sys.exc_info()
      self._finish()

  def queue_loop(self):
    self.event.clear()
    self.dir_q = []
    self.dir_q_wait = set()
    self.file_q_wait = set()
    self.enabled = True
    # Put the first directory in the dir_q
    self._register_directory(self.parent.remote_path)
    self._list_next_dir()

    exc = None
    while not self.event.isSet():
        try:
          msg = self.msg_q.get(timeout = 1)
          self._process_queue_item(msg)
        except gevent.queue.Empty:
          pass
        except Exception,e:
          exc = e
          break
        gevent.sleep(0)
    self.enabled = False
    self._call_download_handler_finish(exc)
    self._finish()

  def _call_download_handler_finish(self, e):
    try:
      if self.parent.download_handler.finish(e) != False:
        self._exc = sys.exc_info()
    except Exception, e:
      self._exc = sys.exc_info()
      self._logger.exception("exception in download finish handler")

  def _register_directory(self, remote_path):
    """Puts directory and its id into self.dir_q"""
    transid = self._transId(remote_path)
    record = _DirectoryRecord(transid, remote_path)

    self.dir_q.append(record)

  def _register_file(self, pathtransid, remote_path):
    """Puts file_id into self.file_q wait and triggers DOWNLOAD_FILE request"""
    transid, junk = pathtransid
    fileid = (transid, remote_path)

    self.file_q_wait.add(fileid)
    self._download_file_request(fileid, remote_path)

  def _isFinished(self):
    finished = len(self.dir_q) == 0 and len(self.dir_q_wait) == 0 and len(self.file_q_wait) == 0
    return finished

  def _transId(self, path):
    return (self.parent.transaction_id, path)

  def _get_channel(self):
    ch = self.channel_iter.next()
    return ch

  def _channel_send(self, msg, transid, **kwargs):
    log = self._logger.getChild('_channel_send')
    msgdict = dict(msg = msg, id = transid)
    msgdict.update(kwargs)
    log.debug("Sending request: '%s'", msgdict)
    ch = self._get_channel()
    ch.send(msgdict)

  def _list_dir_request(self, transid, remotepath):
    """Performs the LIST request"""
    self._logger.getChild("_list_dir_request").debug("Listing remote: '%s'", remotepath)
    self._channel_send('LIST', transid, remote_path = remotepath, )

  def _download_file_request(self, transid, remotepath):
    """Performs the DOWNLOAD_FILE request"""
    self._logger.getChild("_download_file_request").debug("Requesting file: '%s'", remotepath)
    self._channel_send('DOWNLOAD_FILE', transid, remote_path = remotepath)

  def _list_next_dir(self):
    """Pops the next entry from the directory_q (self.dir_q), performs LIST request and moves its ID to the directory
    IDs that we're waiting for."""
    if len(self.dir_q) != 0:
      nextdir = self.dir_q.pop()
      self.dir_q_wait.add(nextdir.transid)
      self._list_dir_request(nextdir.transid, nextdir.path)

  def _donext(self):
    if self._isFinished():
      try:
        self.parent.download_handler.finish(None)
      except Exception as e:
        self.enabled = False
        self._exc = sys.exc_info()
        self._logger.exception("Exception during file download")
      finally:
        self._finish()
    else:
      self._list_next_dir()

  def _skip_dir(self, dirid):
    self.dir_q_wait.discard(dirid)
    self._donext()

  def _skip_file(self, fileid):
    self.file_q_wait.discard(fileid)
    self._donext()

  def _process_list_dir_response(self, msg):
    """Used by callback when LIST is received.

    This triggers the following:
      * register directory entries.
      * make file requests
      * check if we're finished and trigger event if we are"""

    transid = msg.get('id', None)

    if not transid in self.dir_q_wait:
      raise DirectoryDownloadException("Unexpected LIST response received for '%s'", transid )

    self.dir_q_wait.discard(transid)

    for f in msg['files']:
      if f['type'] is FILE:
        self._register_file(transid, f['remote_path'])
      elif f['type'] is DIR:
        self._register_directory(f['remote_path'])
        self._write_dir(f)
    self._donext()

  def _process_download_file_response(self, msg):
    """Used by callback when DOWNLOAD_FILE is received"""
    transid = msg.get('id', None)
    if not transid in self.file_q_wait:
      raise DirectoryDownloadException("Unexpected 'DOWNLOAD_FILE' response received for '%s'", transid )

    # Write the file to disc.
    self._write_file(msg)

    # Remove the file from the outstanding files.
    self.file_q_wait.discard(transid)

    # # If there are no more files with this pathid
    # Check if we're finished at this point
    self._donext()

  def _write_dir(self, msg):
    log = self._logger.getChild("_write_dir")
    log.debug('creating directory: %s', msg)
    self.parent.download_handler.mkdir(msg)

  def _write_file(self, msg):
    log = self._logger.getChild("_write_file")
    log.debug('writing files: %s', msg)
    self.parent.download_handler.writefile(msg)

  def _unregister_callback(self):
    cb = self.channel_iter.callback
    if self in cb:
      cb.remove(self)

  def _finish(self):
    """Disable callback and fire event so that start() returns"""
    self.enabled = False
    self._unregister_callback()
    self.parent.exception = self._exc
    self.event.set()

  def cancel(self):
    exc = DownloadCancelledException()
    self.enabled = False
    try:
      self.parent.download_handler.finish(exc)
    except Exception, e:
      self._exc = sys.exc_info()
    self._finish()
    return self.event

