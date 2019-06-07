import unittest

import multiprocessing as mp

import sqlalchemy as sa
import sqlite3

from contextlib import nested, closing
import os
import shutil

from atsim.pro_fit.reporters import  SQLiteReporter
from atsim import pro_fit

from . import test_reporters

class LockerException(Exception):
  pass


def _lock(conn, cursor):
  if cursor:
    _unlock(conn, cursor)
  cursor = conn.cursor()
  cursor.execute("begin exclusive transaction")
  return cursor


def _unlock(conn, cursor):
  if cursor:
    conn.commit()
  return None


def locker(dbfilename, pipe):
  conn = sqlite3.connect(dbfilename)
  cursor = None

  with conn:
    while True:
      try:
        msg = pipe.recv()
        if msg == "stop":
          cursor = _unlock(conn, cursor)
          pipe.send(("okay", None))
          return
        elif msg == "lock":
          cursor = _lock(conn, cursor)
          pipe.send(("okay", None))
        elif msg == "unlock":
          cursor = _unlock(conn, cursor)
          pipe.send(("okay", None))
      except Exception as e:
        pipe.send(("error", e))
        raise


def populatedatabase(dbfilename, pipe):
  try:
    import logging
    import sys
    # retryLogger=  logging.getLogger("atsim.pro_fit.retry")
    # retryLogger.addHandler(logging.StreamHandler(sys.stdout))
    # retryLogger.setLevel(logging.DEBUG)
    initialVariables = test_reporters.SQLiteReporterTestCase.getInitialVariables()
    calculatedVariables = test_reporters.SQLiteReporterTestCase.getCalculatedVariables()
    reporter = SQLiteReporter(dbfilename, initialVariables, calculatedVariables)

    pipe.send("ready")
  except:
    pipe.send("error")
    raise

  v = pipe.recv()
  if v != "lock":
    return

  try:
    engine = reporter._saengine
    engine.echo = True
    minresults = test_reporters.SQLiteReporterTestCase.getVariables()[0]
    reporter(minresults)
    pipe.send(("finished", None))
  except Exception as e:
    pipe.send(("error", e))
    raise



class SQLReporterDatabaseLockTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.reporters.SQLReporter ensuring correct behaviour when database is locked by another process."""

  def _makeLockProcess(self):
    """Spawn a process that can be signalled to begin and end an exclusive transaction in order to lock the sqlite database.

    This method returns a multiprocessing.Pipe instance. The following commands can be sent through the pipe:

      * 'lock' - Establish the database lock.
      * 'unlock' - Return database to unlocked status.
      * 'stop' - Stop the locking process.

    After each message the controlling process should recv() a status message of 'okay' if command was successful or
    'error' if not.

    :return: Return multiprocessing.Pipe instance to allow communication with the locking process.
    """
    pipe_start, pipe_end = mp.Pipe()
    prc = mp.Process(target=locker, args = (self.dbfilename, pipe_end))
    prc.start()
    return pipe_start


  def setUp(self):
    # Create database file in a temporary directory
    import tempfile
    self.tempdir = tempfile.mkdtemp()
    self.dbfilename = os.path.join(self.tempdir, "fitting_run.db")


  def tearDown(self):
    self._lockerPipe.send("stop")
    self._lockerPipe.recv()
    self._lockerPipe.close()

    shutil.rmtree(self.tempdir, ignore_errors = True)

  def lock(self):
    self._lockerPipe.send("lock")
    response = self._lockerPipe.recv()
    if response != "okay":
      raise LockerException("Database lock error")

  def unlock(self):
    self._lockerPipe.send("unlock")
    response = self._lockerPipe.recv()
    if response != "okay":
      raise LockerException("Database unlock error")


  def testCallLockThenUnlock(self):
    """Check that if database is locked when SQLiteReporter is called but subsequently becomes unlocked that data is
    successfully written to the database"""

    def processError(args):
      msg, exc = args
      if msg == "error":
        assert(exc != None)
        raise exc
      assert(msg == "okay")

    popPipeStart, popPipeEnd = mp.Pipe()
    populateProcess = mp.Process(target = populatedatabase,
                                 args = (self.dbfilename,
                                         popPipeEnd))
    populateProcess.start()
    msg = popPipeStart.recv()
    assert(msg == "ready")

    lockerPipe = self._makeLockProcess()
    self._lockerPipe = lockerPipe
    lockerPipe.send("lock")
    processError(lockerPipe.recv())

    popPipeStart.send("lock")
    import time
    time.sleep(10)
    lockerPipe.send("unlock")
    processError(lockerPipe.recv())

    msg, exc = popPipeStart.recv()

    if msg == "error":
      assert(exc != None)
      raise exc
    assert(msg == "finished")

    populateProcess.join()

    engine = sa.create_engine("sqlite:///%s" % self.dbfilename)
    test_reporters.SQLiteReporterTestCase.tstInsertSingleMinimizerResult(self, engine)
