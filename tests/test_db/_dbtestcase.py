import os
import unittest

import sqlalchemy as sa

from .. import testutil

def _getResourceDir():
  return os.path.abspath(os.path.join(os.path.dirname(__file__),'resources'))


class DBTestCase(unittest.TestCase):

  @classmethod
  def dbPath(cls):
    path = os.path.join(_getResourceDir(), cls.dbname)
    return os.path.abspath(path)

  def setUp(self):
    self.dburl = "sqlite:///"+os.path.join(_getResourceDir(), self.dbname)
    self.engine= sa.create_engine(self.dburl)
