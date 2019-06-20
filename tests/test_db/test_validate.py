import os

import sqlalchemy as sa


from atsim.pro_fit.db import validate

def _getResourceDir():
  return os.path.abspath(os.path.join(os.path.dirname(__file__),'resources'))


def test_validategood():
  """Test atsim.pro_fit.db.validate() for a good database"""
  dburl = "sqlite:///"+_getResourceDir()+"/grid_fitting_run.db"
  engine = sa.create_engine(dburl)
  assert(validate(engine))

def test_validatebad(tmpdir):
  """Test atsim.pro_fit.db.validate() for a bad database"""
  os.chdir(str(tmpdir))

  with open('emptyfile', 'w') as outfile:
    pass

  dburl = "sqlite:///emptyfile"
  engine = sa.create_engine(dburl)
  assert(not validate(engine))

