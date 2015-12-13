import os
import tempfile
import shutil

from atsim.pro_fit import db
from atsim.pro_fit.db._tableserialize import _RangeDiscoverIterator

from assertpy import *

import sqlalchemy as sa

import pytest

try:
  import rpy2
  RPY_AVAILABLE=True
except ImportError:
  RPY_AVAILABLE=False

def _getResourceDir():
  return os.path.abspath(os.path.join(os.path.dirname(__file__),'resources'))


def _getdbpath():
  return os.path.join(_getResourceDir(), 'grid_fitting_run.db')


@pytest.mark.skipif(not RPY_AVAILABLE, reason = "requires rpy2")
def test_serializeTableForR():
  """Test atsim.pro_fit.db.serializeTableForR()"""
  engine = sa.create_engine("sqlite:///"+_getdbpath())

  tempdir = tempfile.mkdtemp()
  oldir = os.getcwd()
  try:
    os.chdir(tempdir)

    # Get the IterationSeriesTable
    table = db.IterationSeriesTable(engine,
      candidateFilter = 'all',
      iterationFilter = 'all',
      columns = ['evaluator:mult:mult:val:Z:extracted_value', 'variable:A', 'variable:B'])

    filename = "dget.r"
    with open(filename, "wb") as outfile:
      db.serializeTableForR(table, outfile, 'variable:A', 'variable:B', 'evaluator:mult:mult:val:Z:extracted_value')

    assert_that(filename).exists()

    import rpy2.robjects as robjects

    # Use rpy to load the object using dget and then test against it.
    rstruct = robjects.r["dget"](filename)
    assert_that(type(rstruct)).is_equal_to(robjects.vectors.ListVector)
    assert_that(sorted(['x', 'y', 'z', 'x_name', 'y_name', 'z_name'])).is_equal_to(sorted(rstruct.names))

    # Now check the contents of each list
    expect_x = [1.0,2.0,3.0,4.0,5.0]
    expect_y = [0.0,2.0,4.0,6.0,8.0,10.0]
    expect_z = []
    for x in expect_x:
      for y in expect_y:
        expect_z.append(x*y)

    # ... extract the values from R data structure and compare with lists above.
    actual_x = list(rstruct.rx('x')[0])
    actual_y = list(rstruct.rx('y')[0])
    actual_z = list(rstruct.rx('z')[0])


    assert_that(actual_x).is_equal_to(expect_x)
    assert_that(actual_y).is_equal_to(expect_y)
    assert_that(actual_z).is_equal_to(expect_z)

  finally:
    shutil.rmtree(tempdir, ignore_errors = True)
    os.chdir(oldir)

def test_serializeTableForR_badcolumnkeys():
  """Test that atsim.pro_fit.db.serializeTableForR() throws when bad column keys are specified"""
  engine = sa.create_engine("sqlite:///"+_getdbpath())

  tempdir = tempfile.mkdtemp()
  oldir = os.getcwd()
  try:
    os.chdir(tempdir)

    # Get the IterationSeriesTable
    table = db.IterationSeriesTable(engine,
      candidateFilter = 'all',
      iterationFilter = 'all',
      columns = ['evaluator:mult:mult:val:Z:extracted_value', 'variable:A', 'variable:Bad'])

    filename = "dget.r"
    with open(filename, "wb") as outfile:
      try:
        db.serializeTableForR(table, outfile, 'variable:A', 'variable:B', 'evaluator:mult:mult:val:Z:extracted_value')
        fail("KeyError not raised")
      except KeyError:
        pass
  finally:
    shutil.rmtree(tempdir, ignore_errors = True)
    os.chdir(oldir)

def test_serializeTableForR_rangeDiscoveIterator():
  """Tests for atsim.pro_fit._db._tableSerialize._RangeDiscoverIterator()"""
  # import pdb;pdb.set_trace()
  rangeDiscover = _RangeDiscoverIterator()

  rows = [
    (0,0),
    (0,1),
    (0,2),
    (0,3) ]

  for x,y in rows:
    print rangeDiscover
    rangeDiscover.feed(x,y)

  assert_that(rangeDiscover.x_range).is_equal_to([0])
  assert_that(rangeDiscover.y_range).is_equal_to([0,1,2,3])

  rangeDiscover = _RangeDiscoverIterator()

  rows = [
    (0,0),
    (1,0),
    (2,0),
    (3,0) ]

  for x,y in rows:
    print rangeDiscover
    rangeDiscover.feed(x,y)

  assert_that(rangeDiscover.x_range).is_equal_to([0,1,2,3])
  assert_that(rangeDiscover.y_range).is_equal_to([0])

def test_serializeTableForR_rangeDiscoveIterator_two_axes_changing():
  """Tests for atsim.pro_fit._db._tableSerialize._RangeDiscoverIterator()"""
  rangeDiscover = _RangeDiscoverIterator()

  rows = [
    (0,0),
    (0,1),
    (0,2),

    (1,0),
    (1,1),
    (1,2),

    (2,0),
    (2,1),
    (2,2)]

  for x,y in rows:
    print rangeDiscover
    rangeDiscover.feed(x,y)

  assert_that(rangeDiscover.x_range).is_equal_to([0,1,2])
  assert_that(rangeDiscover.y_range).is_equal_to([0,1,2])


def test_serializeTableForR_rangeDiscoveIterator_negativeAxis():
  """Tests for atsim.pro_fit._db._tableSerialize._RangeDiscoverIterator()"""
  rangeDiscover = _RangeDiscoverIterator()

  rows = [
    (0,3),
    (0,2),
    (0,1),
    (0,0) ]

  for x,y in rows:
    print rangeDiscover
    rangeDiscover.feed(x,y)

  assert_that(rangeDiscover.x_range).is_equal_to([0])
  assert_that(rangeDiscover.y_range).is_equal_to([3,2,1,0])
