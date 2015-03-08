import os
import subprocess
from assertpy import assert_that

def _getdbpath():
  from test_db.test_iterationseries import ColumnKeysTestCase
  return ColumnKeysTestCase.dbPath()

def _run_ppdump(args):
  output = subprocess.check_output("ppdump %s" % " ".join(args), shell = True)
  actual = output.split(os.linesep)
  return actual

def _columnKeys_test(arg, columnSetKey, sort = True):
  from test_db.test_iterationseries import ColumnKeysTestCase
  actual = _run_ppdump([arg, "-f '%s'" % _getdbpath()])
  actual = [ v for v in actual if v]
  if sort:
    actual = sorted(actual)

  expect = getattr(ColumnKeysTestCase,columnSetKey+"Expect")()
  assert_that(expect).is_equal_to(actual)


def testOption_list_it_columns():
  """Test ppdump --list-it-columns"""
  _columnKeys_test("--list-it-columns", "runningFilter")

def testOption_list_stat_columns():
  """Test ppdump --list-stat-columns"""
  _columnKeys_test("--list-stat-columns", "stat")

def testOption_list_variable_columns():
  """Test ppdump --list-variable-columns"""
  _columnKeys_test("--list-variable-columns", "variable")

def testOption_list_evaluator_columns():
  """Test ppdump --list-evaluator-columns"""
  _columnKeys_test("--list-evaluator-columns", "evaluator")

def test_list_columns():
  """Test ppdump --list-columns"""
  _columnKeys_test("--list-columns", "all", sort = False)

def testOption_numiterations():
  """Test ppdump --num-iterations"""
  dbPath = _getdbpath()
  expect = '2'
  actual = _run_ppdump(["-f %s" % dbPath, "-n"])
  assert_that([expect, '']).is_equal_to(actual)

  actual = _run_ppdump(["-f %s" % dbPath, "--num-iterations"])
  assert_that([expect, '']).is_equal_to(actual)

def testOptionOutput():
  """Test ppdump --output"""
  assert False

def testDumpNoOptions():
  """Test ppdump with default options"""
  expect = [
    [  'iteration_number','candidate_number', 'merit_value'],
    [  0                 , 0                , 2416.9492376805674],
    [  1                 , 0                , 2296.3213811719497],
    [  2                 , 0                , 2255.5581525661546 ]]

  outputlines = _run_ppdump(["-f %s" % _getdbpath()])
  outputlines = [ v for v in outputlines if v]

  assert_that(outputlines).is_length(len(expect))

  for i,(line, eline) in enumerate(zip(outputlines, expect)):
    tokens = line.split(",")
    assert_that(tokens).is_length(len(eline))

    actual = tokens
    if i != 0:
      actual = [ int(tokens[0]), int(tokens[1]), float(tokens[2])]

      assert_that(actual[0]).is_equal_to(eline[0])
      assert_that(actual[1]).is_equal_to(eline[1])
      assert_that(actual[2]).is_close_to(eline[2], 1e-5)


    else:
      assert_that(eline).is_equal_to(actual)

