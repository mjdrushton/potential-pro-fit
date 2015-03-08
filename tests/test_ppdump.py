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


def _check_table(outputlines, extracols = None):
  expect = [
    [  'iteration_number','candidate_number', 'merit_value'],
    [  0                 , 0                , 2416.9492376805674],
    [  1                 , 0                , 2296.3213811719497],
    [  2                 , 0                , 2255.5581525661546 ]]

  if extracols != None:
    for erow, extrarow in zip(expect, extracols):
      erow.extend(extrarow)

  outputlines = [ v.strip() for v in outputlines if v]
  assert_that(outputlines).is_length(len(expect))

  for i,(line, eline) in enumerate(zip(outputlines, expect)):
    tokens = line.split(",")
    assert_that(tokens).is_length(len(eline))

    actual = tokens
    if i != 0:
      actual = [ int(tokens[0]), int(tokens[1]), float(tokens[2])]
      actual.extend([float(v) for v in tokens[3:]])

      assert_that(actual[0]).is_equal_to(eline[0])
      assert_that(actual[1]).is_equal_to(eline[1])
      assert_that(actual[2]).is_close_to(eline[2], 1e-5)

      for acol, ecol in zip(actual[3:], eline[3:]):
        # assert_that(acol).is_digit()
        assert_that(acol).is_close_to(ecol, 1e-5)

    else:
      assert_that(eline).is_equal_to(actual)


def testOptionOutput(tmpdir):
  """Test ppdump --output"""
  outputfilename = str(tmpdir.join("output.csv", abs = True))
  outputlines = _run_ppdump(["-f %s" % _getdbpath(), "-o %s" % outputfilename])
  assert_that(outputfilename).is_file()

  with open(outputfilename) as infile:
    lines = infile.readlines()
    _check_table(lines)


def testOptionColumn():
  """Test ppdump --columns option"""
  colkeys = ["evaluator:T=1600K_Gd_x=0.1:T=1600K_Gd_x=0.1:Volume:V:extracted_value",
              "variable:M_charge"]

  evaluator = [85314.6263020015,
               85254.8501058408,
               85183.2203962876]
  mcharge = [1.6656, 1.6656, 1.6656]

  extracols = [colkeys]
  extracols.extend(zip(evaluator, mcharge))

  outputlines = _run_ppdump(["-f %s" % _getdbpath(), "-c %s" % " ".join(colkeys)])
  print outputlines
  _check_table(outputlines, extracols)

def testDumpNoOptions():
  """Test ppdump with default options"""
  outputlines = _run_ppdump(["-f %s" % _getdbpath()])
  _check_table(outputlines)

