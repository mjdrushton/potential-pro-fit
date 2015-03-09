import os
import subprocess
from assertpy import assert_that

def _getdbpath():
  from test_db.test_iterationseries import ColumnKeysTestCase
  return ColumnKeysTestCase.dbPath()

def _getpopdbpath():
  from test_db.test_iterationseries import IterationSeriesTestCase
  return IterationSeriesTestCase.dbPath()

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

def testOption_list_fit_variable_columns():
  """Test ppdump --list-fit-variable-columns"""
  _columnKeys_test("--list-fit-variable-columns", "fittingVariable")


def testOption_list_evaluator_columns():
  """Test ppdump --list-evaluator-columns"""
  _columnKeys_test("--list-evaluator-columns", "evaluator")

def test_list_columns():
  """Test ppdump --list-columns"""
  _columnKeys_test("--list-columns", "all", sort = False)

def testGetColumnList():
  from test_db.test_iterationseries import ColumnKeysTestCase
  import sqlalchemy as sa
  engine = sa.create_engine("sqlite:///"+_getdbpath())
  from atsim.pro_fit.tools import ppdump

  columns = ppdump._getColumnList(engine, None, [ppdump._VARIABLE_COLUMN_SET])
  assert_that(columns).is_equal_to(
    ColumnKeysTestCase.variableExpect())

  columns = ppdump._getColumnList(engine, None, [ppdump._FIT_VARIABLE_COLUMN_SET])
  assert_that(columns).is_equal_to(
    ColumnKeysTestCase.fittingVariableExpect())


  columns = ppdump._getColumnList(engine, None, [ppdump._EVALUATOR_COLUMN_SET])
  assert_that(columns).is_equal_to(
    ColumnKeysTestCase.evaluatorExpect())



  expect = []
  expect.extend(ColumnKeysTestCase.evaluatorExpect())
  expect.extend(ColumnKeysTestCase.variableExpect())

  columns = ppdump._getColumnList(engine, [], [ppdump._EVALUATOR_COLUMN_SET, ppdump._VARIABLE_COLUMN_SET])
  assert_that(columns).is_equal_to(expect)

  expect = ['variable:M_charge']
  expect.extend(ColumnKeysTestCase.evaluatorExpect())
  expect.extend([ v for v in ColumnKeysTestCase.variableExpect() if v != "variable:M_charge"])

  columns = ppdump._getColumnList(engine, ["variable:M_charge"], [ppdump._EVALUATOR_COLUMN_SET, ppdump._VARIABLE_COLUMN_SET])
  assert_that(columns).is_equal_to(expect)



def testColumnSets():
  """Tests ppdump --variable-columns --evaluator-columns --all-columns options"""
  from test_db.test_iterationseries import ColumnKeysTestCase

  prefix = "iteration_number,candidate_number,merit_value"
  vkeys = ",".join(ColumnKeysTestCase.variableExpect())
  ekeys = ",".join(ColumnKeysTestCase.evaluatorExpect())
  fvkeys = ",".join(ColumnKeysTestCase.fittingVariableExpect())

  outputlines = _run_ppdump(["-f %s" % _getdbpath(), "--variable-columns"])
  assert_that(outputlines[0]).is_equal_to(",".join([prefix, vkeys]))

  outputlines = _run_ppdump(["-f %s" % _getdbpath(), "--fit-variable-columns"])
  assert_that(outputlines[0]).is_equal_to(",".join([prefix, fvkeys]))


  outputlines = _run_ppdump(["-f %s" % _getdbpath(), "--evaluator-columns"])
  assert_that(outputlines[0]).is_equal_to(",".join([prefix,ekeys]))

  outputlines = _run_ppdump(["-f %s" % _getdbpath(), "--evaluator-columns --variable-columns"])
  assert_that(outputlines[0]).is_equal_to(",".join([prefix,ekeys,vkeys]))

  outputlines = _run_ppdump(["-f %s" % _getdbpath(), "-c variable:M_charge --evaluator-columns --variable-columns"])

  expect = ",".join([prefix, "variable:M_charge"])
  expect = ",".join([expect, ekeys])
  vfiltered = ",".join([v for v in ColumnKeysTestCase.variableExpect() if v != 'variable:M_charge'])
  expect = ",".join([expect, vfiltered])
  assert_that(outputlines[0]).is_equal_to(expect)



def testOption_numiterations():
  """Test ppdump --num-iterations"""
  dbPath = _getdbpath()
  expect = '2'
  actual = _run_ppdump(["-f %s" % dbPath, "-n"])
  assert_that([expect, '']).is_equal_to(actual)

  actual = _run_ppdump(["-f %s" % dbPath, "--num-iterations"])
  assert_that([expect, '']).is_equal_to(actual)


def _check_table(outputlines, extracols = None, expect = None):
  if not expect:
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
  _check_table(outputlines, extracols = extracols)


def testDumpNoOptions():
  """Test ppdump with default options"""
  outputlines = _run_ppdump(["-f %s" % _getdbpath()])
  _check_table(outputlines)


def testCandidateFilterAll():
  """Test ppdump with --candidate-filter=all"""
  outputlines = _run_ppdump(["-f %s" % _getpopdbpath(), "--candidate-filter=all"])
  expect = [['iteration_number','candidate_number','merit_value'],
            [0,0,3329.44833],
            [0,1,56979.43601],
            [0,2,973.78207],
            [0,3,4336.72706],
            [1,0,5283.62466],
            [1,1,5096.59874],
            [1,2,3329.44833],
            [1,3,973.78207],
            [2,0,1546.33659],
            [2,1,5096.59874],
            [2,2,3329.44833],
            [2,3,973.78207],
            [3,0,980.44924],
            [3,1,973.78207],
            [3,2,1546.33659],
            [3,3,973.78207],
            [4,0,2300.90601],
            [4,1,964.64312],
            [4,2,973.78207],
            [4,3,973.78207],
            [5,0,1998.33524],
            [5,1,12634.65516],
            [5,2,973.78207],
            [5,3,964.64312]]
  _check_table(outputlines, expect = expect)

def testCandidateFilterMin():
    """Test ppdump with --candidate-filter=min"""
    outputlines = _run_ppdump(["-f %s" % _getpopdbpath(), "--candidate-filter=min"])
    expect = [['iteration_number','candidate_number','merit_value'],
              [0,2,973.78207],
              [1,3,973.78207],
              [2,3,973.78207],
              [3,1,973.78207],
              [4,1,964.64312],
              [5,3,964.64312]]
    _check_table(outputlines, expect = expect)

def testCandidateFilterMax():
  """Test ppdump with --candidate-filter=max"""
  outputlines = _run_ppdump(["-f %s" % _getpopdbpath(), "--candidate-filter=max"])
  expect = [['iteration_number','candidate_number','merit_value'],
            [0,1,56979.43601],
            [1,0,5283.62466],
            [2,1,5096.59874],
            [3,2,1546.33659],
            [4,0,2300.90601],
            [5,1,12634.65516]]
  _check_table(outputlines, expect = expect)

