from atomsscripts import mathutil
from atomsscripts.fitting.reporters import SQLiteReporter
from _util import calculatePercentageDifference

_metadata = SQLiteReporter.getMetaData()

import sqlalchemy as sa

import functools
import contextlib
import operator
import re


class _RunningFilter(object):
  def __init__(self, op):
    self.lastv = None
    self.first = True
    self.op = op

  def __call__(self, v):
    if self.first:
      self.first = False
      retval =  True
    elif self.op(v, self.lastv):
      retval =  True
    else:
      retval =  False

    if retval:
      self.lastv = v
    return retval


def _RunningMinFilter():
  return _RunningFilter(operator.lt)

def _RunningMaxFilter():
  return _RunningFilter(operator.gt)

def _NullFilter():
  def f(v):
    return True
  return f

class _RunningFilterColumnProvider(object):
  def __init__(self, primaryColumnKey, columnName, runningFilter):
    self._primaryColumnKey = primaryColumnKey
    self._columnName = columnName
    self._RunningFilter = runningFilter

  def __call__(self, iterationNumber, candidateNumber, rowDict):
    return [(self._columnName, self._RunningFilter(rowDict[self._primaryColumnKey]))]


class _VariablesColumnProvider(object):
  """Column provider for series of variable  values.

  Column-label has format:
    variable:VARIABLE_NAME"""

  def __init__(self, conn, tempMeta, columnLabel):
    self.conn = conn
    self.variableName= self._parseColumnLabel(columnLabel)
    self.columnLabel = columnLabel
    self.results = self._performQuery(conn, tempMeta)


  def _parseColumnLabel(self, columnLabel):
    """Extracts variable name for column label of form variable:VARIABLE_NAME

    Raises KeyError if VARIABLE_NAME is not value.

    @param columnLabel Column identifier.
    @return VARIABLE_NAME"""
    variableName = columnLabel.split(":")[1]
    t = _metadata.tables['variable_keys']
    query = sa.select([t.c.variable_name]).where(t.c.variable_name == variableName)
    with contextlib.closing(self.conn.execute(query)) as results:
      if not results.fetchone():
        raise KeyError("Variable not defined in database:"+variableName)
    return variableName

  def _performQuery(self, conn, tempMeta):
    tt = tempMeta.tables['temp_iterationseries']
    tv = _metadata.tables['variables']

    query = sa.select([
      tt.c.candidate_id,
      tt.c.iteration_number,
      tt.c.candidate_number,
      tv.c.value,
      ]).where(
        sa.and_(
          tv.c.variable_name == self.variableName,
         tt.c.candidate_id == tv.c.candidate_id))
    return conn.execute(query)

  def __call__(self, iterationNumber, candidateNumber, rowDict):
    resrow = self.results.fetchone()
    assert(iterationNumber == resrow[1] and candidateNumber == resrow[2])
    return [(self.columnLabel, resrow[3])]


class _EvaluatorColumnProvider(object):
  """Column provider for evaluator values.

  Column-label has format:

    evaluator:JOB_NAME:EVALUATOR_NAME:VALUE_NAME:VALUE_TYPE

  Where:
    JOB_NAME - Name of job
    EVALUATOR_NAME - Name of evaluator for which value should be produced
    VALUE_NAME - Name of value extracted by evaluator
    VALUE_TYPE - 'merit' or 'extract' which give merit value or extracted value for evaluator respectively."""

  _labelSplitRegex = re.compile(r'^evaluator:(?P<jobName>.*?):(?P<evaluatorName>.*):(?P<valueName>.*):(?P<valueType>merit|extract|percent)$')


  def __init__(self, conn, tempMeta, columnLabel):
    self.conn = conn
    self.columnLabel = columnLabel
    self.results = self._performQuery(conn, tempMeta)

  @staticmethod
  def _splitColumnLabel(columnLabel):
    match = _EvaluatorColumnProvider._labelSplitRegex.match(columnLabel)

    if not match:
      raise KeyError("Bad evaluator column definition: '%s'" % columnLabel)

    groupdict = match.groupdict()
    return  groupdict

  def _performQuery(self, conn, tempMeta):

    groupdict = self._splitColumnLabel(self.columnLabel)

    # jobName = groupdict['jobName']
    evaluatorName = groupdict['evaluatorName']
    valueName = groupdict['valueName']
    valueType = groupdict['valueType']

    allowedValues = ['merit', 'extract', 'percent']
    if not (valueType in allowedValues):
      raise KeyError("Requested value should be one of %s for column label: %s", (str(allowedValues), self.columnLabel))

    tt = tempMeta.tables['temp_iterationseries']
    tj = _metadata.tables['jobs']
    te = _metadata.tables['evaluated']

    # Dictionary mapping valuetype to tuple containing columns and a row dictionary poste process function used to extract column.
    extracols, self.rowpostprocess = {
      'merit'   : ([te.c.merit_value], operator.itemgetter('merit_value')),
      'extract' : ([te.c.extracted_value], operator.itemgetter('extracted_value')),
      'percent' : ([te.c.extracted_value, te.c.expected_value], calculatePercentageDifference)}[valueType]

    columns = [
      tt.c.candidate_id,
      tt.c.iteration_number,
      tt.c.candidate_number]

    columns.extend(extracols)

    query = sa.select(columns)\
      .where(
        sa.and_(
          tt.c.candidate_id == tj.c.candidate_id,
          te.c.job_id == tj.c.id,
          te.c.evaluator_name == evaluatorName,
          te.c.value_name == valueName))
    return conn.execute(query)

  def __call__(self, iterationNumber, candidateNumber, rowDict):
    resrow = self.results.fetchone()
    extrarowdict = dict(zip(self.results.keys(), resrow))
    value = self.rowpostprocess(extrarowdict)
    assert(iterationNumber == resrow[1] and candidateNumber == resrow[2])
    return [(self.columnLabel, value)]


class _StatColumnProvider(object):
  """Class responsible for fetching column of data for a particular iteration and
  then applying different statistical calculations to it."""

  _calculators = {'stat:min' : min,
                  'stat:max' : max,
                  'stat:mean' : mathutil.mean,
                  'stat:median' : mathutil.median,
                  'stat:std_dev' : mathutil.stdev,
                  'stat:quartile1': functools.partial(mathutil.quartile,q=1),
                  'stat:quartile2': functools.partial(mathutil.quartile,q=2),
                  'stat:quartile3': functools.partial(mathutil.quartile,q=3)}


  def __init__(self, conn, columnKey, columnLabels):
    """@param conn SQLAlchemy conn
       @param columnKey Column for which stats will be generated
       @param columnLabels List of strings identifying stats to be applied"""

    self.conn = conn
    self.columnKey = columnKey
    self.calculators = self._createCalculators(columnLabels)

  def _createCalculators(self, columnLabels):
    outlist = []
    for cn in columnLabels:
      outlist.append((cn, self._calculators[cn]))
    return outlist

  def __call__(self, iterationNumber, candidateNumber, rowDict):
    candidates = _metadata.tables['candidates']
    query = sa.select([
      candidates.c.iteration_number,
      candidates.c.candidate_number,
      candidates.c.merit_value
      ]).where(candidates.c.iteration_number == iterationNumber)

    results = self.conn.execute(query)
    whichcol = [ i for (i,cn) in enumerate(results.keys()) if cn == self.columnKey][0]
    values = [ row[whichcol] for row in results]

    outlist = []
    for cn, calc in self.calculators:
      v = calc(values)
      outlist.append((cn, v))
    return outlist
