from _util import calculatePercentageDifference

from _metadata import getMetadata

_metadata = getMetadata()

import sqlalchemy as sa

import functools
import contextlib
import operator
import re
import math

def _mean(data):
  """Calculate mean of values in data.

  @param data List of numbers for which mean should be calculated.
  @return Mean of data"""
  return sum(data)/ float(len(data))

def _median(data):
  """Calculate the median of values in data.

  @param data List of numbers for which median should be determined.
  @return Median of data"""
  data = sorted(data)
  # Even length
  if len(data) % 2 == 0 :
    idx = len(data)/2
    v1 = data[idx]
    v2 = data[idx-1]
    return (v1+v2) / float(2.0)
  # Odd length
  else:
    return data[len(data)/2]

def _quartile(data, q):
  """Calculate quartiles.

  @param data Data for which quartile should be calculated.
  @param q Number of quartile to be calculated. One of 1,2,3"""

  if not q in [1,2,3]:
    raise ValueError('q should be 1,2 or 3')

  m = _median(data)
  if q == 1:
    return _median([v for v in data if v < m ])

  if q == 2:
    return m

  if q == 3:
    return _median([v for v in data if v > m ])

def _stdev(data):
  """Calculate the standard deviation of values in data.

  @param data List of data for which standard deviation should be calculated.
  @return Standard deviation of data."""
  m = _mean(data)
  sd = 0.0
  for v in data:
    sd += (v - m)**2.0
  sd /= float(len(data))
  sd = math.sqrt(sd)
  return sd

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


  @classmethod
  def validKeys(cls, engine):
    """Return list of column keys associated with this column provider.

    :param engine: SQL Alchemy Engine
    :return: List of strings containing valid column keys"""
    return ["it:is_running_min", "it:is_running_max"]


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

  @classmethod
  def validKeys(cls, engine):
    """Returns a list of valid variable:VARIABLE_NAME column keys.

    :param engine: SQL Alchemy object supporting execute() method.
    :return list: List of column keys."""

    tv = _metadata.tables['variable_keys']
    query = sa.select([tv.c.variable_name ])
    keys = engine.execute(query).fetchall()
    keys = ["variable:"+k[0] for k in keys]
    return keys


class _EvaluatorColumnProvider(object):
  """Column provider for evaluator values.

  Column-label has format::

    evaluator:JOB_NAME:EVALUATOR_NAME:VALUE_NAME:VALUE_TYPE

  * **Where:**
    - ``JOB_NAME`` - Name of job
    - ``EVALUATOR_NAME`` - Name of evaluator for which value should be produced
    - ``VALUE_NAME`` - Name of value extracted by evaluator
    - ``VALUE_TYPE`` - 'merit_value', 'extracted_value' or 'percent_difference' which give merit, extracted or percentage
      difference value for evaluator respectively."""

  _labelSplitRegex = re.compile(r'^evaluator:(?P<jobName>.*?):(?P<evaluatorName>.*):(?P<valueName>.*):(?P<valueType>merit_value|extracted_value|percent_difference)$')
  _suffixes = ['merit_value','extracted_value','percent_difference']

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

    allowedValues = ['merit_value', 'extracted_value', 'percent_difference']
    if not (valueType in allowedValues):
      raise KeyError("Requested value should be one of %s for column label: %s", (str(allowedValues), self.columnLabel))

    tt = tempMeta.tables['temp_iterationseries']
    tj = _metadata.tables['jobs']
    te = _metadata.tables['evaluated']

    # Dictionary mapping valuetype to tuple containing columns and a row dictionary post process function used to extract column.
    extracols, self.rowpostprocess = {
      'merit_value'   : ([te.c.merit_value], operator.itemgetter('merit_value')),
      'extracted_value' : ([te.c.extracted_value], operator.itemgetter('extracted_value')),
      'percent_difference' : ([te.c.extracted_value, te.c.expected_value], calculatePercentageDifference)}[valueType]

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


  @classmethod
  def validKeys(cls, engine):
    """Returns a list of valid evaluator: column keys.

    :param engine: SQL Alchemy object supporting execute() method.
    :return list: List of column keys."""

    #select distinct job_name, evaluator_name, value_name from evaluated, jobs where evaluated.job_id = jobs.id;

    et = _metadata.tables['evaluated']
    jt = _metadata.tables['jobs']
    query = sa.select(
      [jt.c.job_name,
       et.c.evaluator_name,
       et.c.value_name]).distinct().where(
        sa.and_(et.c.job_id == jt.c.id))

    results = engine.execute(query)

    keys = []
    for row in results.fetchall():
      rowdict = dict(zip(results.keys(), row))
      key = "evaluator:%(job_name)s:%(evaluator_name)s:%(value_name)s" % rowdict
      for suffix in cls._suffixes:
        keys.append(":".join([key, suffix]))
      keys = [k.encode("utf-8") for k in keys]
    return keys



class _StatColumnProvider(object):
  """Class responsible for fetching column of data for a particular iteration and
  then applying different statistical calculations to it."""

  _calculators = {'stat:min' : min,
                  'stat:max' : max,
                  'stat:mean' : _mean,
                  'stat:median' : _median,
                  'stat:std_dev' : _stdev,
                  'stat:quartile1': functools.partial(_quartile,q=1),
                  'stat:quartile2': functools.partial(_quartile,q=2),
                  'stat:quartile3': functools.partial(_quartile,q=3)}


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


  @classmethod
  def validKeys(cls, engine):
    """Returns a list of valid stat: column keys.

    :param engine: SQL Alchemy object supporting execute() method.
    :return list: List of column keys."""
    return cls._calculators.keys()
