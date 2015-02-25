import sqlalchemy as sa

from _util import retry_backoff

import db

import logging
_retryLogger = logging.getLogger("atsim.pro_fit.reporters.SQLiteReporter.retry")

class SQLiteReporter(object):
  """Minimizer stepCallback that places results in a sqlite database"""

  _metadata = db.getMetadata()

  @staticmethod
  def getMetaData():
    """@return sqlalchemy.MetaData describing database schema used by this class"""
    # Create tables
    return SQLiteReporter._metadata

  def __init__(self, dbfilename, initialVariables, calculatedVariables, title = None):
    """@param dbfilename Filename for database, None if in-memory database is to be used.
       @param initialVariables Variables instance containing variables before minimization.
       @param calculatedVariables CalculatedVariables instance used by Merit object."""
    self.dbfilename = dbfilename
    self._createDatabase()
    with self._saengine.begin() as conn:
      self._populateVariableKeysTable(conn, initialVariables, calculatedVariables)
      self._populateStatus(conn, title)
    self.iterationNum = 0

  def _createDatabase(self):
    """Create sqlite database"""

    if self.dbfilename:
      engine = sa.create_engine('sqlite:///%s' % self.dbfilename)
    else:
      engine = sa.create_engine('sqlite:///:memory:')

    # Create database tables
    metadata = self._metadata
    metadata.create_all(engine)

    self._saengine = engine
    self._metadata = metadata

  def _populateVariableKeysTable(self, conn, variables, calculatedVariables):
    """Populate the variable_keys table with initial variables"""
    variableKeysTable = self._metadata.tables['variable_keys']
    insertQuery = variableKeysTable.insert()

    insertData = []
    for (k,v,ff), bounds in zip(variables.flaggedVariablePairs, variables.bounds):
      if bounds:
        lowbound,highbound = bounds
      else:
        lowbound,highbound = None, None

      insertData.append(
        dict(variable_name = k,
          fit_flag = ff,
          low_bound = lowbound, upper_bound = highbound,
          calculated_flag = False, calculation_expression = None))

    # Process the CalculatedVariables fields
    for (k,v) in calculatedVariables.nameExpressionTuples:
      insertData.append(
        dict(variable_name = k,
          fit_flag = False,
          low_bound = None, upper_bound = None,
          calculated_flag = True, calculation_expression = v))

    conn.execute(insertQuery, insertData)

  def _populateStatus(self, conn, title):
    """Initially populate the 'runstatus' table

    @param conn Database connection instance
    @param title Title for the run"""
    table = self._metadata.tables['runstatus']
    insert = table.insert()
    conn.execute(insert, dict(runstatus = 'Running', title=title))

  def _createCandidateRecord(self, conn, iterationNum, candidateNum, meritValue):
    # Create candidate instance
    candidateTable = self._metadata.tables['candidates']
    candidateInsert = candidateTable.insert()
    result = conn.execute(candidateInsert,
      iteration_number = iterationNum,
      candidate_number = candidateNum,
      merit_value = meritValue)
    candidateId = result.inserted_primary_key[0]
    return candidateId

  def _createVariables(self, conn, candidateId, variables):
    variablesTable = self._metadata.tables['variables']
    insert = variablesTable.insert()
    insertValues = [ dict(candidate_id = candidateId, variable_name = k, value = v) for (k,v) in variables.variablePairs]
    conn.execute(insert, insertValues)

  def _createEvaluators(self, conn, jobId, job):
    evaluatorTable = self._metadata.tables['evaluated']
    insert = evaluatorTable.insert()

    # Create insertion dictionaries
    staticValues = {'job_id' : jobId}
    insertValues = []
    for evaluator in job.evaluatorRecords:
      for record in evaluator:
        if record.errorFlag:
          errorId = self._insertEvaluatorError(conn, record)
        else:
          errorId = None

        v = dict(
          evaluator_name = record.evaluatorName,
          value_name = record.name,
          expected_value = record.expectedValue,
          extracted_value = record.extractedValue,
          weight = record.weight,
          merit_value = record.meritValue,
          evaluatorerror_id = errorId
          )
        v.update(staticValues)
        insertValues.append(v)
    conn.execute(insert, insertValues)

  def _insertEvaluatorError(self, conn, record):
    table = self._metadata.tables['evaluatorerror']
    insert = table.insert()
    result = conn.execute(insert, msg = str(record.exception))
    return result.lastrowid

  def _createJobs(self, conn, candidateId, joblist):
    jobTable = self._metadata.tables['jobs']
    insert = jobTable.insert()

    for j in joblist:
      result = conn.execute(insert, job_name = j.name, candidate_id = candidateId)
      jobId = result.inserted_primary_key[0]
      # Create associated evaluator records
      self._createEvaluators(conn, jobId, j)

  @retry_backoff([sa.exc.OperationalError], initialSleep = 1, maxSleep = 60, logger = _retryLogger)
  def finished(self, error = False):
    """Update status table to 'Finished'"""
    table = self._metadata.tables['runstatus']
    update = table.update().where(table.c.id == 1)

    with self._saengine.begin() as conn:
      if error:
        conn.execute(update, dict(runstatus = 'Error'))
      else:
        conn.execute(update, dict(runstatus = 'Finished'))

  @retry_backoff([sa.exc.OperationalError], initialSleep = 1, maxSleep = 60, logger = _retryLogger)
  def __call__(self, minimizerResults):
    meritValues = minimizerResults.meritValues
    jobLists = minimizerResults.candidateJobList
    with self._saengine.begin() as conn:
      for (candidateNum, (mval, (variables, joblist))) in enumerate(zip(meritValues, jobLists)):
        # Create record in the 'candidates' table
        candidateId = self._createCandidateRecord(conn,
          self.iterationNum, candidateNum, mval)

        # Create records in the 'variables' table
        self._createVariables(conn, candidateId, variables)

        # Create records in the 'jobs' table
        self._createJobs(conn, candidateId, joblist)

    self.iterationNum += 1


class LogReporter(object):
  """Minimizer stepCallback that logs the best-ever variables and merit-value to
  a given logging.Logger. Logs at info level."""

  def __init__(self, logger):
    """@param logger logging.Logger instance"""
    self.iteration = 0
    self.bestIteration = None
    self._logger = logger

  def __call__(self, minimizerResults):
    self._updateBest(minimizerResults)
    self._log()
    self.iteration += 1


  def _updateBest(self, minimizerResults):
    if self.bestIteration is None:
      self.bestIteration = minimizerResults
    elif minimizerResults < self.bestIteration:
      self.bestIteration = minimizerResults

  def _log(self):
    self._logger.info("Iteration: %d" % self.iteration)
    self._logger.info("Merit-value: %f. Variables:" % self.bestIteration.bestMeritValue)
    variables = self.bestIteration.bestVariables
    fitkeys = set(variables.fitKeys)
    for k, v in variables.variablePairs:
      if k in fitkeys:
        fflag = ' *'
      else:
        fflag = ''
      self._logger.info("  %s = %.10f %s" % (k,v,fflag))
