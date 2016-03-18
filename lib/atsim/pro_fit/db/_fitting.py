"""Module for querying the fitting_run.db"""

import math
import sqlalchemy as sa
from _util import calculatePercentageDifference
import _metadata

metadata = _metadata.getMetadata()

class Fitting(object):

  def __init__(self, engine):
    """:param engine: SQLAlchemy engine instance"""
    self.engine = engine

  def _getCandidateID(self, iterationNumber, candidateNumber):
    iterationNumber = int(iterationNumber)
    candidateNumber = int(candidateNumber)

    candidates = metadata.tables['candidates']
    cidQuery = sa.select([candidates.c.id])\
      .where(sa.and_(candidates.c.iteration_number == iterationNumber , candidates.c.candidate_number == candidateNumber))

    results = self.engine.execute(cidQuery)
    cid = results.fetchone()[0]
    return cid

  def current_iteration(self):
    """Returns the current generation (i.e. the largest iteration in the database).

    :return int: Maximum iteration number in database"""

    select = sa.select([sa.func.max(metadata.tables['candidates'].c.iteration_number).label('max_iteration')])
    results = self.engine.execute(select)
    maxIteration = results.fetchone()[0]
    results.close()
    return maxIteration

  def best_candidate(self):
    """Returns dictionary containing data for the best candidate in the database.

    The dictionary has the following form:

    .. code-block:: python

      {
        'id' :  id of solution within candidates table,
        'iteration_number' : generation number in which best candidate was found,
        'candidate_number' : solution number within generation,
        'merit_value' : best merit value
      }

    :return dict: Best candidate information."""

    table = metadata.tables['candidates']
    query = sa.select([
      table.c.id,
      table.c.candidate_number,
      table.c.iteration_number,
      sa.func.min(table.c.merit_value).label('merit_value')
      ])
    results = self.engine.execute(query)
    row = dict(zip(results.keys(), results.fetchone()))
    return row

  def run_status(self):
    """Returns status of fitting run as a dictionary.

    Dictionary has the format:

    .. code-block:: python

    { 'run_status' : status of run, one of "Running", "Finished" or "Error",
      'title'      : title of the run }


    :return dict: Dictionary of the format described above"""
    table = metadata.tables['runstatus']
    query = sa.select([table.c.runstatus, table.c.title])
    results = self.engine.execute(query)
    row = dict(zip(results.keys(), results.fetchone()))
    return row

  def iteration_overview(self, iterationNumber):
    """Returns overview and statistics for a given iteration.

    :parameter int iterationNumber: iteration for which statistics should be returned.

    Resulting dictionary has the following format:

    .. code-block:: python

        {
        'iteration_number' : iteration to which statistics relate,
        'num_candidates' : iteration population size,
        'mean' : average merit value,
        'standard_deviation' : merit value standard deviation,
        'minimum' : CANDIDATE_RECORD for population member with minimum merit value,
        'maximum' : CANDIDATE_RECORD for population member with maximum merit value,
      }

    The format of ``CANDIDATE_RECORD`` as returned for ``'minimum'`` and ``'maximum'`` has
    following format:

    .. code-block:: javascript

      {
       'id' :  id of solution within candidates table,
       'iteration_number' : generation number in which best candidate was found,
       'candidate_number' : solution number within generation,
       'merit_value' : best merit value
      } """
    table = metadata.tables['candidates']

    # Get maximum record
    query = sa.select([
      table.c.id,
      table.c.iteration_number,
      table.c.candidate_number,
      sa.func.max(table.c.merit_value).label('merit_value')])\
        .where(table.c.iteration_number == iterationNumber)
    results = self.engine.execute(query)
    maxRecord = dict(zip(results.keys(), results.fetchone()))

    # Get minimum record
    query = sa.select([
      table.c.id,
      table.c.iteration_number,
      table.c.candidate_number,
      sa.func.min(table.c.merit_value).label('merit_value')])\
        .where(table.c.iteration_number == iterationNumber)
    results = self.engine.execute(query)
    minRecord = dict(zip(results.keys(),results.fetchone()))

    # Get number of population members and mean
    query = sa.select([
      sa.func.count(table.c.id),
      sa.func.avg(table.c.merit_value)])\
        .where(table.c.iteration_number == iterationNumber)
    results = self.engine.execute(query)
    num_candidates, mean = results.fetchone()

    # Calculate standard deviation
    query = sa.select([table.c.merit_value]).where(table.c.iteration_number == iterationNumber)
    results = self.engine.execute(query)
    def calc():
      stdev = 0.0
      for row in results:
        v = row[0]
        if v == None:
          return None, None
        stdev += (v - mean)**2.0
      stdev /= float(num_candidates)
      stdev = math.sqrt(stdev)
      return mean, stdev
    mean,stdev = calc()

    return {
      'iteration_number' : int(iterationNumber),
      'num_candidates' : num_candidates,
      'mean' : mean,
      'standard_deviation' : stdev,
      'minimum' : minRecord,
      'maximum' : maxRecord
    }

  def variables(self, iterationNumber, candidateNumber):
    """Returns dictionary representing variables for given iterationNumber and candidateNumber within that iteration's
    population.

    :parameter int iterationNumber: Number of the iteration for which information is returned.
    :parameter int candidateNumber: Index of candidate within population of parameter sets for given iteration.

    Returned dictionary has the form:

    .. code-block:: python

      {
        'variable_name' : name of variable,
        'fit_flag'      : boolean, true if variable is changed during fitting ,
        'low_bound'     : variable low bound or null if no bound set,
        'upper_bound'   : variable's upper bound or null if no bound set,
        'calculated_flag' : boolean, true if variable is a calculated variable,
        'calculation_expression' : calculated_flag is true then this field gives the expression used to calculate field,
        'value' : current value of variable
      }

    :return dict: Dictionary of form given above.

    """

    variables = metadata.tables['variables']
    variable_keys = metadata.tables['variable_keys']

    cid = self._getCandidateID(iterationNumber, candidateNumber)

    query = sa.select([variable_keys.c.variable_name,
      variable_keys.c.fit_flag,
      variable_keys.c.low_bound,
      variable_keys.c.upper_bound,
      variable_keys.c.calculated_flag,
      variable_keys.c.calculation_expression,
      variables.c.value
      ]).where(sa.and_(variable_keys.c.variable_name == variables.c.variable_name , variables.c.candidate_id == cid))

    results = self.engine.execute(query)
    output = []
    for row in results:
      d = dict(zip(results.keys(), row))
      if d['low_bound'] and math.isinf(float(d['low_bound'])):
        d['low_bound'] = None

      if d['upper_bound'] and math.isinf(float(d['upper_bound'])):
        d['upper_bound'] = None
      output.append(d)

    return output

  def evaluated(self, iterationNumber, candidateNumber):
    """Returns dictionary representing evaluator fields for given iterationNumber and candidateNumber within that
    iteration's population.

    :parameter int iterationNumber: Number of the iteration for which information is returned.
    :parameter int candidateNumber: Index of candidate within population of parameter sets for given iteration.

    Returned dictionary has the form:

    .. code-block:: python


      {
        'evaluator_name' : Name of evaluator that generated record (format JOB:EVALUATOR_NAME),
        'value_name' : Name of value within evaluator,
        'expected_value' : Expectd value,
        'extracted_value' : Value extracted by evaluator,
        'weight' : Weighting factor for value resulting from comparison with expected_value,
        'merit_value' : Merit value as used for global merit value sum,
        'error_message' : If error occurred this field contains error message, null otherwise,
        'job_name' : Name of job to which this record belongs.
      }

    """
    cid = self._getCandidateID(iterationNumber, candidateNumber)

    # Required query:
    # SELECT * FROM evaluated JOIN jobs ON evaluated.job_id = jobs.id LEFT OUTER JOIN evaluatorerror ON evaluated.evaluatorerror_id = evaluatorerror.id WHERE jobs.candidate_id = 12;

    evaluated = metadata.tables['evaluated']
    jobs = metadata.tables['jobs']
    evaluatorerror = metadata.tables['evaluatorerror']

    query = sa.select([
      evaluated.c.evaluator_name,
      evaluated.c.value_name,
      evaluated.c.expected_value,
      evaluated.c.extracted_value,
      evaluated.c.weight,
      evaluated.c.merit_value,
      evaluatorerror.c.msg.label('error_message'),
      jobs.c.job_name
    ], whereclause = jobs.c.candidate_id == cid,
       from_obj=[sa.sql.expression.Join(evaluated, jobs, evaluated.c.job_id == jobs.c.id)\
          .outerjoin(evaluatorerror, evaluated.c.evaluatorerror_id == evaluatorerror.c.id)])

    results = self.engine.execute(query)

    output = []
    for row in results:
      d = dict(zip(results.keys(), row))
      d['percent_difference'] = calculatePercentageDifference(d)
      output.append(d)

    return output
