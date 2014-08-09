#! /isr/bin/env python

import cherrypy
from cherrypy import tools

import pkgutil
import os
import math
import optparse
import contextlib

import sqlalchemy as sa

from atsim.pro_fit import reporters


from atsim.pro_fit._sqlalchemy_cherrypy_integration import session, configure_session
import  _jinja_cherrypy_integration # noqa

from _columnproviders import  _VariablesColumnProvider, _StatColumnProvider, _EvaluatorColumnProvider
from _columnproviders import _RunningFilterColumnProvider, _NullFilter, _RunningMaxFilter, _RunningMinFilter
from _util import calculatePercentageDifference

class Root:
  extensionToResponseHeader = {
    '.js' : 'text/javascript',
    '.css' : 'text/css',
    '.html' : 'text/html',
    '.png' : 'image/png',
    '.svg' : 'image/svg+xml'
  }

  @cherrypy.expose
  @cherrypy.tools.jinja(template='index.html')
  def index(self):
    """Serve index.html performing jinja template substitution"""
    return {}

  def _serveFromPkg(self, *args):
    resourceurl = ['webresources','static']
    resourceurl.extend(args)
    resourceurl = "/".join(resourceurl)
    junk, extension = os.path.splitext(resourceurl)
    try:
      data = pkgutil.get_data(__package__, resourceurl)
    except IOError:
      raise cherrypy.NotFound
    return extension, data

  def _serveFromFile(self, *args):
    staticpath = cherrypy.request.config.get('static_path', None)
    filepath = os.path.join(staticpath, *args)
    try:
      with open(filepath) as infile:
        data = infile.read()
    except IOError:
      raise cherrypy.NotFound
    junk, extension = os.path.splitext(filepath)
    return extension, data


  @cherrypy.expose
  def resources(self, *args):
    """Serve static resources from files stored in pkg_resources"""
    staticpath = cherrypy.request.config.get('static_path', None)
    if not staticpath:
      extension,data = self._serveFromPkg(*args)
    else:
      extension,data = self._serveFromFile(*args)

    # Set response headers based on extension
    cherrypy.response.headers['Content-Type']= self.extensionToResponseHeader.get(extension, 'text/html')
    return data

metadata = reporters.SQLiteReporter.getMetaData()
class Fitting:

  @cherrypy.expose
  @tools.json_out(on=True)
  def current_iteration(self):
    """Returns JSON containing current generation (i.e. the largest iteration found in the ``candidates`` table of the :ref:`extending_fitting_rundb`).
    Resulting JSON is of form:

    .. code-block:: javascript

      {
        'current_iteration' : current fitting run step (int)
      }

    '"""
    configure_session(cherrypy.request.config['tools.SATransaction.dburi'])
    select = sa.select([sa.func.max(metadata.tables['candidates'].c.iteration_number).label('max_iteration')])
    results = session.execute(select)
    maxIteration = results.fetchone()[0]
    results.close()
    return {'current_iteration' : maxIteration}

  @cherrypy.expose
  @tools.json_out(on=True)
  def best_candidate(self):
    """Returns JSON containing identity of best candidate within database.

    Returned JSON record has following format:

    .. code-block:: javascript

      {
        'id' :  id of solution within candidates table,
        'iteration_number' : generation number in which best candidate was found,
        'candidate_number' : solution number within generation,
        'merit_value' : best merit value
      }


      """
    configure_session(cherrypy.request.config['tools.SATransaction.dburi'])
    table = metadata.tables['candidates']
    query = sa.select([
      table.c.id,
      table.c.candidate_number,
      table.c.iteration_number,
      sa.func.min(table.c.merit_value).label('merit_value')
      ])
    results = session.execute(query)
    row = dict(zip(results.keys(), results.fetchone()))
    return row


  @cherrypy.expose
  @tools.json_out(on=True)
  def run_status(self):
    """Returns json containing status of run.

    Resulting JSON contains a single key: 'run_status'

    .. code-block:: javascript

      { 'run_status' : status of run }

    ``run_status`` can have values of ``Running``, ``Finished`` or ``Error``.

    """
    configure_session(cherrypy.request.config['tools.SATransaction.dburi'])
    table = metadata.tables['runstatus']
    query = sa.select([table.c.runstatus, table.c.title])
    results = session.execute(query)
    row = dict(zip(results.keys(), results.fetchone()))
    return row

  @cherrypy.expose
  @tools.json_out(on=True)
  def iteration_overview(self, iterationNumber):
    """Returns overview and statistics for a given iteration.

    :parameter int iterationNumber: iteration for which statistics should be returned.

    Resulting JSON has the following format:

    .. code-block:: javascript

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
      }

      """
    configure_session(cherrypy.request.config['tools.SATransaction.dburi'])
    table = metadata.tables['candidates']

    # Get maximum record
    query = sa.select([
      table.c.id,
      table.c.iteration_number,
      table.c.candidate_number,
      sa.func.max(table.c.merit_value).label('merit_value')])\
        .where(table.c.iteration_number == iterationNumber)
    results = session.execute(query)
    maxRecord = dict(zip(results.keys(), results.fetchone()))

    # Get minimum record
    query = sa.select([
      table.c.id,
      table.c.iteration_number,
      table.c.candidate_number,
      sa.func.min(table.c.merit_value).label('merit_value')])\
        .where(table.c.iteration_number == iterationNumber)
    results = session.execute(query)
    minRecord = dict(zip(results.keys(),results.fetchone()))

    # Get number of population members and mean
    query = sa.select([
      sa.func.count(table.c.id),
      sa.func.avg(table.c.merit_value)])\
        .where(table.c.iteration_number == iterationNumber)
    results = session.execute(query)
    num_candidates, mean = results.fetchone()

    # Calculate standard deviation
    query = sa.select([table.c.merit_value]).where(table.c.iteration_number == iterationNumber)
    results = session.execute(query)
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

  def _getCandidateID(self, iterationNumber, candidateNumber):
    iterationNumber = int(iterationNumber)
    candidateNumber = int(candidateNumber)

    candidates = metadata.tables['candidates']
    cidQuery = sa.select([candidates.c.id])\
      .where(sa.and_(candidates.c.iteration_number == iterationNumber , candidates.c.candidate_number == candidateNumber))

    results = session.execute(cidQuery)
    cid = results.fetchone()[0]
    return cid

  @cherrypy.expose
  @tools.json_out(on=True)
  def variables(self, iterationNumber, candidateNumber):
    """Returns json representing variables for given iterationNumber and candidateNumber within that iteration.

    :parameter int iterationNumber: Number of the iteration for which information is returned.
    :parameter int candidateNumber: Index of candidate within population of parameter sets for given iteration.

    Returned JSON has the form:

    .. code-block:: javascript

      {
        'variable_name' : name of variable,
        'fit_flag'      : boolean, true if variable is changed during fitting ,
        'low_bound'     : variable low bound or null if no bound set,
        'upper_bound'   : variable's upper bound or null if no bound set,
        'calculated_flag' : boolean, true if variable is a calculated variable,
        'calculation_expression' : calculated_flag is true then this field gives the expression used to calculate field,
        'value' : current value of variable
      }

    """
    configure_session(cherrypy.request.config['tools.SATransaction.dburi'])
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

    results = session.execute(query)
    output = []
    for row in results:
      d = dict(zip(results.keys(), row))
      if d['low_bound'] and math.isinf(float(d['low_bound'])):
        d['low_bound'] = None

      if d['upper_bound'] and math.isinf(float(d['upper_bound'])):
        d['upper_bound'] = None
      output.append(d)
    return output

  @cherrypy.expose
  @tools.json_out(on=True)
  def evaluated(self, iterationNumber, candidateNumber):
    """Returns json representing evaluator fields for given iterationNumber and candidateNumber within that iteration.

    :parameter int iterationNumber: Number of the iteration for which information is returned.
    :parameter int candidateNumber: Index of candidate within population of parameter sets for given iteration.

    Returned JSON has the form:

    .. code-block:: javascript


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

    results = session.execute(query)

    output = []
    for row in results:
      d = dict(zip(results.keys(), row))
      d['percent_difference'] = calculatePercentageDifference(d)
      output.append(d)

    return output


def _formatResults(results):
  """Takes SA results and formats them in table format expected by the pprofitmon front end."""
  columns = results.keys()
  outdict = {'columns' : columns}
  values = [ list(r) for r in results]
  outdict['values'] = values
  return outdict

class _FilterWrapper(object):
  """Wraps results and filters for particular key"""

  def __init__(self, results, columnName, columnFilter):
    self.results = results
    self.columns = results.keys()
    self.columnName = columnName
    self.columnFilter = columnFilter

  def __iter__(self):
    whichcol = [ i for (i, c) in enumerate(self.columns) if c == self.columnName][0]
    for row in self.results:
      v = row[whichcol]
      if self.columnFilter(v):
        yield row

  def keys(self):
    return self.results.keys()


def _createColumnProviders(conn, tempMeta, primaryColumnKey, columnLabels):
  """Creates list of ColumnProvider like objects for _Columns class"""
  outlist = []
  statcols = []
  for cl in columnLabels:
    if cl.startswith('stat:'):
      statcols.append(cl)
    elif cl.startswith('it:is_running_min'):
      outlist.append(_RunningFilterColumnProvider(primaryColumnKey, cl, _RunningMinFilter()))
    elif cl.startswith('it:is_running_max'):
      outlist.append(_RunningFilterColumnProvider(primaryColumnKey, cl, _RunningMaxFilter()))
    elif cl.startswith('variable:'):
      outlist.append(_VariablesColumnProvider(conn, tempMeta, cl))
    elif cl.startswith('evaluator:'):
      outlist.append(_EvaluatorColumnProvider(conn, tempMeta, cl))
    else:
      raise KeyError("Unknown column label: %s" % cl)

  # Create _StatColumnProvider
  outlist.append(_StatColumnProvider(conn, primaryColumnKey, statcols))
  return outlist




class _Columns(object):
  """Maintains a list of columns for IterationSeries.

  In particular is responsible for fetching an entire iteration's data
  for a particular key and passing to summarising stat functions"""

  def __init__(self, results, columnLabels, columnProviders):
    """:param results: SQL Result set (normally iteration filtered at this point) wrapped by this object.
       :param columnLabels: Columns for which this class will provide data.
       :param primaryColumn: Primary column label for which stats should be collected"""

    self.results = results
    self.columnLabels = columnLabels
    self.columnProviders = columnProviders

  def keys(self):
    columns = list(self.results.keys())
    columns.extend(self.columnLabels)
    return columns

  def __iter__(self):
    rkeys = self.results.keys()
    for row in self.results:
      outrow = list(row)
      rowdict = dict(zip(rkeys, row))

      extracols = []
      for cp in self.columnProviders:
        colValPairs = cp(rowdict['iteration_number'],
          rowdict['candidate_number'], rowdict)
        extracols.extend(colValPairs)
      extracols = dict(extracols)

      for cn in self.columnLabels:
        outrow.append(extracols[cn])
      yield outrow

class IterationSeries:

  @contextlib.contextmanager
  def _temporaryCandidateContextManager(self, primaryColumnKey, iterationFilter, candidateFilter):
    """Context manager that creates (and drops) a temporary table containing
    iteration_number and candidate_number for the iteration series defined by the arguments to this
    method.

    :param primaryColumnKey: Column key used to choose iteration/candidate pairs.
    :param iterationFilter: Keyword used in iteration selection.
    :param candidateFilter: Keyword used to identify candidate within each iteration.

    :return: Context manager returns tuple (connection, meta) where connection is the connection associated with
      temporary table and meta is MetaData containing temporary table definition."""

    # Create the table
    engine = session.get_bind()
    conn = engine.connect()

    tempMetaData = sa.MetaData(bind=conn)

    t = sa.Table(
      'temp_iterationseries',
      tempMetaData,
      sa.Column('id', sa.Integer, primary_key=True),
      sa.Column('candidate_id', sa.Integer),
      sa.Column('iteration_number', sa.Integer),
      sa.Column('candidate_number', sa.Integer),
      sa.Column('primary_value', sa.Integer),
      prefixes = ['TEMPORARY'])
    t.create(bind=conn, checkfirst=True)

    # Now populate the table
    popnresults = self._getSeriesCandidates(primaryColumnKey, iterationFilter, candidateFilter)
    insertQuery = t.insert()
    insertData = []
    for row in popnresults:
      insertData.append(dict(zip(popnresults.keys(), row)))
    conn.execute(insertQuery, insertData)

    yield conn, tempMetaData

  def _getSeriesCandidates(self, primaryColumnKey, iterationFilter, candidateFilter):
    """Get SQLAlchemy results for the relevant iteration/candidate pairs for
    primaryColumnKey, iterationFilter, candidateFilter.

    :param primaryColumnKey: Column key used to choose iteration/candidate pairs.
    :param iterationFilter: Keyword used in iteration selection.
    :param candidateFilter: Keyword used to identify candidate within each iteration.
    :return: SQLAlchemy results"""
    iterationFilterFunc = {'all' : _NullFilter,
                           'running_min' : _RunningMinFilter,
                           'running_max' : _RunningMaxFilter}[iterationFilter]

    candidateFilterFunc = {'min' : sa.func.min,
                           'max' : sa.func.max}[candidateFilter]

    candidates = metadata.tables['candidates']
    query = sa.select([
      candidates.c.id.label('candidate_id'),
      candidates.c.iteration_number,
      candidates.c.candidate_number,
      candidateFilterFunc(candidates.c.merit_value).label('primary_value')
      ]).group_by(candidates.c.iteration_number)

    results = session.execute(query)
    results = _FilterWrapper(results, 'primary_value', iterationFilterFunc())

    return results


  @cherrypy.expose
  @tools.json_out(on=True)
  def merit_value(self, iterationFilter, candidateFilter, columns=None):
    """Returns a data table where each row represents an iteration within the fitting run. The primary data column
    returned by this JSON call is candidate merit value.


    **Filtering**

    * The candidates chosen for inclusion in the data table are controlled using the ``candidateFilter`` parameter:

      - If ``candidateFilter`` is ``min``, then the candidate with the **lowest** merit value is selected for each
        iteration.
      - If it is ``max`` then the candidate with the **largest** merit is chosen.

    * The ``iterationFilter`` parameter can be used to filter the the rows returned:

      - ``all`` : when ``iterationFilter`` has this value, no row filtering takes place and all iterations are returned.
      - ``running_min``: only includes rows where the merit value decreases with respect to the previous row.
        + If iteration 0,1,2 and 3 had merit values of 10,11,8,4, using ``running_min`` would return values for iterations
          0,2 and 3.
        + As the merit value increased between iterations 0 and 1, iteration 1 is removed from the
          table.
      - ``running_max``: similar to ``running_min`` with only those rows constituting an increase in overall merit value
        being included.


    **Column Selection**

    * By default the following columns are included in the table returned by ``/fitting/iteration_series/merit_value``:

      - ``"iteration_number"``: step number of ``pprofit`` run.
      - ``"candidate_number"``: column showing the id of the candidate selected for each row.
      - ``"merit_value"``: column containing the merit value obtained for each row's selected candidate.

    * Additional columns can be specified through the optional ``columns`` parameter specified in the URL query string.
    * The argument to ``columns`` is a string containing a comma separated list of column keys.
    * Column keys fall into different categories identified by a prefix:

      - ``evaluator:`` Include evaluator values (see :ref:`extending_json_iterationseries_columns_evaluator`).
      - ``variable:``  Include variable values in table  (see :ref:`extending_json_iterationseries_columns_variable`).
      - ``stat:`` Popuation statistics columns (see :ref:`extending_json_iterationseries_columns_stats`).
      - ``it:`` Iteration meta-data (see :ref:`extending_json_iterationseries_columns_itmetadata`).


    **JSON Format**

    This call returns JSON with the following general format.

    .. code-block:: javascript

      {
        'columns' : COLUMN_LIST,
        'values' : VALUE_LIST
      }

    **Where:**
      * ``COLUMN_LIST`` is a list of column keys. For ``/fitting/iteration_series/merit_value/...``, ``COLUMN_LIST``
        has the form:

          .. code-block:: javascript

            ["iteration_number", "candidate_number", "merit_value"]


          - Where the "iteration_number", "candidate_number", "merit_value", define the default columns described above.
          - If additional columns have been specified using the optional ``columns`` argument, the column names
            appear at the end of the ``COLUMN_LIST``.

      *  ``VALUE_LIST`` is a list containing sub-lists, one per iteration selected. Each iteration is represented by a
          list of values. One value appears per column defined in ``COLUMN_LIST``.


    **Example of JSON Output:**

    * The URL::

      /fitting/iteration_series/merit_value/all/min


    * when used with the ``pprofitmon`` server produces:

      .. code-block:: javascript

        {
          "columns": ["iteration_number", "candidate_number", "merit_value"],
          "values": [
            [0, 0, 259.83689200000003],
            [1, 0, 193.31817600000002],
            [2, 0, 222.484335],
            [3, 0, 205.81304400000002],
            [4, 0, 151.776856],
            [5, 0, 101.330231],
          ]
        }


    * Which represents the table:

      +------------------+------------------+--------------------+
      | iteration_number | candidate_number | merit_value        |
      +==================+==================+====================+
      | 0                | 0                | 259.83689200000003 |
      +------------------+------------------+--------------------+
      | 1                | 0                | 193.31817600000002 |
      +------------------+------------------+--------------------+
      | 2                | 0                | 222.484335         |
      +------------------+------------------+--------------------+
      | 3                | 0                | 205.81304400000002 |
      +------------------+------------------+--------------------+
      | 4                | 0                | 151.776856         |
      +------------------+------------------+--------------------+
      | 5                | 0                | 101.330231         |
      +------------------+------------------+--------------------+

    ---

    :param iterationFilter: Describes row filter. Can be one of ``all``, ``running_min`` and ``running_max``.
    :param candidateFilter: Determines the candidate selected from each iteration.
      The value of this parameter can be one of ``min`` and ``max``.
    :param columns: Comma separated list of additional column keys to include in the returned table.


    """



    primaryColumnKey = 'merit_value'
    configure_session(cherrypy.request.config['tools.SATransaction.dburi'])

    with self._temporaryCandidateContextManager(primaryColumnKey, iterationFilter, candidateFilter) as (conn,tempMeta):
      t = tempMeta.tables['temp_iterationseries']
      query = sa.select([
        t.c.iteration_number,
        t.c.candidate_number,
        t.c.primary_value.label(primaryColumnKey)
        ])
      results = conn.execute(query)

      # Process extra columns
      if columns:
        columns = columns.split(',')
        columnProviders = _createColumnProviders(conn, tempMeta, primaryColumnKey, columns)
        results = _Columns(results, columns, columnProviders)

      return _formatResults(results)



def _setPort(portNumber):
  """Sets the port on which the web monitor runs.

  :param portNumber: Port number"""
  cherrypy.config.update({'server.socket_port': portNumber})

def _setStaticPath(staticPath):
  """Set the path from which static files should be served.

  :param str staticPath: Path to static files. If not specified serve files from egg"""
  cherrypy.config.update({'static_path' : staticPath})


def _processCommandLineOptions():
  parser = optparse.OptionParser()
  parser.add_option("-p", "--port", dest="port", metavar = "PORT",
    help = "Set the port on which fitting monitor runs",
    default = 8080,
    type="int",
    action = "store")

  developgroup = optparse.OptionGroup(parser, "Developer Options",
    description = "Options useful for developers of pprofitmon")
  developgroup.add_option("-s", "--static-files", dest="static_files", metavar = "PATH",
    help = "Serve statice files (css/javascript/images) from PATH rather than pprofit egg file.")

  parser.add_option_group(developgroup)

  if not os.path.exists('fit.cfg'):
    parser.error("pprofitmon must be run from same directory as fitting run.")

  options, args = parser.parse_args()
  _setPort(options.port)
  _setStaticPath(options.static_files)

def _setupCherryPy(sqliteURL):
  root = Root()
  root.fitting = Fitting()
  root.fitting.iteration_series = IterationSeries()
  cherrypy.tree.mount(root, '', {'/': {
    'tools.SATransaction.on' : True,
    'tools.SATransaction.dburi' : sqliteURL,
    'tools.SATransaction.echo' : True,
    'tools.encode.encoding':'utf8',
    }})
  return root

def main():
  _processCommandLineOptions()
  # Build cherrypy tree
  _setupCherryPy('sqlite:///fitting_run.db')
  cherrypy.engine.start()

if __name__ == "__main__":
  main()
