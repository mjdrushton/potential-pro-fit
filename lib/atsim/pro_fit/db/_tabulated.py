import sqlalchemy as sa

import contextlib

from ._columnproviders import (
    _VariablesColumnProvider,
    _StatColumnProvider,
    _EvaluatorColumnProvider,
)
from ._columnproviders import (
    _RunningFilterColumnProvider,
    _NullFilter,
    _RunningMaxFilter,
    _RunningMinFilter,
)
from ._util import calculatePercentageDifference
from . import _metadata

from ._fitting import Fitting

metadata = _metadata.getMetadata()


class _BestIteration(object):
    def __call__(self, engine):
        f = Fitting(engine)
        result = f.best_candidate()
        retval = result["iteration_number"]
        return retval


class _LastIteration(object):
    def __call__(self, engine):
        f = Fitting(engine)
        retval = f.current_iteration()
        return retval


class BadFilterCombinationException(Exception):
    """Thrown when a bad combination of candidate and iteration filters are made"""

    pass


class _FilterWrapper(object):
    """Wraps results and filters for particular key"""

    def __init__(self, results, columnName, columnFilter):
        self.results = results
        self.columns = list(results.keys())
        self.columnName = columnName
        self.columnFilter = columnFilter

    def __iter__(self):
        whichcol = [
            i for (i, c) in enumerate(self.columns) if c == self.columnName
        ][0]
        for row in self.results:
            v = row[whichcol]
            if self.columnFilter(v):
                yield row

    def keys(self):
        return list(self.results.keys())


def _createColumnProviders(conn, tempMeta, primaryColumnKey, columnLabels):
    """Creates list of ColumnProvider like objects for _Columns class"""
    outlist = []
    statcols = []
    for cl in columnLabels:
        if cl.startswith("stat:"):
            statcols.append(cl)
        elif cl.startswith("it:is_running_min"):
            outlist.append(
                _RunningFilterColumnProvider(
                    primaryColumnKey, cl, _RunningMinFilter()
                )
            )
        elif cl.startswith("it:is_running_max"):
            outlist.append(
                _RunningFilterColumnProvider(
                    primaryColumnKey, cl, _RunningMaxFilter()
                )
            )
        elif cl.startswith("variable:"):
            outlist.append(_VariablesColumnProvider(conn, tempMeta, cl))
        elif cl.startswith("evaluator:"):
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
        rkeys = list(self.results.keys())
        for row in self.results:
            outrow = list(row)
            rowdict = dict(list(zip(rkeys, row)))

            extracols = []
            for cp in self.columnProviders:
                colValPairs = cp(
                    rowdict["iteration_number"],
                    rowdict["candidate_number"],
                    rowdict,
                )
                extracols.extend(colValPairs)
            extracols = dict(extracols)

            for cn in self.columnLabels:
                outrow.append(extracols[cn])
            yield outrow


class IterationSeriesTable(object):
    def __init__(
        self,
        engine,
        primaryColumnKey="merit_value",
        iterationFilter="all",
        candidateFilter="min",
        columns=None,
    ):
        """

    Returns a data table where each row represents an iteration within the fitting run. The primary data column
    returned by this JSON call is candidate merit value.


    **Filtering**

    * The candidates chosen for inclusion in the data table are controlled using the ``candidateFilter`` parameter:

      - If ``candidateFilter`` is ``min``, then the candidate with the **lowest** merit value is selected for each
        iteration.
      - If it is ``max`` then the candidate with the **largest** merit is chosen.
      - If it is ``all`` then all candidates are dumped - if `all` specified, the `all` must also be used as the ``iterationFilter``.

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

    * By default the following columns are included when ``columns`` is ``None``:

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

    ---

    :param engine: SQL Alchemy engine instance.
    :param iterationFilter: Describes row filter. Can be one of ``all``, ``running_min`` and ``running_max``.
    :param candidateFilter: Determines the candidate selected from each iteration.
      The value of this parameter can be one of ``min`` and ``max``.
    :param columns: List of additional column keys to include in the returned table."""

        # Why was this in place?
        if candidateFilter == "all" and iterationFilter in [
            "running_min",
            "running_max",
        ]:
            raise BadFilterCombinationException(
                "the 'all' candidate filter can not be used with the '{}' iteration filter.".format(
                    iterationFilter
                )
            )

        self.engine = engine

        self.primaryColumnKey = primaryColumnKey
        self.iterationFilter = iterationFilter
        self.candidateFilter = candidateFilter
        self._columns = columns

        self._iter = self._createIterator()

    def __next__(self):
        return list(next(self._iter))

    def __iter__(self):
        return self

    @classmethod
    def validKeys(cls, engine):
        """Returns a list of keys that can be used with the constructor's ``columns`` argument.

    :param engine: SQL Alchemy Engine instance for a valid fitting_run.db.
    :return list: List of strings giving valid column keys"""
        keys = []
        keys.extend(cls.validIterationKeys(engine))
        keys.extend(cls.validStatisticsKeys(engine))
        keys.extend(cls.validEvaluatorKeys(engine))
        keys.extend(cls.validVariableKeys(engine))
        return keys

    @classmethod
    def validEvaluatorKeys(cls, engine):
        """Returns a list of keys relevant to evaluator values that can be used with the constructor's ``columns`` argument.

    :param engine: SQL Alchemy Engine instance for a valid fitting_run.db.
    :return list: List of strings giving valid column keys"""
        return sorted(_EvaluatorColumnProvider.validKeys(engine))

    @classmethod
    def validVariableKeys(cls, engine):
        """Returns a list of keys relevant to variable values that can be used with the constructor's ``columns`` argument.

    :param engine: SQL Alchemy Engine instance for a valid fitting_run.db.
    :return list: List of strings giving valid column keys"""
        return sorted(_VariablesColumnProvider.validKeys(engine))

    @classmethod
    def validFittingVariableKeys(cls, engine):
        """Returns a list of keys for variables subject to change in fitting run that can be used with the constructor's ``columns`` argument.

    :param engine: SQL Alchemy Engine instance for a valid fitting_run.db.
    :return list: List of strings giving valid column keys"""
        variable_keys = metadata.tables["variable_keys"]
        query = (
            sa.select([variable_keys.c.variable_name])
            .where(variable_keys.c.fit_flag == True)
            .order_by(variable_keys.c.variable_name)
        )
        results = engine.execute(query)
        return ["variable:%s" % v[0] for v in results.fetchall()]

    @classmethod
    def validIterationKeys(cls, engine):
        """Returns a list of keys relevant to iteration (it: prefix) values that can be used with the constructor's ``columns`` argument.

    :param engine: SQL Alchemy Engine instance for a valid fitting_run.db.
    :return list: List of strings giving valid column keys"""
        return sorted(_RunningFilterColumnProvider.validKeys(engine))

    @classmethod
    def validStatisticsKeys(cls, engine):
        """Returns a list of keys relevant to population statistics (stat: prefix) values that can be used with the constructor's ``columns`` argument.

    :param engine: SQL Alchemy Engine instance for a valid fitting_run.db.
    :return list: List of strings giving valid column keys"""
        return sorted(_StatColumnProvider.validKeys(engine))

    @contextlib.contextmanager
    def _temporaryCandidateContextManager(
        self, primaryColumnKey, iterationFilter, candidateFilter
    ):
        """Context manager that creates (and drops) a temporary table containing
    iteration_number and candidate_number for the iteration series defined by the arguments to this
    method.

    :param primaryColumnKey: Column key used to choose iteration/candidate pairs.
    :param iterationFilter: Keyword used in iteration selection.
    :param candidateFilter: Keyword used to identify candidate within each iteration.

    :return: Context manager returns tuple (connection, meta) where connection is the connection associated with
      temporary table and meta is MetaData containing temporary table definition."""

        # Create the table
        conn = self.engine.connect()

        tempMetaData = sa.MetaData(bind=conn)

        t = sa.Table(
            "temp_iterationseries",
            tempMetaData,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("candidate_id", sa.Integer),
            sa.Column("iteration_number", sa.Integer),
            sa.Column("candidate_number", sa.Integer),
            sa.Column("primary_value", sa.Integer),
            prefixes=["TEMPORARY"],
        )
        t.create(bind=conn, checkfirst=True)

        # Now populate the table
        popnresults = self._getSeriesCandidates(
            primaryColumnKey, iterationFilter, candidateFilter
        )
        insertQuery = t.insert() # pylint: disable=no-value-for-parameter
        insertData = []
        for row in popnresults:
            insertData.append(dict(list(zip(list(popnresults.keys()), row))))
        conn.execute(insertQuery, insertData)

        yield conn, tempMetaData

    def _getSeriesCandidates(
        self, primaryColumnKey, iterationFilter, candidateFilter
    ):
        """Get SQLAlchemy results for the relevant iteration/candidate pairs for
    primaryColumnKey, iterationFilter, candidateFilter.

    :param primaryColumnKey: Column key used to choose iteration/candidate pairs.
    :param iterationFilter: Keyword used in iteration selection.
    :param candidateFilter: Keyword used to identify candidate within each iteration.
    :return: SQLAlchemy results"""

        candidates = metadata.tables["candidates"]

        candidateFilterFunc, groupby = {
            "all": (lambda c: c, False),
            "min": (sa.func.min, True),
            "max": (sa.func.max, True),
        }[candidateFilter]

        query = sa.select(
            [
                candidates.c.id.label("candidate_id"),
                candidates.c.iteration_number,
                candidates.c.candidate_number,
                candidateFilterFunc(candidates.c.merit_value).label(
                    "primary_value"
                ),
            ]
        )

        if groupby:
            query = query.group_by(candidates.c.iteration_number)

        specific_iteration_filters = {
            "global_min": _BestIteration,
            "last": _LastIteration,
        }

        if (
            iterationFilter.startswith("n(")
            or iterationFilter in specific_iteration_filters
        ):
            iterationFilterFunc = _NullFilter

            if iterationFilter in specific_iteration_filters:
                itnum_callable = specific_iteration_filters[iterationFilter]
                chosen_it = itnum_callable()(self.engine)
            else:
                iterationFilter = iterationFilter[2:].strip()
                iterationFilter = iterationFilter[:-1]
                chosen_it = int(iterationFilter)

            query = query.where(candidates.c.iteration_number == chosen_it)

        else:
            iterationFilterFunc = {
                "all": _NullFilter,
                "running_min": _RunningMinFilter,
                "running_max": _RunningMaxFilter,
            }[iterationFilter]

        results = self.engine.execute(query)
        results = _FilterWrapper(
            results, "primary_value", iterationFilterFunc()
        )

        return results

    def _createIterator(self):
        with self._temporaryCandidateContextManager(
            self.primaryColumnKey, self.iterationFilter, self.candidateFilter
        ) as (conn, tempMeta):
            t = tempMeta.tables["temp_iterationseries"]
            query = sa.select(
                [
                    t.c.iteration_number,
                    t.c.candidate_number,
                    t.c.primary_value.label(self.primaryColumnKey),
                ]
            )
            results = conn.execute(query)

            # Process extra columns
            if self._columns:
                columnProviders = _createColumnProviders(
                    conn, tempMeta, self.primaryColumnKey, self._columns
                )
                results = _Columns(results, self._columns, columnProviders)

            yield list(results.keys())

            for row in results:
                yield row
