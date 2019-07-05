import sqlalchemy as sa
import tabulate

from ._util import retry_backoff

from . import db

import logging
import os
from io import StringIO

_retryLogger = logging.getLogger(
    "atsim.pro_fit.reporters.SQLiteReporter.retry"
)


class SQLiteReporter(object):
    """Minimizer stepCallback that places results in a sqlite database"""

    _metadata = db.getMetadata()

    @staticmethod
    def getMetaData():
        """@return sqlalchemy.MetaData describing database schema used by this class"""
        # Create tables
        return SQLiteReporter._metadata

    def __init__(
        self, dbfilename, initialVariables, calculatedVariables, title=None
    ):
        """@param dbfilename Filename for database, None if in-memory database is to be used.
       @param initialVariables Variables instance containing variables before minimization.
       @param calculatedVariables CalculatedVariables instance used by Merit object."""
        self.dbfilename = dbfilename
        self._createDatabase()
        with self._saengine.begin() as conn:
            self._populateVariableKeysTable(
                conn, initialVariables, calculatedVariables
            )
            self._populateStatus(conn, title)
        self.iterationNum = 0

    def _createDatabase(self):
        """Create sqlite database"""

        if self.dbfilename:
            engine = sa.create_engine("sqlite:///%s" % self.dbfilename)
        else:
            engine = sa.create_engine("sqlite:///:memory:")

        # Create database tables
        metadata = self._metadata
        metadata.create_all(engine)

        self._saengine = engine
        self._metadata = metadata

    def _populateVariableKeysTable(self, conn, variables, calculatedVariables):
        """Populate the variable_keys table with initial variables"""
        variableKeysTable = self._metadata.tables["variable_keys"]
        insertQuery = variableKeysTable.insert()

        insertData = []
        for (k, v, ff), bounds in zip(
            variables.flaggedVariablePairs, variables.bounds
        ):
            if bounds:
                lowbound, highbound = bounds
            else:
                lowbound, highbound = None, None

            insertData.append(
                dict(
                    variable_name=k,
                    fit_flag=ff,
                    low_bound=lowbound,
                    upper_bound=highbound,
                    calculated_flag=False,
                    calculation_expression=None,
                )
            )

        # Process the CalculatedVariables fields
        for (k, v) in calculatedVariables.nameExpressionTuples:
            insertData.append(
                dict(
                    variable_name=k,
                    fit_flag=False,
                    low_bound=None,
                    upper_bound=None,
                    calculated_flag=True,
                    calculation_expression=v,
                )
            )

        conn.execute(insertQuery, insertData)

    def _populateStatus(self, conn, title):
        """Initially populate the 'runstatus' table

    @param conn Database connection instance
    @param title Title for the run"""
        table = self._metadata.tables["runstatus"]
        insert = table.insert()
        conn.execute(insert, dict(runstatus="Running", title=title))

    def _createCandidateRecord(
        self, conn, iterationNum, candidateNum, meritValue
    ):
        # Create candidate instance
        candidateTable = self._metadata.tables["candidates"]
        candidateInsert = candidateTable.insert()
        result = conn.execute(
            candidateInsert,
            iteration_number=iterationNum,
            candidate_number=candidateNum,
            merit_value=meritValue,
        )
        candidateId = result.inserted_primary_key[0]
        return candidateId

    def _createVariables(self, conn, candidateId, variables):
        variablesTable = self._metadata.tables["variables"]
        insert = variablesTable.insert()
        insertValues = [
            dict(candidate_id=candidateId, variable_name=k, value=v)
            for (k, v) in variables.variablePairs
        ]
        conn.execute(insert, insertValues)

    def _createEvaluators(self, conn, jobId, job):
        evaluatorTable = self._metadata.tables["evaluated"]
        insert = evaluatorTable.insert()

        # Create insertion dictionaries
        staticValues = {"job_id": jobId}
        insertValues = []
        for evaluator in job.evaluatorRecords:
            for record in evaluator:
                if record.errorFlag:
                    errorId = self._insertEvaluatorError(conn, record)
                else:
                    errorId = None

                v = dict(
                    evaluator_name=record.evaluatorName,
                    value_name=record.name,
                    expected_value=record.expectedValue,
                    extracted_value=record.extractedValue,
                    weight=record.weight,
                    merit_value=record.meritValue,
                    evaluatorerror_id=errorId,
                )
                v.update(staticValues)
                insertValues.append(v)
        conn.execute(insert, insertValues)

    def _insertEvaluatorError(self, conn, record):
        table = self._metadata.tables["evaluatorerror"]
        insert = table.insert()
        result = conn.execute(insert, msg=str(record.exception))
        return result.lastrowid

    def _createJobs(self, conn, candidateId, joblist):
        jobTable = self._metadata.tables["jobs"]
        insert = jobTable.insert()

        for j in joblist:
            result = conn.execute(
                insert, job_name=j.name, candidate_id=candidateId
            )
            jobId = result.inserted_primary_key[0]
            # Create associated evaluator records
            self._createEvaluators(conn, jobId, j)

    @retry_backoff(
        [sa.exc.OperationalError],
        initialSleep=1,
        maxSleep=60,
        logger=_retryLogger,
    )
    def finished(self, error=False):
        """Update status table to 'Finished'"""
        table = self._metadata.tables["runstatus"]
        update = table.update().where(table.c.id == 1)

        with self._saengine.begin() as conn:
            if error:
                conn.execute(update, dict(runstatus="Error"))
            else:
                conn.execute(update, dict(runstatus="Finished"))

    @retry_backoff(
        [sa.exc.OperationalError],
        initialSleep=1,
        maxSleep=60,
        logger=_retryLogger,
    )
    def __call__(self, minimizerResults):
        meritValues = minimizerResults.meritValues
        jobLists = minimizerResults.candidateJobList
        with self._saengine.begin() as conn:
            for (candidateNum, (mval, (variables, joblist))) in enumerate(
                zip(meritValues, jobLists)
            ):
                # Create record in the 'candidates' table
                candidateId = self._createCandidateRecord(
                    conn, self.iterationNum, candidateNum, mval
                )

                # Create records in the 'variables' table
                self._createVariables(conn, candidateId, variables)

                # Create records in the 'jobs' table
                self._createJobs(conn, candidateId, joblist)

        self.iterationNum += 1


class LogReporter(object):
    """Minimizer stepCallback that logs the best-ever variables and merit-value to
  a given logging.Logger. Logs at info level."""

    def __init__(self):
        """@param logger logging.Logger instance"""
        self.currentIterationNumber = 0
        self.bestIterationNumber = 0

        self.firstIteration = None
        self.lastIteration = None
        self.bestIteration = None
        self._logger = logging.getLogger(__name__).getChild("LogReporter")

    def __call__(self, minimizerResults):
        self._updateBest(minimizerResults)
        self._log(minimizerResults)
        self.currentIterationNumber += 1
        self.lastIteration = minimizerResults

    def _updateBest(self, minimizerResults):
        if self.bestIteration is None:
            self.firstIteration = minimizerResults
            self.bestIteration = minimizerResults
        elif minimizerResults < self.bestIteration:
            self.bestIteration = minimizerResults
            self.bestIterationNumber = self.currentIterationNumber

    def _variableKeyLabelPairs(self):
        variables = self.bestIteration.bestVariables
        variableKeys = self.firstIteration
        fitkeys = set(variables.fitKeys)
        l = []
        for k, v in variables.variablePairs:
            if k in fitkeys:
                fflag = " *"
            else:
                fflag = ""
            l.append((k, "%s%s" % (k, fflag)))
        return l

    def _getVariableColumn(self, variableLabelPairs, minimizerResults):
        variables = dict(minimizerResults.bestVariables.variablePairs)
        column = []
        for k, label in variableLabelPairs:
            v = variables[k]
            column.append(float(v))
        return column

    def _makeResultsTable(self, minimizerResults):
        blankRow = ["", "", "", "", ""]

        variableLabelPairs = self._variableKeyLabelPairs()
        variableNames = [label for (k, label) in variableLabelPairs]

        firstVariables = self._getVariableColumn(
            variableLabelPairs, self.firstIteration
        )
        if self.lastIteration is None:
            lastVariables = [""] * len(variableLabelPairs)
        else:
            lastVariables = self._getVariableColumn(
                variableLabelPairs, self.lastIteration
            )

        currentVariables = self._getVariableColumn(
            variableLabelPairs, minimizerResults
        )
        bestVariables = self._getVariableColumn(
            variableLabelPairs, self.bestIteration
        )

        table = [["Variables", "", "", "", ""]]
        for row in zip(
            variableNames,
            firstVariables,
            lastVariables,
            currentVariables,
            bestVariables,
        ):
            table.append(list(row))

        table.append(blankRow)
        table.append(
            [
                "Merit = ",
                self.firstIteration.bestMeritValue,
                self.lastIteration.bestMeritValue
                if not self.lastIteration is None
                else "NA",
                minimizerResults.bestMeritValue,
                self.bestIteration.bestMeritValue,
            ]
        )

        if self.currentIterationNumber == 0:
            previousIt = ""
        else:
            previousIt = " (it=%d)" % (self.currentIterationNumber - 1,)

        columnHeadings = [
            "",
            "First (it=%d)" % 0,
            "Previous%s" % previousIt,
            "Current (it=%d)" % self.currentIterationNumber,
            "Best (i=%d)" % self.bestIterationNumber,
        ]

        tabulated = tabulate.tabulate(table, headers=columnHeadings)
        return tabulated

    def _errorsForJob(self, variables, job):
        errorlist = []
        for ers in job.evaluatorRecords:
            for er in ers:
                if er.errorFlag:
                    errorlist.append("  " + str(er))

        if errorlist:
            lines = ["Evaluator errors found for job: %s" % job.name]
            lines.append("with: %s" % variables)
            lines.extend(errorlist)
            return os.linesep.join(lines)

        return None

    def _formatEvaluatorErrors(self, minimizerResults):
        errorsFound = False
        sbuild = StringIO()

        for v, jobs in minimizerResults.candidateJobList:
            for job in jobs:
                jobErrors = self._errorsForJob(v, job)
                if jobErrors:
                    errorsFound = True
                    sbuild.write(jobErrors)
                    print("", file=sbuild)

        if errorsFound:
            return sbuild.getvalue()
        return None

    def _log(self, minimizerResults):
        sbuild = StringIO()
        print("", file=sbuild)
        print(
            "Results at iteration: %d" % self.currentIterationNumber,
            file=sbuild,
        )
        print("", file=sbuild)

        evaluatorErrors = self._formatEvaluatorErrors(minimizerResults)
        if evaluatorErrors:
            sbuild.write(evaluatorErrors)
            print("", file=sbuild)
            print("", file=sbuild)

        print("Variables and Merit Values:", file=sbuild)
        table = self._makeResultsTable(minimizerResults)
        sbuild.write(table)
        print("", file=sbuild)
        print("", file=sbuild)
        self._logger.info(sbuild.getvalue())
