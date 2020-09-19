import tempfile
import os
import shutil

from atsim.pro_fit import jobfactories

import gevent

from atsim.pro_fit._util import MultiCallback


def _sumValuesReductionFunction(evaluatedJobs):
    """Default reduction function, for each list enter sub lists and sum values"""
    ret = []
    for candidates in evaluatedJobs:
        v = 0.0
        for j in candidates:
            for e in j.evaluatorRecords:
                v += sum([er.meritValue for er in e])
        ret.append(v)
    return ret


class Replace_Merit_After_Evaluation_Callback(object):
    """Callback for use with Merit.afterEvaluation to replace merit values based on a predicate"""

    def __init__(self, false_predicate, replacement_value):
        """Replace merit values of evaluators based on when predicate is false.

        Arguments:
            false_predicate {callable} -- Unary predicate that accepts merit value as its argument. Returns True for evaluator records that should not be modified.
            replacement_value {float} -- merit value to be used when predicate returns false.
        """
        self.false_predicate = false_predicate
        self.replacement_value = replacement_value

    def __call__(self, candidate_job_pairs):
        for _v, jobs in candidate_job_pairs:
            for j in jobs:
                for erl in j.evaluatorRecords:
                    for er in erl:
                        if not self.false_predicate(er.meritValue):
                            er.meritValue = self.replacement_value


class Merit(object):
    """Class defining merit function within pprofit.

  The class is responsible for coordinating the job creation, job batching, running and
  result evaluation.

  Merit support a few callbacks which can be set as properties:
    beforeRun : this callable is invoked after job creation but before jobs are run.
                The signature for beforeRun is:
                beforeRun(candidateJobPairs)

                Where candidateJobPairs is a list of (CANDIDATE, JOB_LIST) pairs (one for each
                candidate solution). CANDIDATE is atsim.pro_fit.variables.Variables instance
                and JOB_LIST is a list of the atsim.pro_fit.jobfactories.Job instances
                representing each job belonging to the candidate.

    afterRun  : this is called after the jobs have been run, but before merit values are
                evaluated. It is passed the same variables as beforeRun. This is an instance of
                MultiCallback meaning new callbacks are registered by appending to the afterRun property.

    afterEvaluation : called after evaluation but before values are reduced to produce
                merit values and also before job directories are removed. This is an instance of
                MultiCallback meaning new callbacks are registered by appending to the afterEvaluation property.

                As for afterRun and beforeRun, list of (CANDIDATE, JOB_LIST) pairs are passed
                to callback and job.evaluate() methods have been called therefore job.evaluatorRecords is
                accessible at this point.

    afterMerit : Called after reduction function (but before job directories are deleted) is applied and before calculate() returns.
                Has signature: afterMerit(meritValues, candidateJobPairs). This is an instance of
                MultiCallback meaning new callbacks are registered by appending to the afterMerit property.

                Where candidateJobPairs is as before and meritValues is a list of merit values one
                per candidate. """

    def __init__(
        self,
        runners,
        jobfactories,
        metaevaluators,
        calculatedVariables,
        jobdir,
    ):
        """@param runners List of job runners (see atsim.pro_fit.runners module for examples).
       @param jobfactories List of jobfactories (see atsim.pro_fit.jobfactories module for examples).
       @param metaevaluators List of meta-evaluators to apply following job evaluation.
       @param calculatedVariables Callable that accepts Variables and returns Variables that are then used during job creation.
       @param jobdir Directory in which job files should be created"""

        # Initialize callbacks to be None
        self._beforeRun = MultiCallback()
        self._afterRun = MultiCallback()
        self._afterEvaluation = MultiCallback()
        self._afterMerit = MultiCallback()

        self._runners = runners
        self._jobfactories = jobfactories
        self._metaevaluators = metaevaluators
        self.calculatedVariables = calculatedVariables
        self._jobdir = jobdir
        self._reductionFunction = _sumValuesReductionFunction

    def _getjobdir(self):
        return self._jobdir

    jobdir = property(fget=_getjobdir)

    def calculate(self, candidates, returnCandidateJobPairs=False):
        """Calculate Merit value for each Variables object in candidates and return list of merit values.

    @param candidates List of Variables instances
    @return List of merit values if returnCandidateJobPairs is False or if True return tuple (meritValueList, candidateJobPairs),
      where candidateJobPairs is a list of (CANDIDATE, JOB_LIST) pairs (one for each candidate solution).
      CANDIDATE is atomsscripts.fitting.variables.Variables instance and JOB_LIST is a list of the atsim.pro_fit.jobfactories.Job
      instances representing each job belonging to the candidate. This is equivalent to the values passed to afterMerit callback"""

        batchpaths, batchedJobs, candidate_job_lists = self._prepareJobs(
            candidates
        )
        try:
            finishedEvents = self._runBatches(batchedJobs)
            gevent.wait(objects=finishedEvents)

            # Call the afterRun callback
            self.afterRun(candidate_job_lists)  # pylint: disable=E1102

            # Apply evaluators
            self._applyEvaluators(batchedJobs)
            self._applyMetaEvaluators(
                [joblist for (v, joblist) in candidate_job_lists]
            )

            # Call the afterEvaluation callback (first zip evaluated lists with their jobs)
            self.afterEvaluation(candidate_job_lists)  # pylint: disable=E1102

            # Reduce evaluated dictionary into single values
            meritvals = self._reductionFunction(
                [joblist for (v, joblist) in candidate_job_lists]
            )

            self.afterMerit(
                meritvals, candidate_job_lists
            )  # pylint: disable=E1102

            if returnCandidateJobPairs:
                return (meritvals, candidate_job_lists)
            else:
                return meritvals
        finally:
            self._cleanBatches(batchpaths)

    def _prepareJobs(self, candidateVariables):
        """Create job directories and populate them with files from jobfactories.
    Returns Job instances and batches them together with their correct JobRunner.

    @param candidateVariables
    @return Tuple (batch_directories, job_lists, candidate_job_lists ).
            Where batch_directories : list of directories each batch is contained within.
            job_lists : list of lists of Job instances. Returned list is parallel to the self._runners list
            with each sub-list containing the jobs for a particular runner.
            candidate_job_lists : list of tuples (one pair per candidate), with pairs of variable instances and list of jobs for those variables."""
        runnerBatches = {}
        candidate_job_lists = []
        batchpaths = []
        for candidate in candidateVariables:
            candidate = self.calculatedVariables(candidate)
            cpath = tempfile.mkdtemp(dir=self._jobdir)
            batchpaths.append(cpath)
            candidate_job_lists.append((candidate, []))
            for factory in self._jobfactories:
                # Create job path as combination of candidate temporary directory
                # and factory.name.
                jobpath = os.path.join(cpath, factory.name)
                os.mkdir(jobpath)
                job = factory.createJob(jobpath, candidate)
                candidate_job_lists[-1][1].append(job)
                # Assign job to correct batch
                runnerBatches.setdefault(factory.runnerName, []).append(job)

        # Call the beforeRun callback
        self.beforeRun(candidate_job_lists)  # pylint: disable=E1102

        # Convert runnerBatches into list of lists
        return (
            batchpaths,
            [runnerBatches[r.name] for r in self._runners],
            candidate_job_lists,
        )

    def _runBatches(self, jobBatches):
        """Execute the runBatch command on the job batches and return threading.Event objects
    indicating when each batch completes

    @param jobBatches List of batched jobs as returned by _prepareJobs.
    @return List of gevent.event.Event objects representing each submitted batch"""
        events = []
        for runner, batch in zip(self._runners, jobBatches):
            f = runner.runBatch(batch)
            events.append(f.finishedEvent)
        return events

    def _cleanBatches(self, batchpaths):
        for p in batchpaths:
            shutil.rmtree(p, ignore_errors=True)

    def _applyEvaluators(self, batchedJobs):
        """Apply job evaluators.

    @param batchedJobs Batched Jobs as returned by _prepareJobs."""
        for batch in batchedJobs:
            for j in batch:
                j.evaluate()

    def _applyMetaEvaluators(self, batchedJobs):
        """Apply meta evaluators. This adds a Job like container to the end of each
    job batch with an evaluatorRecords property populated with EvaluatorRecord like instances
    obtained by passing each job list to the callables stored in self._metaevaluators

    @param batchedJobs Batched Jobs as returned by _prepareJobs."""
        if self._metaevaluators:
            for batch in batchedJobs:
                evaluatorRecords = []
                for metaEvaluator in self._metaevaluators:
                    evaluatorRecords.append(metaEvaluator(batch))
                metaEvaluatorJob = jobfactories.MetaEvaluatorJob(
                    "meta_evaluator", evaluatorRecords, batch[0].variables
                )
                batch.append(metaEvaluatorJob)

    @property
    def beforeRun(self):
        return self._beforeRun

    @property
    def afterRun(self):
        return self._afterRun

    @property
    def afterEvaluation(self):
        return self._afterEvaluation

    @property
    def afterMerit(self):
        return self._afterMerit
