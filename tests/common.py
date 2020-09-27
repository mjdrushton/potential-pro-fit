from atsim.pro_fit.jobfactories import Job
import multiprocessing
import subprocess
import os
import logging
import stat
import shutil
import gevent

logger = logging.getLogger("fitting_test")


class MockRunner(object):
    def __init__(self, name):
        self.name = name

    def runBatch(self, jobs):
        batch = MockBatch()
        finishedEvent = batch.finishedEvent

        def waiter(g):
            finishedEvent.set()

        g = gevent.Greenlet(mockfuture, jobs)
        g.start()
        g.link(waiter)
        gevent.sleep(0)
        return batch


def mockfuture(jobs):
    for job in jobs:
        # Copy files.
        jfdir = os.path.abspath(os.path.join(job.path, "job_files"))
        outputdir = os.path.abspath(os.path.join(jfdir, "output"))
        rundir = os.path.abspath(os.path.join(job.path, "rundir"))
        shutil.copytree(jfdir, rundir)

        try:
            # Make runjob executable
            logger.debug("Directory contents: %s" % os.listdir(rundir))
            os.chmod(os.path.join(rundir, "runjob"), stat.S_IRWXU)
            status = subprocess.check_call("./runjob", shell=True, cwd=rundir)
            logger.debug("Runjob status: %s" % status)
            with open(os.path.join(rundir, "STATUS"), "w") as outfile:
                print("%d" % status, file=outfile)
            logger.debug("Directory contents after run: %s" % os.listdir(rundir))
        finally:
            os.rename(rundir, outputdir)


class MockEvaluator(object):
    def __init__(self, evalfunc, name="v"):
        self.evalfunc = evalfunc
        self.name = name

    def __call__(self, job):
        opath = os.path.join(job.path, "job_files", "output", "output.res")
        logger.debug("Call output path: %s" % opath)
        d = {}
        with open(opath, "r") as infile:
            for line in infile:
                tokens = line.split(":")
                k = tokens[0].strip()
                v = float(tokens[1].strip())
                d[k] = v
        from atsim import pro_fit

        return [
            pro_fit.evaluators.EvaluatorRecord(
                self.name, None, None, meritValue=self.evalfunc(d)
            )
        ]


def e1(d):
    return (d["A"] + d["B"]) / (d["C"] - d["D"])


def e2(d):
    return sum(d.values())


def e3(d):
    return d["A"] - d["B"] - d["C"] - d["D"]


class MockBatch(object):
    def __init__(self):
        self.finishedEvent = gevent.event.Event()


class MockJobFactory(object):
    def __init__(self, runnerName, jobName, evaluators):
        self.name = jobName
        self.runnerName = runnerName
        self.evaluators = evaluators

    def runTasksBeforeRun(self, job):
        pass

    def runTasksAfterRun(self, job):
        pass

    def createJob(self, destdir, variables):
        jfdir = os.path.join(destdir, "job_files")
        os.mkdir(jfdir)

        rfdir = os.path.join(destdir, "runner_files")
        os.mkdir(rfdir)

        with open(os.path.join(rfdir, "testfile"), "w") as _testfile:
            pass

        # Create runjob
        logger.debug("createJob destdir: %s" % destdir)
        runjobfilename = os.path.join(jfdir, "runjob")
        with open(runjobfilename, "w") as outfile:
            print("#! /bin/bash", file=outfile)
            print("#Job: %s" % self.name, file=outfile)
            print("#Runner: %s" % self.runnerName, file=outfile)
            print("#Candidate: %d" % variables.id, file=outfile)
            for k, v in variables.variablePairs:
                print("#Variable:%s:%f" % (k, v), file=outfile)
                print("echo %s:%f >> output.res" % (k, v), file=outfile)

            print("ls ../runner_files > runner_files_contents", file=outfile)

        logger.debug("createJob directory content: %s" % os.listdir(destdir))
        return Job(self, destdir, variables)
