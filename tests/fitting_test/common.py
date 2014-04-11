import multiprocessing
import subprocess
import os
import logging
import stat
import shutil

logger = logging.getLogger('fitting_test')

class MockRunner(object):
  def __init__(self, name):
    self.name = name

  def runBatch(self, jobs):
    process = multiprocessing.Process(target = mockfuture, args = (jobs,))
    process.start()
    return process


def mockfuture(jobs):
  for job in jobs:
    # Copy files.
    outputdir = os.path.join(job.path, 'output')
    logger.debug('Outputdir: %s' % outputdir)
    shutil.copytree(job.path, outputdir)

    oldcwd = os.getcwd()
    try:
      os.chdir(outputdir)
      #Make runjob executable
      logger.debug('Directory contents: %s' % os.listdir('.'))
      os.chmod('runjob', stat.S_IRWXU)
      status = subprocess.check_call('./runjob', shell = True)
      logger.debug('Runjob status: %s' % status)
      with open('STATUS', 'wb') as outfile:
        print >>outfile, "%d" % status
      logger.debug('Directory contents after run: %s' % os.listdir('.'))
    finally:
      os.chdir(oldcwd)

class MockEvaluator(object):
  def __init__(self, evalfunc):
    self.evalfunc = evalfunc

  def __call__(self, job):
    opath = os.path.join(job.path, 'output', 'output.res')
    logger.debug("Call output path: %s" % opath)
    d = {}
    with open(opath, 'rb') as infile:
      for line in infile:
        tokens = line.split(':')
        k = tokens[0].strip()
        v = float(tokens[1].strip())
        d[k] = v
    from atsim import pro_fit
    return [pro_fit.evaluators.EvaluatorRecord('v', None, None, meritValue = self.evalfunc(d))]

def e1(d):
  return (d['A'] + d['B'])/(d['C'] - d['D'])

def e2(d):
  return sum(d.values())

def e3(d):
  return (d['A'] - d['B'] - d['C'] - d['D'])

from atsim.pro_fit.jobfactories import Job as MockJob

class MockJobFactory(object):
  def __init__(self, runnerName, jobName, evaluators):
    self.name = jobName
    self.runnerName = runnerName
    self.evaluators = evaluators

  def createJob(self, destdir, variables):
    #Create runjob
    logger.debug("createJob destdir: %s" % destdir)
    runjobfilename = os.path.join(destdir, 'runjob')
    with open(runjobfilename, 'wb') as outfile:
      print >>outfile, "#! /bin/bash"
      print >>outfile, "#Job: %s" % self.name
      print >>outfile, "#Runner: %s" % self.runnerName
      print >>outfile, "#Candidate: %d" % variables.id
      for k,v in variables.variablePairs:
        print >>outfile, "#Variable:%s:%f" % (k,v)
        print >>outfile, "echo %s:%f >> output.res" % (k,v)

    logger.debug("createJob directory content: %s" % os.listdir(destdir))
    return MockJob(self, destdir, variables)
