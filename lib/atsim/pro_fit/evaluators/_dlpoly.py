from _common import * # noqa

from atomsscripts import dlpoly

import os

class DLPOLY_STATISEvaluatorException(Exception):
  pass

class DLPOLY_STATISEvaluator(object):
  """Evaluator used for extracting property time averages from a DL_POLY STATIS file.

  Assumes that job output directory contains CONFIG and STATIS files for a given job.

  The evaluator supports the same key names as the statisToCSV.py tool."""

  def __init__(self, name, startTime, keyExpectTriples):
    """@param name Name of evaluator.
       @param startTime Time averages will be taken from startTime to end of STATIS file
       @param keyExpectTriples (propertyKey, expectedValue, weight) tuples defining values to be extracted"""
    self.name = name
    self._startTime = startTime
    self._keyExpectTriples = keyExpectTriples

  def __call__(self, job):
    """Returns list of RMSEvaluatorRecord instances, one per property, for dlpoly job found in job.path

    @param job Job instance for dlpoly run.
    @return List of RMSEvaluatorRecord instances"""

    configFilename = os.path.join(job.path, "output", "CONFIG")
    controlFilename = os.path.join(job.path, "output", "CONTROL")
    statisFilename = os.path.join(job.path, "output", "STATIS")

    # Parse the config
    try:
      with open(configFilename, 'rb') as configfile:
        config = dlpoly.parse.parseCONFIG(configfile)
      with open(controlFilename,'rb') as controlfile:
        nptflag = self._isNPT(controlfile)
      with open(statisFilename, 'rb') as statisfile:
        statisIterator = dlpoly.parse.parseSTATIS(statisfile, config, nptflag)
        rows = self._extractValues(statisIterator)
        return self._makeRecords(rows)
    except Exception as exc:
      return self._makeErrorRecords(exc)

  def _extractValues(self, statisIterator):
    rows = []
    for row in statisIterator:
      if row['time'] >= self._startTime:
        rows.append(row)
    if row['time'] < self._startTime:
      raise DLPOLY_STATISEvaluatorException("Maximum time in STATIS file < start_time")
    return rows

  def _calculateAverage(self, k, rows):
    values = [r[k] for r in rows]
    a = sum(values)/float(len(values))
    return a

  def _makeRecords(self, rows):
    records = []
    for k,e,w in self._keyExpectTriples:
      try:
        r = RMSEvaluatorRecord(k, e, self._calculateAverage(k,rows), w)
      except Exception as exc:
        r = ErrorEvaluatorRecord(k, e, exc, w)
      r.evaluatorName = self.name
      records.append(r)
    return records

  def _makeErrorRecords(self, exc):
    records = []
    for k,e, w in self._keyExpectTriples:
      r = ErrorEvaluatorRecord(k, e, exc, w)
      r.evaluatorName = self.name
      records.append(r)
    return records


  def _isNPT(self, controlfile):
    for line in controlfile:
      line = line.strip()
      if line.startswith('ensemble'):
        tokens = line.split()
        if tokens[1] in ['npt', 'nst']:
          return True
    return False

  @staticmethod
  def createFromConfig(name, jobpath, cfgitems):
    allowedFields = set(dlpoly.parse.statisColumnKeys)
    allowedFields.update(set(
      ['stressxx', 'stressxy', 'stressxz',
       'stressyx', 'stressyy', 'stressyz',
       'stresszx', 'stresszy', 'stresszz']))
    allowedFields.update(set(
      ['cella_x', 'cella_y', 'cella_z',
      'cellb_x', 'cellb_y', 'cellb_z',
      'cellc_x', 'cellc_y', 'cellc_z'
      ] ))

    cfgdict = dict(cfgitems)
    startTime = cfgdict.get('start_time', 0.0)
    try:
      startTime = float(startTime)
    except ValueError:
      raise ConfigException("'start_time' value not a valid float for evaluator '%s'" % name)

    del cfgdict['type']

    if cfgdict.has_key('start_time'):
      del cfgdict['start_time']

    triplets = []
    for k,v in cfgdict.items():
      tokens = v.split()
      if len(tokens) == 2:
        try:
          weight = float(tokens[1])
        except ValueError:
          raise ConfigException("'%s' record's weight not a valid float for Evaluator '%s': '%s'" % (k,name, tokens[1]))
      else:
        weight = 1.0

      try:
        value = float(tokens[0])
      except ValueError:
        raise ConfigException("Value '%s' record value not a valid float for Evaluator '%s': '%s'" % (k,name, tokens[0]))

      # Check to see if field is valid.
      if not ((k in allowedFields) or k.startswith('msd_')):
        raise ConfigException("Invalid field name: '%s' for Evaluator '%s'" % (k,name))

      triplets.append((k, value, weight))
    return DLPOLY_STATISEvaluator(name, startTime, triplets)
