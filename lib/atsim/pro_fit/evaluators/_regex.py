import re
import collections
import os

import logging

from atsim.pro_fit.fittool import ConfigException


from ._common import RMSEvaluatorRecord, ErrorEvaluatorRecord

_logger = logging.getLogger("atsim.pro_fit.evaluators.RegexEvaluator")

class RegexEvaluatorException(Exception):
  pass

class _RegexSubEvaluator(object):

  _configRegex = re.compile(r'/(.*?)/\s+(.*)')

  def __init__(self, name, configString, outputFilename):
    self.name = name
    m = self._configRegex.match(configString)
    if not m:
      raise ConfigException("Could not parse configuration for Regex evaluator, variable '%s': %s " % (name, configString))

    self._variableRegex = re.compile(m.groups()[0])
    self._outputFilename = outputFilename

    #Parse the remaining the components of the configuration
    self._configure(m.groups()[1])

  def __call__(self, job):
    outputfilename = os.path.join(job.outputPath, self._outputFilename)

    foundinstance = None
    try:
      with open(outputfilename,'rb') as infile:
        for line in infile:
          line = line[:-1]
          m = self._variableRegex.search(line)
          if m:
            _logger.debug("Found match using '%s' within line '%s'" % (self._variableRegex.pattern, line))
            if foundinstance == None:
              foundinstance = 0
            else:
              foundinstance += 1

            if foundinstance == self.fileInstance:
              v = m.groups()[self.groupNum]
              _logger.debug("Converting value to float: '%s'" % v)
              v = float(v)
              return RMSEvaluatorRecord(self.name, self.expectedValue, v, self.weight)
      raise RegexEvaluatorException("Regular expression did not match file contents or did not match sufficient times")
    except Exception as e:
      return ErrorEvaluatorRecord(self.name, self.expectedValue, e, self.weight)



  def _configure(self, configstring):
    tokens = configstring.split()
    self.weight = 1.0
    self.groupNum = 0
    self.fileInstance = 0

    if not tokens:
      raise ConfigException("When configuring Regex evaluator '%s', you must, at a minimum specify a regular expression and expected value" % self.name)

    try:
      self.expectedValue = float(tokens.pop(0))
    except ValueError:
      raise ConfigException("Could not parse expected value for variable '%s': '%s'" % (self.name, tokens[0]))


    if not tokens:
      return

    try:
      self.weight = float(tokens.pop(0))
    except ValueError:
      raise ConfigException("Could not parse weight for variable '%s':" % (self.name, ))

    if not tokens:
      return

    t = tokens.pop(0)
    t = t.split(":")
    if len(t) >= 1:
      try:
        self.groupNum = int(t[0])-1
      except ValueError:
        raise ConfigException("Could not parse group number for variable '%s':" % (self.name, ))

    if len(t) > 1:
      try:
        self.fileInstance = int(t[1])-1
      except ValueError:
        raise ConfigException("Could not parse file instance for variable '%s':" % (self.name, ))

    if self.fileInstance < 0:
      raise ConfigException("File instance cannot be  <1")

    if self.groupNum < 0:
      raise ConfigException("Group number cannot be  <1")




class RegexEvaluator(object):
  """Evaluator that searches through files and, based on regular expressions, returns EvaluatorRecords
  from matched values"""

  def __init__(self, name, variables):
    """@param name Name of evaluator
       @param variables Instances of _RegexSubEvaluator"""

    self.evaluatorName = name
    self._subevalList = variables


  def __call__(self, job):
    output = []
    for subeval in self._subevalList:
      evaluated = subeval(job)
      evaluated.evaluatorName = self.evaluatorName
      output.append(evaluated)
    return output


  @staticmethod
  def createFromConfig(name, jobpath, cfgitems):
    od = collections.OrderedDict(cfgitems)
    try:
      filename = od['filename']
    except KeyError:
      raise ConfigException("Could not find 'filename' record in configuration for '%s' Regex evaluator." % name)

    del od['filename']
    del od['type']

    variables = []
    for k, v in od.items():
      sube = _RegexSubEvaluator(k, v, filename)

      variables.append(sube)
    return RegexEvaluator(name, variables)
