import itertools
import functools
import sys
import optparse

import csv

class ArgumentException(Exception):
  pass

class GridGenerator(object):
  """docstring for GridGenerator"""

  def __init__(self, parameterRanges):
    """Create an iterator over the given parameter ranges.

    ``parameterRanges`` is a list of (NAME, LOW_VALUE, STEP_SIZE, NUM_STEPS) tuples.

    Where:
      * ``NAME`` is the parameter name.
      * ``LOW_VALUE`` is the first value in the range
      * ``STEP_SIZE` increment along the axis represented by this parameter.
      * ``NUM_STEPS`` number of steps along this axis.


    Iterator returns dictionaries with keys = parameter names and values obtained
    by nested iteration over each axis"""
    self._parameterRanges = parameterRanges
    self.rows = 0

  @property
  def fieldnames(self):
    return [name for (name, l, s, ns) in self._parameterRanges]

  def __iter__(self):
    iterables = [self._makeAxis(prange) for prange in self._parameterRanges]
    iterable = itertools.product(*iterables)
    for i, v in enumerate(iterable):
      self.rows = i+1
      # import pdb;pdb.set_trace()
      yield dict(v)

  def _makeAxis(self, prange):
    name, low, gridInc, steps = prange
    v = low
    for i in xrange(steps):
      yield (name, v + i * gridInc)


def _argsToGridRanges(rangeFlag, args):
  ranges = []
  def err(a, extramsg=""):
    extramsg = extramsg + "."
    if rangeFlag:
      raise ArgumentException("%s Ranges should be of the form 'NAME:LOW_VALUE,HIGH_VALUE,NUM_STEPS'.  Could not parse: '%s'" % (a,extramsg))
    else:
      raise ArgumentException("%s Ranges should be of the form 'VARIABLE_NAME:LOW_VALUE,STEP_SIZE,NUM_STEPS'. Could not parse: '%s'" % (a,extramsg))

  for a in args:
    try:
      label,rstring = a.split(":", 1)
      a1,a2,a3 = rstring.split(",")
    except ValueError:
      err(a)

    if rangeFlag:
      try:
        low = float(a1)
        high = float(a2)
        steps = int(a3)
      except ValueError as e:
        err(a, e.args[0])

      # Calculate step size
      if steps == 0:
        err("Number of steps cannot be 0")
      else:
        stepsize = (high - low)/ (steps-1)
      ranges.append((label, low, stepsize, steps))
    else:
      try:
        low = float(a1)
        stepsize = float(a2)
        steps = int(a3)
      except ValueError as e:
        err(a, e.args[0])
      if steps == 0:
        err("Number of steps cannot be 0")
      if stepsize == 0.0:
        err("Step size cannot be 0")
      ranges.append((label, low, stepsize, steps))
  return ranges


def _parseCmdLine():
  parser = optparse.OptionParser("""%prog [OPTIONS] ARGS

Create a CSV file containing an n-dimensional grid suitable for use with the pprofit Spreadsheet minimiser.

The arguments to %prog specify each grid axis. Grid dimensions and resolution can be specified in one of two ways:

  1. By default each grid axis is specified as:

      NAME:LOW_VALUE,STEP_SIZE,NUM_STEPS

    Where:
      * NAME      - Is variable name.
      * LOW_VALUE - Axis start value.
      * STEP_SIZE - Increment between consecutive values along grid axis.
      * NUM_STEPS - Number of axis grid values.


  2. If the --range option is used then the following format is used:

      NAME:LOW_VALUE,HIGH_VALUE,NUM_STEPS

    Where:
      * NAME       - Variable name
      * LOW_VALUE  - Axis start value.
      * HIGH_VALUE - Axis end value.
      * NUM_STEPS  - Number of axis grid values.


Example 1:
----------
To create two dimensional 10x10 grid for variables A and B with ranges:
  10 <= A <= 20
  0  <= B <= 5

the following command line could be used to write grid into a file named grid.csv:

  %prog --range A:10,20,10 B:0,5,10 -o grid.csv


Example 2:
----------
To create a 5x10x5 grid with running from 0 to 5 on axes A,B and C with specific grid increments, writing to STDOUT
this could be used:

  %prog A:0,1,5 B:0,0.5,10 C:0,1,5

 """)

  parser.add_option("-r", "--range",
    action = 'store_true', dest = 'range', default = False,
    help = "Arguments are specified as NAME:LOW,HIGH,NUM_STEPS combinations.")

  parser.add_option("-o", "--output",
    action = 'store', dest = 'outfilename', metavar = 'FILENAME',
    help = "Write output data in CSV format to FILENAME. If not specified, output is written to STDOUT.")

  opts,args =  parser.parse_args()
  if len(args) == 0:
   parser.error("You need to specify at least one grid axis.")

  return opts,args


def main():
  opts, args = _parseCmdLine()
  try:
    griddims = _argsToGridRanges(opts.range, args)
  except ArgumentException as e:
    print >> sys.stderr, "Error:", e.message
    sys.exit(1)

  if opts.outfilename:
    outfile = open(opts.outfilename, 'wb')
  else:
    outfile = sys.stdout

  gen = GridGenerator(griddims)
  dw = csv.DictWriter(outfile, gen.fieldnames)
  dw.writeheader()
  dw.writerows(gen)

  print >> sys.stderr, "%d rows written" % gen.rows





if __name__ == '__main__':
  main()

