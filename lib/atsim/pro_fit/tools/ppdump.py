import argparse
import sys

import sqlalchemy as sa
from atsim.pro_fit import db

def parseCommandLine():
  parser = argparse.ArgumentParser(
    prog = "ppdump",
    description = "Dump potential pro-fit fitting_run.db into a CSV file for post-processing.")

  parser.add_argument("-f", "--dbfilename",
    metavar = "DB_FILENAME",
    default = "fitting_run.db",
    nargs = "?",
    help = "Specify path of 'fitting_run.db' file.")

  metadataGroup = parser.add_argument_group("Fitting Run Information")

  metadataGroup.add_argument("--list-columns",
    dest = 'list_columns',
    action = "store_const", const= "all",
    help = "list available columns from the database")

  metadataGroup.add_argument("--list-variable-columns",
    dest = 'list_columns',
    action = "store_const", const= "variable",
    help = "list available variable: prefix columns from the database")

  metadataGroup.add_argument("--list-evaluator-columns",
    dest = 'list_columns',
    action = "store_const", const= "evaluator",
    help = "list available evaluator: prefix columns from the database")

  metadataGroup.add_argument("--list-it-columns",
    dest = 'list_columns',
    action = "store_const", const= "it",
    help = "list available it: prefix columns from the database")

  metadataGroup.add_argument("--list-stat-columns",
    dest = 'list_columns',
    action = "store_const", const= "stat",
    help = "list available stat: prefix columns from the database")


  metadataGroup.add_argument("-n", "--num-iterations",
    action = "store_true",
    help = "output the number of iterations in the database")

  dumpGroup = parser.add_argument_group("Dump Options",
    description = "Options for controlling the dump.")

  dumpGroup.add_argument("-o", "--output",
    dest = 'output_file',
    nargs = '?',
    type=argparse.FileType('w'),
    default = sys.stdout,
    metavar = 'OUTPUT_FILE',
    help = "write output into OUTPUT_FILE. If not specified then output is written to STDOUT.")

  dumpGroup.add_argument("-c", "--columns",
    nargs = '*',
    metavar = "COLUMN_LABEL",
    help = "List of column keys to be included the dump. If not specified all columns will be included in dump.")

  options = parser.parse_args()

  return options

def listColumns(engine, whichSet = 'all'):
  """List column keys to stdout

  :param engine: SQL Alchemy database engine"""
  ItT = db.IterationSeriesTable
  colsets = {'all' : ItT.validKeys,
    'variable' : ItT.validVariableKeys,
    'evaluator' : ItT.validEvaluatorKeys,
    'it' : ItT.validIterationKeys,
    'stat' : ItT.validStatisticsKeys}

  keys = colsets[whichSet](engine)
  for key in keys:
    print key

def outputNumIterations(engine):
  """Display number of iterations in the file.

  :param engine: SQL Alchemy database engine"""
  f = db.Fitting(engine)
  print f.current_iteration()

def outputTable(engine, outfile):
  iterationSeriesTable = db.IterationSeriesTable(engine)
  for row in iterationSeriesTable:
    print >>outfile, ",".join([str(v) for v in row])

def main():
  options = parseCommandLine()

  engine = sa.create_engine("sqlite:///"+options.dbfilename)

  if options.list_columns:
    listColumns(engine, options.list_columns)
  elif options.num_iterations:
    outputNumIterations(engine)
  else:
    outputTable(engine, options.output_file)


if __name__ == '__main__':
  main()
