# -*- coding: utf-8 -*-
import cStringIO
import numbers
import os
import re
import shutil
import string
import sys

import logging

class CSVBuildKeyError(KeyError):
  """Raised when a bad substitution variable is encountered."""
  templateFilename = ""

class _StringFormatter(string.Formatter):
  
  def format_field(self, value, conversion):
    # Floating point types
    if conversion and conversion[-1] in 'eEfFgGn%':
      # Check that value is a number
      if not isinstance(value, numbers.Number):
        # Convert string to a number
        value = float(value)
    elif conversion and conversion[-1] in 'bcdoxX':
      if not isinstance(value, numbers.Number):
        value = int(value)
    return string.Formatter.format_field(self, value, conversion)


def _includeHandler(placeholder, substitutionDict, skelpath):
  if not placeholder.startswith('INCLUDE:'):
    return (False, None)

  fnameToken = placeholder[8:]
  fname = substitutionDict[fnameToken]

  if not os.path.isabs(fname):
    fname = os.path.join(skelpath, fname)

  logging.getLogger('csvbuild._includeHandler').debug('@INCLUDE: filename="%s"' % fname)
  with open(fname, 'rb') as infile:
    includedTemplate = infile.read()
    substituted = _templateSubstitution(includedTemplate, substitutionDict, skelpath)
    return (True, substituted)


class _DefaultHandler(object):

  def __init__(self, stringFormatter):
    self._stringFormatter = stringFormatter

  def __call__(self, placeholder, substitutionDict, skelpath):
    try:
      tokens = placeholder.split(":", 1)
      if len(tokens) > 1:
        placeholder, fmt = tokens
        fmtstring = "{"+placeholder+":"+fmt+"}"
      else:
        fmtstring = "{"+placeholder+"}"
      
      retstring = self._stringFormatter.format(fmtstring, **substitutionDict)
    except KeyError, e:
      raise CSVBuildKeyError(*e.args)
    return (True, retstring)

_defaultHandler = _DefaultHandler(_StringFormatter())

_handlers = [_includeHandler, _defaultHandler]
def _placeholderHandler(placeholder, substitutionDict, skelpath):
  for handler in _handlers:
    status, retstring = handler(placeholder, substitutionDict, skelpath)
    if status:
      return retstring


def _templateSubstitution(template, substitutionDict, skelpath):
  """Substitute @PLACE_HOLDER@ for values in substitutionDict

  :param template: Template containing @PLACE_HOLDER@ tags
  :param substitutionDict: Values to be substituted into template

  :return: Substituted string"""
  splitRegex = re.compile(r"((?<!\\)@(.*?)(?<!\\)@)")
  sbuild = cStringIO.StringIO()

  tokens = splitRegex.split(template)
  while len(tokens) > 0:
    token = tokens.pop(0)
    if token.startswith('@'):
      placeholder = tokens.pop(0)
      token = _placeholderHandler(placeholder, substitutionDict, skelpath)

    #Remove escape characters from @ signs.
    token = re.sub(r'\\@', '@', token)
    sbuild.write(token)
  return sbuild.getvalue()


class _DirectoryWalker(object):
  _logger = logging.getLogger('csvbuild._DirectoryWalker')

  def __init__(self, skeletonDirectory, destinationDirectory, templateSuffix):
    self.skeletonDirectory = skeletonDirectory
    self.destinationDirectory = destinationDirectory
    self.templateSuffix = templateSuffix
    self.overwrite = False

    self._logger.debug("init with: skeletonDirectory= %s, destinationDirectory= %s, templateSuffix= %s " % (self.skeletonDirectory,
      self.destinationDirectory,
      self.templateSuffix))

  def _pathToDestPath(self, skelPath, row):
    #Create the destination directory
    skelPath = os.path.normpath(skelPath)
    pathTokens = skelPath.split(os.path.sep)
    #... change first component of path to dest
    pathTokens = pathTokens[1:]
    pathTokens.insert(0, self.destinationDirectory)
    destPath = os.path.join(*pathTokens)
    #... perform substitution
    destPath = _templateSubstitution(destPath, row, self.skeletonDirectory)
    return destPath

  def _processFile(self, sourcedirname, destdirname, filename, row):
    """Copies file to destination performing template substitution if necessary"""
    destfilename = _templateSubstitution(filename, row, self.skeletonDirectory)

    if not destfilename.endswith(self.templateSuffix):
      # Doesn't need any extra processing, just copy
      srcpath = os.path.join(sourcedirname, filename)
      dstpath = os.path.join(destdirname, destfilename)
      if self.overwrite == False and os.path.isfile(dstpath):
        self._logger.debug("'%s' exists, will not overwrite" % dstpath)
        return
      self._logger.debug("Copying: %s ---> %s" % (srcpath,dstpath))
      shutil.copy( srcpath, dstpath)
    else:
      # Perform template substitution
      destfilename = destfilename[:-len(self.templateSuffix)]
      srcpath = os.path.join(sourcedirname, filename)
      dstpath = os.path.join(destdirname, destfilename)
      if self.overwrite == False and os.path.isfile(dstpath):
        self._logger.debug("'%s' exists, will not overwrite" % dstpath)
        return
      self._logger.debug("Template processing: %s ---> %s" % (srcpath, dstpath))
      with open(srcpath, 'rb') as infile:
        filecontents = infile.read()

      try:
        filecontents = _templateSubstitution(filecontents, row, self.skeletonDirectory)
      except CSVBuildKeyError, e:
        augmentedException = CSVBuildKeyError(*e.args)
        augmentedException.templateFilename = srcpath
        raise augmentedException

      with open(dstpath, 'wb') as outfile:
        outfile.write(filecontents)


  def _processDirectory(self, row, dirname, names):
    destdirname = self._pathToDestPath(dirname,row)
    self._logger.debug("Found directory: %s. Destination Directory: %s" % (dirname, destdirname))
    if not os.path.isdir(destdirname):
      self._logger.debug("Directory doesn't exist, creating.")
      os.mkdir(destdirname)
    shutil.copystat(dirname, destdirname)
    for n in names:
      fname = os.path.join(dirname, n)
      if os.path.isdir(fname):
        os.path.walk(fname, self._processDirectory, row)
      else:
        self._processFile(dirname, destdirname, n, row)

  def processRow(self, row):
    os.path.walk(self.skeletonDirectory, self._processDirectory, row)


def buildDirs(rows,
    skeletonDirectory,
    destinationDirectory,
    templateSuffix = '.in',
    extraVariables = {},
    overwrite = False):
  """Create a directory structure from a CSV file.

  :param rows: List of dictionaries containing column value pairs (as returned by csv.DictReader)
  :param skeletonDirectory: Source directory containing files with which to populate the create directory structure.
  :param destinationDirectory: Path giving the root of the created directory structure
  :param templateSuffix: Files within skeletonDirectory with this suffix are subject to filename and file content variable substitution from rows
  :param extraVariables: Dictionary giving extra key value pairs which should be added to spreadsheet row before processing templates
  :param overwrite: If True, overwrite existing files"""
  dw = _DirectoryWalker(skeletonDirectory, destinationDirectory, templateSuffix)
  dw.overwrite = overwrite
  logger = logging.getLogger('csvbuild.buildDirs')
  if overwrite:
    logger.debug("File overwriting enabled")
  else:
    logger.debug("File overwriting disabled")

  for row in rows:
    row = dict(row)
    row.update(extraVariables)
    dw.processRow(row)

def _verboseLogging():
  """Set-up python logging to display to stderr"""
  _setupLogging(logging.DEBUG)
  logger = logging.getLogger('csvbuild')
  logger.info("Verbose output enabled")

def _setupLogging(level=logging.WARN):
  """Set-up python logging to display to stderr"""
  logger = logging.getLogger('csvbuild')
  logger.setLevel(level)
  stderrHandler = logging.StreamHandler(sys.stderr)
  formatter = logging.Formatter('[%(levelname)s] %(message)s')
  stderrHandler.setFormatter(formatter)
  logger.addHandler(stderrHandler)

def _commandLineParser():
  usage = u"""usage: %prog [options] CSV_FILENAME TEMPLATE_DIRECTORY DESTINATION_DIRECTORY

A tool that can be thought of as the equivalent of mail-merge for files and directories.
Create a directory structure from the contents of a CSV file and a template directory structure.

Arguments:
  CSV_FILENAME          - Path to CSV formatted spreadsheet.
  TEMPLATE_DIRECTORY    - Root of directory structure providing template for created files.
  DESTINATION_DIRECTORY - Root of directory structure where files should be created.

Description
===========
For each row in a CSV spreadsheet, the files within a template directory copied to a destination
directory. These filenames may contain placeholders of the form @COL_NAME@ (where COL_NAME is the
name of a column within the spreadsheet). During the copying process, these are replaced with
the pertinent value from the current row in order to produce the destination filename. In this way
large numbers of related files/directory structures may be produced.

In addition, value substitution will be performed for any file  with a name ending with '.in'
(see the -s/--suffix option). Meaning that @COL_NAME@ placeholders within the destination file
will be replaced with values taken from the spreadsheet. The destination filename has the suffix
removed and undergoes value substitution itself. To use @ signs within your filenames or '.in'
files you must escape them.

Including Files:
---------------
Files can be included during template substitution by using the @INCLUDE:col_name@ placeholder.
Where 'col_name' is itself a variable placeholder for a spreadsheet column containing the name
of the file you wish to include.

Note: filename are specified relative to the template directory root (unless the filepath is
      itself absolute).

Example 1:
=========
The following example shows how a number of DL_POLY runs could be created for a series of temperatures
from a spreadsheet.

Contents of spreadsheet.csv:
  temperature, run_num
        300.0,       1
        400.0,       2
        500.0,       3

Template directory structure:
  template/
  └── T@temperature@
      ├── CONFIG                <-- DL_POLY CONFIG
      ├── CONTROL.in            <-- DL_POLY CONTROL file containing line: temperature @temperature@
      ├── FIELD                 <-- DL_POLY FIELD
      └── submit_@run_num@.sh   <-- queue submission script

The values in the spreadsheet can be combined with the template directory structure using the following
command line:
  %prog spreadsheet.csv template dl_poly_runs

This would create the following directory structure in which the CONTROL files contain the appropriate
temperature values:

  dl_poly_runs/
  ├── T300.0
  │   ├── CONFIG
  │   ├── CONTROL
  │   ├── FIELD
  │   └── submit_1.sh
  ├── T400.0
  │   ├── CONFIG
  │   ├── CONTROL
  │   ├── FIELD
  │   └── submit_2.sh
  └── T500.0
      ├── CONFIG
      ├── CONTROL
      ├── FIELD
      └── submit_3.sh

"""

  import optparse
  parser = optparse.OptionParser(usage)
  parser.add_option("-s", "--suffix",
      action='store',
      dest='suffix',
      type = 'string',
      default = '.in',
      help = "Specify filename suffix for files that should undergo template substitution.")
  parser.add_option("-p", "--variable",
      action='append',
      dest='extra_variables',
      type= 'string',
      default= None,
      metavar = "PAIR",
      help = "Specify extra place-holder values which augment or override those read from the spreadsheet. PAIR takes form of PLACEHOLDER:VALUE, meaning that @PLACEHOLDER@ in template files will be replaced by VALUE.")
  parser.add_option("-v", "--verbose",
      action='store_true',
      dest='verbose',
      default=False,
      help="Provide verbose output.")
  parser.add_option("-f", "--force",
      action='store_true',
      dest='overwrite',
      default=False,
      help="If set, overwrite existing files. Otherwise existing files will not be overwritten.")
  return parser

def main():
  optionparser = _commandLineParser()
  options,args = optionparser.parse_args()
  if len(args) != 3:
    optionparser.error("Wrong number of arguments.")

  suffix = options.suffix
  csvfilename, templatedir, destpath = args

  #Process verbose option
  if options.verbose:
    _verboseLogging()
  else:
    _setupLogging()

  #Process -p options
  if options.extra_variables != None:
    extraVariables = {}
    for var in options.extra_variables:
      key,value = var.split(':')
      extraVariables[key] = value
  else:
    extraVariables = {}

  import csv
  try:
    infile = open(csvfilename, 'rUb')
  except IOError:
    optionparser.error("Couldn't open: %s" % csvfilename)

  dr = csv.DictReader(infile)

  if os.path.realpath(csvfilename) == os.path.realpath(destpath):
    optionparser.error("Template directory and destination directories cannot be the same")

  try:
    buildDirs(dr, templatedir, destpath, suffix, extraVariables = extraVariables, overwrite = options.overwrite)
  except CSVBuildKeyError as e:
    logger = logging.getLogger('csvbuild')
    msg  = "Unknown variable name '%s' specified in template: '%s'" % (e.args[0], e.templateFilename)
    logger.error(msg)
    sys.exit(3)


