"""Module to support serializing fitting_run.db data into various table formats."""


class _RangeDiscoverIterator(object):

  class RangeException(Exception):
    pass

  def __init__(self):
    self.x_range = []
    self.y_range = []
    self._i = 0
    self._j = 0
    self._first = True
    self._xIsMajor = None
    self._xdir = None
    self._ydir = None
    self._xcomplete = False
    self._ycomplete = False

  def feed(self, x,y):
    if self._first:
      self._firstStep(x,y)
    elif self._i == 0 and self._j == 0:
      self._secondStep(x,y)
    else:
      self._nextStep(x,y)

  def _firstStep(self,x,y):
    self.x_range.append(x)
    self.y_range.append(y)
    self._first = False

  def _secondStep(self, x,y):
    lastx =  self.x_range[0]
    lasty =  self.y_range[0]

    xdir = cmp(x, lastx)
    ydir = cmp(y, lasty)

    if xdir != 0 and ydir == 0:
      self._xIsMajor = False
    elif xdir == 0 and ydir != 0:
      self._xIsMajor = True
    else:
      raise RangeException("Could not identify major and minor axes. The major axis should remain the same whilst the other varies.")

    if self._xIsMajor:
      self._j +=1
      self.y_range.append(y)
    else:
      self._i +=1
      self.x_range.append(x)

    if xdir != 0:
      self._xdir = xdir

    if ydir != 0:
      self._ydir = ydir

  def _nextStep(self, x,y):
    lastx = self.x_range[self._i]
    lasty = self.y_range[self._j]

    xdir = cmp(x,lastx)
    ydir = cmp(y,lasty)

    if xdir == 0 and not self._xIsMajor:
      raise RangeException("X-axis is not the major axis but x-value did not increase for current table row")

    if ydir == 0 and self._xIsMajor:
      raise RangeException("Y-axis is not the major axis but x-value did not increase for current table row")


    if xdir != 0 and self._xdir is None:
      self._xdir = xdir
      self.x_range.append(x)
      self._i += 1
    elif xdir != 0 and xdir != self._xdir:
      # X-axis has reset
      self._i = 0
      if x != self.x_range[0]:
        raise RangeException("X-axis reset. Was expecting a value of %f but found %f" % (self.x_range[0], x))
      self._xcomplete = True
    elif xdir == self._xdir:
      self._i += 1
      if self._xcomplete:
        # Check that the grid value is correct
        if x != self.x_range[self._i]:
          raise RangeException("X-axis unexpected x value found. Expected %f but found %f" % (self.x_range[self._i], x))
      else:
        self.x_range.append(x)

    if ydir != 0 and self._ydir is None:
      self._ydir = ydir
      self.y_range.append(y)
      self._j += 1
    elif ydir != 0 and ydir != self._ydir:
      # X-axis has reset
      self._j = 0
      if y != self.y_range[0]:
        raise RangeException("Y-axis reset. Was expecting a value of %f but found %f" % (self.y_range[0], y))
      self._ycomplete = True
    elif ydir == self._ydir:
      self._j += 1
      if self._ycomplete:
        # Check that the grid value is correct
        if y != self.y_range[self._j]:
          raise RangeException("X-axis unexpected y value found. Expected %f but found %f" % (self.y_range[self._i], y))
      else:
        self.y_range.append(y)


  def __str__(self):
    s = ["_RangeDiscoverIterator(",
          "x_range =", self.x_range,
          ", y_range=", self.y_range,
          ", _i=", self._i,
          ", _j=", self._j,
          ", _first=", self._first,
          ", _xIsMajor=", self._xIsMajor,
          ", _xdir=", self._xdir,
          ", _ydir=", self._ydir,
          ", _xcomplete=", self._xcomplete,
          ",_ycomplete=", self._ycomplete,
          ")"]

    s = [str(t) for t in s]
    return "".join(s)

def _populateArrays(table, xcolumn, ycolumn, zcolumn, missingValues, transposeZ = False):
  rowkeys = next(table)

  if not (xcolumn in rowkeys):
    raise KeyError("xcolumn named '%s' not found in table" % xcolumn)

  if not (ycolumn in rowkeys):
    raise KeyError("ycolumn named '%s' not found in table" % ycolumn)

  if not (zcolumn in rowkeys):
    raise KeyError("zcolumn named '%s' not found in table" % zcolumn)

  xidx = rowkeys.index(xcolumn)
  yidx = rowkeys.index(ycolumn)
  zidx = rowkeys.index(zcolumn)

  # Now iterate over the table and write the z matrix.
  # Collect the x and y values on the way.

  rangeDiscover = _RangeDiscoverIterator()
  zvals = []
  for row in table:
    x = row[xidx]
    y = row[yidx]
    z = row[zidx]

    rangeDiscover.feed(x,y)

    if z is None:
      z = missingValues
    zvals.append(z)


  if transposeZ:
    newz = []
    numcol = len(rangeDiscover.y_range)
    numrow = len(rangeDiscover.x_range)
    for col in range(numcol):
      for row in range(numrow):
        zidx = row * numcol + col
        newz.append(zvals[zidx])
    zvals = newz

  return rangeDiscover.x_range, rangeDiscover.y_range, zvals


def _serializeRArray(l):
  tl = []

  for v in l:
    if v == None:
      tl.append('NA')
    else:
      tl.append(v)

  s = "c(" + ",".join([str(v) for v in tl])+")"
  return s

def _serializeRMatrix(xdim, ydim, vals):
  s = "structure("+_serializeRArray(vals)+",.Dim=c("+str(xdim)+"L,"+str(ydim)+"L))"
  return s

def serializeTableForR(table, outfile, xcolumn, ycolumn, zcolumn, missingValues = None):
  """Convert atsim.pro_fit.db.IterationSeriesTable instance into an R data-structure that can be used to create a
  3D surface plot.

  The table is serialized in a format that can be read using R's `dget` function. The created object is an R list, this
  has the following attributes:

  * `x` : x-axis values
  * `y` : y-axis values
  * `z` :matrix (with dimension `length(x)` by `length(y)`)
  * `x_name` : string with value of `xcolumn` parameter passed to this function.
  * `y_name` : string containing value of `ycolumn` parameter passed to this function.
  * `z_name` : string containing value of `zcolumn` parameter passed to this function.

  :param table atsim.pro_fit.db.IterationSeriesTable: table to be serialised.
  :param outfile: Python file into which serialized data should be written.
  :param xcolumn: Valid column key for use with `table` defining x-axis of 3D surface.
  :param ycolumn: Column key for `table` defining the y-axis of 3D surface.
  :param zcolumn: Column key for `table` z-values.
  :param missingValues: None values will be replaced by the value of this argument if provided."""

  x_range, y_range, zvals = _populateArrays(table, xcolumn, ycolumn, zcolumn, missingValues, transposeZ = True)

  outfile.write("structure(list(x_name='%s',y_name='%s',z_name='%s'," % (xcolumn, ycolumn, zcolumn))

  # Write the ranges
  outfile.write("x=%s," % _serializeRArray(x_range))
  outfile.write("y=%s," % _serializeRArray(y_range))

  outfile.write("z=%s" % _serializeRMatrix( len(x_range),
                                            len(y_range),
                                            zvals))
  outfile.write("))")

def serializeTableForGNUPlot(table, outfile, xcolumn, ycolumn, zcolumn, missingValues = None):
  """Write atsim.pro_fit.db.IterationSeriesTable instance to `outfile` in the format read by GNUplot's `splot` command.

  :param table atsim.pro_fit.db.IterationSeriesTable: table to be serialised.
  :param outfile: Python file into which serialized data should be written.
  :param xcolumn: Valid column key for use with `table` defining x-axis of 3D surface.
  :param ycolumn: Column key for `table` defining the y-axis of 3D surface.
  :param zcolumn: Column key for `table` z-values.
  :param missingValues: None values will be replaced by the value of this argument if provided."""

  x_range, y_range, zvals = _populateArrays(table, xcolumn, ycolumn, zcolumn, missingValues)

  # Write header row
  outfile.write("#%s %s %s\n" % (xcolumn, ycolumn, zcolumn))

  zit = iter(zvals)
  for x in x_range:
    for y in y_range:
      zval = next(zit)
      if zval == None:
        zval = missingValues
      outfile.write("%s %s %s\n" % (x,y,zval))
    outfile.write("\n")

