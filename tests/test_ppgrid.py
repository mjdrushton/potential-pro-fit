import unittest

import testutil

from atsim.pro_fit.tools import ppgrid


class PPGridTestCase(unittest.TestCase):
  """Tests for the ppgrid tool"""

  def testGridGenerator(self):
    gridgen = ppgrid.GridGenerator([
      ('A', -1, 1, 6),
      ('B', 0.1, 0.1, 3),
      ('C', 1, 1, 1)])
    self.assertEquals(['A','B','C'], gridgen.fieldnames)

    expect = [
      {'A' : -1 , 'B' : 0.1, 'C' : 1},
      {'A' : -1 , 'B' : 0.2, 'C' : 1},
      {'A' : -1 , 'B' : 0.3, 'C' : 1},
      {'A' : 0, 'B' : 0.1, 'C' : 1},
      {'A' : 0, 'B' : 0.2, 'C' : 1},
      {'A' : 0, 'B' : 0.3, 'C' : 1},
      {'A' : 1, 'B' : 0.1, 'C' : 1},
      {'A' : 1, 'B' : 0.2, 'C' : 1},
      {'A' : 1, 'B' : 0.3, 'C' : 1},
      {'A' : 2, 'B' : 0.1, 'C' : 1},
      {'A' : 2, 'B' : 0.2, 'C' : 1},
      {'A' : 2, 'B' : 0.3, 'C' : 1},
      {'A' : 3, 'B' : 0.1, 'C' : 1},
      {'A' : 3, 'B' : 0.2, 'C' : 1},
      {'A' : 3, 'B' : 0.3, 'C' : 1},
      {'A' : 4, 'B' : 0.1, 'C' : 1},
      {'A' : 4, 'B' : 0.2, 'C' : 1},
      {'A' : 4, 'B' : 0.3, 'C' : 1}]

    grid = list(gridgen)
    testutil.compareCollection(self,expect,grid)

  def testArgsToGridRanges(self):
    # Low, step size, steps style
    opts = ['A:0,0.1,10', 'Blah:-10,5,2']
    expect = [('A', 0.0, 0.1, 10.0),  ('Blah', -10, 5, 2)]
    actual = ppgrid._argsToGridRanges(False, opts)
    testutil.compareCollection(self, expect, actual)

  def testArgsToGridRanges_RangeStyle(self):
    # Low, step size, steps style
    opts = ['A:0,0.1,11', 'Blah:-10,0,5']
    expect = [('A', 0.0, 0.01, 11),  ('Blah', -10, 2.5, 5)]
    actual = ppgrid._argsToGridRanges(True, opts)
    testutil.compareCollection(self, expect, actual)

