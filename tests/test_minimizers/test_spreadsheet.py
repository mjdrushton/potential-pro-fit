import unittest
from .. import testutil

import os
import ConfigParser
import StringIO

from atsim import pro_fit

from _common import *

class SpreadsheetTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.minimizers.SpreadsheetMinimizer"""

  def testMinimiser(self):
    """End to end test of SpreadsheetMinimizer"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")
    config = """[Minimizer]
type : SpreadSheet
filename : %(filename)s

    """ % {'filename' : spreadfilename}

    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, False),
      ('B', 20.0, True),
      ('C', 30.0, False),
      ('D', 40.0, True)])

    minimizer = pro_fit.minimizers.SpreadsheetMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(pro_fit.minimizers.SpreadsheetMinimizer, type(minimizer))

    minimizer.stepCallback = StepCallBack()
    optimized = minimizer.minimize(MockMerit())

    testutil.compareCollection(self,
      [('A', 10.0, False),
       ('B', 2.0, True),
       ('C', 30.0, False),
       ('D', 4.0, True)],
      optimized.bestVariables.flaggedVariablePairs)

    stepcallbackexpect = [
      dict(A=10.0, B=2.0, C=30.000000, D=4.0, meritval = 1020.0),
      dict(A=10.0, B=7, C=30.000000,   D=9, meritval = 1130),
      dict(A=10.0, B=12, C=30.000000,  D=14, meritval = 1340),
      dict(A=10.0, B=17, C=30.000000,  D=19, meritval = 1650),
    ]
    testutil.compareCollection(self,stepcallbackexpect, minimizer.stepCallback.stepDicts)

  def testStartRow(self):
    """Test the start_row configuration option for the SpreadsheetMinimizer"""

    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")
    config = """[Minimizer]
type : SpreadSheet
filename : %(filename)s
start_row : 1

    """ % {'filename' : spreadfilename}

    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, False),
      ('B', 20.0, True),
      ('C', 30.0, False),
      ('D', 40.0, True)])

    minimizer = pro_fit.minimizers.SpreadsheetMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(pro_fit.minimizers.SpreadsheetMinimizer, type(minimizer))

    minimizer.stepCallback = StepCallBack()
    minimizer.minimize(MockMerit())


    stepcallbackexpect = [
      dict(A=10.0, B=7,   C=30.000000,  D=9,   meritval = 1130),
      dict(A=10.0, B=12,  C=30.000000,  D=14,  meritval = 1340),
      dict(A=10.0, B=17,  C=30.000000,  D=19,  meritval = 1650),
    ]
    testutil.compareCollection(self,stepcallbackexpect, minimizer.stepCallback.stepDicts)


  def testEndRow(self):
    """Test end_row configuration option for the SpreadsheetMinimizer"""

    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")
    config = """[Minimizer]
type : SpreadSheet
filename : %(filename)s
end_row : 2

    """ % {'filename' : spreadfilename}

    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, False),
      ('B', 20.0, True),
      ('C', 30.0, False),
      ('D', 40.0, True)])

    minimizer = pro_fit.minimizers.SpreadsheetMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(pro_fit.minimizers.SpreadsheetMinimizer, type(minimizer))

    minimizer.stepCallback = StepCallBack()
    minimizer.minimize(MockMerit())

    stepcallbackexpect = [
      dict(A=10.0, B=2.0, C=30.000000, D=4.0, meritval = 1020.0),
      dict(A=10.0, B=7, C=30.000000,   D=9, meritval = 1130),
      dict(A=10.0, B=12, C=30.000000,  D=14, meritval = 1340)
    ]
    testutil.compareCollection(self,stepcallbackexpect, minimizer.stepCallback.stepDicts)


  def testStartAndEndRow(self):
    """Test the start_row and end_row configuration options for the SpreadsheetMinimizer"""

    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")
    config = """[Minimizer]
type : SpreadSheet
filename : %(filename)s
start_row : 1
end_row : 2

    """ % {'filename' : spreadfilename}

    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, False),
      ('B', 20.0, True),
      ('C', 30.0, False),
      ('D', 40.0, True)])

    minimizer = pro_fit.minimizers.SpreadsheetMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(pro_fit.minimizers.SpreadsheetMinimizer, type(minimizer))

    minimizer.stepCallback = StepCallBack()
    minimizer.minimize(MockMerit())

    stepcallbackexpect = [
      dict(A=10.0, B=7, C=30.000000,   D=9, meritval = 1130),
      dict(A=10.0, B=12, C=30.000000,  D=14, meritval = 1340)
    ]
    testutil.compareCollection(self,stepcallbackexpect, minimizer.stepCallback.stepDicts)

  def testBatchSize(self):
    """Test SpreadsheetMinimizer 'batch_size' configuration option"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")
    config = """[Minimizer]
type : SpreadSheet
filename : %(filename)s
batch_size : 2

    """ % {'filename' : spreadfilename}

    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, False),
      ('B', 20.0, True),
      ('C', 30.0, False),
      ('D', 40.0, True)])

    minimizer = pro_fit.minimizers.SpreadsheetMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(pro_fit.minimizers.SpreadsheetMinimizer, type(minimizer))

    cvalpairs = []
    def afterMerit(meritvals, candvalpairs):
      cvalpairs.append((meritvals, candvalpairs))

    merit = MockMerit()
    merit.afterMerit = afterMerit
    optimized = minimizer.minimize(merit)

    testutil.compareCollection(self,
      [('A', 10.0, False),
       ('B', 2.0, True),
       ('C', 30.0, False),
       ('D', 4.0, True)],
      optimized.bestVariables.flaggedVariablePairs)

    expect = [
      ([1020.0, 1130.0],
       [dict(A=10.0, B=2.0, C=30.000000, D=4.0),
        dict(A=10.0, B=7, C=30.000000,   D=9)]),
      ([1340.0, 1650.0],
       [dict(A=10.0, B=12.0, C=30.000000, D=14.0),
        dict(A=10.0, B=17, C=30.000000,   D=19)])]

    actual = []
    for (meritvals, cvp) in cvalpairs:
      cvp = [ dict(v.variablePairs) for (v, j) in cvp]
      actual.append((meritvals, cvp))

    testutil.compareCollection(self, expect, actual)

  def testBatchSize_withStartRow(self):
    """Test SpreadsheetMinimizer 'batch_size' configuration option"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")
    config = """[Minimizer]
type : SpreadSheet
filename : %(filename)s
start_row : 1
batch_size : 2

    """ % {'filename' : spreadfilename}

    cfg = ConfigParser.SafeConfigParser()
    cfg.optionxform = str
    cfg.readfp(StringIO.StringIO(config))
    configitems = cfg.items('Minimizer')

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, False),
      ('B', 20.0, True),
      ('C', 30.0, False),
      ('D', 40.0, True)])

    minimizer = pro_fit.minimizers.SpreadsheetMinimizer.createFromConfig(variables, configitems)
    self.assertEquals(pro_fit.minimizers.SpreadsheetMinimizer, type(minimizer))

    cvalpairs = []
    def afterMerit(meritvals, candvalpairs):
      cvalpairs.append((meritvals, candvalpairs))

    merit = MockMerit()
    merit.afterMerit = afterMerit
    optimized = minimizer.minimize(merit)

    testutil.compareCollection(self,
      [('A', 10.0, False),
       ('B', 7.0, True),
       ('C', 30.0, False),
       ('D', 9.0, True)],
      optimized.bestVariables.flaggedVariablePairs)

    expect = [
      ([1130.0, 1340.0],
       [dict(A=10.0, B=7, C=30.000000,  D=9),
        dict(A=10.0, B=12.0, C=30.000000, D=14.0)
        ]),
      ([1650.0],
       [dict(A=10.0, B=17, C=30.000000, D=19)])]

    actual = []
    for (meritvals, cvp) in cvalpairs:
      cvp = [ dict(v.variablePairs) for (v, j) in cvp]
      actual.append((meritvals, cvp))

    testutil.compareCollection(self, expect, actual)



  def testBatchSize_withStartRow_AndRowStep(self):
      """Test SpreadsheetMinimizer 'batch_size' configuration option"""
      spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")
      config = """[Minimizer]
type : SpreadSheet
filename : %(filename)s
start_row : 1
batch_size : 2
row_step : 2

      """ % {'filename' : spreadfilename}

      cfg = ConfigParser.SafeConfigParser()
      cfg.optionxform = str
      cfg.readfp(StringIO.StringIO(config))
      configitems = cfg.items('Minimizer')

      variables = pro_fit.fittool.Variables([
        ('A', 10.0, False),
        ('B', 20.0, True),
        ('C', 30.0, False),
        ('D', 40.0, True)])

      minimizer = pro_fit.minimizers.SpreadsheetMinimizer.createFromConfig(variables, configitems)
      self.assertEquals(pro_fit.minimizers.SpreadsheetMinimizer, type(minimizer))

      cvalpairs = []
      def afterMerit(meritvals, candvalpairs):
        cvalpairs.append((meritvals, candvalpairs))

      merit = MockMerit()
      merit.afterMerit = afterMerit
      optimized = minimizer.minimize(merit)

      testutil.compareCollection(self,
        [('A', 10.0, False),
         ('B', 7.0, True),
         ('C', 30.0, False),
         ('D', 9.0, True)],
        optimized.bestVariables.flaggedVariablePairs)

      expect = [
        ([1130.0, 1650.0],
         [dict(A=10.0, B=7, C=30.000000,  D=9),
          dict(A=10.0, B=17, C=30.000000,  D=19)
          ]) ]

      actual = []
      for (meritvals, cvp) in cvalpairs:
        cvp = [ dict(v.variablePairs) for (v, j) in cvp]
        actual.append((meritvals, cvp))

      testutil.compareCollection(self, expect, actual)


class SpreadsheetRowIteratorTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit.minimizers._spreadsheet._SpreadsheetRowIterator"""

  def testAllFit(self):
    """Test when all variables are fitting variables"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")

    with open(spreadfilename) as infile:
      variables = pro_fit.fittool.Variables([
        ('A', 10.0, True),
        ('B', 20.0, True),
        ('C', 30.0, True),
        ('D', 40.0, True)])

      from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator
      rowit = _SpreadsheetRowIterator(variables, infile)
      actual = [ dict(v.variablePairs) for v in rowit]

    expect = [
      dict(A = 1.0 , B= 2.0 , C = 3  , D = 4.0),
      dict(A = 6.0 , B= 7   , C = 8  , D = 9  ),
      dict(A = 11  , B= 12  , C = 13 , D = 14 ),
      dict(A = 16  , B= 17  , C = 18 , D = 19 )]

    testutil.compareCollection(self, expect, actual)

  def testSomeFit(self):
    """Test when some variables are fitting variables"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")

    with open(spreadfilename) as infile:
      variables = pro_fit.fittool.Variables([
        ('A', 10.0, False),
        ('B', 20.0, False),
        ('C', 30.0, True),
        ('D', 40.0, True)])

      from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator
      rowit = _SpreadsheetRowIterator(variables, infile)
      actual = [ dict(v.variablePairs) for v in rowit]

    expect = [
      dict(A = 10.0, B= 20.0, C = 3  , D = 4.0),
      dict(A = 10.0, B= 20.0, C = 8  , D = 9  ),
      dict(A = 10.0, B= 20.0, C = 13 , D = 14 ),
      dict(A = 10.0, B= 20.0, C = 18 , D = 19 )]

    testutil.compareCollection(self, expect, actual)

  def testMissingColumn(self):
    """Test that iterator throws when spreadsheet does not contain required column"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")

    variables = pro_fit.fittool.Variables([
        ('A', 10.0, True),
        ('B', 20.0, False),
        ('C', 30.0, True),
        ('D', 40.0, True),
        ('Missing', 1.0, True)])

    with open(spreadfilename) as infile:
      from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator, _MissingColumnException
      rowit = _SpreadsheetRowIterator(variables, infile)

      with self.assertRaises(_MissingColumnException):
        for row in rowit:
          pass

    with open(spreadfilename) as infile:
      rowit = _SpreadsheetRowIterator(variables, infile)
      try:
        for row in rowit:
          pass
      except _MissingColumnException as e:
        self.assertEqual('Missing', e.columnKey)

  def testBadvalue(self):
    """Test that iterator throws when spreadsheet contains value that cannot be converted to a float"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")

    variables = pro_fit.fittool.Variables([
        ('Label', 10.0, True),
        ('A', 10.0, True),
        ('B', 20.0, False),
        ('C', 30.0, True),
        ('D', 40.0, True)])

    with open(spreadfilename) as infile:
      from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator, _BadValueException
      rowit = _SpreadsheetRowIterator(variables, infile)

      with self.assertRaises(_BadValueException):
        for row in rowit:
          pass

    with open(spreadfilename) as infile:
      rowit = _SpreadsheetRowIterator(variables, infile)
      try:
        for row in rowit:
          pass
      except _BadValueException as e:
        self.assertEqual('Label', e.columnKey)
        self.assertEqual(2, e.lineno)
        self.assertEqual("Iteration 1", e.value)

  def testOutOfBounds(self):
    """Test that iterator throws when a spreadsheet value is out of bounds for the variable it represents"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")
    variables = pro_fit.fittool.Variables([('A', 10.0, True)], [(float("-inf"), 15.0)])

    with open(spreadfilename) as infile:
      from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator, _OutOfBoundsException
      rowit = _SpreadsheetRowIterator(variables, infile)

      with open(spreadfilename) as infile:
        rowit = _SpreadsheetRowIterator(variables, infile)
        try:
          for row in rowit:
            pass
          self.fail("Test should raise _OutOfBoundsException")
        except _OutOfBoundsException as e:
          self.assertEqual('A', e.columnKey)
          self.assertEqual(5, e.lineno)
          self.assertEqual(16.0, e.value)


  def testStartAndEndRow(self):
    """Test startRow and endRow constructor arguments"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, True),
      ('B', 20.0, True),
      ('C', 30.0, True),
      ('D', 40.0, True)])

    from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator, _RowRangeException
    with open(spreadfilename) as infile:
      rowit = _SpreadsheetRowIterator(variables, infile, startRow = 0, endRow = 0)
      actual = [ dict(v.variablePairs) for v in rowit]
      expect = [
        dict(A = 1.0 , B= 2.0 , C = 3  , D = 4.0),
        # dict(A = 6.0 , B= 7   , C = 8  , D = 9  ),
        # dict(A = 11  , B= 12  , C = 13 , D = 14 ),
        # dict(A = 16  , B= 17  , C = 18 , D = 19 )
        ]
      testutil.compareCollection(self, expect, actual)

    with open(spreadfilename) as infile:
      rowit = _SpreadsheetRowIterator(variables, infile, startRow = 3, endRow = 3)
      actual = [ dict(v.variablePairs) for v in rowit]
      expect = [
        # dict(A = 1.0 , B= 2.0 , C = 3  , D = 4.0),
        # dict(A = 6.0 , B= 7   , C = 8  , D = 9  ),
        # dict(A = 11  , B= 12  , C = 13 , D = 14 ),
        dict(A = 16  , B= 17  , C = 18 , D = 19 )]
      testutil.compareCollection(self, expect, actual)

    with open(spreadfilename) as infile:
      rowit = _SpreadsheetRowIterator(variables, infile, startRow = 1, endRow = 2)
      actual = [ dict(v.variablePairs) for v in rowit]
      expect = [
        # dict(A = 1.0 , B= 2.0 , C = 3  , D = 4.0),
        dict(A = 6.0 , B= 7   , C = 8  , D = 9  ),
        dict(A = 11  , B= 12  , C = 13 , D = 14 ),
        # dict(A = 16  , B= 17  , C = 18 , D = 19 )
        ]

      testutil.compareCollection(self, expect, actual)


  def testBadStartRow(self):
    """Test when an invalid startRow is specified"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, True),
      ('B', 20.0, True),
      ('C', 30.0, True),
      ('D', 40.0, True)])

    from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator, _RowRangeException
    with open(spreadfilename) as infile:
      rowit = _SpreadsheetRowIterator(variables, infile, startRow = 6)

      with self.assertRaises(_RowRangeException):
        for row in rowit:
          pass

  def testBadEndRow(self):
    """Test when an invalid endRow is specified"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, True),
      ('B', 20.0, True),
      ('C', 30.0, True),
      ('D', 40.0, True)])

    from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator, _RowRangeException
    with open(spreadfilename) as infile:
      rowit = _SpreadsheetRowIterator(variables, infile, endRow = 6)

      with self.assertRaises(_RowRangeException):
        for row in rowit:
          pass

  def testBlankSpreadSheet(self):
    """Test that empty spreadsheets raise appropriate exceptions."""

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, True),
      ('B', 20.0, True),
      ('C', 30.0, True),
      ('D', 40.0, True)])

    from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator, _RowRangeException

    import StringIO

    sio = StringIO.StringIO()
    print >>sio, "A,B,C,D"
    sio.seek(0)
    rowit = _SpreadsheetRowIterator(variables, sio)
    with self.assertRaises(_RowRangeException):
      for row in rowit:
        pass

  def testRowIncrement(self):
    """Test _SpreadsheetRowIterator rowIncrement constructor variable"""
    spreadfilename = os.path.join(getResourceDir(), "spreadsheet_minimizer", "spreadsheet.csv")

    variables = pro_fit.fittool.Variables([
      ('A', 10.0, True),
      ('B', 20.0, True),
      ('C', 30.0, True),
      ('D', 40.0, True)])

    # rowIncrement = 1
    with open(spreadfilename) as infile:
      from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator
      rowit = _SpreadsheetRowIterator(variables, infile, startRow = 1, endRow = 3, rowIncrement = 1)
      actual = [ dict(v.variablePairs) for v in rowit]

    expect = [
      dict(A = 6.0 , B= 7   , C = 8  , D = 9  ),
      dict(A = 11  , B= 12  , C = 13 , D = 14 ),
      dict(A = 16  , B= 17  , C = 18 , D = 19 )]

    testutil.compareCollection(self, expect, actual)

    # rowIncrement = 2
    with open(spreadfilename) as infile:
      from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator
      rowit = _SpreadsheetRowIterator(variables, infile, startRow = 1, endRow = 3, rowIncrement = 2)
      actual = [ dict(v.variablePairs) for v in rowit]

    expect = [
      dict(A = 6.0 , B= 7   , C = 8  , D = 9  ),
      dict(A = 16  , B= 17  , C = 18 , D = 19 )]

    testutil.compareCollection(self, expect, actual)

    # rowIncrement = 3
    with open(spreadfilename) as infile:
      from atsim.pro_fit.minimizers._spreadsheet import _SpreadsheetRowIterator
      rowit = _SpreadsheetRowIterator(variables, infile, rowIncrement = 3)
      actual = [ dict(v.variablePairs) for v in rowit]

    expect = [
      dict(A = 1.0 , B= 2.0 , C = 3  , D = 4.0),
      dict(A = 16  , B= 17  , C = 18 , D = 19 )]

    testutil.compareCollection(self, expect, actual)
