import unittest
import tempfile
import os
import glob
import shutil

from atsim.pro_fit.tools import csvbuild


def _getResourceDirectory():
    """Returns path to resources used by this test module (currently assumed to be sub-directory
    of test module called resources)"""
    return os.path.join(os.path.dirname(__file__), 'resources')

class CSVBuildTestCase(unittest.TestCase):
  """TestCase for csvbuild tool"""

  def setUp(self):
    self.tempdir = tempfile.mkdtemp()
    self.oldDir = os.getcwd()
    os.chdir(self.tempdir)

  def tearDown(self):
    os.chdir(self.oldDir)
    shutil.rmtree(self.tempdir, ignore_errors = True)

  def testTemplateSubstitution(self):
    """Test substitution of placeholder with column values"""
    s = "@blah@_@blah@ Moo bar. Ding dong @clang@"
    d = { "blah" : "Moop",
          "clang" : "blibble" }
    expect = "Moop_Moop Moo bar. Ding dong blibble"
    actual = csvbuild._templateSubstitution(s, d, None)
    self.assertEquals(expect, actual)

    # Test escaping of @
    s = r'@blah@ \@blah\@'
    d = { 'blah' : "Moop" }
    expect = "Moop @blah@"
    actual = csvbuild._templateSubstitution(s,d, None)
    self.assertEquals(expect, actual)

    # Test for no placeholders
    s = 'blah blah'
    actual = csvbuild._templateSubstitution(s,{'blah' : 'Moop'}, None)
    self.assertEquals(s, actual)

  def testTemplateSubstitutionFloatingPointNumbers(self):
    """Test template substitution when floating point formatting options are specified"""
    s = "@label@_@fnum:.3f@ is a filename. @fnum:04.3f@."

    d = {'label' : "Label",
         'fnum' : 1.2345}

    expect = "{label}_{fnum:.3f} is a filename. {fnum:04.3f}.".format(**d)
    actual = csvbuild._templateSubstitution(s,d, None)
    self.assertEquals(expect, actual)

    # Now do the same test for when fnum is passed in as a string
    d = {'label' : "Label",
         'fnum' : "1.2345"}
    actual = csvbuild._templateSubstitution(s,d, None)
    self.assertEquals(expect, actual)

    # Now do the same test for when fnum is passed in as an int
    d = {'label' : "Label",
         'fnum' : int(1),
         'enum' : "1.2e-5"}
    expect = "{label}_{fnum:.3f} is a filename. {fnum:04.3f}. {enum:.6f}".format(fnum = 1, enum = 1.2e-5, label = "Label")
    s = s+" @enum:.6f@"
    actual = csvbuild._templateSubstitution(s,d, None)
    self.assertEquals(expect, actual)
    
    # Now test with a string that can't be converted to a float
    
    with self.assertRaises(ValueError):
      csvbuild._templateSubstitution("@bad_float:.f@", {'bad_float': 'bad'}, None)

  def testTemplateSubstitutionDecimalNumbers(self):
    """Test template substitution when floating point formatting options are specified"""
    s = "@label@_@dnum:06d@ is a filename."

    d = {'label' : "Label",
         'dnum' : int(12)}

    expect = "{label}_{dnum:06d} is a filename.".format(**d)
    actual = csvbuild._templateSubstitution(s,d, None)
    self.assertEquals(expect, actual)

    # Now do the same test for when fnum is passed in as a string
    d = {'label' : "Label",
         'dnum' : "12"}
    actual = csvbuild._templateSubstitution(s,d, None)
    self.assertEquals(expect, actual)
    
    with self.assertRaises(ValueError):
      csvbuild._templateSubstitution("@bad_num:.d@", {'bad_num': 'bad'}, None)

  def testTemplateSubstitutionAllNumberTypes(self):

    s = "@fnum:e@ @fnum:E@ @fnum:f@ @fnum:F@ @fnum:g@ @fnum:G@ @fnum:n@ @fnum:%@"
    sf = "{fnum:e} {fnum:E} {fnum:f} {fnum:F} {fnum:g} {fnum:G} {fnum:n} {fnum:%}"

    s1 = "@dnum:b@ @dnum:c@ @dnum:d@ @dnum:o@ @dnum:x@ @dnum:X@"
    s1f = "{dnum:b} {dnum:c} {dnum:d} {dnum:o} {dnum:x} {dnum:X}"

    d = {"fnum" : "1.234",
         "dnum" : "12"}
    
    df = {"fnum": 1.234,
          "dnum" : 12}

    self.assertEquals(sf.format(**df),
      csvbuild._templateSubstitution(s, df, None))

    self.assertEquals(sf.format(**df),
      csvbuild._templateSubstitution(s, d, None))

    self.assertEquals(s1f.format(**df),
      csvbuild._templateSubstitution(s1, df, None))

    self.assertEquals(s1f.format(**df),
      csvbuild._templateSubstitution(s1, d, None))


  def testSimpleFiles(self):
    """Test copy to flat file system"""

    rows = [{ 'species' : 'Gd', 'run' : 1},
            {'species' : 'Y', 'run' : 2}]

    os.mkdir('dest')
    os.mkdir('skel')

    j = os.path.join

    open(j('skel', '@species@_@run@.gp3.in'), 'w').close()

    csvbuild.buildDirs(rows, 'skel', 'dest')

    expect = sorted([ j('dest', 'Gd_1.gp3'), j('dest', 'Y_2.gp3') ])
    actual = glob.glob(j('dest', '*'))
    actual.sort()

    self.assertEquals(expect, actual)


  def testInclude(self):
    """Test @INCLUDE:token@ placeholder"""
    os.mkdir('skel')
    os.mkdir('dest')

    with open('include_me', 'wb') as outfile:
      print >>outfile, "Hello"
      print >>outfile, "Goodbye"

    with open(os.path.join('skel', 'boom.in'), 'wb') as outfile:
      print >>outfile, "@INCLUDE:rel_filename@"
      print >>outfile, "@INCLUDE:abs_filename@"

    d = [{'rel_filename' : os.path.join(os.path.pardir, 'include_me'),
         'abs_filename' : os.path.abspath(os.path.join(self.tempdir, 'include_me'))}]

    csvbuild.buildDirs(d, 'skel', 'dest')

    expect = """Hello
Goodbye

Hello
Goodbye

"""

    actual = open(os.path.join('dest', 'boom'), 'rb').read()
    self.assertEquals(expect, actual)


  def testFileHierarchy(self):
    """Test creation of directory hierarchy"""
    import tarfile
    tf = tarfile.TarFile(os.path.join(_getResourceDirectory(), 'csvbuild_skel.tar'))
    tf.extractall()

    os.mkdir('dest')

    self.assertTrue(os.path.isdir('skel'))

    rows = [ dict(run=1),
             dict(run=2),
             dict(run=3) ]

    expect = [
        ('dest', '1'),
        ('dest', 'DL_POLY_1'),
        ('dest', 'DL_POLY_1', 'CONFIG'),
        ('dest', 'DL_POLY_1', 'output'),
        ('dest', 'DL_POLY_1', 'support_1'),
        ('dest', 'DL_POLY_1', 'support_1', 'file1_1'),
        ('dest', 'DL_POLY_1', 'support_1', 'file2'),

        ('dest', '2'),
        ('dest', 'DL_POLY_2'),
        ('dest', 'DL_POLY_2', 'CONFIG'),
        ('dest', 'DL_POLY_2', 'output'),
        ('dest', 'DL_POLY_2', 'support_2'),
        ('dest', 'DL_POLY_2', 'support_2', 'file1_2'),
        ('dest', 'DL_POLY_2', 'support_2', 'file2'),

        ('dest', '3'),
        ('dest', 'DL_POLY_3'),
        ('dest', 'DL_POLY_3', 'CONFIG'),
        ('dest', 'DL_POLY_3', 'output'),
        ('dest', 'DL_POLY_3', 'support_3'),
        ('dest', 'DL_POLY_3', 'support_3', 'file1_3'),
        ('dest', 'DL_POLY_3', 'support_3', 'file2')]

    expect = [ os.path.join(*p) for p in expect]
    expect.sort()

    csvbuild.buildDirs(rows, 'skel', 'dest')
    actual = []

    def visit(arg, dirname, names):
      for n in names:
        ln = os.path.join(dirname, n)
        actual.append(ln)
        if os.path.isdir(n):
          os.path.walk(n, visit, None)

    os.path.walk('dest', visit, None)
    actual.sort()
    self.assertEquals(expect, actual)

    self.assertEquals('1', open(os.path.join('dest', 'DL_POLY_1', 'support_1', 'file2'), 'rb').readline()[:-1])
