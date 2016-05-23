
from setuptools import setup, find_packages
from distutils.command.build_py import build_py

import sys
import os
import re
import tempfile


def preprocess(infilename):
  # execnet requires pure modules to create channels (or inline source). This makes code re-use difficult.
  # To allow some form of code re-use, some of the execnet remote_exec modules are generated at build time.
  # In C pre-procssor style, the required functions are #INCLUDed into the generated modules.
  # This is performed by this function which is called during the setup.py build stage by the custom
  # build_py subclass below.

  include_regex = re.compile(r'^#INCLUDE "(.*)"')
  oldir = os.getcwd()
  outfile, outfilename = tempfile.mkstemp(suffix=".py")
  outfile = os.fdopen(outfile,'w')
  try:
    path = os.path.dirname(infilename)
    bname = os.path.basename(infilename)
    os.chdir(path)
    with open(bname, 'rb') as infile:
      for inline in infile:
        m = include_regex.match(inline)
        if m:
          includefilename = m.groups()[0]
          with open(includefilename, 'rb') as includefile:
            outfile.write(includefile.read())
        else:
          outfile.write(inline)
    return outfilename
  finally:
    outfile.close()
    os.chdir(oldir)

preprocess_files = ["lib/atsim/pro_fit/filetransfer/remote_exec/file_cleanup_remote_exec.py",
                    "lib/atsim/pro_fit/filetransfer/remote_exec/file_transfer_remote_exec.py"]

class my_build(build_py):

    def build_module(self, module, module_file, package):
      if module_file in preprocess_files:
        processedfile = preprocess(module_file)
        try:
          return build_py.build_module(self, module, processedfile, package)
        finally:
          os.remove(processedfile)
      return build_py.build_module(self, module, module_file, package)


setup(name="potential-pro-fit",
  package_dir = {'' : 'lib'},
  packages = find_packages('lib', exclude=["tests"]),
  namespace_packages = ["atsim"],
  cmdclass = {'build_py' : my_build},
  install_requires = ["setuptools",
                      'sqlalchemy',
                      'cherrypy',
                      'Jinja2',
                      'inspyred',
                      'cexprtk',
                      'mystic==0.2a1',
                      'execnet'],
  tests_require = ['pytest',
                  'wsgi_intercept',
                   'mechanize',
                   'assertpy'],

  dependency_links = ['https://github.com/uqfoundation/mystic/zipball/master#egg=mystic-0.2a2.dev0'],

  include_package_data = True,

  entry_points = {
    'console_scripts' : [
      'pprofit = atsim.pro_fit.tools.fittingTool:main',
      'pprofitmon = atsim.pro_fit.webmonitor:main',
      'csvbuild = atsim.pro_fit.tools.csvbuild:main',
      'ppgrid = atsim.pro_fit.tools.ppgrid:main',
      'ppdump = atsim.pro_fit.tools.ppdump:main'
    ]
  },

  # Meta-data for PyPI
  author = "M.J.D. Rushton",
  author_email = "m.rushton@imperial.ac.uk",
  license = "Apache License (2.0)",
  url = "https://bitbucket.org/mjdr/potential-pro-fit")
