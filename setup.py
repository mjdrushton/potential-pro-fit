
from setuptools import setup, find_packages
from distutils.command.build_py import build_py

import sys
import os
import re
import tempfile

from ppversion import readversion


def preprocess(infilename):
  # execnet requires pure modules to create channels (or inline source). This makes code re-use difficult.
  # To allow some form of code re-use, some of the execnet remote_exec modules are generated at build time.
  # In C pre-procssor style, the required functions are #INCLUDed into the generated modules.
  # This is performed by this function which is called during the setup.py build stage by the custom
  # build_py subclass below.
  include_regex = re.compile(r'^#\s?INCLUDE "(.*)"')
  oldir = os.getcwd()
  outfile, outfilename = tempfile.mkstemp(suffix=".py")
  outfile = os.fdopen(outfile,'w')
  try:
    path = os.path.dirname(infilename)
    bname = os.path.basename(infilename)
    os.chdir(path)
    with open(bname, 'r', encoding = 'utf-8') as infile:
      for inline in infile:
        m = include_regex.match(inline)
        if m:
          includefilename = m.groups()[0]
          with open(includefilename, 'r', encoding = 'utf-8') as includefile:
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


def package_files(directory):
  cwd = os.getcwd()
  os.chdir(directory)
  paths = []
  dirname = os.path.basename(directory)
  for (path, _directories, filenames) in os.walk('.'):
    for filename in filenames:
      p = os.path.join(dirname, path, filename)
      p = os.path.normpath(p)
      paths.append(p)
  os.chdir(cwd)
  return paths

# pkgs = find_packages('lib', exclude=["tests"])
pkgs = ['atsim.pro_fit',
        'atsim.pro_fit.tools',
        'atsim.pro_fit.minimizers',
        'atsim.pro_fit.resources',
        'atsim.pro_fit.webmonitor',
        'atsim.pro_fit.evaluators',
        'atsim.pro_fit.filetransfer',
        'atsim.pro_fit.db',
        'atsim.pro_fit.runners',
        'atsim.pro_fit.console',
        'atsim.pro_fit.minimizers._inspyred',
        'atsim.pro_fit.filetransfer.remote_exec',
        'atsim.pro_fit.runners.templates']

setup(name="potential-pro-fit",
  version = readversion(),
  package_dir = {'' : 'lib/'},
  packages = pkgs,
  cmdclass = {'build_py' : my_build},
  install_requires = ["setuptools",
                      'sqlalchemy',
                      'more-itertools>6.0.0',
                      'cherrypy>18.0.0',
                      'Jinja2>=2.10',
                      'inspyred>=1.0.1',
                      'cexprtk>=0.3.0',
                      'urwid>=2.0.1',
                      'mystic>=0.3.3',
                      'execnet>=1.6',
                      'gevent>=1.3',
                      'tabulate>=0.8.3'],
  tests_require = ['pytest',
                  'wsgi_intercept',
                  'mechanize',
                  'assertpy'],

  #dependency_links = ['https://github.com/uqfoundation/mystic/zipball/master#egg=mystic-0.2a2.dev0'],

  include_package_data = True,
  package_data = {
    'atsim.pro_fit.webmonitor' : package_files('lib/atsim/pro_fit/webmonitor/webresources'),
    'atsim.pro_fit' : package_files('lib/atsim/pro_fit/resources'),
    # 'atsim.pro_fit' : package_files('lib/atsim/pro_fit/resources'),
    'atsim.pro_fit.runners' : package_files('lib/atsim/pro_fit/runners/templates')
  },

  entry_points = {
    'console_scripts' : [
      'pprofit = atsim.pro_fit.tools.pprofit:main',
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
