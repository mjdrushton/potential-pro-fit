
from setuptools import setup, find_packages

from setuptools.command.test import test as TestCommand
import sys

class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ["tests"]

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)



setup(name="potential-pro-fit",
  package_dir = {'' : 'lib'},
  packages = find_packages('lib', exclude=["tests"]),
  namespace_packages = ["atsim"],
  cmdclass = {'test' : PyTest},
  install_requires = ["setuptools",
                      'sqlalchemy',
                      'cherrypy',
                      'Jinja2',
                      'inspyred',
                      'cexprtk',
                      'mystic==0.2a2.dev0',
                      'execnet'],
  tests_require = ['pytest',
                  'wsgi_intercept',
                   'mechanize',
                   'assertpy'],

  dependency_links = ['https://github.com/uqfoundation/mystic/zipball/master#egg=mystic-0.2a2.dev0'],

  include_package_data = True,

  #zip_safe = True,

  entry_points = {
    'console_scripts' : [
      'pprofit = atsim.pro_fit.tools.fittingTool:main',
      'pprofitmon = atsim.pro_fit.webmonitor:main',
      'csvbuild = atsim.pro_fit.tools.csvbuild:main',
      'ppmakegrid = atsim.pro_fit.tools.ppmakegrid:main',
      'ppdump = atsim.pro_fit.tools.ppdump:main'
    ]
  },

  # Meta-data for PyPI
  # description = "atsim.potentials provides tools for working with pair and embedded atom method potential models including tabulation routines for DL_POLY and LAMMPS",
  # long_description = open('README.rst').read(),
  author = "M.J.D. Rushton",
  author_email = "m.rushton@imperial.ac.uk",
  license = "Apache License (2.0)",
  url = "https://bitbucket.org/mjdr/potential-pro-fit")
