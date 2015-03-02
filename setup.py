
from setuptools import setup, find_packages

setup(name="potential-pro-fit",
  # use_hg_version=True,
  package_dir = {'' : 'lib'},
  packages = find_packages('lib', exclude=["tests"]),
  namespace_packages = ["atsim"],
  test_suite = "nose.collector",
  # setup_requires = ["hgtools"],
  install_requires = ["setuptools",
                      'sqlalchemy',
                      'cherrypy',
                      'Jinja2',
                      'inspyred',
                      'mystic',
                      'cexprtk >= 0.2.0',
                      'execnet'],
  tests_require = ['nose',
                   'wsgi_intercept',
                   'mechanize'],

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
