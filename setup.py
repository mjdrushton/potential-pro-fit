
from setuptools import setup, find_packages

setup(name="potential-pro-fit",
  # use_hg_version=True,
  package_dir = {'' : 'lib'},
  packages = find_packages('lib', exclude=["tests"]),
  namespace_packages = ["atsim"],
  test_suite = "tests",
  # setup_requires = ["hgtools"],
  install_requires = ["setuptools",
                      'sqlalchemy',
                      'cherrypy',
                      'Jinja2',
                      'inspyred',
                      'mystic',
                      'cexprtk'],

  #zip_safe = True,

  # Meta-data for PyPI
  # description = "atsim.potentials provides tools for working with pair and embedded atom method potential models including tabulation routines for DL_POLY and LAMMPS",
  # long_description = open('README.rst').read(),
  author = "M.J.D. Rushton",
  author_email = "m.rushton@imperial.ac.uk",
  license = "Apache License (2.0)",
  url = "https://bitbucket.org/mjdr/potential-pro-fit")
