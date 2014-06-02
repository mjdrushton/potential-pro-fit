from _common import *

import logging

class SpreadsheetMinimizer(object):
  """ """

  _logger = logging.getLogger('atsim.pro_fit.minimizers.SpreadsheetMinimizer')

  def __init__(self, variables):
    """Create SingleStepMinimizer.

    @param variables Variables instance giving run values."""
    pass

  def minimize(self, merit):
    """Perform minimization.

    @param merit atsim.pro_fit.fittool.Merit instance.
    @return MinimizerResults containing values obtained after merit function evaluation"""
    pass

  @staticmethod
  def createFromConfig(variables, configitems):
    pass
