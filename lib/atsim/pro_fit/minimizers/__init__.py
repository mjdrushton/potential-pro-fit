"""Contains minimizers and related classes.

The basic interface for a Minimizer is as follows:

  class Minimizer:

    def minimize(self, merit):
      Invoked to perform minimisation of merit values calculated by
      Merit object (arg: merit).

      @param merit atsim.pro_fit.fittool.Merit instance
      @return MinimizerResults instance giving minimized values.
      ...

    @staticmethod
    def createFromConfig(variables, configitems):
      Allows creation of Minimizer from data [Minimizer] section of
      fit.cfg file.

      @param variables atsim.pro_fit.fittool.Variables instance
                       representing starting values for fitting.
      @param configitems list of key value pairs extracted from [Minimizer]
                       section of fit.cfg
      @return Minimizer instance
      ...

In addition classes implementing the Minimizer interface should support
a property named 'stepCallback', this is called after each minimisation iteration.
This supports logging and progress monitoring. The callback is callable with the
function prototype:

  def stepCallback(minimizerResults):
    @param minimizerResults Instance of atsim.pro_fit.minimizers.MinimizerResults
                  By convention the minimizerResults should contain the best results
                  from the last minimiztion iteration (this is because several minimizers
                  perform several sub-steps before finalizing variable updates and completing
                  a single iteration).
      ...
"""

# flake8: noqa
from _common import *
from _mystic import NelderMeadMinimizer
from _single import SingleStepMinimizer
from _inspyred import DEAMinimizer, Simulated_AnnealingMinimizer, Particle_SwarmMinimizer
from _spreadsheet import SpreadsheetMinimizer
