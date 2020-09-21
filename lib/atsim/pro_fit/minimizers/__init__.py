"""Contains minimizers and related classes.

The basic interface for a Minimizer is described in: atsim.pro_fit.minimizers.base_minimizers.Minimizer_Abstract_Base

"""

from . import base_minimizers
from ._common import MinimizerResults
from ._inspyred import (DEAMinimizer, Particle_SwarmMinimizer,
                        Simulated_AnnealingMinimizer)
from ._mystic import NelderMeadMinimizer
from ._single import SingleStepMinimizer
from ._spreadsheet import SpreadsheetMinimizer
