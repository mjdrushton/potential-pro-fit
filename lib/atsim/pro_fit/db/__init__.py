"""Module for querying the fitting_run.db"""

from _metadata import getMetadata
from _fitting import Fitting
from _tabulated import IterationSeriesTable, BadFilterCombinationException
from _tableserialize import serializeTableForR
