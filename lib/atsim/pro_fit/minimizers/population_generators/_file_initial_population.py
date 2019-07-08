from ._predefined_initial_population import Predefined_Initial_Population

from atsim.pro_fit.variables import VariableException

import csv


class File_Initial_Population(Predefined_Initial_Population):
    """Allows a population to be created from a csv file"""

    def __init__(self, initialVariables, infile, max_population_size=None):
        """Create population from CSV data contained in infile.

        Column titles are used as variable names.
        
        Arguments:
            initialVariables {atsim.pro_fit.variables.Variables} -- Initial variables
            infile {file} -- File containing data
        
        Keyword Arguments:
            max_population_size {int} -- If specified limit number of rows read from infile to this number (default: {None})
        """
        row_it = self._row_iterator(infile)
        super().__init__(
            initialVariables, row_it, max_population_size=max_population_size
        )

    def _row_iterator(self, infile):
        dr = csv.DictReader(infile)
        for row in dr:
            yield row


class Ppdump_File_Initial_Population(File_Initial_Population):
    """Reads variable data from a ppdump created file. This is equivalent to File_Initial_Population
    however only columns starting in "VARIABLE:" are used in this case."""

    def _row_iterator(self, infile):
        for row in super()._row_iterator(infile):
            items = [
                (k.replace("variable:", "", 1), v)
                for (k, v) in row.items()
                if k.startswith("variable:")
            ]

            if len(items) == 0:
                raise VariableException(
                    "No variables (columns starting in 'variable:' found in data: {}".format(
                        row
                    )
                )

            yield dict(items)
