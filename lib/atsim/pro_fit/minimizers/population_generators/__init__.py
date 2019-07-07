from atsim.pro_fit.variables import BoundedVariableBaseClass, VariableException

from ._exceptions import (
    Missing_Distribution_Exception,
    Candidate_Length_Exception,
)

from ._candidate_generator import Candidate_Generator

from ._variable_distributions import (
    Variable_Distribution,
    Variable_Distributions,
    Uniform_Variable_Distribution,
)

from ._latin_hypercube_initial_population import (
    Latin_Hypercube_InitialPopulation,
)

from ._uniform_random_initial_population import (
    Uniform_Random_Initial_Population,
)

import numpy as np


class Null_Initial_Population(object):
    """Returns zero sized population"""

    def __init__(self):
        self.population_size = 0

    def generate_candidates(self):
        return np.array([])


class Predefined_Initial_Population(object):
    """Allow populations to be defined using a list of dictionaries or from
    arrays."""

    def __init__(self, initialVariables, from_dict=None, from_array=None):
        """Generate a predefined population from nested lists (from_array) or 
        using dictionaries.

        The format used by `from_dict` is a list of dictionaries mapping 
        variable label to variable value. These dictionaries must include keys
        for all the fitting variables (they can include more than these but
        only fit keys will be used). Example:
        
        ```
            [
                {'a' :  1.0}, 'c' : 2.0},
                {'a' :  4.0}, 'c' : 5.0}
            ]
        ```

        The `from_array` argument accepts a 2D table. Each row
        represents a candidate. Value must be specified in the order
        returned by the `fitKeys` property of the `initialVariables` argument.

        e.g.

        ```
        [
            [1.0, 2.0],
            [4.0, 5.0]

        ]
        ```

        The validity of variable arguments are checked against the bounds
        stored in `initialVariables`.

        Arguments:
            initialVariables {atsim.pro_fit.variables.Variables} -- Variables defining
                fitting keys and variable bounds.
        
        Keyword Arguments:
            from_dict {list} -- See above for format. (default: {None})
            from_array {list} -- See above for format. (default: {None})
        """

        if from_dict is None and from_array is None:
            raise ValueError(
                "No data specified using either from_dict or from_array arguments"
            )

        if from_dict is not None and from_array is not None:
            raise ValueError(
                "only one of from_dict or from_array can be specified. Not both."
            )

        self.initialVariables = initialVariables

        if from_dict is not None:
            data = self._dict_to_list(from_dict)
        else:
            data = from_array

        self.candidates = self._check_data(data)
        self.population_size = self.candidates.shape[0]

    def _dict_to_list(self, dicts):
        data = []
        fit_keys = self.initialVariables.fitKeys

        for row in dicts:
            data_row = []
            for fk in fit_keys:
                if fk not in row:
                    raise VariableException(
                        "Fitting variable '{}' not found in row when creating population: {}".format(
                            fk, row
                        )
                    )
                data_row.append(row[fk])
            data.append(data_row)
        return data

    def _check_data(self, data):
        try:
            data = np.array(data, dtype=np.double)
        except ValueError as e:
            raise VariableException(str(e))

        fit_keys = self.initialVariables.fitKeys

        if data.ndim != 2:
            raise VariableException("Data should have two dimensions")

        if len(fit_keys) != data.shape[1]:
            raise VariableException(
                "Data does not contain the same number of columns as there are fitting variables when creating population. {} != {}".format(
                    data.shape[1], len(fit_keys)
                )
            )

        if not np.alltrue(np.isfinite(data)):
            raise VariableException(
                "Some values were not finite when creating population."
            )

        # Now check bounds.
        for i, (fk, col) in enumerate(zip(fit_keys, data.T)):
            for j, v in enumerate(col):
                if not self.initialVariables.inBounds(fk, v):
                    low_bound, upper_bound = self.initialVariables.fitBounds[i]
                    msg = "Value for fitting variable '{}' was out of bounds ({},{}) in row {}: {}".format(
                        fk, low_bound, upper_bound, j, v
                    )
                    raise VariableException(msg)
        return data

    def generate_candidates(self):
        """Return numpy array containing list of candidates (1 candidate per row)"""
        return self.candidates
