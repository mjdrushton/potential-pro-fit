import numpy as np

from ._exceptions import Candidate_Length_Exception


class Candidate_Generator(object):
    """Used to generate lists of candidates from a Population_Design"""

    def __init__(self, variable_distributions):
        self.variable_distributions = variable_distributions

    def _check_shape(self, m):
        if not m.ndim in [1, 2]:
            raise Candidate_Length_Exception(
                "Number of dimensions must be 1 or 2. Found: {}".format(m.ndim)
            )

        expect_n = len(self.variable_distributions.initial_variables.fitKeys)
        if m.ndim == 1:
            actual_n = m.shape[0]
        else:
            actual_n = m.shape[1]

        if actual_n != expect_n:
            msg = "Candidate lenth wrong: {} does not equal the number of fit variables {}".format(
                actual_n, expect_n
            )
            raise Candidate_Length_Exception(msg)

    def generate_candidates(self, norm_values):
        """This method takes candidates, expressed as lists of numbers between
        0 and 1 and passes them through each variable's statistical
        distribution (managed by the VariableDistributions class) to to 
        produce a candidate in variable space.

        `norm_values` can be a 1D list or numpy array with length 
        equal to the number of fitting values.

        *or*

        `norm_values` can be 2D list or numpy.array where:
            * each row is a candidate solution
                - each candidate solution's length == number of fit variables
            * the number of rows is the number of candidates in the population
            * each column represents a fitting variable.

        Arguments:
            norm_values {list or numpy.array} -- See description above.

        Returns:

            {numpy.array} -- Array of same shape as `norm_values` containing transformed values.
        """

        try:
            norm_values = np.array(norm_values, dtype=np.double)
        except ValueError as e:
            raise Candidate_Length_Exception(str(e))

        self._check_shape(norm_values)
        if norm_values.ndim == 1:
            orig_shape = norm_values.shape
            norm_values = norm_values.reshape(1, norm_values.shape[0])
            out_values = self.variable_distributions.apply(
                norm_values
            ).reshape(orig_shape)
            return out_values

        out_values = self.variable_distributions.apply(norm_values)
        return out_values
