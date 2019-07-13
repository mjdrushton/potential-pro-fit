from atsim.pro_fit.variables import VariableException

from ._exceptions import Missing_Distribution_Exception

import math
from collections import OrderedDict
import operator

import numpy as np
import scipy.stats


class Variable_Distribution(object):
    """Encapsulates a distribution function that maps the range 0...1 
    into the domain of a particular fitting variable"""

    def __init__(self, variable_label, distn_function):
        """Create object for mapping 0 to 1 values into
        variable domain.
        
        Arguments:
            variable_label {str} -- Fitting variable label
            distn_function {callable} -- Single argument callable that performs conversion.
        """

        self.variable_label = variable_label
        self._distn_function = distn_function

    def apply(self, norm_array):
        """Apply the distribution function to values in norm_array.
        
        Arguments:
            norm_array {numpy.array} -- 0...1 values
        """
        out_array = self._distn_function(norm_array)
        return out_array


class _Bounded_Variable_Distribution(Variable_Distribution):
    """Base class that ensures variable has bounds.

    Implementing classes should override:

        _init_distn_function()

    """

    def __init__(self, variable_label, initial_variables):
        """Create norm to variable transformation for a given variable.

        The range 0 to 1 will linearly map on the domain between
        the given variable's lower and upper bounds
        
        Arguments:
            variable_label {str} -- variable in `initial_variables` that this object represents.
            initial_variables {atsim.pro_fit.variables.Variables} -- Variables instance.
        """

        self.initial_variables = initial_variables
        self._init_bounds(variable_label)
        distn_function = self._init_distn_function()
        super().__init__(variable_label, distn_function)

    def _init_bounds(self, variable_label):
        idx = self.initial_variables.fitKeys.index(variable_label)
        bounds = self.initial_variables.fitBounds[idx]

        if not bounds:
            msg = "variable distribution needs bounds. Non found for variable '{}'".format(
                variable_label
            )
            raise VariableException(msg)

        lower_bound, upper_bound = bounds
        if not math.isfinite(lower_bound):
            msg = "variable distribution lower bound was not finite for variable '{}'".format(
                variable_label
            )
            raise VariableException(msg)

        if not math.isfinite(upper_bound):
            msg = "variable distribution upper bound was not finite for variable '{}'".format(
                variable_label
            )
            raise VariableException(msg)

        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

    def _init_distn_function(self):
        raise NotImplementedError("Not implemented")


class Uniform_Variable_Distribution(_Bounded_Variable_Distribution):
    def __init__(self, variable_label, initial_variables):
        """Create norm to variable transformation for a given variable.

        The range 0 to 1 will linearly map on the domain between
        the given variable's lower and upper bounds
        
        Arguments:
            variable_label {str} -- variable in `initial_variables` that this object represents.
            initial_variables {atsim.pro_fit.variables.Variables} -- Variables instance.
        """
        super().__init__(variable_label, initial_variables)

    def _init_distn_function(self):
        distn_func = scipy.stats.uniform(
            loc=self.lower_bound, scale=self.upper_bound - self.lower_bound
        ).ppf
        return distn_func


class PERT_Variable_Distribution(_Bounded_Variable_Distribution):
    def __init__(self, variable_label, initial_variables, shape=10):

        """Create norm to variable transformation for a given variable.

        The range 0 to 1 will linearly map on the domain between
        the given variable's lower and upper bounds but will be biased towards
        the variable's current value. 

        The shape parameter can be used to localise the solution around the variable's
        initial value. A value of 0 will produce the uniform distribution, whilst large
        values will produce candidates very close to the variable's initial value.
        
        Arguments:
            variable_label {str} -- variable in `initial_variables` that this object represents.
            initial_variables {atsim.pro_fit.variables.Variables} -- Variables instance.

        Optional parameters:
            shape {float} -- shape parameter (default = {10.0})
        """
        self.shape = shape
        idx = initial_variables.fitKeys.index(variable_label)
        self.mode = initial_variables.fitValues[idx]
        super().__init__(variable_label, initial_variables)

    def _mean(self):
        mode = self.mode
        lower_bound = self.lower_bound
        upper_bound = self.upper_bound
        shape = self.shape

        mu = (lower_bound + shape * mode + upper_bound) / (shape + 2)
        return mu

    def _beta_distribution_params(self, mu):
        shape = self.shape
        mode = self.mode
        lower_bound = self.lower_bound
        upper_bound = self.upper_bound

        if mu == mode:
            alpha_1 = 1.0 + shape / 2.0
        else:
            alpha_1 = (
                (mu - lower_bound) * (2 * mode - lower_bound - upper_bound)
            ) / ((mode - mu) * (upper_bound - lower_bound))
            alpha_2 = (alpha_1 * (upper_bound - mu)) / (mu - lower_bound)

        return alpha_1, alpha_2

    def _init_distn_function(self):
        mu = self._mean()
        alpha_1, alpha_2 = self._beta_distribution_params(mu)

        distn_func = scipy.stats.beta(
            alpha_1,
            alpha_2,
            loc=self.lower_bound,
            scale=self.upper_bound - self.lower_bound,
        ).ppf

        return distn_func


class Variable_Distributions(object):
    """Maintains a collection of of Variable_Distribution objects"""

    def __init__(self, initial_variables, variable_distributions):
        """Associate fitting variables with a collection of Variable_Distribution objects 
        
        Arguments:
            initial_variables {atsim.pro_fit.variables.Variables} -- Variables to which
                Variable_Distribution objects are related.
            variable_distributions {iterable} -- Iterable returning Variable_Distribution objects.
        """
        self.initial_variables = initial_variables
        self._distn_dict = OrderedDict()

        for vd in variable_distributions:
            self._distn_dict[vd.variable_label] = vd

        for fk in initial_variables.fitKeys:
            if fk not in self._distn_dict:
                msg = "Variable_Distribution not found for fitting variable '{}'".format(
                    fk
                )
                raise Missing_Distribution_Exception(msg)

    def apply(self, norm_matrix):
        """Apply variable distributions to norm_matrix.

        This is assumed to be a numpy array with shape = (num_candidates, num_fit_variables)

        Arguments:
            norm_matrix {numpy.array} -- 0...1 variables to transform"""

        out_arr = np.zeros(norm_matrix.shape)

        for i, (col, fk) in enumerate(
            zip(norm_matrix.T, self.initial_variables.fitKeys)
        ):
            distn = self._distn_dict[fk]
            out_arr[:, i] = distn.apply(col)
        return out_arr

    @property
    def variable_distribution_dict(self):
        """Dictionary linking variable label to instances of Variable_Distribution"""
        return self._distn_dict


class Uniform_Variable_Distributions(Variable_Distributions):
    """Convenience class that intialises Variable_Distribution objects
    to uniform distribution between upper and lower bounds of each fitting
    variable"""

    def __init__(self, initial_variables):
        vds = self._init_variable_distributions(initial_variables)
        super().__init__(initial_variables, vds)

    def _init_variable_distributions(self, initial_variables):
        vd = [
            Uniform_Variable_Distribution(fk, initial_variables)
            for fk in initial_variables.fitKeys
        ]
        return vd


class PERT_Variable_Distributions(Variable_Distributions):
    """Convenience class that intialises Variable_Distribution objects
    to PERT distribution between upper and lower bounds of each fitting
    variable"""

    def __init__(self, initial_variables, shape=10.0):
        self.shape = shape
        vds = self._init_variable_distributions(initial_variables)
        super().__init__(initial_variables, vds)

    def _init_variable_distributions(self, initial_variables):
        vd = [
            PERT_Variable_Distribution(fk, initial_variables, self.shape)
            for fk in initial_variables.fitKeys
        ]
        return vd
