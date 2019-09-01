"""Classes and functions to help write createFromConfig() methods"""

from atsim.pro_fit.exceptions import ConfigException

import operator
import sys


class _Converter(object):
    def __init__(
        self,
        clsname,
        key,
        convfunc,
        bounds,
        bounds_inclusive=(True, True),
        destination_typename=None,
    ):

        self.clsname = clsname
        self.key = key
        self.convfunc = convfunc
        self.bounds = bounds
        self.bounds_inclusive = bounds_inclusive
        self.destination_typename = destination_typename

    def __call__(self, v):
        try:
            v = convfunc(v)
        except:
            raise ConfigException(
                "Could not parse option '{}' for {}: {}".format(
                    self.key, self.clsname, v
                )
            )

        if self.bounds:
            low_inc, high_inc = self.bounds_inclusive
            lowop = operator.ge if low_inc else operator.gt
            highop = operator.le if high_inc else operator.lt

            if not (lowop(v, self.bounds[0]) and highop(v, self.bounds[1])):
                raise ConfigException(
                    "Option value does not lie within bounds ({}, {}). Option key '{}' for {}: {}".format(
                        self.bounds[0],
                        self.bounds[1],
                        self.key,
                        self.clsname,
                        v,
                    )
                )
        return v


def convert_factory(
    clsname, key, convfunc, bounds, bounds_inclusive=(True, True)
):
    """Creates a callable which takes a single argument.

    This argument is passed to convfunc - if this raises an exception
    this is converted to ConfigException with an appropriate error 
    message.
    
    Arguments:
        clsname {str} -- used in error messages to show which class value
            conversion is being performed.
        key {str} -- gives option name in error messages.
        convfunc {callable} -- single argument callable used to perform conversion
        bounds {tuple} -- (low, high) tuple describing allowed bounds.
        bounds_inclusive {tuple} -- Two value tuple. True value indicates inclusive range.
            So (True, True) means high and low values shoud be included in range (low <= v <= high).
            (True, Fale) means (low <= v < high)
    
    Returns:
        callable -- conversion function that takes single value and converts it
            from a string to desired type. If this fails, raises ConfigException. """
    converter = _Converter(clsname, key, convfunc, bounds, bounds_inclusive)
    return converter

def str_convert(clsname, key):
    converter = convert_factory(clsname, key, str, bounds=None)
    converter.destination_typename = "str"
    return converter

def int_convert(clsname, key, bounds=None, bounds_inclusive=(True, True)):
    """Function factory for converting option values to integers.
    
    Arguments:
        clsname {str} -- Used in error messages to describe class where conversion is being performed.
        key {str} -- Defines option name used in error messages.
    
    Keyword Arguments:
        bounds {tuple} -- (low, high) tuple giving allowed bounds for option (default: {None})
        bounds_inclusive {tuple} -- Two value tuple. True value indicates inclusive range.
            So (True, True) means high and low values shoud be included in range (low <= v <= high).
            (True, Fale) means (low <= v < high)

    
    Returns:
        callable -- Option conversion function
    """
    converter = convert_factory(clsname, key, int, bounds, bounds_inclusive)
    converter.destination_typename = "int"
    return converter


def float_convert(clsname, key, bounds=None, bounds_inclusive=(True, True)):
    """Function factory for converting option values to floats.
    
    Arguments:
        clsname {str} -- Used in error messages to describe class where conversion is being performed.
        key {str} -- Defines option name used in error messages.
    
    Keyword Arguments:
        bounds {tuple} -- (low, high) tuple giving allowed bounds for option (default: {None})
        bounds_inclusive {tuple} -- Two value tuple. True value indicates inclusive range.
            So (True, True) means high and low values shoud be included in range (low <= v <= high).
            (True, Fale) means (low <= v < high)

    
    Returns:
        callable -- Option conversion function
    """
    converter = convert_factory(clsname, key, float, bounds, bounds_inclusive)
    converter.destination_typename = "float"
    return converter


def random_seed_option(clsname, key):
    """Function factory for providing random seed options in configuration.

     If None is passed to the returned function then a random seed is
     generated from the current time. Otherwise the conversion function expects an
     integer.
    
    Arguments:
        clsname {str} -- Used in error messages to describe class where conversion is being performed.
        key {str} -- Defines option name used in error messages.
    
    Returns:
        callable -- Option conversion function
    """

    import time

    iconv = int_convert(clsname, key, (0, sys.maxsize))

    def convfunc(v):
        if v is None:
            return int(time.time())
        else:
            return int(v)

    iconv.convfunc = convfunc
    return iconv


def choice_convert(clsname, key, choices):
    """Function factory for options that can only take a limited set of choices.
    
    Arguments:
        clsname {str} -- Used in error messages to describe class where conversion is being performed.
        key {str} -- Defines option name used in error messages.
        choices {list} -- List of options allowed as input to conversion function.
    
    Returns:
        callable -- Option conversion function
    """

    class _Choice_Converter(_Converter):
        def __init__(self, clsname, key, choices):
            super().__init__(clsname, key, None, None, None)
            self.choices = choices
            self.destination_typename = "(see allowed values)"

            self.choicestring = sorted(list(choices))
            if len(self.choicestring) == 2:
                self.choicestring = " or ".join(self.choicestring)
            else:
                self.choicestring = ", ".join(self.choicestring)

        def __call__(self, v):
            v = v.strip()
            vt = v.split()[0]
            if not vt in self.choices:
                raise ConfigException(
                    "Could not parse option '{}' for {}. Value '{}' should be one of {}. ".format(
                        self.key, self.clsname, v, self.choicestring
                    )
                )
            return v

    conv = _Choice_Converter(clsname, key, choices)
    return conv


def boolean_convert(clsname, key):
    """Function factory for parsing values into booleans from "True" or "False"
    inputs.
    
    Arguments:
        clsname {str} -- Used in error messages to describe class where conversion is being performed.
        key {str} -- Defines option name used in error messages.
    
    Returns:
        callable -- Option conversion function
    """

    sconv = choice_convert(clsname, key, ["True", "False"])

    def f(v):
        if not (v is True or v is False):
            v = sconv(v)

        if v is True or v == "True":
            return True
        else:
            return False

    converter = convert_factory(
        clsname, key, f, bounds=None, bounds_inclusive=None
    )
    converter.destination_typename = "bool"
    converter.choicestring = sconv.choicestring
    return converter


def infile_convert(clsname, key):
    """Config converter that checks for existence of input file"""

    import os

    def f(v):
        if v is None:
            return None
        if not os.path.isfile(v):
            raise ConfigException(
                "File specified for {} option {} does not exist: '{}'".format(
                    clsname, key, v
                )
            )
        return v

    converter = convert_factory(clsname, key, f, bounds=None)
    converter.destination_typename = "file path"
    return converter
