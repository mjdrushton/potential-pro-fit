"""Classes and functions to help write createFromConfig() methods"""

from exceptions import ConfigException


def convert_factory(clsname, key, convfunc, bounds):
    """Creates a callable which takes a single argument.

    This argument is passed to convfunc - if this raises an exceeption
    this is converted to ConfigException with an appropriate error 
    message.
    
    Arguments:
        clsname {str} -- used in error messages to show which class value
            conversion is being performed.
        key {str} -- gives option name in error messages.
        convfunc {callable} -- single argument callable used to perform conversion
        bounds {tuple} -- (low, high) tuple describing allowed bounds.
    
    Returns:
        callable -- conversion function that takes single value and converts it
            from a string to desired type. If this fails, raises ConfigException.
    """

    def f(v):
        try:
            v = convfunc(v)
        except:
            raise ConfigException(
                "Could not parse option '{}' for {}: {}".format(
                    key, clsname, v
                )
            )

        if bounds:
            if not (v >= bounds[0] and v <= bounds[1]):
                raise ConfigException(
                    "Option value does not lie within bounds ({}, {}). Option key '{}' for {}: {}".format(
                        bounds[0], bounds[1], key, clsname, v
                    )
                )
        return v

    return f


def int_convert(clsname, key, bounds=None):
    """Function factory for converting option values to integers.
    
    Arguments:
        clsname {str} -- Used in error messages to describe class where conversion is being performed.
        key {str} -- Defines option name used in error messages.
    
    Keyword Arguments:
        bounds {tuple} -- (low, high) tuple giving allowed bounds for option (default: {None})
    
    Returns:
        callable -- Option conversion function
    """
    return convert_factory(clsname, key, int, bounds)


def float_convert(clsname, key, bounds=None):
    """Function factory for converting option values to floats.
    
    Arguments:
        clsname {str} -- Used in error messages to describe class where conversion is being performed.
        key {str} -- Defines option name used in error messages.
    
    Keyword Arguments:
        bounds {tuple} -- (low, high) tuple giving allowed bounds for option (default: {None})
    
    Returns:
        callable -- Option conversion function
    """
    return convert_factory(clsname, key, float, bounds)


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
    iconv = int_convert(clsname, key, (0, float("inf")))
    import time

    def f(v):
        if v:
            return iconv(v)
        else:
            return int(time.time())

    return f


def choice_convert(clsname, key, choices):
    """Function factory for options that can only take a limited set of choices.
    
    Arguments:
        clsname {str} -- Used in error messages to describe class where conversion is being performed.
        key {str} -- Defines option name used in error messages.
        choices {list} -- List of options allowed as input to conversion function.
    
    Returns:
        callable -- Option conversion function
    """

    choices = set(choices)
    choicestring = sorted(list(choices))
    if len(choicestring) == 2:
        choicestring = " or ".join(choicestring)
    else:
        choicestring = ", ".join(choicestring)

    def f(v):
        v = v.strip()
        vt = v.split()[0]
        if not vt in choices:
            raise ConfigException(
                "Could not parse option '{}' for {}. Value '{}' should be one of {}. ".format(
                    key, clsname, v, choicestring
                )
            )
        return v

    return f


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
        v = sconv(v)

        if v == "True":
            return True
        else:
            return False

    return f


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

    return f
