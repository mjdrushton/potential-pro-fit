import sys

import pytest

from deepdiff import DeepDiff

from atsim.pro_fit.exceptions import ConfigException

from atsim.pro_fit.cfg import Create_From_Config_Parser


class NullClass(object):
    def __init__(
        self,
        variables,
        redfactor,
        deltainit,
        deltatol,
        feps,
        errorcontrol,
        funcNinit,
        funcmultfactor,
        alpha,
    ):
        pass


@pytest.fixture
def cfgparser():
    cfgparse = Create_From_Config_Parser("Test Parser")
    cfgparse.add_float_option(
        "reduction_factor",
        "redfactor",
        bounds=(1.0, float("inf")),
        bounds_inclusive=(False, True),
        default=2.0,
    ).add_float_option(
        "initial_pattern_size",
        "deltainit",
        bounds=(0.0, float("inf")),
        bounds_inclusive=(False, True),
        default=1.0,
    ).add_float_option(
        "target_pattern_size",
        "deltatol",
        bounds=(0.0, float("inf")),
        bounds_inclusive=(False, True),
        default=1.0e-3,
    ).add_float_option(
        "merit_tolerance", "feps", bounds=(0, float("inf")), default=1e-15
    ).add_boolean_option(
        "error_control", "errorcontrol", default=False
    ).add_int_option(
        "ec_initial_merit_evaluations",
        "funcNinit",
        bounds=(1, sys.maxsize),
        default=30,
    ).add_float_option(
        "ec_merit_factor",
        "funcmultfactor",
        bounds=(1.0, float("inf")),
        default=2.0,
    ).add_float_option(
        "ec_alpha",
        "alpha",
        bounds=(0.0, float("inf")),
        bounds_inclusive=(False, True),
        default=0.05,
    )

    # Add constraints
    cfgparse.add_depends_on_constraint(
        "ec_initial_merit_evaluations", "error_control", True
    ).add_depends_on_constraint(
        "ec_merit_factor", "error_control", True
    ).add_depends_on_constraint(
        "ec_alpha", "error_control", True
    )

    return cfgparse


def test_cfg_parser_defaults(cfgparser):
    defaults = cfgparser.parse({}, delete_options=[])

    actual = dict([(d.option.out_key, d.value) for d in defaults])

    expect = dict(
        redfactor=2.0,
        deltainit=1.0,
        deltatol=1e-3,
        feps=1e-15,
        errorcontrol=False,
        funcNinit=30,
        funcmultfactor=2.0,
        alpha=0.05,
    )

    assert DeepDiff(expect, actual) == {}


def test_cfg_parser_errorcontrol_constraint(cfgparser):

    for other_key, value in {
        "ec_initial_merit_evaluations": "30",
        "ec_merit_factor": "2.0",
        "ec_alpha": "0.1",
    }.items():
        cfgparser.parse(
            {"error_control": "True", other_key: value}, delete_options=[]
        )
        with pytest.raises(ConfigException):
            cfgparser.parse(
                {"error_control": "False", other_key: value}, delete_options=[]
            )


def test_options_to_function_args(cfgparser):

    opts = cfgparser.parse({}, delete_options=[])

    extra_args = {"variables": "variables"}

    expect_args = ("variables", 2.0, 1.0, 1e-3, 1e-15, False, 30, 2.0, 0.05)

    actual = cfgparser.options_to_function_args(
        opts, NullClass.__init__, extra_args, drop_self=True
    )
    assert DeepDiff(expect_args, actual) == {}


def test_sub_parser():
    class Mock_Parser(object):
        def __init__(self, one, two, three):
            self.one = one
            self.two = two
            self.three = three

    def mk_mock(one, two, three):
        return Mock_Parser(one, two, three)

    cfgparser = Create_From_Config_Parser("Test parser")
    cfgparser.add_float_option("float_option", "float_option", required=True)
    sub_parser = cfgparser.add_sub_parser("sub_option", mk_mock)

    sub_parser.add_int_option("one", "one", required=True)
    sub_parser.add_float_option("two", "two", default=2)
    sub_parser.add_int_option("three", "three", default=3)

    inputs = [("float_option", "1.23"), ("one", "1"), ("three", "4")]

    parsed = cfgparser.parse(inputs, delete_options=[])

    assert len(parsed) == 2

    assert parsed[0].option.out_key == "float_option"
    assert parsed[0].value == 1.23

    assert parsed[1].option.out_key == "sub_option"
    assert parsed[1].value.one == 1.0
    assert parsed[1].value.two == 2
    assert parsed[1].value.three == 4
