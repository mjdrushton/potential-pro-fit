from atsim.pro_fit.cfg import (
    float_convert,
    int_convert,
    boolean_convert,
    infile_convert,
    choice_convert,
    random_seed_option,
    str_convert
)

from atsim.pro_fit.exceptions import ConfigException

import sys
import collections
import logging
import inspect
import operator


class Depends_On_Constraint(object):
    """Create_From_Config_Parser constraint to allow one option to rely on the value of another"""

    def __init__(self, clsname, arg_key, depends_on_key, depends_on_value):
        self.clsname = clsname
        self.arg_key = arg_key
        self.depends_on_key = depends_on_key
        self.depends_on_value = depends_on_value

    def __call__(self, out_args):
        relevant_value = [
            v for v in out_args if v.option.cfg_key == self.arg_key
        ]
        if not relevant_value:
            raise ConfigException(
                "Required option '{}' not found for constraint when parsing options for {}".format(
                    self.arg_key, self.clsname
                )
            )
        relevant_value = relevant_value[0]

        depends_on_value = [
            v for v in out_args if v.option.cfg_key == self.depends_on_key
        ]
        if not depends_on_value:
            raise ConfigException(
                "Required option '{}' not found for constraint when parsing options for {}".format(
                    self.arg_key, self.clsname
                )
            )
        depends_on_value = depends_on_value[0]

        if (
            not relevant_value.is_default
            and depends_on_value.value != self.depends_on_value
        ):
            raise ConfigException(
                "{} option '{}' can only be specified if '{}'=={}".format(
                    self.clsname,
                    self.arg_key,
                    self.depends_on_key,
                    self.depends_on_value,
                )
            )

        return out_args


class Mutually_Exclusive_Constraint(object):
    def __init__(self, only_one_allowed_keys):
        self.only_one_allowed_keys = set(only_one_allowed_keys)

    def __call__(self, out_args):
        specified_keys = set([o.option.cfg_key for o in out_args if o.value])

        specified_keys = specified_keys.intersection(
            self.only_one_allowed_keys
        )

        if len(specified_keys) > 1:
            specified_keys_str = ",".join(list(specified_keys))
            allowed_keys_str = ",".join(list(self.only_one_allowed_keys))
            msg = "Only one of {} may be specified. {} were given".format(
                specified_keys_str, allowed_keys_str
            )
            raise ConfigException(msg)
        return out_args


class Create_From_Config_Parser(object):

    _Option = collections.namedtuple(
        "_Option", ["cfg_key", "out_key", "convert", "default", "required"]
    )
    _Value = collections.namedtuple(
        "_Value", ["value", "is_default", "option"]
    )

    def __init__(self, clsname):
        self.clsname = clsname
        self._options = []
        self._sub_parsers = []
        self._constraints = []

    @property
    def _assembled_options(self):
        options = [o for o in self._options]
        for sp in self._sub_parsers:
            options.extend(sp._assembled_options)
        return options

    @property
    def _required_options(self):
        options = [o for o in self._assembled_options if o.required]
        return options

    @property
    def _optional_options(self):
        options = [o for o in self._assembled_options if not o.required]
        return options

    @property
    def _allowed_option_keys(self):
        option_keys = [o.cfg_key for o in self._options]
        for sp in self._sub_parsers:
            option_keys.extend(sp._allowed_option_keys)
        option_keys = set(option_keys)
        return option_keys

    def _apply_constraints(self, out_args):
        for constraint in self._constraints:
            out_args = constraint(out_args)
        return out_args

    def add_sub_parser(self, out_key, factory_function):
        sub_parser = _Sub_Parser(self.clsname, out_key, factory_function)
        self._sub_parsers.append(sub_parser)
        return sub_parser

    def add_option(
        self, cfg_key, out_key, convert, default=None, required=False
    ):
        option = self._Option(cfg_key, out_key, convert, default, required)
        self._options.append(option)
        return self

    def add_str_option(self, cfg_key, out_key, default=None, required=False):
        converter = str_convert(self.clsname, cfg_key)
        self.add_option(cfg_key, out_key, converter,
                        default=default, required=required)
        return self

    def add_float_option(
        self,
        cfg_key,
        out_key,
        bounds=None,
        bounds_inclusive=(True, True),
        default=None,
        required=False,
    ):
        converter = float_convert(
            self.clsname,
            cfg_key,
            bounds=bounds,
            bounds_inclusive=bounds_inclusive,
        )
        self.add_option(
            cfg_key, out_key, converter, default=default, required=required
        )
        return self

    def add_int_option(
        self,
        cfg_key,
        out_key,
        bounds=None,
        bounds_inclusive=(True, True),
        default=None,
        required=False,
    ):
        converter = int_convert(
            self.clsname,
            cfg_key,
            bounds=bounds,
            bounds_inclusive=bounds_inclusive,
        )
        self.add_option(
            cfg_key, out_key, converter, default=default, required=required
        )
        return self

    def add_boolean_option(
        self, cfg_key, out_key, default=None, required=False
    ):
        converter = boolean_convert(self.clsname, cfg_key)
        self.add_option(
            cfg_key, out_key, converter, default=default, required=required
        )
        return self

    def add_choices_option(
        self, cfg_key, out_key, choices, default=None, required=False
    ):
        converter = choice_convert(self.clsname, cfg_key, choices)
        self.add_option(
            cfg_key, out_key, converter, default=default, required=required
        )

        return self

    def add_infile_option(
        self, cfg_key, out_key, default=None, required=False
    ):
        converter = infile_convert(self.clsname, cfg_key)
        self.add_option(
            cfg_key, out_key, converter, default=default, required=required
        )
        return self

    def add_random_seed_option(self, cfg_key, out_key, required=False):
        converter = random_seed_option(self.clsname, cfg_key)
        self.add_option(
            cfg_key, out_key, converter, default=None, required=required
        )
        return self

    def add_constraint(self, constraint):
        self._constraints.append(constraint)
        return self

    def add_depends_on_constraint(
        self, arg_key, depends_on_key, depends_on_value
    ):
        constraint = Depends_On_Constraint(
            self.clsname, arg_key, depends_on_key, depends_on_value
        )
        self.add_constraint(constraint)
        return self

    def add_mutually_exclusive_constraint(self, *exclusive_cfg_keys):
        constraint = Mutually_Exclusive_Constraint(exclusive_cfg_keys)
        self.add_constraint(constraint)
        return self

    def parse(self, config_items, delete_options=["type"]):
        cfg_dict = dict(config_items)
        for k in delete_options:
            del cfg_dict[k]

        # Required options
        for o in self._required_options:
            if o.cfg_key not in cfg_dict:
                errmsg = "Required option '{}' not found for {}".format(
                    o.cfg_key, self.clsname
                )
                raise ConfigException(errmsg)

        # Check for options in cfg_dict that aren't in self._options
        allowed_option_keys = self._allowed_option_keys
        actual_option_keys = set(cfg_dict.keys())
        # ... identify options that aren't in the allowed set
        actual_option_keys -= allowed_option_keys

        if actual_option_keys:
            errmsg = ",".join(list(actual_option_keys))
            errmsg = "Unknown option(s) specified for {}: {}".format(
                self.clsname, errmsg
            )
            raise ConfigException(errmsg)

        # Built argument dictionary.
        out_args = []
        for o in self._options:
            if o.cfg_key in cfg_dict:
                v = cfg_dict[o.cfg_key]
                v = o.convert(v)
                v = self._Value(v, False, o)
            else:
                v = self._Value(o.default, True, o)

            out_args.append(v)

        # Process sub-parser
        for sp in self._sub_parsers:
            arg = sp.parse(config_items)
            out_args.append(arg)

        # Apply constraints
        out_args = self._apply_constraints(out_args)

        return out_args

    def log_options(
        self, parsed_options, logger, log_level=logging.INFO, indent="  "
    ):
        for o in parsed_options:
            is_default = " (default)" if o.option.default else ""
            msg = "{}* '{}' = {}{}".format(
                indent, o.option.cfg_key, o.value, is_default
            )
            logger.log(log_level, msg)

    def options_to_function_args(
        self, parsed_options, call_me, extra_args={}, drop_self=False
    ):
        """Take the options returned by `parse()` to produce an (args,kwargs) tuple
        that may be used to call `call_me`. That is, options are re-ordered and assigned
        to the args or kwargs lists based on their `out_key` names. 

        This assumes call_me takes the same number of arguments as there are options.

        Arguments:
            parsed_options {list} -- List of options as returned by `parse()`
            call_me {callable} -- Callable to introspected, the arguments returned by this method will be suitable
                for calling this function.

        Keyword Arguments:
            extra_args {dict} -- Additional arguments to call_me that do not appear in parsed_options (default: {{}})
            drop_self {bool} -- If True assume first arg of call_me is self and do not include in args.

        Return:
            {tuple} -- Tuple containing function arguments.
        """
        # Convert parsed_options to a dictionary
        parsed_dict = dict(
            [(d.option.out_key, d.value) for d in parsed_options]
        )

        # Merge with extra_args
        shared_keys = set(parsed_dict.keys()).intersection(
            set(extra_args.keys())
        )
        if shared_keys:
            # Throw if either share keys
            raise TypeError(
                "Args from parsed_options and extra_args cannot overlap: {}".format(
                    list(shared_keys)
                )
            )

        parsed_dict.update(extra_args)

        # Introspect `call_me`, determine names in kwargs and args
        signature = inspect.signature(call_me)
        # determine order for args

        arg_keys = list(signature.parameters.keys())
        if drop_self:
            arg_keys = arg_keys[1:]

        key_set = set(arg_keys)
        key_set -= set(parsed_dict.keys())
        if key_set:
            raise TypeError(
                "call_me has arguments that aren't in parsed_options: {}".format(
                    list(key_set)
                )
            )

        key_set = set(parsed_dict.keys())
        key_set -= set(arg_keys)
        if key_set:
            raise TypeError(
                "parsed_options contains argument names that call_me doesn't have: {}".format(
                    list(key_set)
                )
            )

        # build output lists.
        out_args = []
        for k in arg_keys:
            v = parsed_dict[k]
            out_args.append(v)
        out_args = tuple(out_args)
        return out_args

    def _option_to_doc(self, o, outfile):
        outfile.write(":Name: {} \n".format(o.cfg_key))

        if o.convert.destination_typename:
            outfile.write(
                ":Arg-type: {} \n".format(o.convert.destination_typename)
            )

        outfile.write(":Default: {} \n".format(o.default))

        l_br = "("
        r_br = ")"
        if o.convert.bounds:
            if not o.convert.bounds_inclusive[0]:
                l_br = "["
            if not o.convert.bounds_inclusive[1]:
                r_br = "]"

            lowbound, highbound = o.convert.bounds

            if type(highbound) is int and highbound == sys.maxsize:
                highbound = "inf"

            bounds = "{}{}, {}{}".format(l_br, lowbound, highbound, r_br)
            outfile.write(":Bounds: {} \n".format(bounds))
        if hasattr(o.convert, "choicestring"):
            outfile.write(
                ":Allowed Values: {}\n".format(o.convert.choicestring)
            )
        outfile.write(":Description:\n")

    def to_docs(self, outfile):
        """Introspect this parser and format its options in restructured text
        suitable for including in documentation.

        Arguments:
            outfile {file} -- File into which documentation will be written.
        """

        outfile.write("{} \n".format(self.clsname))
        outfile.write("{} \n\n".format("^" * len(self.clsname)))

        if self._required_options:
            outfile.write("Required Fields\n")
            outfile.write("===============\n\n")

            for o in sorted(self._required_options, key=operator.attrgetter('cfg_key')):
                self._option_to_doc(o, outfile)
                outfile.write("\n\\ \n\n")

        if self._optional_options:
            outfile.write("Optional Fields\n")
            outfile.write("===============\n\n")

            for o in sorted(self._optional_options, key=operator.attrgetter('cfg_key')):
                self._option_to_doc(o, outfile)
                outfile.write("\n\n\\ \n\n")


class _Sub_Parser(Create_From_Config_Parser):
    def __init__(self, clsname, out_key, call_me, drop_self=False):
        super().__init__(clsname)

        self.out_key = out_key
        self.call_me = call_me
        self.drop_self = drop_self

    def parse(self, config_items):

        # Pre-process config_items so only options relevant to the sub-parser are considered.
        filtered_items = []

        configured_options = set([o.cfg_key for o in self._options])

        for k, v in config_items:
            if k in configured_options:
                filtered_items.append((k, v))

        parsed_options = super().parse(filtered_items, [])
        args = self.options_to_function_args(
            parsed_options, self.call_me, drop_self=self.drop_self
        )
        value = self.call_me(*args)

        o = self._Option(self.out_key, self.out_key, None, None, False)
        v = self._Value(value, False, o)

        return v
