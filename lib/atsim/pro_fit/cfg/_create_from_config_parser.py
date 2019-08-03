from atsim.pro_fit.cfg import float_convert, int_convert, boolean_convert
from atsim.pro_fit.exceptions import ConfigException

import sys
import collections
import logging
import inspect


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
        self._constraints = []

    @property
    def _required_options(self):
        options = [o for o in self._options if o.required]
        return options

    @property
    def _allowed_option_keys(self):
        option_keys = [o.cfg_key for o in self._options]
        option_keys = set(option_keys)
        return option_keys

    def _apply_constraints(self, out_args):
        for constraint in self._constraints:
            out_args = constraint(out_args)
        return out_args

    def add_option(
        self, cfg_key, out_key, convert, default=None, required=False
    ):
        option = self._Option(cfg_key, out_key, convert, default, required)
        self._options.append(option)
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
