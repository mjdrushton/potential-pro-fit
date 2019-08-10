import logging

from ._base_minimizer import Abstract_Base_Minimizer
from atsim.pro_fit.cfg import Create_From_Config_Parser


class Configured_Minimizer_Abstract_Base(Abstract_Base_Minimizer):
    """Base class that provides a createFromConfig() that
    will configure and instantiate minimizer using 
    an atsim.pro_fit.cfg.Create_From_Config_Parser
    configured by the _populate_cfg_parser() method
    provided by implementing classes"""

    _clsname = "Abstract Minimizer CHANGE ME"

    def __init__(self, variables):
        super().__init_()
        self.variables = variables

    @classmethod
    def _populate_cfg_parser(self, cfgparse):
        raise NotImplementedError(
            "Implementing classes should override _populate_cfg_parser"
        )

    @classmethod
    def _create_cfg_parser(cls):
        cfgparse = Create_From_Config_Parser(cls._clsname)
        cls._populate_cfg_parser(cfgparse)
        return cfgparse

    @classmethod
    def createFromConfig(cls, variables, configitems):
        cfgparse = cls._create_cfg_parser()
        opts = cfgparse.parse(configitems)

        logger = logging.getLogger(__name__).getChild(cls._clsname)
        logger.info(
            "Creating '{}' minimizer with following options".format(
                cls._clsname
            )
        )
        cfgparse.log_options(opts, logger)
        args = cfgparse.options_to_function_args(
            opts, cls.__init__, {"variables": variables}, drop_self=True
        )
        minimizer = cls(*args)
        return minimizer
