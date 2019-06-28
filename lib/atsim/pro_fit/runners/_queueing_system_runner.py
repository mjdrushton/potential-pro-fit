from atsim.pro_fit.exceptions import ConfigException
from ._queueing_system_client import QueueingSystemClient
from ._queueing_system_runner_batch import QueueingSystemRunnerBatch

from ._base_remoterunner import BaseRemoteRunner
from ._base_remoterunner import RemoteRunnerCloseThreadBase

import logging
import os


class _QueueingSystemRunnerCloseThread(RemoteRunnerCloseThreadBase):
    def closeRunClient(self):
        self.runner._pbsclient.close(closeChannel=False)
        evts = self._closeChannel(
            self.runner._pbsclient.channel, "closeRunClient"
        )
        return evts


class QueueingSystemRunnerBaseClass(BaseRemoteRunner):
    """Generic runner class for batch queueing systems. This is an abstract base class designed to be used as the basis for publicly accesible runners.
  
  Sub-classes should override this class and provide an implementation of the `makeRunChannel` method."""

    """Suffix appended to uuid to produce channel's ID"""
    id_suffix = "_qs"

    def __init__(
        self,
        name,
        url,
        header_include,
        batch_size,
        qselect_poll_interval,
        identityfile=None,
        extra_ssh_options=[],
        do_cleanup=True,
    ):
        """Create runner.

    Args:
      name (str): Runner name.
      url (str): SSH remote host job directory url and path, as accepted by BaseRemoteRunner.
      batch_size (int): Number of pprofit jobs to be included in each queueing system array job.
      qselect_poll_interval (float): Time interval (seconds) at which queueing system is queried to establish state of array jobs.
      identityfile (str) : Path to ssh private key file used to log into remote host (or None if system defaults are to be used).
      extra_ssh_options (list): List of strings giving any extra ssh configuration options.
      do_cleanup (bool): If True perform file clean-up on queueing system submission host. If False leave temporary files on remote machine (for debugging)."""

        self.header_include = []
        if not header_include is None:
            self.header_include = header_include.split(os.linesep)

        self.qselect_poll_interval = qselect_poll_interval
        self.batch_size = batch_size

        logger = self._get_logger()
        logger.debug("Instantiating runner with following values:")
        logger.debug("  * name = {}".format(name))
        logger.debug("  * url = {}".format(url))
        logger.debug("  * header_include = {}".format(header_include))
        logger.debug("  * batch_size = {}".format(batch_size))
        logger.debug(
            "  * qselect_poll_interval = {}".format(qselect_poll_interval)
        )
        logger.debug("  * identityfile = {}".format(identityfile))
        logger.debug("  * extra_ssh_options = {}".format(extra_ssh_options))

        super(QueueingSystemRunnerBaseClass, self).__init__(
            name, url, identityfile, extra_ssh_options, do_cleanup
        )

    @property
    def _logger(self):
        return self._get_logger()

    def _get_logger(self):
        return logging.getLogger(__name__).getChild(
            "QueueingSystemRunnerBaseClass"
        )

    def initialiseRun(self):
        channel_id = self._uuid + self.id_suffix
        self._pbschannel = self.makeRunChannel(channel_id)
        self._pbsclient = QueueingSystemClient(
            self._pbschannel, pollEvery=self.qselect_poll_interval
        )

    def makeRunChannel(self, channel_id):
        """Method which returns an execnet channel suitable for use as argument to atsim.pro_fit.runners._queuing_system_client.QueueingSystemClient constructor.

    Args:
      channel_id (str) : String used to identify created channel.
      
    Returns:
      atsim.pro_fit._channel.AbstractChannel : Channel instance that supports the protocol expected by QueueingSystemClient."""
        raise NotImplementedError(
            "Sub-classes must provide implementation for makeRunChannel."
        )

    def createBatch(self, batchDir, jobs, batchId):
        return QueueingSystemRunnerBatch(
            self, batchDir, jobs, batchId, self._pbsclient, self.header_include
        )

    def makeCloseThread(self):
        return _QueueingSystemRunnerCloseThread(self)

    @classmethod
    def allowedConfigKeywords(cls):
        """Returns list of standard keywords accepted by parseConfig_* class methods"""
        kws = super(QueueingSystemRunnerBaseClass, cls).allowedConfigKeywords()
        kws.extend(["header_include", "arraysize", "pollinterval"])
        return kws

    @classmethod
    def parseConfigItem_header_include(cls, runnerName, fitRootPath, cfgitems):
        """Convenience method to provide consistent provision of `header_include` option in sub-classes.

    Args:
      runnerName (str) : Label identifying runner.
      fitRootPath (str) : Path to directory containing 'fit.cfg'
      cfgdict (list) : List of (key, value) pairs identifying relecant section of configuration file.

    Returns:
      dict: If option is found returned dictionary is of form `{ 'header_include' : VALUE}` where `VALUE` is contents
            of file referred to by the `header_include` configuration option. If option is not found, `VALUE` is `None`.
      
    Raises:
      atsim.pro_fit.exceptions.ConfigException: thrown if configuration problem found."""
        cfgdict = dict(cfgitems)
        header_include = cfgdict.get("header_include", None)
        if header_include:
            try:
                header_include = open(header_include, "r").read()
            except IOError:
                raise ConfigException(
                    "Could not open file specified by 'header_include' directive: %s"
                    % header_include
                )

        return {"header_include": header_include}

    @classmethod
    def parseConfigItem_arraysize(cls, runnerName, fitRootPath, cfgitems):
        """Convenience method to provide consistent provision of `arraysize` option in sub-classes.

    Args:
      runnerName (str) : Label identifying runner.
      fitRootPath (str) : Path to directory containing 'fit.cfg'
      cfgdict (list) : List of (key, value) pairs identifying relecant section of configuration file.

    Returns:
      dict: If option is dictionary `{ 'arraysize' : VALUE}` is returned. Where `VALUE` is value of `arraysize` configuration option. 
            If option is not found, `VALUE` is `None`.
      
    Raises:
      atsim.pro_fit.exceptions.ConfigException: thrown if configuration problem found."""
        cfgdict = dict(cfgitems)
        arraysize = cfgdict.get("arraysize", None)
        if arraysize != None and arraysize.strip() == "None":
            arraysize = None

        if not arraysize is None:

            try:
                arraysize = int(arraysize)
            except ValueError:
                raise ConfigException(
                    "Invalid numerical value for 'arraysize' configuration option: %s"
                    % arraysize
                )

            if not arraysize >= 1:
                raise ConfigException(
                    "Value of 'arraysize' must >= 1. Value was %s" % arraysize
                )
        return {"arraysize": arraysize}

    @classmethod
    def parseConfigItem_pollinterval(
        cls, runnerName, fitRootPath, cfgitems, default=30.0
    ):
        """Convenience method to provide consistent provision of `pollinterval` option in sub-classes.

    Args:
      runnerName (str)  : Label identifyingrunner.
      fitRootPath (str) : Path to directory containing'fit.cfg'
      cfgdict (list)    : List of (key, value) pairs identifying relecant section of configurationfile.
      default (float)   : Default interval inseconds.

    Returns:
      dict: If option is dictionary `{ 'pollinterval' : VALUE}` is returned. Where `VALUE` is value of `pollinterval` configuration option. 
      
    Raises:
      atsim.pro_fit.exceptions.ConfigException: thrown if configuration problem found."""
        cfgdict = dict(cfgitems)
        pollinterval = cfgdict.get("pollinterval", 30.0)
        try:
            pollinterval = float(pollinterval)
        except ValueError:
            raise ConfigException(
                "Invalid numerical value for 'pbspollinterval': %s"
                % pollinterval
            )

        if not pollinterval > 0.0:
            raise ConfigException(
                "Value of 'pbspollinterval' must > 0.0. Value was %s"
                % pollinterval
            )
        return {"pollinterval": pollinterval}

    @classmethod
    def parseConfig(cls, runnerName, fitRootPath, cfgitems):
        """Convenience function to help sub-classes implement their `createFromConfig()` methods.

    This parses the standard options into a dictionary with the keys:

    * `remotehost` (str): `ssh://[username@]hostname/remote_path` url string.
    * `header_include` (str): contents of file specified by the `header_include` config item.
    * `arraysize` (int): size of bath arrays
    * `pollinterval` (float): time interval at which queue stat is polled.
    * `do_cleanup` (bool): read from the `debug.disable_cleanup` configuration item.

    Args:
      runnerName (str) : Name of runner.
      fitRootPath (str): Path to directory containing `fit.cfg`.
      cfgitems (list): List of key, value items from `fit.cfg` defining runner.

    Returns:
      dict : Dictionary of the form listed above.

    Raises:
      atsim.pro_fit.exceptions.ConfigException : Thrown if invalide configuration values found"""
        option_dict = super(QueueingSystemRunnerBaseClass, cls).parseConfig(
            runnerName, fitRootPath, cfgitems
        )
        option_dict.update(
            cls.parseConfigItem_header_include(
                runnerName, fitRootPath, cfgitems
            )
        )
        option_dict.update(
            cls.parseConfigItem_arraysize(runnerName, fitRootPath, cfgitems)
        )
        option_dict.update(
            cls.parseConfigItem_pollinterval(runnerName, fitRootPath, cfgitems)
        )
        return option_dict
