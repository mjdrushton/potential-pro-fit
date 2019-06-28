class ConfigException(Exception):
    def __init__(self, msg):
        self._msg = msg
        super(ConfigException, self).__init__(msg)

    @property
    def message(self):
        return "Configuration error - %s" % self._msg


class MultipleSectionConfigException(ConfigException):
    pass
