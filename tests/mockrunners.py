class MockRunner1Runner(object):
    def __init__(self, name, remote_dir, header_filename, fitpath):
        self.name = name
        self.remote_dir = remote_dir
        self.header_filename = header_filename
        self.fitpath = fitpath

    @staticmethod
    def createFromConfig(runnerName, fitRootPath, cfgitems):
        cdict = dict(cfgitems)
        return MockRunner1Runner(
            runnerName,
            cdict["remote_dir"],
            cdict["header_filename"],
            fitRootPath,
        )


class MockRunner2Runner(object):
    def __init__(self, name, remote_dir, ncpus, fitpath):
        self.name = name
        self.remote_dir = remote_dir
        self.ncpus = ncpus
        self.fitpath = fitpath

    @staticmethod
    def createFromConfig(runnerName, fitRootPath, cfgitems):
        cdict = dict(cfgitems)
        return MockRunner2Runner(
            runnerName, cdict["remote_dir"], int(cdict["ncpus"]), fitRootPath
        )
