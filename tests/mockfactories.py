class MockJobFactory(object):
    def __init__(self, runnername, evaluators):
        self.runnerName = runnername
        self.evaluators = evaluators

    @staticmethod
    def createFromConfig(
        jobpath, fit_root_path, runnername, jobname, evaluators, cfgitems
    ):
        return MockJobFactory(runnername, evaluators)
