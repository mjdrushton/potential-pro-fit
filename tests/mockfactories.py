class MockJobFactory(object):
    def __init__(self, runnername, evaluators):
        self.runnerName = runnername
        self.evaluators = evaluators

    def runTasksBeforeRun(self, job):
        pass

    def runTasksAfterRun(self, job):
        pass

    @staticmethod
    def createFromConfig(
        jobpath, fit_root_path, runnername, jobname, evaluators, jobtasks, cfgitems
    ):
        return MockJobFactory(runnername, evaluators)
