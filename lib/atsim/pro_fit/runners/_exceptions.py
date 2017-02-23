class RunnerClosedException(Exception):
  pass

class JobKilledException(Exception):
  pass

class NonZeroExitStatus(Exception):
  pass

class BatchAlreadyFinishedException(Exception):
  pass

class BatchKilledException(Exception):
  pass

class BatchDirectoryLockException(Exception):
  pass
