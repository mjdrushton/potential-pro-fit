import unittest

from atsim.pro_fit import _util


class RetryTestCase(unittest.TestCase):
  """Tests for atsim.pro_fit._util.retry() and associated callbacks."""

  def testRetryTimesSuccess(self):
    """Test retry_times when no exception is raised"""

    def success(v, r):
      return v+5 + r

    success = _util.retry_times(success, [Exception], 5)
    self.assertEquals(14, success(5,4))

  def testRetryTimesAlwaysFail(self):
    """Test retry_times for a function that always fails"""
    d = {'numcalls' : 0}
    def failure(v):
      d['numcalls'] += 1
      raise Exception("I'm bad")

    failure = _util.retry_times(failure, [Exception], 5, 0.01)
    with self.assertRaises(Exception):
      failure(1)

    self.assertEquals(5, d['numcalls'])

  def testExceptionSpecificity(self):
    """Test that retry_times throws immediately if it experiences an unknown exception"""

    class RegisteredException(Exception):
      pass

    class UnregisteredException(Exception):
      pass

    d = {'numcalls' : 0}

    def func(v):
      d['numcalls'] += 1
      if v == 0 and d['numcalls'] < 3:
        raise RegisteredException()
      elif v == 2:
        raise UnregisteredException()
      else:
        return v

    func = _util.retry_times(func, [RegisteredException], 5)
    self.assertEquals(1, func(1))
    d['numcalls'] = 0
    v = func(0)
    self.assertEquals(3, d['numcalls'])
    self.assertEquals(0, v)

    d['numcalls'] = 0
    with self.assertRaises(UnregisteredException):
      func(2)
    self.assertEquals(1, d['numcalls'])

  def testRetryBackoff(self):
    """Test retry_backoff"""

    class RegisteredException(Exception):
      pass

    class UnregisteredException(Exception):
      pass

    d = {'numcalls' : 0}

    def func(v):
      d['numcalls'] += 1
      if v == 0 and d['numcalls'] < 3:
        raise RegisteredException()
      elif v == 2:
        raise UnregisteredException()
      else:
        return v

    func = _util.retry_backoff(func, [RegisteredException], initialSleep = 0.01, maxSleep =5, times = 5)
    self.assertEquals(1, func(1))
    d['numcalls'] = 0
    v = func(0)
    self.assertEquals(3, d['numcalls'])
    self.assertEquals(0, v)

    d['numcalls'] = 0
    with self.assertRaises(UnregisteredException):
      func(2)
    self.assertEquals(1, d['numcalls'])

  def testExponentialBackoff(self):
    """Test atsim.pro_fit._util._exponentialBackoff()"""
    eb = _util._exponentialBackoff
    self.assertEquals(3, eb(3, None, 1))
    self.assertEquals(6, eb(3, None, 2))
    self.assertEquals(12, eb(3, None, 3))
    self.assertEquals(24, eb(3, None, 4))

  def testExponentialBackoffWithMax(self):
    """Test atsim.pro_fit._util._exponentialBackoff()"""
    eb = _util._exponentialBackoff
    self.assertEquals(2, eb(2, 20, 1))
    self.assertEquals(4, eb(2, 20, 2))
    self.assertEquals(8, eb(2, 20, 3))
    self.assertEquals(16, eb(2, 20, 4))
    self.assertEquals(20, eb(2, 20, 5))
