import csv
import functools
import itertools

import logging

_logger = logging.getLogger("atsim.pro_fit.retry")

def retry(func, handledExceptions, retryCallback, logger=None):
  """Function decorator that can be used to re-execute wrapped function."""

  if logger == None:
    logger = _logger

  @functools.wraps(func)
  def wrapper(*args, **kwargs):
      for callcount in itertools.count(1):
        try:
          return func(*args, **kwargs)
        except Exception as exc:
          if type(exc) in handledExceptions and retryCallback(exc, callcount,logger):
            continue
          raise
  return wrapper


def retry_times(*args, **kwargs):
  """Decorator supporting retry behaviour see :func:`retry_times_wrapper` for details"""
  def wrapped(func):
    return retry_times_wrapper(func, *args, **kwargs)
  return wrapped

def retry_backoff(*args, **kwargs):
  """Decorator supporting retry behaviour see :func:`retry_backoff_wrapper` for details"""
  def wrapped(func):
    return retry_backoff_wrapper(func, *args, **kwargs)
  return wrapped

def retry_times_wrapper(func, handledExceptions, times=5, sleep = None, logger = None):
  """Function wrapper. If `func` throws any of the functions within
  the `handledExceptions` list then `func` is called up to `times` retries.
  If function suceeds then function's value is returned. After maximum retries,
  exception is raised.

  :param func: Function to be wrapped.
  :param handledExceptions: List of exception
  :param times: Number of times to retry `func`. If None make unlimited retries.
  :param sleep: Wait `sleep` seconds before retrying. If None, do not wait before
    before retrying.
  :param logger: If logging.Logger instance specified, then retry messages will be logged
  at info level. Otherwise they are logged to the `atsim.pro_fit.retry` logger.

  :returns: Wrapped function"""
  import time
  def retryLogic(exc, callcount, log):
    retval = False
    sleeptime = None
    logmsg = str(exc)
    if times:
      logmsg = logmsg + ". Call %d/%d" % (callcount, times)
    else:
      logmsg = logmsg + ". Call %d" % callcount

    if times == None or callcount < times:
      if sleep:
        logmsg = logmsg+". Will retry in %f seconds." % sleep
        sleeptime = sleep
      retval = True
    else:
      logmsg = logmsg+". No more retries, exception will be raised."
      retval = False
    log.warning(logmsg)
    if sleeptime:
        time.sleep(sleeptime)
    return retval
  return retry(func, handledExceptions, retryLogic)

def retry_backoff_wrapper(func, handledExceptions, initialSleep = 0.5, maxSleep = None, times = None, logger=None):
  """Function wrapper. If `func` throws any of the functions within
  the `handledExceptions` list then `func` is called up to `times` retries (or forever if `times` is None).
  If function suceeds then function's value is returned. After maximum retries,
  exception is raised.

  This function will sleep between `func` invocations using an exponential back-off scheme.
  The function will wait `initialSleep` between first and second calls, 2*`initialSleep` for the next,
  4*`initialSleep` for the next and so on.

  If `maxSleep` is specified then waiting time is capped at this value.

  :param func: Function to be wrapped
  :param handledExceptions: List of exception classes to be handled by retry logic.
  :param sleep: Wait `sleep` seconds before retrying. If None, do not wait before
    before retrying.
  :param logger: If logging.Logger instance specified, then retry messages will be logged
    at info level. Otherwise they are logged to the `atsim.pro_fit.retry` logger.

  :returns: Wrapped function"""
  import time
  def retryLogic(exc, callcount, log):
    retval = False
    sleeptime = None
    logmsg = str(exc)
    if times:
      logmsg = logmsg + ". Call %d/%d" % (callcount, times)
    else:
      logmsg = logmsg +". Call %d" % callcount

    if times == None or callcount < times:
      sleeptime = _exponentialBackoff(initialSleep, maxSleep, callcount)
      logmsg = logmsg+". Will retry in %f seconds." % sleeptime
      retval = True
    else:
      logmsg = logmsg+". No more retries, exception will be raised."
      retval = False
    log.warning(logmsg)
    if sleeptime:
      time.sleep(sleeptime)
    return retval
  return retry(func, handledExceptions, retryLogic)

def _exponentialBackoff(sleep, maxSleep, numcalls):
  """Calculate sleep time for exponential back-off based on numcall.

  Returns value of `sleep` that is doubled for every increment of `numcalls`

  e.g. if sleep = 3:

    numcalls 1 2 3  4  5
    returns  3 6 12 24 48

  If `maxSleep` != None then function returns this as its maximum value.

  :param sleep: Initial time in seconds.
  :param maxSleep: Maximum value that can be returned by this function
    use None for no maximum.
  :param numcalls: Call number for which sleep time should be calculated.

  :return: Back-off time"""

  btime = 2.0**(numcalls-1) * sleep
  if maxSleep and btime > maxSleep:
    return maxSleep
  return btime


class SkipWhiteSpaceDictReader(csv.DictReader):
  """Version of csv.DictReader that strips whitespace from column names and values"""

  def __init__(self, *args, **kwargs):
    csv.DictReader.__init__(self, *args , **kwargs)

  @csv.DictReader.fieldnames.getter
  def fieldnames(self):
    orignames = csv.DictReader.fieldnames.fget(self)
    if orignames is None:
      return None
    return [f.strip() for f in orignames]

  def next(self):
    origdict = csv.DictReader.next(self)
    if not origdict:
      return origdict
    vals = []
    for k,v in origdict.iteritems():
      if not k is None and hasattr(k, 'strip'):
        k = k.strip()
      if not v is None and hasattr(v, 'strip'):
        v = v.strip()
      vals.append((k,v))
    return dict(vals)


class MultiCallback(list):
  """Class for combining callbacks"""

  def __call__(self, *args, **kwargs):
    retvals = []
    for cb in self:
      retvals.append(cb(*args, **kwargs))
    return retvals
