import threading
import logging
import execnet

# Execnet job runner, used by _InnerRunner
def _runJob(channel):
  import subprocess
  import os
  import shutil
  import stat

  channel.send( ('ready', None) )
  for x in channel:
    msgtype, msgdata = x
    if msgtype == 'run':
      batchid = msgdata['batchid']
      jobpath = msgdata['jobpath']
      jobid = msgdata['jobid']
      oldchdir = os.getcwd()
      destdir = os.path.join(jobpath, 'output')
      try:
        shutil.copytree(jobpath, destdir)
        os.chdir(destdir)
        # Make runjob executable
        os.chmod('runjob', stat.S_IRWXU)
        status = subprocess.call('./runjob', shell = True)
        with open('STATUS', 'wb') as outfile:
          print >>outfile, "%d" % status
        channel.send( ('finished', dict(batchid=batchid, jobid = jobid) ))
      except Exception as e:
        channel.send( ('error', dict(errormsg = str(e), batchid = batchid, jobid = jobid)))
      finally:
        os.chdir(oldchdir)
    else:
      channel.send( ('error', dict(errormsg = 'Unknown command: %s' % msgtype)))

# Define a callback for channels.
class _CallBack(object):
  def __init__(self, innerrun, numchannels, channelreadyevent, logger):
    self.readycount = 0
    self.numchannels = numchannels
    self.channelreadyevent = channelreadyevent
    self.innerrun = innerrun
    self._logger = logger

  def __call__(self, o):
    msg_type, msg_dict = o
    if msg_type == 'ready':
      # Wait for channels to be ready.
      self.readycount += 1
      self._logger.debug('%d/%d Channels ready' % (self.readycount, self.numchannels))
      if self.readycount == self.numchannels:
        self.channelreadyevent.set()
    elif msg_type == 'finished':
      batchid = msg_dict['batchid']
      jobid = msg_dict['jobid']
      self.innerrun._jobFinished(jobid, batchid)
    elif msg_type == 'error':
      batchid = msg_dict['batchid']
      jobid = msg_dict['jobid']
      msg = msg_dict['errormsg']
      self.innerrun._jobError(jobid, batchid, msg)

class _InnerRunner(threading.Thread):
  """Thread responsible for managing batches.

  Waits for batches coming from LocalRunner on threading.Queue
  and submits them to execnet channels.

  When complete signals the threading.Event object internal
  to LocalRunnerFuture to stop blocking."""

  _logger = logging.getLogger('atomsscripts.fitting.runners._InnerRunner')

  def __init__(self, queue, nprocs, execnetURL = None):
    """@param queue threading.Queue instance from which job batches are received
       @param nprocs Number of execnet channels to set-up
       @param execnet Url used to create execnet gateways"""
    threading.Thread.__init__(self)
    self._batchqueue = queue
    self._nprocs = nprocs
    self._batchid = 0
    self._gwurl = execnetURL
    self._lock = threading.Lock()
    self.daemon = True

    # Links batch number to job information and blocking Event
    # Batch D
    self._batchDict = {}

  def run(self):
    import itertools
    channels = [ execnet.makegateway(self._gwurl).remote_exec(_runJob) for i in xrange(self._nprocs) ]
    channeliter = itertools.cycle(channels)

    # Communication with channels is in form of ( msg_type, msg_dict )
    channelreadyevent = threading.Event()

    cb = _CallBack(self, len(channels), channelreadyevent, self._logger)
    for ch in channels:
      ch.setcallback(cb)

    # Wait for channels to be ready
    if not channelreadyevent.wait(5.0):
      self._logger.error("Channel start-up timed out")
      return

    self._logger.debug('Starting batch queue monitoring loop')
    while True:
      batch = self._batchqueue.get(True)
      self._startBatch(batch, channeliter)
      self._batchqueue.task_done()


  def _startBatch(self, batch, channeliter):
    """@param batch ( event, inputs) tuple. Where event is threading.Event instance and
                    inputs is a list of directory names.
    @param channeliter Iterator over execnet.Channel instances"""
    self._logger.debug('Starting batch: %d' % self._batchid)
    batchid = self._batchid
    jobs = batch._jobs

    self._batchDict[batchid] = (len(jobs), batch, [])

    batchdata = []
    for i, job in enumerate(jobs):
      d = dict(batchid = batchid, jobid = i, jobpath = job.path)
      batchdata.append( ('run', d) )
    self._batchid += 1

    for channel, d in zip(channeliter, batchdata):
      channel.send(d)

  def _jobFinished(self, jobid, batchid):
    with self._lock:
      stillrunning, batch, errorlist = self._batchDict[batchid]
      event = batch._e
      stillrunning = stillrunning - 1
      self._logger.debug('Job %d finished in batch %d. %d are still running.' %  (jobid, batchid, stillrunning))
      if stillrunning == 0:
        del self._batchDict[batchid]

        # Update jobsWithErrors and errorflag
        if not errorlist:
          batch.errorFlag = False
        else:
          batch.errorFlag = True

        jobsWithErrors = [ (batch._jobs[i], msg) for (i,msg) in sorted(errorlist) ]
        batch.jobsWithErrors = jobsWithErrors

        event.set()
        self._logger.debug('Batch %d finished' % batchid)
      else:
        self._batchDict[batchid] = stillrunning, batch, errorlist


  def _jobError(self, jobid, batchid, errormsg):
    with self._lock:
      stillrunning, batch, errorlist = self._batchDict[batchid]
      errorlist.append( (jobid, errormsg) )
      self._logger.debug('Job %d in batch %d, experienced error condition: %s' %  (jobid, batchid, errormsg ))
    self._jobFinished(jobid, batchid)
