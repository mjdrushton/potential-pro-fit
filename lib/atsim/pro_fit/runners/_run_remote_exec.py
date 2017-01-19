
import threading
import subprocess
import os
import collections
import Queue
from multiprocessing import cpu_count
import traceback
import time

JobTuple = collections.namedtuple('JobTuple', ['job_id', 'path'])


class RunCmd(threading.Thread):

  def __init__(self, parent, shell, hardkill_timeout, semval):
    self.lock = threading.RLock()
    self.parent = parent
    self.kill_called = False
    self.path = None
    self.job_id = None
    self.semval = semval
    self.finishedEvent = threading.Event()

    # Seconds   before kill -9 called when killjob invoked
    self.hardkill_timeout = hardkill_timeout

    # Shell that should be used to execute runjob
    self.shell = shell

    self.popen = None

    threading.Thread.__init__(self)

  def run(self):
    try:
      with self.lock:
        if self.kill_called:
          return self.jobdone(True)

      rjpath = os.path.join(self.path, 'runjob')

      if not os.path.isfile(rjpath):
        self.parent.jobstarterror(self.job_id, "PATH_ERROR, '%s' does not exist or is not a file." % rjpath)
        return

      with self.lock:
        self.popen = subprocess.Popen([self.shell, 'runjob'], cwd = self.path, close_fds=True)
        self.parent.jobstarted(self.job_id, self.popen.pid, self.semval)

      self.popen.wait()

      with self.lock:
        if self.kill_called:
          self.jobdone(True)
        else:
          self.jobdone(False)
    finally:
      self.parent.runcmdfinished(self)
      self.finishedEvent.set()

  def killjob(self, wait = False):
    olkk = self.kill_called
    self.kill_called = True
    if not olkk and not self.popen is None:
      self.popen.terminate()

      def hardkill(self, popen):
        if self.isAlive() and popen.poll() is None:
          popen.kill()

      t =  threading.Timer(self.hardkill_timeout, hardkill, [self, self.popen])
      t.start()

      if wait:
        self.finishedEvent.wait()

  def jobdone(self, killed):
    with self.lock:
      try:
        with open(os.path.join(self.path, 'STATUS'), 'w') as statusfile:
          statusfile.write("%s\n" % self.popen.returncode)
      except IOError:
        pass
      self.parent.jobdone(self.job_id, self.popen.returncode, killed = killed)



class Runners(threading.Thread):

  def __init__(self, parent, nprocesses):
    threading.Thread.__init__(self,)
    self._parent = parent
    self._semaphore = threading.BoundedSemaphore(nprocesses)
    self._lock = threading.RLock()
    self._queued_jobs = Queue.Queue()
    self._runcmd =  set()
    self._killevent = threading.Event()
    self._timeout = 0.01

  def run(self):
    while not self._killevent.is_set():
      # Prepare a runner
      while not self._semaphore.acquire(False):
        if self._killevent.is_set():
          return
        time.sleep(self._timeout)

      runcmd = self._makeruncmd()

      job = None
      while not job:
        if self._killevent.is_set():
          return
        try:
          job = self._queued_jobs.get(True, self._timeout)
        except Queue.Empty:
          pass

      if self._killevent.is_set():
        return
      runcmd.job_id = job.job_id
      runcmd.path = job.path
      with runcmd.lock:
        killcalled = runcmd.kill_called
      if not  killcalled:
        runcmd.start()
      else:
        self.jobdone(runcmd.job_id)


  def runjob(self, job_id, job_path):
      if self._killevent.is_set():
        return
      jobtuple = JobTuple(job_id, job_path)
      self._queued_jobs.put(jobtuple)

  def killjob(self, job_id):
    # Has job been run? If not remove from the queued jobs
    with self._queued_jobs.mutex:
      jobtuples = [ j for j in self._queued_jobs.queue if j.job_id == job_id]
      if jobtuples:
        self._queued_jobs.queue.remove(jobtuples[0])
        self.jobdone(job_id, None, killed = True)
        return True

    # Is the job currently running?
    found = None
    for runcmd in list(self._runcmd):
      if runcmd.job_id == job_id:
        found = runcmd
        break
    if found:
      if found.isAlive():
        found.killjob()
      else:
        with found.lock:
          found.kill_called = True
        # self._runcmd.remove(found)
      return True
    return False


  def runcmdfinished(self, runcmd):
    self._semaphore.release()
    with self._lock:
      try:
        self._runcmd.remove(runcmd)
      except IndexError:
        pass

  def jobdone(self, job_id, returncode, **kwargs):
    return self._parent.jobdone(job_id, returncode, **kwargs)

  def jobstarterror(self, job_id, msg):
    return self._parent.jobstarterror(job_id, msg)

  def jobstarted(self, job_id, pid, semaphore):
    return self._parent.jobstarted(job_id, pid, semaphore)

  def _makeruncmd(self):
    semval = self._semaphore._initial_value -  self._semaphore._Semaphore__value
    cmd = RunCmd(self, self._parent.shell, self._parent.hardkill_timeout, semval)
    with self._lock:
      self._runcmd.add(cmd)
    return cmd

  def terminate(self):
    # self._killevent.set()
    # with self._lock:
    #   for runcmd in list(self._runcmd):
    #     with runcmd.lock:
    #       runcmd.hardkill_timeout = 0
    #     runcmd.killjob(wait=True)

    self._killevent.set()
    with self._queued_jobs.mutex:
      job_ids = [ j.job_id for j in self._queued_jobs.queue]
    for job_id in job_ids:
      self.killjob(job_id)
    joins = []
    job_ids = []
    for runcmd in list(self._runcmd):
      if runcmd.job_id != None:
        self.killjob(runcmd.job_id)
      if runcmd.isAlive():
        joins.append(runcmd)

    for j in joins:
      j.join(60.0)


class EventLoop(threading.Thread):

  def __init__(self, channel):
    self.lock = threading.RLock()
    self.channel = channel
    self.busy_flag = False
    self.runcmd = None
    threading.Thread.__init__(self)

  def send(self, msg):
    self.channel.send(msg)

  def sendargs(self, **kwargs):
    self.send(kwargs)

  def ready(self):
    rdy = dict(msg = 'READY', channel_id = self.channel_id)
    self.send(rdy)

  def jobstarted(self, job_id, pid, semaphore):
    self.sendargs(msg = 'JOB_START', channel_id = self.channel_id, job_id = job_id, pid = pid, semaphore = semaphore)

  def jobstarterror(self, job_id, reason):
    self.sendargs(msg ='JOB_START_ERROR', channel_id = self.channel_id, job_id = job_id, reason = reason)

  def jobdone(self, job_id, returncode, **kwargs):
      msg = dict(
        msg = 'JOB_END',
        channel_id = self.channel_id,
        returncode = returncode,
        job_id = job_id)
      msg.update(kwargs)
      self.send(msg)

  def run(self):

    uuid_msg = self.channel.receive()
    try:
      uuid_msg['msg'] == 'START_CHANNEL'
      self.channel_id = uuid_msg['channel_id']
      self.shell = uuid_msg.get('shell', '/bin/bash')
      self.hardkill_timeout = uuid_msg.get('hardkill_timeout', 60)
      try:
        self.nprocesses = uuid_msg.get('nprocesses', cpu_count())
      except NotImplementedError:
        self.nprocesses = 1
    except:
      self.send(dict(msg="ERROR", reason= traceback.format_exc()))
      return

    # Check that the specified shell exists and is runnable
    if not (os.path.isfile(self.shell) and os.access(self.shell, os.X_OK)):
      self.send(dict(msg="ERROR", reason= "shell cannot be executed: '%s'" % self.shell, channel_id = self.channel_id))
      return

    self.ready()

    self.runners = Runners(self, self.nprocesses)
    self.runners.start()
    for msg in self.channel:

      if msg is None:
        break

      mtype = msg.get('msg', None)
      if not mtype:
        continue

      if mtype == 'JOB_START':
        try:
          job_id = msg["job_id"]
          path = msg["job_path"]
        except KeyError:
          self.sendargs(msg="JOB_START_ERROR", channel_id = self.channel_id, reason = "MISSING_ARGUMENTS")
          continue

        self.runners.runjob(job_id, path)
      elif mtype == 'JOB_KILL':
        try:
          job_id = msg["job_id"]
        except KeyError:
          self.sendargs(msg = "ERROR", channel_id = self.channel_id, reason = "missing job_id for JOB_KILL")

        self.runners.killjob(job_id)

      else:
        self.sendargs(msg = "ERROR", reason = "UNKNOWN_MSG_TYPE", msg_type = mtype)

    self.runners.terminate()
    self.runners.join()
    return

# Used by LocalRunner and RemoteRunner to start and kill jobs
def remote_exec(channel):
  eloop = EventLoop(channel)
  eloop.start()
  eloop.join()

if __name__ == '__channelexec__':
  remote_exec(channel)
