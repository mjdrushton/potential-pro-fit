
import threading
import subprocess
import os


class RunCmd(threading.Thread):

  def __init__(self, parent, path, job_id, shell, hardkill_timeout):
    self.parent = parent
    self.path = path
    self.job_id = job_id
    self.kill_called = False

    # Seconds before kill -9 called when killjob invoked
    self.hardkill_timeout = hardkill_timeout

    # Shell that should be used to execute runjob
    self.shell = shell

    threading.Thread.__init__(self)

  def run(self):
    rjpath = os.path.join(self.path, 'runjob')

    if not os.path.isfile(rjpath):
      self.parent.jobstarterror(self.job_id, "PATH_ERROR, '%s' does not exist or is not a file." % rjpath)
      return

    self.popen = subprocess.Popen([self.shell, 'runjob'], cwd = self.path, close_fds=True)
    self.parent.jobstarted(self.job_id, self.popen.pid)

    self.popen.wait()

    if self.kill_called:
      self.jobdone(True)
    else:
      self.jobdone(False)

  def killjob(self, job_id, wait = False):
    if not self.kill_called and self.job_id == job_id and not self.popen is None:
      self.kill_called = True
      self.popen.terminate()

      def hardkill(self, popen):
        if self.isAlive() and popen.poll() is None:
          popen.kill()

      t =  threading.Timer(self.hardkill_timeout, hardkill, [self, self.popen])
      t.start()

      if wait:
        self.popen.wait()

  def jobdone(self, killed):
    try:
      with open(os.path.join(self.path, 'STATUS'), 'w') as statusfile:
        statusfile.write("%s\n" % self.popen.returncode)
    except IOError:
      pass
    self.parent.jobdone(self.job_id, self.popen.returncode, killed = killed)



class StatusHeartBeat(object):

  def __init__(self, eventLoop, heart_beat_period = 0.5):
    """Timed loop that sends 'READY|BUSY' messages every `heart_beat_period`.

    Args:
        eventLoop (EventLoop): Parent event loop.
        heart_beat_period (float, optional): Time between status messages.
    """
    self.lock = threading.RLock()
    self.eventLoop = eventLoop
    self.heart_beat_period = heart_beat_period
    self.timer = None
    self.enabled = True

  def resetTimer(self):
    with self.lock:
      if not self.enabled:
        return
      def timerCallback():
        self.eventLoop.sendStatus()
        self.timer = None
      self.timer = threading.Timer(self.heart_beat_period, timerCallback)
      self.timer.start()

class EventLoop(threading.Thread):

  def __init__(self, channel):
    self.lock = threading.RLock()
    self.channel = channel
    self.busy_flag = False
    self.runcmd = None
    self.last_status_sent = None
    self.heartbeat = StatusHeartBeat(self)
    threading.Thread.__init__(self)

  def send(self, msg):
    with self.lock:
      self.channel.send(msg)

  def sendargs(self, **kwargs):
    self.send(kwargs)

  def ready(self):

    with self.lock:
      rdy = dict(msg = 'READY', channel_id = self.channel_id)
      self.send(rdy)
      self.last_status_sent = 'READY'

  def busy(self):
    with self.lock:
      bsy = dict(msg = 'BUSY', channel_id = self.channel_id)
      self.send(bsy)
      self.last_status_sent = 'BUSY'

  def jobstarted(self, job_id, pid):
    self.sendargs(msg = 'JOB_START', channel_id = self.channel_id, job_id = job_id, pid = pid)

  def jobstarterror(self, job_id, reason):
    self.sendargs(msg ='JOB_START_ERROR', channel_id = self.channel_id, job_id = job_id, reason = reason)
    with self.lock:
      self.runcmd = None
      self.busy_flag = False
      self.sendStatus()

  def jobdone(self, job_id, returncode, **kwargs):
    with self.lock:
      self.busy_flag = False
      msg = dict(
        msg = 'JOB_END',
        channel_id = self.channel_id,
        returncode = returncode,
        job_id = job_id)
      msg.update(kwargs)
      self.runcmd = None
      self.send(msg)
      self.sendStatus()

  def run(self):
    uuid_msg = self.channel.receive()

    try:
      uuid_msg['msg'] == 'START_CHANNEL'
      self.channel_id = uuid_msg['channel_id']
      self.shell = uuid_msg.get('shell', '/bin/bash')
      self.hardkill_timeout = uuid_msg.get('hardkill_timeout', 60)
      self.heartbeat.enabled = uuid_msg.get('heartbeat_enabled', True)
    except:
      self.send(dict(msg="ERROR"))
      return

    # Check that the specified shell exists and is runnable
    if not (os.path.isfile(self.shell) and os.access(self.shell, os.X_OK)):
      self.send(dict(msg="ERROR", reason= "shell cannot be executed: '%s'" % self.shell, channel_id = self.channel_id))

    self.sendStatus()

    while True:
      msg = self.channel.receive()

      if msg is None:
        self.killJob(job_id = None, wait = True)
        return

      mtype = msg.get('msg', None)
      if not mtype:
        continue

      with self.lock:
        bf = self.busy_flag

      if mtype in ['READY', 'BUSY']:
        self.receiveStatus(msg)
      elif mtype == 'JOB_START':
        try:
          job_id = msg["job_id"]
          path = msg["job_path"]
        except KeyError:
          self.sendargs(msg="JOB_START_ERROR", channel_id = self.channel_id, reason = "MISSING_ARGUMENTS")
          continue

        with self.lock:
          if bf:
            self.sendargs(msg = "JOB_START_ERROR", channel_id = self.channel_id, job_id = job_id, reason = "BUSY")
            continue
          self.runcmd = RunCmd(self, path, job_id, self.shell, self.hardkill_timeout)
          self.busy_flag = True
          self.runcmd.start()
      elif mtype == 'JOB_KILL':
        try:
          job_id = msg["job_id"]
        except KeyError:
          self.sendargs(msg = "ERROR", channel_id = self.channel_id, reason = "missing job_id for JOB_KILL")

        self.killJob(job_id)

      else:
        self.sendargs(msg = "ERROR", reason = "UNKNOWN_MSG_TYPE", msg_type = mtype)

  def getStatus(self):
    """Returns 'BUSY' or 'READY' depending on whether a job is running or not.

    Returns:
        str: 'BUSY' or 'READY'
    """
    with self.lock:
      if self.busy_flag:
        return 'BUSY'
      else:
        return 'READY'

  def receiveStatus(self, msg):
    with self.lock:
      receivedStatus = msg['msg']
      self.last_status_sent = None

  def sendStatus(self):
    with self.lock:
      self.heartbeat.resetTimer()

      if not self.last_status_sent is None:
        return

      status = self.getStatus()

      try:
        if status == 'READY':
          self.ready()
        else:
          self.busy()
      except IOError:
        self.heartbeat.timer.cancel()

  def killJob(self, job_id = None, wait = False):
    with self.lock:
      if not self.runcmd is None:
        if job_id is None:
          job_id = self.runcmd.job_id
        self.runcmd.killjob(job_id, wait)

# Used by LocalRunner and RemoteRunner to start and kill jobs
def remote_exec(channel):
  eloop = EventLoop(channel)
  eloop.start()
  eloop.join()

if __name__ == '__channelexec__':
  remote_exec(channel)
