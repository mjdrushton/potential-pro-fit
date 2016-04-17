
import threading
import subprocess
import os

lock = threading.RLock()

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
      self.parent.jobstarterror(self.job_id, 'PATH_ERROR')
      return

    self.popen = subprocess.Popen([self.shell, 'runjob'], cwd = self.path, close_fds=True)
    self.parent.jobstarted(self.job_id)

    self.popen.wait()

    if self.kill_called:
      self.parent.jobdone(self.job_id, self.popen.returncode, killed = True)
    else:
      self.parent.jobdone(self.job_id, self.popen.returncode)

  def killjob(self, job_id):
    if not self.kill_called and self.job_id == job_id and not self.popen is None:
      self.kill_called = True
      self.popen.terminate()

      def hardkill(self, popen):
        if self.isAlive() and popen.poll() is None:
          popen.kill()

      t =  threading.Timer(self.hardkill_timeout, hardkill, [self, self.popen])
      t.start()


class EventLoop(threading.Thread):

  def __init__(self, channel):
    self.channel = channel
    self.busy_flag = False
    self.runcmd = None
    threading.Thread.__init__(self)

  def send(self, msg):
    with lock:
      self.channel.send(msg)

  def sendargs(self, **kwargs):
    self.send(kwargs)

  def ready(self):
    rdy = dict(msg = 'READY', channel_id = self.channel_id)
    self.send(rdy)

  def jobstarted(self, job_id):
    self.sendargs(msg = 'JOB_START', channel_id = self.channel_id, job_id = job_id)

  def jobstarterror(self, job_id, reason):
    self.sendargs(msg ='JOB_START_ERROR', channel_id = self.channel_id, job_id = job_id, reason = reason)
    with lock:
      self.runcmd = None
      self.busy_flag = False
      self.ready()


  def jobdone(self, job_id, returncode, **kwargs):
    with lock:
      self.busy_flag = False
      msg = dict(
        msg = 'JOB_END',
        channel_id = self.channel_id,
        returncode = returncode,
        job_id = job_id)
      msg.update(kwargs)
      self.runcmd = None
    self.send(msg)
    self.ready()

  def run(self):
    uuid_msg = self.channel.receive()

    try:
      uuid_msg['msg'] == 'START_CHANNEL'
      self.channel_id = uuid_msg['channel_id']
      self.shell = uuid_msg.get('shell', '/bin/bash')
      self.hardkill_timeout = uuid_msg.get('hardkill_timeout', 60)
    except:
      self.send(dict(msg="ERROR"))
      return

    self.ready()

    while True:
      msg = self.channel.receive()
      if msg is None:
        return

      mtype = msg.get('msg', None)
      if not mtype:
        continue

      with lock:
        bf = self.busy_flag

      if mtype == 'READY_QUERY':
        if not bf:
          self.ready()
        else:
          self.sendargs(msg='BUSY', channel_id = self.channel_id)
      elif mtype == 'JOB_START':
        try:
          job_id = msg["job_id"]
          path = msg["job_path"]
        except KeyError:
          self.sendargs(msg="JOB_START_ERROR", channel_id = self.channel_id, reason = "MISSING_ARGUMENTS")
          continue

        if bf:
          self.sendargs(msg = "JOB_START_ERROR", channel_id = self.channel_id, job_id = job_id, reason = "BUSY")
          continue

        self.runcmd = RunCmd(self, path, job_id, self.shell, self.hardkill_timeout)
        with lock:
          self.busy_flag = True
        self.runcmd.start()
      elif mtype == 'JOB_KILL':
        try:
          job_id = msg["job_id"]
        except KeyError:
          self.sendargs(msg = "ERROR", channel_id = self.channel_id, reason = "missing job_id for JOB_KILL")

        with lock:
          if not self.runcmd is None:
            self.runcmd.killjob(job_id)
      else:
        self.sendargs(msg = "ERROR", reason = "UNKNOWN_MSG_TYPE", msg_type = mtype)


# Used by LocalRunner and RemoteRunner to start and kill jobs
def remote_exec(channel):
  eloop = EventLoop(channel)
  eloop.start()
  eloop.join()


if __name__ == '__channelexec__':
  remote_exec(channel)
