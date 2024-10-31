import gevent
from gevent import select
from urwid import ExitMainLoop, EventLoop
from collections import deque
import signal
import sys
import functools
import logging

class GeventLoop(EventLoop):
    """
    Event loop based on gevent
    """
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self._alarms = []
        self._watch_files = {}
        self._idle_callbacks = {}
        self._completed_greenlets = deque()
        self._idle_event = gevent.event.Event()
        self._exc = None
        self.exit_flag = False  # Flag to signal exit for _file_watch_loop

    def run_in_executor(self, executor, func, *args, **kwargs):
        """Run callable in executor."""
        return executor.submit(func, *args, **kwargs)

    def alarm(self, seconds, callback):
        """Set an alarm with a delay in seconds."""
        greenlet = gevent.spawn_later(seconds, self.handle_exit(callback))
        greenlet.name = f"GeventLoop_alarm-{greenlet.name}"
        greenlet.link(self._greenlet_completed)
        self._alarms.append(greenlet)
        return greenlet

    def remove_alarm(self, handle):
        """Remove an alarm."""
        try:
            handle.kill()
            self._alarms.remove(handle)
            return True
        except ValueError:
            return False

    def watch_file(self, fd, callback):
        """Watch a file descriptor for input."""
        greenlet = gevent.spawn(self._file_watch_loop, fd, self.handle_exit(callback))
        greenlet.name = f"GeventLoop_watch_file-{greenlet.name}"
        self._watch_files[fd] = greenlet
        return fd

    def _file_watch_loop(self, fd, callback):
        """File watch loop with a timeout and exit condition."""
        while not self.exit_flag:
            ready, _, _ = select.select([fd], [], [], 0.1)  # 0.1-second timeout
            if ready:
                callback()

    def remove_watch_file(self, handle):
        """Remove a file descriptor watch."""
        if handle in self._watch_files:
            self._watch_files[handle].kill()
            del self._watch_files[handle]
            return True
        return False

    def enter_idle(self, callback):
        """Add a callback for entering idle."""
        handle = id(callback)
        self._idle_callbacks[handle] = callback
        return handle

    def remove_enter_idle(self, handle):
        """Remove an idle callback."""
        return self._idle_callbacks.pop(handle, None) is not None

    def _run_idle_callbacks(self):
        """Run all idle callbacks."""
        for callback in list(self._idle_callbacks.values()):
            callback()

    def run(self):
        """Start the event loop and handle exceptions."""
        try:
            while True:
                # Process completed greenlets
                while self._completed_greenlets:
                    self._completed_greenlets.popleft().get(block=False)

                # Poll stdin for input
                if sys.stdin in select.select([sys.stdin], [], [], 0.01)[0]:
                    keys = sys.stdin.read(1)
                    self.process_input([keys])

                # Run idle callbacks
                self._run_idle_callbacks()
                
                # Wait for any idle events or alarms
                self._idle_event.wait(timeout=0.01)
                self._idle_event.clear()
        except ExitMainLoop:
            pass
        finally:
            self.stop()
            if self._exc:
                raise self._exc

    def _greenlet_completed(self, greenlet):
        """Handle greenlet completion."""
        self._completed_greenlets.append(greenlet)
        self._idle_event.set()

    def stop(self):
        """Stop the event loop by killing alarms, file watchers, and idle callbacks."""
        self.exit_flag = True  # Set exit flag to terminate _file_watch_loop
        for alarm in self._alarms:
            alarm.kill()
        for greenlet in self._watch_files.values():
            greenlet.kill()
        self._alarms.clear()
        self._watch_files.clear()
        self._idle_callbacks.clear()
        self._completed_greenlets.clear()

    def set_signal_handler(self, signum, handler):
        """Sets a signal handler using gevent's signal handling."""
        if handler == signal.SIG_IGN:
            return
        if handler == signal.SIG_DFL:
            return
        gevent.signal_handler(signum, self.handle_exit(handler))

    def handle_exit(self, f):
        """Decorator that handles exit and exception in callbacks."""
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except ExitMainLoop:
                self.stop()
            except BaseException as exc:
                self._exc = exc
                self.stop()
            return False
        return wrapper