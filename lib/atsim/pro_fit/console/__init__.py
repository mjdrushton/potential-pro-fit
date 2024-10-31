from ._mainframe import MainFrame
from ._model import ConsoleModel, RunnerModel, _RunnerModel
from ._controller import ConsoleController
from ._palette import palette

import urwid
from ._urwid_geventloop import GeventLoop

import gevent

import atsim.pro_fit.runners
import atsim.pro_fit
import atsim.pro_fit._version

import logging


class JobObserver(atsim.pro_fit.runners.RunnerJobObserverAdapter):
    def __init__(self, batchObserver, runnerModel):
        self.batchObserver = batchObserver
        self.runnerModel = runnerModel

    def jobUploadFinished(self, job):
        self.runnerModel.uploaded += 1
        self.batchObserver._updated()

    def jobPidSet(self, job):
        self.runnerModel.running += 1
        self.batchObserver._updated()

    def jobFinished(self, job, exception):
        self.runnerModel.downloaded += 1
        self.batchObserver._updated()


class BatchObserver(atsim.pro_fit.runners.BaseRemoteRunnerObserverAdapter):
    def __init__(self, runnername, runner, consoleRunnerModel):
        self._runnerName = runnername
        self._runner = runner
        self._consoleRunnerModel = consoleRunnerModel
        self._batches = {}

        # Register this listener with the runner
        runner.observers.append(self)

    def batchCreated(self, runner, batch):
        rm = _RunnerModel()
        rm.total_jobs = len(batch.jobs)
        self._batches[batch] = rm
        self._registerJobs(batch, rm)
        self._updated()

    def batchFinished(self, runner, batch, exception):
        del self._batches[batch]
        self._updated()

    def _registerJobs(self, batch, rm):
        jo = JobObserver(self, rm)
        for job in batch.jobs:
            job.observers.append(jo)

    def _updated(self):
        rm = _RunnerModel()

        attrs = ["total_jobs", "uploaded", "running", "downloaded"]
        for batch in self._batches.values():
            for attr in attrs:
                v = getattr(batch, attr)
                setattr(rm, attr, getattr(rm, attr) + v)

        # Now assign values to the consoleRunnerModel to trigger GUI update
        for attr in attrs:
            v = getattr(rm, attr)
            setattr(self._consoleRunnerModel, attr, v)

class Console(object):
    def __init__(self):
        self.started = False
        self.model = ConsoleModel()
        self.mainframe = MainFrame()
        self._controller = ConsoleController(self.model, self.mainframe)
        self._greenlet = None
        self.model.messages.visible = True
        gevent.sleep(0)

    def registerConfig(self, cfg):
        """Initialize the console with a FitConfig object and display main GUI."""
        self.model.fit_cfg_end_event = cfg.endEvent

        def show_shutdown(evt):
            gevent.wait([evt])
            if evt.is_set():
                self._monitor_shutdown()

        grn = gevent.spawn(show_shutdown, self.model.fit_cfg_end_event)
        grn.name = f"Console-registerConfig-{grn.name}"

        self.model.run_name = cfg.title
        self._initialiseRunnerModel(cfg)

        # Partially populate variables table
        self.model.current_iteration.variables = cfg.variables
        self.mainframe.showMainPage()

        # Hide initial messages after a delay of one second
        def hidemessages():
            self.model.messages.visible = False

        grn = gevent.spawn_later(1.0, hidemessages)
        grn.name = f"Hide_Messages-{grn.name}"

    def _initialiseRunnerModel(self, cfg):
        """Initialize runners for the model based on configuration."""
        for runnername, runner in cfg.runners.items():
            rm = RunnerModel()
            self.model.runners.append(rm)
            _listener = BatchObserver(runnername, runner, rm)
            rm.title = runnername

    def stepCallback(self, minimizerResults):
        """Update current iteration based on minimizer results."""
        self.model.current_iteration.iteration_number += 1
        self.model.current_iteration.merit_value = minimizerResults.bestMeritValue
        self.model.current_iteration.variables = minimizerResults.bestVariables

        if (
            self.model.best_iteration.merit_value is None
            or self.model.current_iteration.merit_value < self.model.best_iteration.merit_value
        ):
            self.model.best_iteration.iteration_number = self.model.current_iteration.iteration_number
            self.model.best_iteration.merit_value = self.model.current_iteration.merit_value
            self.model.best_iteration.variables = self.model.current_iteration.variables

    def terminalError(self, message):
        """Show a modal dialog indicating a terminal error."""
        self.model.messages.visible = False
        evt = gevent.event.Event()

        def dialogue_close_callback(loop=None, data=None):
            evt.set()

        self.mainframe.showErrorMessage(message, dialogue_close_callback)
        return evt

    def _run_main_loop(self):
        """Start the main event loop."""
        self._main_loop.run()

    def _exit_on_q(self, key):
        """Exit the application if 'q' or 'Q' is pressed."""
        if key in ("q", "Q"):
            if self.model and self.model.fit_cfg_end_event:
                self.model.fit_cfg_end_event.set()

    def _monitor_shutdown(self):
        """Log and display shutdown message."""
        logger = logging.getLogger("console.shutdown")
        self.model.messages.lines[:] = []
        self.model.messages.visible = True
        logger.info("Potential Pro-Fit Shutting Down Now")

    def _killMainLoop(self):
        """Trigger the main loop to exit by raising ExitMainLoop."""
        def term(loop=None, data=None):
            raise urwid.ExitMainLoop()

        self._gevent_loop.enter_idle(term)

    def start(self):
        """Initialize and start the main event loop."""
        self._gevent_loop = GeventLoop()
        self._main_loop = urwid.MainLoop(
            self.mainframe,
            palette,
            event_loop=self._gevent_loop,
            unhandled_input=self._exit_on_q,
        )
        
        # Initialize idle_handle via GeventLoop's enter_idle method
        self._main_loop.idle_handle = self._gevent_loop.enter_idle(self._main_loop.draw_screen)

        self._greenlet = gevent.spawn(self._run_main_loop)
        self._greenlet.name = f"Console_Main_Loop-{self._greenlet.name}"
        self.started = True

    def stop(self):
        """Stop the main loop and perform any necessary cleanup."""
        # Remove idle_handle if it was set, then clear reference
        if self._main_loop.idle_handle is not None:
            self._gevent_loop.remove_enter_idle(self._main_loop.idle_handle)
            self._main_loop.idle_handle = None

    def close(self):
        """Gracefully close the console."""
        waitable = [self.model.close(), self._greenlet]
        self._killMainLoop()

        def _close(closed_event, waitable):
            """Wait for resources to close and stop the main loop."""
            try:
                gevent.wait(waitable)
                # Catch and ignore AttributeError if idle_handle is missing
                try:
                    self._main_loop.stop()
                except AttributeError:
                    logging.getLogger("console").debug("MainLoop idle_handle already cleared.")
                closed_event.set()
            except gevent.exceptions.LoopExit:
                logging.getLogger("console").debug("Caught LoopExit during close.")
            finally:
                gevent.sleep(0)  # Ensure gevent processes final cleanup

        evt = gevent.event.Event()
        grn = gevent.spawn(_close, evt, waitable)
        grn.name = f"Console-close-{grn.name}"
        return evt