from _mainframe import MainFrame
from _model import ConsoleModel, RunnerModel, _RunnerModel
from _controller import ConsoleController
from _palette import palette

import urwid
from _urwid_geventloop import GeventLoop

import gevent

import atsim.pro_fit

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

    attrs = ['total_jobs', 'uploaded', 'running', 'downloaded']
    for batch in self._batches.itervalues():
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

    self.model.messages.lines.append("Potential Pro-Fit v%s" % atsim.pro_fit.__version__)
    self.model.messages.lines.append("")
    self.model.messages.lines.append("Starting...")
    self.model.messages.visible = True

  def registerConfig(self, cfg):
    """Initialise the console with a FitConfig object.

    This triggers the display of the main  GUI.

    Args:
        cfg (atsim.pro_fit.fittool.FitConfig): Config used to initialise the console

    """
    self.model.endEvent = cfg.endEvent
    self.model.closedEvent = cfg.closedEvent
    gevent.spawn(self._killMainLoopOnEvent, self.model.closedEvent)

    self.model.run_name = cfg.title
    self._initialiseRunnerModel(cfg)

    # Partially populate the variables table
    self.model.current_iteration.variables = cfg.variables

    self.mainframe.showMainPage()

    # Hide the initial messages after a delay of one second
    def hidemessages():
      self.model.messages.visible = False
    gevent.spawn_later(1.0, hidemessages)

  def _initialiseRunnerModel(self, cfg):
    # Create the runners
    for runnername, runner in cfg.runners.iteritems():
      rm = RunnerModel()
      self.model.runners.append(rm)
      listener = BatchObserver(runnername, runner, rm)
      rm.title = runnername


  def stepCallback(self, minimizerResults):
    self.model.current_iteration.iteration_number += 1
    self.model.current_iteration.merit_value = minimizerResults.bestMeritValue
    self.model.current_iteration.variables = minimizerResults.bestVariables

    if self.model.best_iteration.merit_value is None or self.model.current_iteration.merit_value < self.model.best_iteration.merit_value:
      self.model.best_iteration.iteration_number = self.model.current_iteration.iteration_number
      self.model.best_iteration.merit_value = self.model.current_iteration.merit_value
      self.model.best_iteration.variables = self.model.current_iteration.variables


  def log(self, logger, level, message):
    """Write a message to logger and to the console messages

    Args:
        logger (logging.Logger): Logger to which log message will be written
        level : Log level
        message (str): Message to be logged
    """
    logger.log(level, message)
    self.model.messages.lines.append(message)

  def terminalError(self, message):
    """Show a modal dialog indicating a terminal error.

    When the dialog's exit button is pressed, the console termianted and the
    event returned by this method is set.

    Args:
        message (str): Message to be displayed in the dialog

    Returns:
        gevent.event.Event: Event object that is set when the okay button of the dialog is pressed.
    """

    self.model.messages.visible = False
    evt = gevent.event.Event()

    def exitcallback(loop = None, data = None):
      evt.set()
      self._killMainLoop()
    self.mainframe.showErrorMessage(message, exitcallback)
    return evt

  def _run_main_loop(self):
    self._main_loop.run()

  def _exit_on_q(self, key):
      if key in ('q', 'Q'):
        if self.model and self.model.endEvent:
          self.model.endEvent.set()

  def _killMainLoop(self):
    def term(loop = None, data = None):
      raise urwid.ExitMainLoop()
    self._gevent_loop.enter_idle(term)

  def _killMainLoopOnEvent(self, evt):
    evt.wait()
    self._killMainLoop()

  def start(self):
    self._gevent_loop = GeventLoop()
    self._main_loop = urwid.MainLoop(self.mainframe, palette, event_loop=self._gevent_loop, unhandled_input = self._exit_on_q)
    self._greenlet = gevent.spawn(self._run_main_loop)
    self.started = True

