
class PropertySetHandler(object):

  def __init__(self, setobj, attrname, formatter = str):
    self.setobj = setobj
    self.attrname = attrname
    self.formatter = formatter

  def __call__(self, name, oldvalue, newvalue):
    setattr(self.setobj, self.attrname, self.formatter(newvalue))


class MessagesController(object):

  def __init__(self, widgetModel, mainframe):
    self.mainframe = mainframe
    self.widgetModel = widgetModel
    self._registerHandlers()

  def _registerHandlers(self):
    self.widgetModel.observers.visible = self._visibleHandler
    self.widgetModel.lines.set_modified_callback(self._messageHandler)

  def _visibleHandler(self, name, oldvalue, newvalue):
    if newvalue:
      self.mainframe.showMessageBox()
    else:
      self.mainframe.hideMessageBox()

  def _messageHandler(self):
    self.mainframe.messageBox.messages[:] = self.widgetModel.lines

class RunnerController(object):

  def __init__(self, widgetModel, widget):
    self.widgetModel = widgetModel
    self.widget = widget
    self._registerHandlers()

  def _registerHandlers(self):
    m = self.widgetModel
    m.observers.title = PropertySetHandler(self.widget, 'title')

    progressbars = [self.widget.pb_finished,
                    self.widget.pb_upload,
                    self.widget.pb_running]

    for pb in progressbars:
      m.observers.total_jobs = PropertySetHandler(pb, 'done', int)

    m.observers.uploaded = PropertySetHandler(self.widget.pb_upload, 'current', int)
    m.observers.downloaded = PropertySetHandler(self.widget.pb_finished, 'current', int)
    m.observers.running = PropertySetHandler(self.widget.pb_running, 'current', int)


class RunnersController(object):

  def __init__(self, model, mainframe):
    self.model = model
    self.mainframe = mainframe
    self._registerHandlers()
    self._synchroniseViewAndModel()

  def _registerHandlers(self):
    self.model.runners.set_modified_callback(self._synchroniseViewAndModel)

    # Register handlers for the overview
    RunnerController(self.model.runner_overview, self.mainframe.jobs.totalProgressBars)

  def _synchroniseViewAndModel(self):
    runnerWidgets = []
    for runnerModel in self.model.runners:
      widget = self._makeRunnerWidget(runnerModel)
      runnerWidgets.append(widget)
    self.mainframe.jobs.runners.runnerWidgets[:] = runnerWidgets

  def _makeRunnerWidget(self, runnerModel):
    widget = self.mainframe.jobs.runners.createRunner()
    RunnerController(runnerModel, widget)
    return widget

class ConsoleController(object):
  """The glue between ConsoleModel and MainFrame, this class is responsible for
  registering event handlers on the Model and linking them to the correct
  widgets in MainFrame"""

  def __init__(self, model, mainframe):
    """Create `ConsolController`

    Args:
        model (ConsoleModel): Data model for the console.
        mainframe (MainFrame): Console GUI.
    """
    self.model = model
    self.mainframe = mainframe
    self._registerHeaderHandlers()
    self._runnersController = RunnersController(self.model, self.mainframe)
    self._messagesController = MessagesController(self.model.messages, self.mainframe)

  def _registerHeaderHandlers(self):
    mf = self.mainframe

    # Run Name
    self.model.observers.run_name =PropertySetHandler(mf.header, 'run_name')

    # Iteration numbers
    self.model.current_iteration.observers.iteration_number = PropertySetHandler(mf.header, 'current_iteration_number')
    self.model.best_iteration.observers.iteration_number = PropertySetHandler(mf.header, 'best_iteration_number')

    # Merit values
    self.model.current_iteration.observers.merit_value = PropertySetHandler(mf.header, 'current_merit_value')
    self.model.best_iteration.observers.merit_value = PropertySetHandler(mf.header, 'best_merit_value')

    # Variables
    self.model.observers.variables_table = self._updateVariables

  def _updateVariables(self, name, oldvalue, newvalue):
    self.mainframe.variables.update(self.model.variables_table)







