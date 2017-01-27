

class PropertySetHandler(object):

  def __init__(self, setobj, attrname, formatter = str):
    self.setobj = setobj
    self.attrname = attrname
    self.formatter = formatter

  def __call__(self, name, oldvalue, newvalue):
    setattr(self.setobj, self.attrname, self.formatter(newvalue))


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

  def _registerHeaderHandlers(self):
    mf = self.mainframe

    # Iteration numbers
    self.model.current_iteration.observers.iteration_number = PropertySetHandler(mf.header, 'current_iteration_number')
    self.model.best_iteration.observers.iteration_number = PropertySetHandler(mf.header, 'best_iteration_number')

    # Merit values
    self.model.current_iteration.observers.merit_value = PropertySetHandler(mf.header, 'current_merit_value')
    self.model.best_iteration.observers.merit_value = PropertySetHandler(mf.header, 'best_merit_value')




