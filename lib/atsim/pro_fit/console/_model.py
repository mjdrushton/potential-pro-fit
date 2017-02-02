from collections import OrderedDict

from _variables import CurrentBestTuple

import weakref

class ObserverRegister(object):

  def __init__(self, model):
    super(ObserverRegister,self).__setattr__('_model',model)
    super(ObserverRegister,self).__setattr__('_observerdict',{})

  def __setattr__(self, name, observer):
    getattr(self._model, name)
    self._observerdict.setdefault(name, []).append(observer)


class ObservableObject(object):
  """Object that intercepts when attributes are set. If an attribute has an associated handler, then the handler is called.

  Handlers are set through this object's `.observers` collection. The following object sets a handler for the `alpha`
  property:

  ```observable_model.observers.alpha = handler```

  Where `handler` is a callable with the following signature:

  .. code-block:: python

    def handler(attribute_name, old_value, new_value):
      pass


  Where ``attribute_name`` is the name of the attribute being changed and ``old_value`` is the attribute's value before
  it is changed and ``new_value`` is the value after the change.
  """

  def __init__(self, model):
    """:param model: Plain object which defines the variables settable in the `ObservableObject`"""
    super(ObservableObject, self).__setattr__('_normal_setattr', True)
    self._model = model
    self.observers = ObserverRegister(model)
    self._normal_setattr = False

  def __setattr__(self, name, value):
    if name in self.__dict__ or self._normal_setattr:
      return super(ObservableObject, self).__setattr__(name, value)

    model = self._model
    observers = self.observers
    oldvalue = getattr(model, name)
    handlers = None
    try:
      handlers = observers._observerdict[name]
    except KeyError:
      pass

    setattr(model, name, value)
    if handlers:
      for handler in handlers:
        handler(name, oldvalue, value)

  def __getattr__(self, name):
    return getattr(self._model, name)


class _IterationModel(object):
  def __init__(self):
    self.iteration_number = 0
    self.merit_value = None
    self.variables = None

class IterationModel(ObservableObject):

  def __init__(self):
    super(IterationModel, self).__init__(_IterationModel())


class _VariableUpdateHandler(object):

  def __init__(self, consoleModel):
    self._consoleModel = weakref.proxy(consoleModel)

    # Register this handler with the _consoleModel
    self._consoleModel.current_iteration.observers.variables = self
    self._consoleModel.best_iteration.observers.variables = self

  def _updateVariablesTable(self):
    table = OrderedDict()

    def mkname(n, isfit):
      prefix = '  '
      if isfit:
        prefix = '* '
      return prefix + str(n)

    if self._consoleModel.current_iteration.variables:
      for k, v, isfit in self._consoleModel.current_iteration.variables.flaggedVariablePairs:
        table[k] = {'name' : mkname(k, isfit), 'current' : v, 'best' : None}

    if self._consoleModel.best_iteration.variables:
      for k, v, isfit in self._consoleModel.best_iteration.variables.flaggedVariablePairs:
        table.setdefault(k, {'name' : mkname(k,isfit), 'current' : None, 'best' : None})['best'] = v

    # Convert to tuples
    tuple_table = []
    for d in table.itervalues():
      tuple_table.append(CurrentBestTuple(d['name'], d['current'], d['best']))

    self._consoleModel.variables_table = tuple_table

  def __call__(self, name, oldvalue, newvalue):
    self._updateVariablesTable()


class _ConsoleModel(object):
  def __init__(self):
    self.run_name = "NA"
    self.variables_table = []

class ConsoleModel(ObservableObject):

    def __init__(self):
      super(ConsoleModel, self).__init__(_ConsoleModel())
      self._normal_setattr = True
      self.current_iteration = IterationModel()
      self.best_iteration = IterationModel()
      _VariableUpdateHandler(self)
      self._normal_setattr = False

