
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
    super(ObservableObject, self).__setattr__('_model', model)
    super(ObservableObject, self).__setattr__('_model', model)
    super(ObservableObject, self).__setattr__('observers', ObserverRegister(model))

  def __setattr__(self, name, value):
    if name in self.__dict__:
      return super(ObservableObject, self).__setattr__(name, value)

    model = self._model
    observers = self.observers
    oldvalue = getattr(model, name)
    handlers = None
    try:
      handlers = observers._observerdict[name]
    except KeyError:
      pass

    if handlers:
      for handler in handlers:
        handler(name, oldvalue, value)
    return setattr(model, name, value)

  def __getattr__(self, name):
    return getattr(self._model, name)


class _IterationModel(object):
  def __init__(self):
    self.iteration_number = 0
    self.merit_value = None

class IterationModel(ObservableObject):

  def __init__(self):
    super(IterationModel, self).__init__(_IterationModel())


class ConsoleModel(object):

    def __init__(self):
      self.current_iteration = IterationModel()
      self.best_iteration = IterationModel()


