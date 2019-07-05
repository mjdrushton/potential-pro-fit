from collections import OrderedDict

from ._variables import CurrentBestTuple

import weakref

from urwid import MonitoredList


class ObservedList(MonitoredList):
    def __init__(self):
        super(ObservedList, self).__init__()
        self.callbacks = []
        super(ObservedList, self).set_modified_callback(self._callback)

    def set_modified_callback(self, callback):
        self.callbacks.append(callback)

    def _callback(self):
        for cb in self.callbacks:
            cb()


class ObserverRegister(object):
    def __init__(self, model):
        super(ObserverRegister, self).__setattr__("_model", model)
        super(ObserverRegister, self).__setattr__("_observerdict", {})

    def __setattr__(self, name, observer):
        getattr(self._model, name)
        self._observerdict.setdefault(name, []).append(observer)

    def __getattr__(self, name):
        return self._observerdict[name]


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
        super(ObservableObject, self).__setattr__("_normal_setattr", True)
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


class _MessageBoxModel(object):
    def __init__(self):
        self.visible = False


class MessageBoxModel(ObservableObject):
    def __init__(self):
        super(MessageBoxModel, self).__init__(_MessageBoxModel())
        self._normal_setattr = True
        self._lines = ObservedList()

        self._normal_setattr = False

    def _get_lines(self):
        return self._lines

    lines = property(_get_lines)


class _RunnerModel(object):
    def __init__(self):
        self.title = None
        self.total_jobs = 0
        self.uploaded = 0
        self.running = 0
        self.downloaded = 0


class RunnerModel(ObservableObject):
    def __init__(self):
        super(RunnerModel, self).__init__(_RunnerModel())


class _IterationModel(object):
    def __init__(self):
        self.iteration_number = 0
        self.merit_value = None
        self.variables = None


class IterationModel(ObservableObject):
    def __init__(self):
        super(IterationModel, self).__init__(_IterationModel())


class _VariableUpdateHandler(object):
    """Responsible for updating the ConsoleModel.variables_table when a change is observed in
  ConsoleModel.current_iteration.variables or ConsoleModel.best_iteration.variables"""

    def __init__(self, consoleModel):
        self._consoleModel = weakref.proxy(consoleModel)

        # Register this handler with the _consoleModel
        self._consoleModel.current_iteration.observers.variables = self
        self._consoleModel.best_iteration.observers.variables = self

    def _updateVariablesTable(self):
        table = OrderedDict()

        def mkname(n, isfit):
            prefix = "  "
            if isfit:
                prefix = "* "
            return prefix + str(n)

        if self._consoleModel.current_iteration.variables:
            for (
                k,
                v,
                isfit,
            ) in (
                self._consoleModel.current_iteration.variables.flaggedVariablePairs
            ):
                table[k] = {
                    "name": mkname(k, isfit),
                    "current": v,
                    "best": None,
                }

        if self._consoleModel.best_iteration.variables:
            for (
                k,
                v,
                isfit,
            ) in (
                self._consoleModel.best_iteration.variables.flaggedVariablePairs
            ):
                table.setdefault(
                    k,
                    {"name": mkname(k, isfit), "current": None, "best": None},
                )["best"] = v

        # Convert to tuples
        tuple_table = []
        for d in table.values():
            tuple_table.append(
                CurrentBestTuple(d["name"], d["current"], d["best"])
            )

        self._consoleModel.variables_table = tuple_table

    def __call__(self, name, oldvalue, newvalue):
        self._updateVariablesTable()


class _SumRunnerValues(object):
    def __init__(self, attrname, runmodels, setrunmodel):
        self.models = runmodels
        self.attrname = attrname
        self.setrunmodel = setrunmodel
        self._registerHandlers()

    def __call__(self, name, oldvalue, newvalue):
        total = 0.0
        for rm in self.models:
            total += getattr(rm, self.attrname)
        setattr(self.setrunmodel, self.attrname, total)

    def _registerHandlers(self):
        for rm in self.models:
            setattr(rm.observers, self.attrname, self)

    def unregisterHandlers(self):
        for rm in self.models:
            getattr(rm.observers, self.attrname).remove(self)


class _RunnerOverviewUpdateHandler(object):
    def __init__(self, consoleModel):
        self.model = consoleModel
        self._registerHandlers()
        self._registeredHandlers = []

    def _registerHandlers(self):
        self._register_runnerlist_callback()
        self._register_runner_handlers()

    def _register_runnerlist_callback(self):
        self.model.runners.set_modified_callback(self._synchroniseWithRunners)

    def _synchroniseWithRunners(self):
        self._removeExistingHandlers()
        self._register_runner_handlers()

    def _removeExistingHandlers(self):
        for handler in self._registerHandlers:
            handler.unregisterHandlers()

    def _register_runner_handlers(self):
        runners = list(self.model.runners)
        self._registerHandlers = [
            _SumRunnerValues(
                "total_jobs", runners, self.model.runner_overview
            ),
            _SumRunnerValues("uploaded", runners, self.model.runner_overview),
            _SumRunnerValues("running", runners, self.model.runner_overview),
            _SumRunnerValues(
                "downloaded", runners, self.model.runner_overview
            ),
        ]


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
        self.endEvent = None
        self.closedEvent = None

        # Create VariableUpdateHandler that will keep the variable_table in sync with the variables information in the iteration models.
        _VariableUpdateHandler(self)

        self.runners = ObservedList()
        self.runner_overview = RunnerModel()
        self.runner_overview.title = "Total"
        # self.runner_overview rolls-up the data for the runner models in self.runners, _RunnerOverviewUpdateHandler performs this summary calculation when it detects changes to any runner model.
        _RunnerOverviewUpdateHandler(self)

        # Create a model for the MessageBox
        self.messages = MessageBoxModel()

        self._normal_setattr = False
