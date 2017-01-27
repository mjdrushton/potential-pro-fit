import urwid

class Header(urwid.WidgetWrap):

  def __init__(self):
    super(Header, self).__init__(self._makeWidgets())

  def _makeWidgets(self):
    #Header row
    title_label = urwid.Text(u'Potential Pro-Fit v 0.7.0', wrap = 'clip')
    self._run_name_label = urwid.Text(u'Run Name')
    self._current_iteration_number_label = urwid.Text('NA', align = "right")
    self._best_iteration_number_label = urwid.Text('NA', align = "right")
    self._current_merit_value_label = urwid.Text('NA', align = "right")
    self._best_merit_value_label = urwid.Text('NA', align = "right")

    header = urwid.Columns([
      ('weight', 2,urwid.Pile([
        title_label,
        self._run_name_label,
      ])),

      (5, urwid.Pile([
        urwid.Text(''),
        urwid.Text('Curr:'),
        urwid.Text('Best:')
      ])),

      (20, urwid.Pile([
        urwid.Text("Step", align = "center"),
        self._current_iteration_number_label,
        self._best_iteration_number_label
      ])),

      (20, urwid.Pile([
        urwid.Text("Merit", align = "center"),
        self._current_merit_value_label,
        self._best_merit_value_label
      ])),
    ])

    header = urwid.LineBox(header)
    return header

  def _get_run_name(self):
    return self._run_name_label.get_text()

  def _set_run_name(self, run_name):
    self._run_name_label.set_text(run_name)
  run_name = property(_get_run_name, _set_run_name)

  def _get_current_iteration_number(self):
    return self._current_iteration_number_label.get_text()

  def _set_current_iteration_number(self, iteration_number):
    self._current_iteration_number_label.set_text(iteration_number)
  current_iteration_number = property(_get_current_iteration_number, _set_current_iteration_number)

  def _get_best_iteration_number(self):
    return self._best_iteration_number_label.get_text()

  def _set_best_iteration_number(self, iteration_number):
    self._best_iteration_number_label.set_text(iteration_number)
  best_iteration_number = property(_get_best_iteration_number, _set_best_iteration_number)

  def _get_current_merit_value(self):
    return self._current_merit_value_label.get_text()

  def _set_current_merit_value(self, merit_value):
    self._current_merit_value_label.set_text(merit_value)
  current_merit_value = property(_get_current_merit_value, _set_current_merit_value)

  def _get_best_merit_value(self):
    return self._best_merit_value_label.get_text()

  def _set_best_merit_value(self, merit_value):
    self._best_merit_value_label.set_text(merit_value)
  best_merit_value = property(_get_best_merit_value, _set_best_merit_value)
