import urwid
import math

palette = [

  ('progress_bar_completion', 'black', 'dark cyan'),
  ('progress_bar_rest', 'black', 'light gray'),

]

class RunnerProgressBars(urwid.WidgetWrap):

  ROWS = 6

  def __init__(self):
    self._container = self._buildContents()
    super(RunnerProgressBars, self).__init__(self._container)

  def _buildContents(self):

    self._pb_finished = self._makeProgressBar()
    self._pb_upload = self._makeProgressBar()
    self._pb_running = self._makeProgressBar()
    self._pb_download = self._makeProgressBar()

    self._list_box = urwid.ListBox([
      urwid.Columns([(11, urwid.Text('Finished:')), self._pb_finished]),
      urwid.Columns([(11,urwid.Text('Upload:')), self._pb_upload]),
      urwid.Columns([(11,urwid.Text('Running:')), self._pb_running]),
      urwid.Columns([(11,urwid.Text('Download:')), self._pb_download])
    ])

    self._line_box = urwid.LineBox(self._list_box)
    # return self.BoxAdapter(self._line_box, self.ROWS)
    return self._line_box

  def _makeProgressBar(self):
    pb = urwid.ProgressBar("progress_bar_rest", "progress_bar_completion")
    return pb

  def set_title(self, title):
    self._line_box.set_title(title)

class _RunnerGrid(urwid.WidgetWrap):

  def __init__(self, columns, max_grid_rows = 2):
    self._columns = columns
    self._max_grid_rows = max_grid_rows
    self._num_grid_rows = 1
    self.runnerWidgets = urwid.MonitoredList([])
    self._listBoxWalker = urwid.SimpleFocusListWalker([])
    self._container = urwid.ListBox(self._listBoxWalker)
    super(_RunnerGrid, self).__init__(self._container)

    # Make updates to self.runnerWidgets trigger an update to the widgets in self._listBoxWalker
    self.runnerWidgets.set_modified_callback(self._update)
    # urwid.connect_signal(self.runnerWidgets, "modified", self._update)

  def _update(self):
    # Create self._columns per row.
    # if row is not complete, create an urwid.SolidFill instead of provided widget

    w = list(self.runnerWidgets)

    padlength = int(math.ceil(float(len(w))/float(self._columns)) * self._columns)
    extra = padlength - len(w)

    for i in xrange(extra):
      extra_widget = urwid.SolidFill()
      w.append(extra_widget)

    rows = []
    for i, widget in enumerate(w):
      if i % self._columns == 0:
        row = []
        rows.append(row)
      row.append(widget)

    w = []
    self._num_grid_rows = len(rows)
    for row in rows:
      w.append(self._makeRow(row))
    self._listBoxWalker[:] = w


  def _makeRow(self, row):
    return urwid.BoxAdapter(urwid.Columns(row),RunnerProgressBars.ROWS)

  def addRunner(self):
    rpb = RunnerProgressBars()
    self.runnerWidgets.append(rpb)
    return rpb

  # def rows(self, size, focus = False):
  #   if self._num_grid_rows <= self._max_grid_rows:
  #     rows = self._num_grid_rows * RunnerProgressBars.ROWS
  #   else:
  #     rows = self._max_grid_rows * RunnerProgressBars.ROWS + 2

  #   return rows

class Runners(urwid.WidgetWrap):

  def __init__(self, columns = 3):
    self._columns = columns
    self._container = self._buildContents()
    super(Runners, self).__init__(self._container)

  def _buildContents(self):
    self.totalProgressBars = RunnerProgressBars()
    self.totalProgressBars.set_title("Total")
    self.runners = _RunnerGrid(self._columns)

    container = urwid.Pile([
      (RunnerProgressBars.ROWS, self.totalProgressBars),
      (RunnerProgressBars.ROWS*2 + 2, self.runners)
    ])
    return container
