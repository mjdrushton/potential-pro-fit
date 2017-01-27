import urwid
import collections


CurrentBestTuple = collections.namedtuple("CurrentBestTuple", ["name", "current", "best"])
class CurrentBestListWalker(urwid.ListWalker):

  def __init__(self, variableValues):
    self._list = [ (t, self.makewidget(t)) for t in variableValues ]
    self.focus = self._list[0][1]

  def __getitem__(self, position):
    return self._list[self._posToIdx(position)][1]

  @property
  def _widgets(self):
    return [ t[1] for t in self._list]

  def _posToIdx(self, position):
    widgets = self._widgets
    idx = widgets.index(position)
    return idx

  def next_position(self, position):
    return self._list[self._posToIdx(position)+1][1]

  def prev_position(self, position):
    idx = self._posToIdx(position)-1
    if idx < 0:
      raise IndexError()
    return self._list[idx][1]

  def set_focus(self, position):
    self.focus = position
    self._modified()

  @staticmethod
  def makewidget(t):
    widget = urwid.Columns([
      urwid.Text(t.name, align = 'left'),
      urwid.Text(str(t.current), align = 'right'),
      urwid.Text(str(t.best), align = 'right')
    ])
    return widget


class Variables(urwid.WidgetWrap):

  def __init__(self, variablelist):
    self._variablelist = list(variablelist)
    super(Variables, self).__init__(self._makeWidgets())

  def _makeWidgets(self):
    variable_listbox = urwid.ListBox(CurrentBestListWalker(self._variablelist))
    variable_listbox_header = CurrentBestListWalker.makewidget(CurrentBestTuple("Name", "Current", "Best"))
    headed_variable_listbox = urwid.Pile([('pack',variable_listbox_header), variable_listbox])
    return headed_variable_listbox
