import urwid

from _header import Header
from _variables import Variables, CurrentBestTuple
from _jobs import Runners

class MainFrame(urwid.WidgetPlaceholder):
  """The class which handles the display of the pprofit console GUI"""

  def __init__(self):
    self._makeWidgets()
    super(MainFrame, self).__init__(self.background)
    self._processOverlays()


  def _makeWidgets(self):
    self._overlays = []
    self._currentOverlay = None

    self.background = self._makeBackground()
    self.header = self._makeHeader()
    self.body = self._makeBody()
    self.mainframe = urwid.Frame(self.body, header = self.header)
    self._overlays.append((self.mainframe, ('center', ('relative', 100), 'middle', ('relative', 100))))

  def _makeVariables(self):
    variables = Variables()
    return variables

  def _makeJobs(self):
    jobs = Runners()
    return jobs

  def _makeHeader(self):
    return Header()

  def _makeBackground(self):
    return urwid.SolidFill(' ')

  def _makeBody(self):
    self.variables = self._makeVariables()
    self.jobs = self._makeJobs()

    boxed_variables = urwid.LineBox(self.variables, title = 'Variables')
    boxed_jobs = urwid.LineBox(self.jobs, title = 'Jobs')

    container = urwid.Pile([
      ('weight', 1.0, boxed_variables),
      ('pack', boxed_jobs)
    ])

    return container

  def _processOverlays(self):
    bottom = self.background
    for top, opts in self._overlays:
      args = [top, bottom]
      args.extend(list(opts))
      overlay = urwid.Overlay(*args)
      bottom = overlay
    self.original_widget = overlay

  def showErrorMessage(self, msg, callback, button_text = 'Exit', title = 'Error'):
    """Show a dialogue box to indicate an error condition.

    Args:
        msg (str): Error message
        callback (callable): Function called when the exit button it clicked
        button_text (str, optional): Exit button label
        title (str, optional): Error box title
    """
    text = urwid.Text(msg)
    body = urwid.Padding(text, align = 'center', left = 1, right = 1)
    body = urwid.Filler(body)


    button = urwid.Button("Exit")
    footer = urwid.Padding(button, align = 'center', width = ('relative', 50))
    # footer = urwid.Pile([('pack', urwid.Button("Exit"))])

    frame = urwid.Frame(body, footer = footer)
    errorwidget = urwid.LineBox(frame, title = title)

    frame.focus_position = 'footer'

    overlay = (errorwidget, ('center', ('relative', 60), 'middle', ('relative', 30)))
    self._overlays.append(overlay)

    def cb(button):
      self._removeOverlay(overlay)
      callback()

    urwid.connect_signal(button, 'click', cb)
    self._processOverlays()

  def _removeOverlay(self, overlay):
    self._overlays.remove(overlay)
    self._processOverlays()















