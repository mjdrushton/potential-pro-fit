import urwid

from _header import Header
from _variables import Variables, CurrentBestTuple
from _jobs import Runners

DIALOG_GROUP = 'DIALOG'

class ErrorDialog(urwid.WidgetWrap):

  def __init__(self):
    super(ErrorDialog, self).__init__(self._buildContents())
    self.overlayOptions = MainFrame.DIALOG_OVERLAY_OPTS
    self.overlayExclusiveGroup = DIALOG_GROUP

  def _buildContents(self):
    self._text = urwid.Text("")
    body = urwid.Padding(self._text, align = 'center', left = 1, right = 1)
    body = urwid.Filler(body)

    self.button = urwid.Button("Exit")
    footer = urwid.Padding(self.button, align = 'center', width = ('relative', 50))

    frame = urwid.Frame(body, footer = footer)
    self._linebox = urwid.LineBox(frame)

    frame.focus_position = 'footer'

    return self._linebox

  def set_text(self, msg):
    self._text.set_text(msg)
  text = property(fset=set_text)

  def set_title(self, title):
    self._linebox.set_title(title)
  title = property(fset = set_title)

  def set_button_text(self, label):
    self.button.text = label
  button_text = property(fset=set_button_text)


class MessageBox(urwid.WidgetWrap):

  def __init__(self):
    self._messages = urwid.MonitoredList()
    super(MessageBox, self).__init__(self._buildContents())
    self._messages.set_modified_callback(self._synchroniseListBox)
    self.overlayOptions = MainFrame.DIALOG_OVERLAY_OPTS
    self.overlayExclusiveGroup = DIALOG_GROUP

  def _buildContents(self):
    self._messagelistwalker = urwid.SimpleFocusListWalker([])
    self.listbox = urwid.ListBox(self._messagelistwalker)
    self._linebox = urwid.LineBox(self.listbox)
    return self._linebox

  @property
  def messages(self):
    return self._messages

  def _synchroniseListBox(self):
    widgets = []
    for txt in self._messages:
      widgets.append(self._makeWidget(txt))
    self._messagelistwalker[:] = widgets
    if widgets:
      self._messagelistwalker.set_focus(len(widgets)-1)

  def _makeWidget(self, txt):
    return urwid.Text(txt)


class MainFrame(urwid.WidgetPlaceholder):
  """The class which handles the display of the pprofit console GUI"""

  DIALOG_OVERLAY_OPTS = ('center', ('relative', 60), 'middle', ('relative', 30))

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
    self.mainframe.overlayOptions = ('center', ('relative', 100), 'middle', ('relative', 100))

    self.messageBox = MessageBox()

    self._selectedPage = None

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
    if not self._overlays:
      self.original_widget = self.background
    else:
      bottom = self.background
      for top in self._overlays:
        opts = top.overlayOptions
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
    errorDialog = ErrorDialog()
    errorDialog.text = msg
    errorDialog.title = title
    errorDialog.button_text = button_text

    def cb(button):
      self._removeOverlay(errorDialog)
      callback()

    urwid.connect_signal(errorDialog.button, 'click', cb)
    self._overlays.append(errorDialog)
    self._processOverlays()

  def showMessageBox(self):
    self.hideMessageBox()
    self._overlays.append(self.messageBox)
    self._processOverlays()

  def hideMessageBox(self):
    if self.messageBox in self._overlays:
      self._overlays.remove(self.messageBox)
    self._processOverlays()

  def showMainPage(self):
    if self._selectedPage:
      self._overlays.remove(self._selectedPage)
    self._overlays.insert(0,self.mainframe)
    self._processOverlays()

  def _removeOverlay(self, overlay):
    self._overlays.remove(overlay)
    self._processOverlays()















