import urwid

from _header import Header
from _variables import Variables, CurrentBestTuple
from _jobs import JobProgressBars

class MainFrame(urwid.WidgetWrap):

  def __init__(self):
    super(MainFrame, self).__init__(self._makeWidgets())

  def _makeWidgets(self):
    self.header = self._makeHeader()
    body = self._makeBody()
    mainframe = urwid.Frame(body, header = self.header)

    return mainframe

  def _makeVariables(self):
    variables = Variables([
      CurrentBestTuple('A_O-O', '900.0', '1000.0'),
      CurrentBestTuple('rho_O-O', '0.2', '0.3')
    ])
    return variables

  def _makeJobs(self):
    jobs = JobProgressBars()
    return jobs

  def _makeHeader(self):
    return Header()

  def _makeBody(self):
    self.variables = self._makeVariables()
    self.jobs = self._makeJobs()

    boxed_variables = urwid.LineBox(self.variables, title = 'Variables')
    boxed_jobs = urwid.LineBox(self.jobs, title = 'Jobs')

    container = urwid.Pile([
      ('weight', 1.0, boxed_variables),
      ('weight', 1.0, boxed_jobs)
    ])

    return container
