import urwid

palette = [

  ('progress_bar_completion', 'black', 'dark cyan'),
  ('progress_bar_rest', 'black', 'light gray'),

]

class JobProgressBars(urwid.WidgetWrap):

  def __init__(self):
    self._container = self._buildContents()
    super(JobProgressBars, self).__init__(self._container)

  def _buildContents(self):

    self._pb_finished = self._makeProgressBar()
    self._pb_upload = self._makeProgressBar()
    self._pb_running = self._makeProgressBar()
    self._pb_download = self._makeProgressBar()

    # self._pb_finished.set_completion(50)

    body = urwid.ListBox([
      urwid.Columns([(11, urwid.Text('Finished:')), self._pb_finished]),
      urwid.Columns([(11,urwid.Text('Upload:')), self._pb_upload]),
      urwid.Columns([(11,urwid.Text('Running:')), self._pb_running]),
      urwid.Columns([(11,urwid.Text('Download:')), self._pb_download])
    ])

    return body

  def _makeProgressBar(self):
    pb = urwid.ProgressBar("progress_bar_rest", "progress_bar_completion")
    return pb
