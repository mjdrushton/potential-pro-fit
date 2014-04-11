"""A Jinja Handler and tool.  This code is in the public domain.
Adjusted for CherryPy 3.2

Usage:
@cherrypy.expose
@cherrypy.tools.jinja(template='index.html')
def controller(**kwargs):
  return {

  } # This dict is the template context

Downloaded from:  https://gist.github.com/ckolumbus/2764461#file-gistfile1-py

"""
import cherrypy
import jinja2
TEMPLATES_DIR="webresources/templates"


class JinjaHandler(cherrypy.dispatch.LateParamPageHandler):
  """Callable which sets response.body."""

  def __init__(self, env, template_name, next_handler):
    self.env = env
    self.template_name = template_name
    self.next_handler = next_handler

  def __call__(self):
    env = globals().copy()
    env.update(self.next_handler())
    env.update({
      'request': cherrypy.request,
      'app_url': cherrypy.request.app.script_name,
    })
    cherrypy.request.handler = tmpl = self.env.get_template(self.template_name)
    output = tmpl.render(**env)

    return output

class JinjaLoader(object):
  """A CherryPy 3 Tool for loading Jinja templates."""

  def __init__(self):
    self.env = jinja2.Environment(loader=jinja2.PackageLoader(__package__, TEMPLATES_DIR))

  def __call__(self, template):
    cherrypy.request.handler = JinjaHandler(self.env, template, cherrypy.request.handler)

  def add_filter(self, func):
    """Decorator which adds the given function to jinja's filters."""
    self.env.filters[func.__name__] = func

    return func

  def add_global(self, func):
    """Decorator which adds the given function to jinja's globals."""
    self.env.globals[func.__name__] = func

    return func

loader = JinjaLoader()
cherrypy.tools.jinja = cherrypy.Tool('on_start_resource', loader) #, priority=70)
