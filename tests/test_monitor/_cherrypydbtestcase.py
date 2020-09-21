import json
import os
import unittest

from atsim.pro_fit import webmonitor
from mechanize import Browser

from .. import testutil

cherrypy = webmonitor.cherrypy


def _getResourceDir():
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "resources")
    )


class CherryPyDBTestCaseBase(unittest.TestCase):

    dbname = None
    baseurl = None

    @classmethod
    def setUpClass(cls):
        cls.dburl = "sqlite:///" + os.path.join(_getResourceDir(), cls.dbname)
        webmonitor._setupCherryPy(cls.dburl)
        cherrypy.engine.start()

    @classmethod
    def tearDownClass(cls):
        cherrypy.process.wspbus.bus.exit()
        cherrypy.engine.stop()

    def fetchJSON(self, url):
        br = Browser()
        import urllib.request
        import urllib.parse
        import urllib.error

        url = urllib.parse.quote(url, safe="/?=")
        br.open("%s/%s" % (self.baseurl, url))
        response = br.response() # pylint: disable=no-member
        self.assertEqual("application/json", response.info()["Content-Type"])
        j = json.loads(response.read())
        return j
