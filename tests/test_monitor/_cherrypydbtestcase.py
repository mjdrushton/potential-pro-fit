import os
import unittest
import json

from mechanize import Browser
from atsim.pro_fit import webmonitor

cherrypy = webmonitor.cherrypy

from .. import testutil


def _getResourceDir():
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "resources")
    )


class CherryPyDBTestCaseBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dburl = "sqlite:///" + os.path.join(_getResourceDir(), cls.dbname)
        root = webmonitor._setupCherryPy(cls.dburl)
        cherrypy.engine.start()

    @classmethod
    def tearDownClass(cls):
        cherrypy.process.wspbus.bus.exit()
        cherrypy.engine.stop()

    def fetchJSON(self, url):
        br = Browser()
        import urllib.request, urllib.parse, urllib.error

        url = urllib.parse.quote(url, safe="/?=")
        br.open("%s/%s" % (self.baseurl, url))
        response = br.response()
        self.assertEqual("application/json", response.info()["Content-Type"])
        j = json.loads(br.response().read())
        return j
