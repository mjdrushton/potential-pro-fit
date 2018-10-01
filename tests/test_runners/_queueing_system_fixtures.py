import pytest

from test_pbs_remote_exec import _mkexecnetgw

import atsim.pro_fit.runners._queueing_system_client as generic_client

@pytest.fixture(scope = "session", params = [
                                              "tests.test_runners.sge_runner_test_module",
                                              "tests.test_runners.slurm_runner_test_module",
                                              "tests.test_runners.pbs_runner_test_module"
                                              ])
def queueing_system_test_module(request):
  return pytest.importorskip(request.param)

@pytest.fixture(scope="function")
def channel_class(queueing_system_test_module):
  return queueing_system_test_module.Channel_Class

@pytest.fixture(scope="function")
def runner_class(queueing_system_test_module):
  return queueing_system_test_module.Runner_Class

@pytest.fixture(scope = "session")
def vagrant_box(queueing_system_test_module):
  box = queueing_system_test_module.vagrant_box()
  yield box
  box.suspend()

@pytest.fixture(scope="function")
def gw(vagrant_box):
  gw = _mkexecnetgw(vagrant_box)
  yield gw

@pytest.fixture(scope = "function")
def channel(channel_id, queueing_system_test_module, gw):
  ch = queueing_system_test_module.Channel_Class(gw, channel_id)
  yield ch
  if not ch.isclosed():
    ch.send(None)
  ch.waitclose(20)

@pytest.fixture(scope = "function")
def client(channel):
  client = generic_client.QueueingSystemClient(channel, pollEvery = 1.0)
  return client

@pytest.fixture(scope = "function")
def clearqueue(gw, queueing_system_test_module):
  ch = gw.remote_exec(queueing_system_test_module.clearqueue)
  ch.waitclose(20)
