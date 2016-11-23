
from ..testutil import vagrant_torque, vagrant_basic

from atsim.pro_fit.runners import _pbs_remote_exec
from atsim.pro_fit import _execnet
from atsim.pro_fit.runners._pbs_remote_exec import pbsIdentify, PBSIdentifyRecord
from _runnercommon import channel_id

import py.path
from pytest import fixture

import time

def _mkrunjobs(gw, num):

  def mkrunjob(channel, num):

    import tempfile
    import os
    tmpdir = tempfile.mkdtemp()
    try:
      outpaths = []
      for i in xrange(num):
        nd = os.path.join(tmpdir, str(i))
        os.mkdir(nd)
        filename = os.path.join(nd, 'runjob')
        with open(filename, 'wb') as outfile:
          print >>outfile, "echo Hello > outfile"
        outpaths.append(filename)
      channel.send(outpaths)
      rcv = channel.receive()
    finally:
      import shutil
      shutil.rmtree(tmpdir, ignore_errors = True)
  ch = gw.remote_exec(mkrunjob, num = num)
  runjobs = ch.receive()
  return ch, runjobs

def _mkexecnetgw(vagrant_box):
  with py.path.local(vagrant_box.root).as_cwd():
    group = _execnet.Group()
    gw = group.makegateway("vagrant_ssh=default")
  return gw


@fixture(scope = "function")
def clearqueue(vagrant_torque):

  def clearqueue(channel):
    import subprocess
    subprocess.call("qdel -W 0 all", shell = True)
    import time
    cleared = False
    while not cleared:
      time.sleep(0.5)
      output = subprocess.check_output(["qselect"])
      output = output.strip()
      cleared = not output

  gw = _mkexecnetgw(vagrant_torque)
  ch = gw.remote_exec(clearqueue)
  ch.waitclose(20)

  return vagrant_torque

def send_and_compare(ch, sendmsg, expect):
  pause = 0.5
  for i in xrange(10):
    ch.send(sendmsg)
    msg = ch.receive(2)
    try:
      assert msg == expect
      return
    except AssertionError:
      pass
    time.sleep(pause)
    pause *= 2.0
  assert msg == expect

def testStartChannel(vagrant_torque, channel_id):
  gw = _mkexecnetgw(vagrant_torque)
  ch = gw.remote_exec(_pbs_remote_exec)
  ch.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
  msg = ch.receive(1.0)
  assert msg == {
                  'msg' : 'READY', 'channel_id' : channel_id,
                  'pbs_identify' : {'arrayFlag': '-t', 'flavour': 'TORQUE', 'arrayIDVariable': 'PBS_ARRAYID'}
                }

def testHostHasNoPbs(vagrant_basic, channel_id):
  gw = _mkexecnetgw(vagrant_basic)
  ch = gw.remote_exec(_pbs_remote_exec)
  ch.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
  msg = ch.receive(1.0)
  expect = {'msg' : 'ERROR', 'channel_id' : channel_id, 'reason' : "PBS not found: Could not run 'qselect'"}
  assert expect == msg

def testPBSIdentify():
  """Given a string from qstat --version identify PBS system as Torque or PBSPro"""
  # Test output from TORQUE
  versionString = "version: 2.4.16"
  actual = pbsIdentify(versionString)
  assert actual.arrayFlag == "-t"
  assert actual.arrayIDVariable == "PBS_ARRAYID"

  versionString = "pbs_version = PBSPro_11.1.0.111761"
  actual = pbsIdentify(versionString)
  assert actual.arrayFlag == "-J"
  assert actual.arrayIDVariable == "PBS_ARRAY_INDEX"

def testQSub(clearqueue, channel_id):
  gw = _mkexecnetgw(clearqueue)
  ch = gw.remote_exec(_pbs_remote_exec)

  ch.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
  assert 'READY' == ch.receive(1)['msg']

  clch, runjobs = _mkrunjobs(gw, 5)
  try:
    ch.send({'msg' : 'QSUB', 'jobs' : runjobs[:-1]})
    expect = {'msg' : 'QSUB', 'pbs_id' : None, 'channel_id' : channel_id}

    actual = ch.receive(2)
    assert sorted(expect.keys()) == sorted(actual.keys()), actual
    del expect['pbs_id']
    del actual['pbs_id']
    assert expect == actual

    ch.send({'msg' : 'QSUB', 'jobs' : [runjobs[-1]],
      'header_lines' : ["#PBS -q blah"]
      })

    expect = {'msg' : 'ERROR', 'reason' : 'qsub: Unknown queue MSG=cannot locate queue', 'channel_id' : channel_id}
    actual = ch.receive(2)
    assert expect == actual
  finally:
    clch.send(None)

def testQSubSingleJob(clearqueue, channel_id):
  gw = _mkexecnetgw(clearqueue)
  ch = gw.remote_exec(_pbs_remote_exec)

  ch.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
  assert 'READY' == ch.receive(1)['msg']

  clch, runjobs = _mkrunjobs(gw, 1)

  try:
    ch.send({'msg' : 'QSUB', 'jobs' : runjobs})
    expect = {'msg' : 'QSUB', 'pbs_id' : None, 'channel_id' : channel_id}
    actual = ch.receive(2)
    assert sorted(expect.keys()) == sorted(actual.keys()), actual
    del expect['pbs_id']
    del actual['pbs_id']
    assert expect == actual
  finally:
    clch.send(None)

def testQSelect(clearqueue, channel_id):
  gw = _mkexecnetgw(clearqueue)
  ch = gw.remote_exec(_pbs_remote_exec)

  ch.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
  assert 'READY' == ch.receive(1)['msg']

  clch, runjobs = _mkrunjobs(gw, 5)
  try:
    ch.send({'msg' : 'QSUB', 'jobs' : runjobs})
    msg = ch.receive(2)
    pbs_id = msg['pbs_id']

    expect = {'msg' : 'QSELECT', 'channel_id' : channel_id,
      'pbs_ids' : [pbs_id]}
    send_and_compare(ch, {'msg' : 'QSELECT'}, expect)

    clch2, runjobs2 = _mkrunjobs(gw,5)
    try:
      ch.send({'msg' : 'QSUB', 'jobs' : runjobs2})
      msg = ch.receive(2)
      pbs_id2 = msg['pbs_id']

      expect = {'msg' : 'QSELECT', 'channel_id' : channel_id,
        'pbs_ids' : [pbs_id, pbs_id2  ]}
      send_and_compare(ch, {'msg' : 'QSELECT'}, expect)
    finally:
      clch2.send(None)
  finally:
    clch.send(None)

def testQRls(clearqueue, channel_id):
  gw = _mkexecnetgw(clearqueue)
  ch = gw.remote_exec(_pbs_remote_exec)

  ch.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
  assert 'READY' == ch.receive(1)['msg']

  clch, runjobs = _mkrunjobs(gw, 6)
  try:
    ch.send({'msg' : 'QSUB', 'jobs' : runjobs[:3]})
    msg = ch.receive(2)
    pbs_id_1 = msg['pbs_id']

    ch.send({'msg' : 'QSUB', 'jobs' : runjobs[3:]})
    msg = ch.receive(2)
    pbs_id_2 = msg['pbs_id']

    expect = {'msg' : 'QSELECT', 'channel_id' : channel_id, 'pbs_ids' : [pbs_id_1, pbs_id_2]}
    send_and_compare(ch, {'msg' : 'QSELECT'}, expect)

    expect = {'msg' : 'QRLS', 'channel_id' : channel_id, 'pbs_id' : pbs_id_1}
    ch.send({'msg' : 'QRLS', 'pbs_id' : pbs_id_1 })
    msg = ch.receive(2)
    assert expect == msg, msg

    expect = {'msg' : 'QSELECT', 'channel_id' : channel_id, 'pbs_ids' :[pbs_id_2]}
    send_and_compare(ch, {'msg' : 'QSELECT'}, expect)
  finally:
    clch.send(None)

def testQDel(clearqueue, channel_id):
  gw = _mkexecnetgw(clearqueue)
  ch = gw.remote_exec(_pbs_remote_exec)

  ch.send({'msg' : 'START_CHANNEL', 'channel_id' : channel_id})
  assert 'READY' == ch.receive(1)['msg']

  clch, runjobs = _mkrunjobs(gw, 6)
  try:
    ch.send({'msg' : 'QSUB', 'jobs' : [runjobs[0]]})
    msg = ch.receive(2)
    pbs_id_1 = msg['pbs_id']

    ch.send({'msg' : 'QSUB', 'jobs' : [runjobs[1],runjobs[2]]})
    msg = ch.receive(2)
    pbs_id_2 = msg['pbs_id']

    ch.send({'msg' : 'QSUB', 'jobs' : runjobs[3:]})
    msg = ch.receive(2)
    pbs_id_3 = msg['pbs_id']

    expect = {'msg' : 'QSELECT', 'channel_id' : channel_id, 'pbs_ids' : [pbs_id_1, pbs_id_2, pbs_id_3]}
    send_and_compare(ch, {'msg' : 'QSELECT'}, expect)

    expect = {'msg' : 'QDEL', 'channel_id' : channel_id, 'pbs_ids' : [pbs_id_1, pbs_id_3]}
    ch.send({'msg' : 'QDEL', 'channel_id' : channel_id, 'pbs_ids' : [pbs_id_1, pbs_id_3]})
    msg = ch.receive(2)
    assert expect == msg, msg

    expect = {'msg' : 'QSELECT', 'channel_id' : channel_id, 'pbs_ids' :[pbs_id_2]}
    send_and_compare(ch, {'msg' : 'QSELECT'}, expect)

    expect = {'msg' : 'QDEL', 'channel_id' : channel_id, 'pbs_ids' : [pbs_id_2]}
    ch.send({'msg' : 'QDEL', 'pbs_ids' : [pbs_id_2]})
    msg = ch.receive(2)
    assert expect == msg, msg

    expect = {'msg' : 'QSELECT', 'channel_id' : channel_id, 'pbs_ids' :[]}
    send_and_compare(ch, {'msg' : 'QSELECT'}, expect)

  finally:
    clch.send(None)

def testEndToEnd(clearqueue, channel_id):
  assert False
