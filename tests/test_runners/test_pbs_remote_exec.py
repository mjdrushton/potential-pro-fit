from ..testutil import vagrant_torque, vagrant_basic

from atsim.pro_fit.runners import _pbs_remote_exec
from atsim.pro_fit import _execnet
from atsim.pro_fit.runners._pbs_remote_exec import (
    pbsIdentify,
    PBSIdentifyRecord,
)
from ._runnercommon import channel_id, mkrunjobs, send_and_compare

import py.path
from pytest import fixture
import pytest

import time


def _mkexecnetgw(vagrant_box):
    with py.path.local(vagrant_box.root).as_cwd():
        group = _execnet.Group()
        gw = group.makegateway("vagrant_ssh=default")
    return gw


@fixture(scope="function")
def clearqueue(vagrant_torque):
    from .pbs_runner_test_module import clearqueue

    gw = _mkexecnetgw(vagrant_torque)
    ch = gw.remote_exec(clearqueue)
    ch.waitclose(20)
    return vagrant_torque


def testStartChannel(vagrant_torque, channel_id):
    gw = _mkexecnetgw(vagrant_torque)
    ch = gw.remote_exec(_pbs_remote_exec)
    ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
    try:
        msg = ch.receive(1.0)
        assert msg == {
            "msg": "READY",
            "channel_id": channel_id,
            "pbs_identify": {
                "arrayFlag": "-t",
                "flavour": "TORQUE",
                "arrayIDVariable": "PBS_ARRAYID",
                "qdelForceFlags": ["-W", "0"],
            },
        }
    finally:
        if not ch.isclosed():
            ch.send(None)
            ch.waitclose(5)


@pytest.mark.skip(
    "Vagrant basic only has python3 reinstate when python2/3 tests implemented"
)
def testHostHasNoPbs(vagrant_basic, channel_id):
    gw = _mkexecnetgw(vagrant_basic)
    ch = gw.remote_exec(_pbs_remote_exec)
    try:
        ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
        msg = ch.receive(1.0)
        expect = {
            "msg": "ERROR",
            "channel_id": channel_id,
            "reason": "PBS not found: Could not run 'qselect'",
        }
        assert expect == msg
    finally:
        if not ch.isclosed():
            ch.send(None)
            ch.waitclose(5)


def testPBSIdentify():
    """Given a string from qstat --version identify PBS system as Torque or PBSPro"""
    # Test output from TORQUE
    versionString = "version: 2.4.16"
    actual = pbsIdentify(versionString)
    assert actual.arrayFlag == "-t"
    assert actual.arrayIDVariable == "PBS_ARRAYID"
    assert actual.qdelForceFlags == ["-W", "0"]
    assert actual.flavour == "TORQUE"

    versionString = "pbs_version = PBSPro_11.1.0.111761"
    actual = pbsIdentify(versionString)
    assert actual.arrayFlag == "-J"
    assert actual.arrayIDVariable == "PBS_ARRAY_INDEX"
    assert actual.qdelForceFlags == ["-Wforce"]
    assert actual.flavour == "PBSPro"


def testQSub(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_pbs_remote_exec)
    try:
        ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
        assert "READY" == ch.receive(1)["msg"]

        clch, runjobs = mkrunjobs(gw, 5)
        try:
            ch.send({"msg": "QSUB", "jobs": runjobs[:-1]})
            expect = {"msg": "QSUB", "job_id": None, "channel_id": channel_id}

            actual = ch.receive(2)
            assert sorted(expect.keys()) == sorted(actual.keys()), actual
            del expect["job_id"]
            del actual["job_id"]
            assert expect == actual

            ch.send(
                {
                    "msg": "QSUB",
                    "jobs": [runjobs[-1]],
                    "header_lines": ["#PBS -q blah"],
                }
            )

            expect = {
                "msg": "ERROR",
                "reason": "qsub: Unknown queue MSG=cannot locate queue",
                "channel_id": channel_id,
            }
            actual = ch.receive(2)
            assert expect == actual
        finally:
            clch.send(None)
            clch.waitclose(5)
    finally:
        ch.send(None)
        ch.waitclose(5)


def testQSubSingleJob(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_pbs_remote_exec)
    try:
        ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
        assert "READY" == ch.receive(1)["msg"]

        clch, runjobs = mkrunjobs(gw, 1)

        try:
            ch.send({"msg": "QSUB", "jobs": runjobs, "transaction_id": "1234"})
            expect = {
                "msg": "QSUB",
                "job_id": None,
                "channel_id": channel_id,
                "transaction_id": "1234",
            }
            actual = ch.receive(2)
            assert sorted(expect.keys()) == sorted(actual.keys()), actual
            del expect["job_id"]
            del actual["job_id"]
            assert expect == actual
        finally:
            clch.send(None)
    finally:
        ch.send(None)
        ch.waitclose(5)


def testQSelect(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_pbs_remote_exec)
    try:
        ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
        assert "READY" == ch.receive(1)["msg"]

        clch, runjobs = mkrunjobs(gw, 5)
        try:
            ch.send({"msg": "QSUB", "jobs": runjobs})
            msg = ch.receive(2)
            job_id = msg["job_id"]

            expect = {
                "msg": "QSELECT",
                "channel_id": channel_id,
                "job_ids": [job_id],
            }
            send_and_compare(ch, {"msg": "QSELECT"}, expect)

            clch2, runjobs2 = mkrunjobs(gw, 5)
            try:
                ch.send({"msg": "QSUB", "jobs": runjobs2})
                msg = ch.receive(2)
                job_id2 = msg["job_id"]

                expect = {
                    "msg": "QSELECT",
                    "channel_id": channel_id,
                    "job_ids": [job_id, job_id2],
                }
                send_and_compare(ch, {"msg": "QSELECT"}, expect)
            finally:
                clch2.send(None)
        finally:
            clch.send(None)
    finally:
        ch.send(None)


def testQRls(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_pbs_remote_exec)

    try:
        ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
        assert "READY" == ch.receive(1)["msg"]

        clch, runjobs = mkrunjobs(gw, 6)
        try:
            ch.send({"msg": "QSUB", "jobs": runjobs[:3]})
            msg = ch.receive(2)
            job_id_1 = msg["job_id"]

            ch.send({"msg": "QSUB", "jobs": runjobs[3:]})
            msg = ch.receive(2)
            job_id_2 = msg["job_id"]

            expect = {
                "msg": "QSELECT",
                "channel_id": channel_id,
                "job_ids": [job_id_1, job_id_2],
            }
            send_and_compare(ch, {"msg": "QSELECT"}, expect)

            expect = {
                "msg": "QRLS",
                "channel_id": channel_id,
                "job_id": job_id_1,
            }
            ch.send({"msg": "QRLS", "job_id": job_id_1})
            msg = ch.receive(2)
            assert expect == msg, msg

            expect = {
                "msg": "QSELECT",
                "channel_id": channel_id,
                "job_ids": [job_id_2],
            }
            send_and_compare(ch, {"msg": "QSELECT"}, expect)
        finally:
            clch.send(None)
    finally:
        ch.send(None)
        ch.waitclose(5)


def testQDel(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_pbs_remote_exec)
    try:
        ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
        assert "READY" == ch.receive(1)["msg"]

        clch, runjobs = mkrunjobs(gw, 6)
        try:
            ch.send({"msg": "QSUB", "jobs": [runjobs[0]]})
            msg = ch.receive(2)
            job_id_1 = msg["job_id"]

            ch.send({"msg": "QSUB", "jobs": [runjobs[1], runjobs[2]]})
            msg = ch.receive(2)
            job_id_2 = msg["job_id"]

            ch.send({"msg": "QSUB", "jobs": runjobs[3:]})
            msg = ch.receive(2)
            job_id_3 = msg["job_id"]

            expect = {
                "msg": "QSELECT",
                "channel_id": channel_id,
                "job_ids": [job_id_1, job_id_2, job_id_3],
            }
            send_and_compare(ch, {"msg": "QSELECT"}, expect)

            expect = {
                "msg": "QDEL",
                "channel_id": channel_id,
                "job_ids": [job_id_1, job_id_3],
            }
            ch.send(
                {
                    "msg": "QDEL",
                    "channel_id": channel_id,
                    "job_ids": [job_id_1, job_id_3],
                }
            )
            msg = ch.receive(2)
            assert expect == msg, msg

            expect = {
                "msg": "QSELECT",
                "channel_id": channel_id,
                "job_ids": [job_id_2],
            }
            send_and_compare(ch, {"msg": "QSELECT"}, expect)

            expect = {
                "msg": "QDEL",
                "channel_id": channel_id,
                "job_ids": [job_id_2],
            }
            ch.send({"msg": "QDEL", "job_ids": [job_id_2], "force": True})
            msg = ch.receive(2)
            assert expect == msg, msg

            expect = {
                "msg": "QSELECT",
                "channel_id": channel_id,
                "job_ids": [],
            }
            send_and_compare(ch, {"msg": "QSELECT"}, expect)

        finally:
            clch.send(None)
    finally:
        ch.send(None)
        ch.waitclose(5)
