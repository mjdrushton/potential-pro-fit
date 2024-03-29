import os
import time

import pytest
from atsim.pro_fit import _execnet
from atsim.pro_fit.runners import _slurm_remote_exec
from pytest import fixture

from ..testutil import vagrant_basic, vagrant_slurm
from ._runnercommon import channel_id, mkrunjobs, send_and_compare


def _mkexecnetgw(vagrant_box):
    old_cwd = os.getcwd()
    try:
        os.chdir(vagrant_box.root)
        group = _execnet.Group()
        gw = group.makegateway("vagrant_ssh=default")
    finally:
        os.chdir(old_cwd)
    return gw


@fixture(scope="function")
def clearqueue(vagrant_slurm):

    from .slurm_runner_test_module import clearqueue

    gw = _mkexecnetgw(vagrant_slurm)
    ch = gw.remote_exec(clearqueue)
    ch.waitclose(20)
    return vagrant_slurm


@pytest.mark.slow
def testStartChannel(vagrant_slurm, channel_id):
    gw = _mkexecnetgw(vagrant_slurm)
    ch = gw.remote_exec(_slurm_remote_exec)
    ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
    try:
        msg = ch.receive(1.0)
        assert msg == {"msg": "READY", "channel_id": channel_id}
    finally:
        ch.send(None)
        ch.waitclose(5)


@pytest.mark.skip(
    "Vagrant basic doesn't have python 3 revisit this when checking that remote execs work with python 2 and 3"
)
def testHostHasNoSlurm(vagrant_basic, channel_id):
    gw = _mkexecnetgw(vagrant_basic)
    ch = gw.remote_exec(_slurm_remote_exec)
    ch.send({"msg": "START_CHANNEL", "channel_id": channel_id})
    msg = ch.receive(1.0)
    expect = {
        "msg": "ERROR",
        "channel_id": channel_id,
        "reason": "Slurm not found: Could not run 'squeue'",
    }
    assert expect == msg


@pytest.mark.slow
def testQSub(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_slurm_remote_exec)
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
                    "header_lines": ["#SBATCH -p blah"],
                }
            )

            expect = {
                "msg": "ERROR",
                "reason": "sbatch: error: invalid partition specified: blah\nsbatch: error: Batch job submission failed: Invalid partition name specified",
                "channel_id": channel_id,
            }
            actual = ch.receive(2)
            assert expect == actual
        finally:
            clch.send(None)
    finally:
        ch.send(None)
        ch.waitclose(5)


@pytest.mark.slow
def testQSubSingleJob(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_slurm_remote_exec)
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


@pytest.mark.slow
def testQSelect(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_slurm_remote_exec)
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


@pytest.mark.slow
def testQRls(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_slurm_remote_exec)

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


@pytest.mark.slow
def testQDel(clearqueue, channel_id):
    gw = _mkexecnetgw(clearqueue)
    ch = gw.remote_exec(_slurm_remote_exec)
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
