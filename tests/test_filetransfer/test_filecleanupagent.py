from atsim.pro_fit.filetransfer.remote_exec import file_cleanup_remote_exec
from atsim.pro_fit.filetransfer import (
    CleanupChannel,
    CleanupClient,
    CleanupChannelException,
)

from ._common import execnet_gw, channel_id

import posixpath
import os
import time

import gevent.event

import py.path


def test_file_cleanup_end_to_end(tmpdir, execnet_gw, channel_id):
    # Create directory structure
    tmpdir.ensure("one", "two", "three", "four", dir=True)
    root = tmpdir.join("one")
    p = tmpdir

    for i, subp in enumerate(["one", "two", "three", "four"]):
        p = p.join(subp)
        for j in range(i + 1):
            j += 1
            p.ensure("%d.txt" % j)

    two = root.join("two")
    two.ensure("a", "b", dir=True)
    two.ensure("a", "c", dir=True)

    def currfiles():
        files = root.visit()
        files = [f.relto(tmpdir) for f in files]
        return set(files)

    allfiles = set(
        [
            "one/two/a/c",
            "one/two",
            "one/two/three/1.txt",
            "one/two/three/four/1.txt",
            "one/two/2.txt",
            "one/two/three/four/2.txt",
            "one/two/three/3.txt",
            "one/two/three/2.txt",
            "one/two/three/four/3.txt",
            "one/two/a",
            "one/two/three",
            "one/two/1.txt",
            "one/two/three/four",
            "one/1.txt",
            "one/two/three/four/4.txt",
            "one/two/a/b",
        ]
    )

    assert currfiles() == allfiles

    ch1 = execnet_gw.remote_exec(file_cleanup_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_CLEANUP_CHANNEL",
                "channel_id": channel_id,
                "remote_path": root.strpath,
            }
        )

        msg = ch1.receive(10)
        assert msg["msg"] == "READY"

        transid = "transid"

        ch1.send(
            {"msg": "LOCK", "id": transid, "remote_path": "two/three/four"}
        )
        msg = ch1.receive(10)
        assert msg == {"msg": "LOCKED", "channel_id": channel_id, "id": transid}

        ch1.send(
            {"msg": "LOCK", "id": transid, "remote_path": ["two", "two/three"]}
        )
        msg = ch1.receive(10)
        assert msg == {"msg": "LOCKED", "channel_id": channel_id, "id": transid}

        ch1.send(
            {
                "msg": "LOCK",
                "id": transid,
                "remote_path": ["two", "two/three", "two/a/b", "two/a/c"],
            }
        )
        msg = ch1.receive(10)
        assert msg == {"msg": "LOCKED", "channel_id": channel_id, "id": transid}

        # Unlock 'one/two/three/four' - four deleted
        ch1.send(
            {"msg": "UNLOCK", "id": transid, "remote_path": ["two/three/four"]}
        )
        msg = ch1.receive(10)
        assert msg == {
            "msg": "UNLOCKED",
            "channel_id": channel_id,
            "id": transid,
        }

        ch1.send({"msg": "FLUSH", "id": transid})
        msg = ch1.receive(10)
        assert msg == {
            "msg": "FLUSHED",
            "channel_id": channel_id,
            "id": transid,
        }

        allfiles = set(
            [
                "one/two/a/c",
                "one/two",
                "one/two/three/1.txt",
                "one/two/2.txt",
                "one/two/three/3.txt",
                "one/two/three/2.txt",
                "one/two/a",
                "one/two/three",
                "one/two/1.txt",
                "one/1.txt",
                "one/two/a/b",
            ]
        )

        assert currfiles() == allfiles

        ch1.send(
            {
                "msg": "UNLOCK",
                "id": transid,
                "remote_path": ["two", "two/three"],
            }
        )
        msg = ch1.receive(10)
        assert msg == {
            "msg": "UNLOCKED",
            "channel_id": channel_id,
            "id": transid,
        }

        ch1.send({"msg": "FLUSH", "id": transid})
        msg = ch1.receive(10)
        assert msg == {
            "msg": "FLUSHED",
            "channel_id": channel_id,
            "id": transid,
        }

        allfiles = set(
            [
                "one/two/a/c",
                "one/two",
                "one/two/2.txt",
                "one/two/a",
                "one/two/1.txt",
                "one/1.txt",
                "one/two/a/b",
            ]
        )

        assert currfiles() == allfiles

        # Unlock 'one/a' - a deleted
        ch1.send(
            {
                "msg": "UNLOCK",
                "id": transid,
                "remote_path": ["two/a", "two/a/b"],
            }
        )
        msg = ch1.receive(10)
        assert msg == {
            "msg": "UNLOCKED",
            "channel_id": channel_id,
            "id": transid,
        }

        ch1.send({"msg": "FLUSH", "id": transid})
        msg = ch1.receive(10)
        assert msg == {
            "msg": "FLUSHED",
            "channel_id": channel_id,
            "id": transid,
        }

        allfiles = set(
            [
                "one/two/a/c",
                "one/two",
                "one/two/2.txt",
                "one/two/a",
                "one/two/1.txt",
                "one/1.txt",
            ]
        )

        assert currfiles() == allfiles
    finally:
        ch1.close()
        ch1.waitclose(5)

    time.sleep(1)
    assert not root.exists()


def test_file_cleanup_start(tmpdir, execnet_gw, channel_id):
    root = tmpdir.ensure("root", dir=True)
    assert os.path.isdir(root.strpath)
    ch1 = execnet_gw.remote_exec(file_cleanup_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_CLEANUP_CHANNEL",
                "channel_id": channel_id,
                "remote_path": root.strpath,
            }
        )
        msg = ch1.receive(10.0)
        assert (
            dict(msg="READY", channel_id=channel_id, remote_path=root.strpath)
            == msg
        )
    finally:
        ch1.close()
        ch1.waitclose(5)
    time.sleep(1)
    assert not os.path.isdir(
        root.strpath
    ), "Root directory still present after cleanup channel closed."


def test_file_cleanup_BadStart_nonexistent_directory(execnet_gw, channel_id):
    ch1 = execnet_gw.remote_exec(file_cleanup_remote_exec)
    try:
        badpath = "/this/is/not/a/path"
        assert not py.path.local(badpath).exists()

        ch1.send(
            {
                "msg": "START_CLEANUP_CHANNEL",
                "channel_id": channel_id,
                "remote_path": badpath,
            }
        )
        msg = ch1.receive(10.0)

        assert "reason" in msg
        assert msg["reason"].startswith("path does not exist")
        del msg["reason"]
        msg == dict(msg="ERROR", channel_id=channel_id, remote_path=badpath)
    finally:
        ch1.close()
        ch1.waitclose(5)


def test_file_cleanup_lock_bad(tmpdir, execnet_gw, channel_id):
    root = tmpdir.ensure("root", dir=True)
    assert os.path.isdir(root.strpath)
    ch1 = execnet_gw.remote_exec(file_cleanup_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_CLEANUP_CHANNEL",
                "channel_id": channel_id,
                "remote_path": root.strpath,
            }
        )
        msg = ch1.receive(10.0)
        assert (
            dict(msg="READY", channel_id=channel_id, remote_path=root.strpath)
            == msg
        )

        ch1.send({"msg": "LOCK", "id": "transid", "remote_path": "../"})
        msg = ch1.receive(10.0)
        expect = {
            "msg": "ERROR",
            "id": "transid",
            "reason": "path does not lie within directory structure",
            "remote_path": "../",
            "error_code": ("PATHERROR", "NOTCHILD"),
        }

        ch1.send({"msg": "LOCK", "id": "transid", "remote_path": "/"})
        msg = ch1.receive(10.0)
        expect = {
            "msg": "ERROR",
            "id": "transid",
            "reason": "path does not lie within directory structure",
            "remote_path": "/",
            "error_code": ("PATHERROR", "NOTCHILD"),
        }
    finally:
        ch1.close()
        ch1.waitclose()

    time.sleep(1)
    assert not os.path.isdir(root.strpath)


def test_file_cleanup_unlock_bad(tmpdir, execnet_gw, channel_id):
    root = tmpdir.ensure("root", dir=True)
    assert os.path.isdir(root.strpath)
    ch1 = execnet_gw.remote_exec(file_cleanup_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_CLEANUP_CHANNEL",
                "channel_id": channel_id,
                "remote_path": root.strpath,
            }
        )
        msg = ch1.receive(10.0)
        assert (
            dict(msg="READY", channel_id=channel_id, remote_path=root.strpath)
            == msg
        )

        ch1.send({"msg": "LOCK", "id": "transid", "remote_path": "blah"})
        msg = ch1.receive(10.0)
        assert msg["msg"] == "LOCKED"

        ch1.send({"msg": "UNLOCK", "id": "transid", "remote_path": "../../"})
        msg = ch1.receive(10.0)
        expect = {
            "msg": "ERROR",
            "id": "transid",
            "reason": "path does not lie within cleanup directory structure",
            "remote_path": "../../",
            "error_code": ("PATHERROR", "NOTCHILD"),
        }

        ch1.send({"msg": "UNLOCK", "id": "transid", "remote_path": "boop"})
        msg = ch1.receive(10.0)
        expect = {
            "msg": "ERROR",
            "id": "transid",
            "reason": "path is not registered with cleanup agent",
            "remote_path": "boop",
            "error_code": ("PATHERROR", "NOT_REGISTERED"),
        }
    finally:
        ch1.close()
        ch1.waitclose()


def test_file_cleanup_unlock_path_normalize(tmpdir, execnet_gw, channel_id):
    # Create directory structure
    tmpdir.ensure("one", dir=True)
    root = tmpdir.join("one")
    p = tmpdir

    root.ensure("a", "b", "c", "d", dir=True)

    def currfiles():
        files = root.visit()
        files = [f.relto(tmpdir) for f in files]
        return set(files)

    allfiles = set(["one/a", "one/a/b", "one/a/b/c", "one/a/b/c/d"])
    assert currfiles() == allfiles

    ch1 = execnet_gw.remote_exec(file_cleanup_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_CLEANUP_CHANNEL",
                "channel_id": channel_id,
                "remote_path": root.strpath,
            }
        )

        msg = ch1.receive(10)
        assert msg["msg"] == "READY"

        transid = "transid"

        ch1.send(
            {
                "msg": "LOCK",
                "id": transid,
                "remote_path": ["a/../a/b/../b", "a/b/c/d"],
            }
        )
        msg = ch1.receive(10)
        assert msg == {"msg": "LOCKED", "channel_id": channel_id, "id": transid}

        ch1.send(
            {"msg": "UNLOCK", "id": transid, "remote_path": ["a/b/c/d/../../"]}
        )
        msg = ch1.receive(10)
        assert msg == {
            "msg": "UNLOCKED",
            "channel_id": channel_id,
            "id": transid,
        }

        ch1.send({"msg": "FLUSH", "id": transid})
        msg = ch1.receive(10)
        assert msg == {
            "msg": "FLUSHED",
            "channel_id": channel_id,
            "id": transid,
        }

        assert currfiles() == allfiles

        ch1.send(
            {"msg": "UNLOCK", "id": transid, "remote_path": ["a/../a/b/c/d"]}
        )
        msg = ch1.receive(10)
        assert msg == {
            "msg": "UNLOCKED",
            "channel_id": channel_id,
            "id": transid,
        }

        ch1.send({"msg": "FLUSH", "id": transid})
        msg = ch1.receive(10)
        assert msg == {
            "msg": "FLUSHED",
            "channel_id": channel_id,
            "id": transid,
        }

        allfiles = set(["one/a"])
        assert currfiles() == allfiles
    finally:
        ch1.close()
        ch1.waitclose()


def test_file_cleanup_client(tmpdir, execnet_gw, channel_id):
    # Create directory structure
    tmpdir.ensure("one", "two", "three", "four", dir=True)
    root = tmpdir.join("one")
    p = tmpdir

    for i, subp in enumerate(["one", "two", "three", "four"]):
        p = p.join(subp)
        for j in range(i + 1):
            j += 1
            p.ensure("%d.txt" % j)

    two = root.join("two")
    two.ensure("a", "b", dir=True)
    two.ensure("a", "c", dir=True)

    def currfiles():
        files = root.visit()
        files = [f.relto(tmpdir) for f in files]
        return set(files)

    allfiles = set(
        [
            "one/two/a/c",
            "one/two",
            "one/two/three/1.txt",
            "one/two/three/four/1.txt",
            "one/two/2.txt",
            "one/two/three/four/2.txt",
            "one/two/three/3.txt",
            "one/two/three/2.txt",
            "one/two/three/four/3.txt",
            "one/two/a",
            "one/two/three",
            "one/two/1.txt",
            "one/two/three/four",
            "one/1.txt",
            "one/two/three/four/4.txt",
            "one/two/a/b",
        ]
    )
    assert currfiles() == allfiles

    ch1 = CleanupChannel(execnet_gw, root.strpath, channel_id)
    try:
        client = CleanupClient(ch1)
        client.lock("two/three/four")
        client.lock("two", "two/three")
        client.lock("two", "two/three", "two/a/b", "two/a/c")
        client.unlock("two/three/four")
        client.flush()
        allfiles = set(
            [
                "one/two/a/c",
                "one/two",
                "one/two/three/1.txt",
                "one/two/2.txt",
                "one/two/three/3.txt",
                "one/two/three/2.txt",
                "one/two/a",
                "one/two/three",
                "one/two/1.txt",
                "one/1.txt",
                "one/two/a/b",
            ]
        )
        assert currfiles() == allfiles

        client.unlock("two", "two/three")
        client.flush()
        allfiles = set(
            [
                "one/two/a/c",
                "one/two",
                "one/two/2.txt",
                "one/two/a",
                "one/two/1.txt",
                "one/1.txt",
                "one/two/a/b",
            ]
        )
        assert currfiles() == allfiles

        # Unlock 'one/a' - a deleted
        client.unlock("two/a", "two/a/b")
        client.flush()

        allfiles = set(
            [
                "one/two/a/c",
                "one/two",
                "one/two/2.txt",
                "one/two/a",
                "one/two/1.txt",
                "one/1.txt",
            ]
        )

        assert currfiles() == allfiles
    finally:
        ch1.close()
        ch1.waitclose()
    time.sleep(1)
    assert not root.exists()


def cbcheck(call, expect_exception=None):
    class TestCallback(object):
        def __init__(self, pause_event, event):
            self.pause_event = pause_event
            self.event = event
            self.called = False
            self.exception = None

        def __call__(self, exception):
            self.pause_event.wait(2)
            self.exception = exception
            self.called = True
            self.event.set()

    evt = gevent.event.Event()
    pevt = gevent.event.Event()
    cb = TestCallback(pevt, evt)
    call(cb)
    pevt.set()
    evt.wait(2)
    assert cb.called

    if expect_exception is None:
        assert cb.exception is None
    else:
        try:
            et, ei, tb = cb.exception
            raise ei.with_traceback(tb)
            # assert False, "Exception %s should have been raised" % expect_exception
        except Exception as e:
            assert isinstance(e, expect_exception)


def test_file_cleanup_client_lock_callback(tmpdir, execnet_gw, channel_id):
    channel = CleanupChannel(execnet_gw, tmpdir.strpath, channel_id=channel_id)
    try:
        client = CleanupClient(channel)

        def call(cb):
            client.lock("one", callback=cb)

        cbcheck(call)

        def badcall(cb):
            client.lock("..", callback=cb)

        cbcheck(badcall, CleanupChannelException)
    finally:
        channel.close()
        channel.waitclose(5)


def test_file_cleanup_client_lock_exception(tmpdir, execnet_gw, channel_id):
    channel = CleanupChannel(execnet_gw, tmpdir.strpath, channel_id=channel_id)
    try:
        client = CleanupClient(channel)

        try:
            client.lock("..")
            assert (
                False
            ), "CleanupChannelException should have been raised and wasn't"
        except CleanupChannelException as e:
            pass
    finally:
        channel.close()
        channel.waitclose(5)


def test_file_cleanup_client_unlock_callback(tmpdir, execnet_gw, channel_id):
    import logging
    import sys

    channel = CleanupChannel(execnet_gw, tmpdir.strpath, channel_id=channel_id)
    try:
        client = CleanupClient(channel)
        client.lock("one")

        def call(cb):
            client.unlock("one", callback=cb)

        cbcheck(call)

        def badcall(cb):
            client.unlock("two", callback=cb)

        cbcheck(badcall, CleanupChannelException)
    finally:
        channel.close()
        channel.waitclose(5)


def test_file_cleanup_client_unlock_exception(tmpdir, execnet_gw, channel_id):
    channel = CleanupChannel(execnet_gw, tmpdir.strpath, channel_id=channel_id)
    try:
        client = CleanupClient(channel)

        try:
            client.unlock("one")
            assert (
                False
            ), "CleanupChannelException should have been raised and wasn't"
        except CleanupChannelException as e:
            pass
    finally:
        channel.close()
        channel.waitclose(5)


def test_file_cleanup_client_flush_callback(tmpdir, execnet_gw, channel_id):
    channel = CleanupChannel(execnet_gw, tmpdir.strpath, channel_id=channel_id)
    client = CleanupClient(channel)

    def call(cb):
        client.flush(callback=cb)

    cbcheck(call)
