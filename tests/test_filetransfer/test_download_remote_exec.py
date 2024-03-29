from atsim.pro_fit.filetransfer.remote_exec import file_transfer_remote_exec
from atsim.pro_fit.filetransfer.remote_exec.file_transfer_remote_exec import (
    FILE,
    DIR,
)

import os
import pathlib

from ._common import execnet_gw, channel_id

def testGoodStart(tmpdir, execnet_gw, channel_id):
    ch1 = execnet_gw.remote_exec(file_transfer_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_DOWNLOAD_CHANNEL",
                "channel_id": channel_id,
                "remote_path": tmpdir.strpath,
            }
        )
        msg = ch1.receive(10.0)
        assert (
            dict(
                msg="READY", channel_id=channel_id, remote_path=tmpdir.strpath
            )
            == msg
        )
    finally:
        ch1.send(None)
        ch1.waitclose(5)


def testBadStart_nonexistent_directory(execnet_gw, channel_id):
    ch1 = execnet_gw.remote_exec(file_transfer_remote_exec)
    try:
        badpath = "/this/is/not/a/path"
        assert not pathlib.Path(badpath).exists()

        ch1.send(
            {
                "msg": "START_DOWNLOAD_CHANNEL",
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
        try:
            ch1.send(None)
            ch1.waitclose(5)
        except IOError:
            pass


def testKeepAlive(tmpdir, execnet_gw, channel_id):
    ch1 = execnet_gw.remote_exec(file_transfer_remote_exec)
    try:
        ch1 = execnet_gw.remote_exec(file_transfer_remote_exec)
        ch1.send(
            {
                "msg": "START_DOWNLOAD_CHANNEL",
                "channel_id": channel_id,
                "remote_path": tmpdir.strpath,
            }
        )
        msg = ch1.receive(10.0)
        assert (
            dict(
                msg="READY", channel_id=channel_id, remote_path=tmpdir.strpath
            )
            == msg
        )

        ch1.send({"msg": "KEEP_ALIVE", "channel_id": channel_id, "id": "1234"})
        msg = ch1.receive(10.0)
        assert msg == {
            "msg": "KEEP_ALIVE",
            "channel_id": channel_id,
            "id": "1234",
        }
    finally:
        ch1.send(None)
        ch1.waitclose(5)


def testListDir(tmpdir, execnet_gw, channel_id):
    files = []
    # Create a fake directory structure
    p = tmpdir.join("0")
    p.mkdir()
    p.chmod(0o700)
    f = dict(remote_path=p.strpath, mode=os.stat(p.strpath).st_mode, type=DIR)
    files.append(f)

    p = tmpdir.join("runjob")
    p.write("")
    p.chmod(0o700)
    f = dict(remote_path=p.strpath, mode=os.stat(p.strpath).st_mode, type=FILE)
    files.append(f)

    p = tmpdir.join("file2")
    p.write("")
    p.chmod(0o600)
    f = dict(remote_path=p.strpath, mode=os.stat(p.strpath).st_mode, type=FILE)
    files.append(f)

    transid = 0

    import operator

    expect = {
        "msg": "LIST",
        "id": transid,
        "channel_id": channel_id,
        "files": sorted(files, key=operator.itemgetter("remote_path")),
    }

    ch1 = execnet_gw.remote_exec(file_transfer_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_DOWNLOAD_CHANNEL",
                "channel_id": channel_id,
                "remote_path": tmpdir.strpath,
            }
        )
        msg = ch1.receive(10.0)
        assert msg == dict(
            msg="READY", channel_id=channel_id, remote_path=tmpdir.strpath
        )

        ch1.send({"msg": "LIST", "id": transid, "remote_path": tmpdir.strpath})
        msg = ch1.receive(10.0)

        assert "files" in msg
        msg["files"] = sorted(
            msg["files"], key=operator.itemgetter("remote_path")
        )
        assert expect == msg
    finally:
        ch1.send(None)
        ch1.waitclose(5)


def testDownloadFile_bad(tmpdir, execnet_gw, channel_id):
    ch1 = execnet_gw.remote_exec(file_transfer_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_DOWNLOAD_CHANNEL",
                "channel_id": channel_id,
                "remote_path": tmpdir.strpath,
            }
        )
        ch1.receive(10.0)

        # Ask for a non existent file
        rpath = tmpdir.join("not_there").strpath
        ch1.send({"msg": "DOWNLOAD_FILE", "id": 1, "remote_path": rpath})
        rcv = ch1.receive(10.0)
        assert {
            "msg": "ERROR",
            "channel_id": channel_id,
            "reason": "file does not exist",
            "error_code": ("IOERROR", "FILEDOESNOTEXIST"),
            "remote_path": rpath,
            "id": 1,
        } == rcv

        rpath = tmpdir.join("directory")
        rpath.mkdir()
        assert rpath.isdir()
        ch1.send(
            {"msg": "DOWNLOAD_FILE", "id": 1, "remote_path": rpath.strpath}
        )
        rcv = ch1.receive(10.0)
        assert {
            "msg": "ERROR",
            "channel_id": channel_id,
            "reason": "path refers to a directory and cannot be downloaded",
            "remote_path": rpath,
            "id": 1,
            "error_code": ("IOERROR", "ISDIR"),
        } == rcv

        rpath = tmpdir.join("unreadable")
        rpath.write("")
        rpath.chmod(0o0)
        ch1.send(
            {"msg": "DOWNLOAD_FILE", "id": 1, "remote_path": rpath.strpath}
        )
        rcv = ch1.receive(10.0)
        assert "exc_msg" in rcv
        del rcv["exc_msg"]
        assert {
            "msg": "ERROR",
            "channel_id": channel_id,
            "reason": "permission denied",
            "remote_path": rpath.strpath,
            "id": 1,
            "error_code": ("IOERROR", "FILEOPEN"),
        } == rcv
        rpath.chmod(0o600)
    finally:
        ch1.send(None)
        ch1.waitclose(5)


def testDownloadFile(tmpdir, execnet_gw, channel_id):
    ch1 = execnet_gw.remote_exec(file_transfer_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_DOWNLOAD_CHANNEL",
                "channel_id": channel_id,
                "remote_path": tmpdir.strpath,
            }
        )
        ch1.receive(10.0)

        chpath = tmpdir.join("0").join("1").join("file")
        contents = b"one two three four"
        chpath.write(contents, ensure=True)

        ch1.send(
            {"msg": "DOWNLOAD_FILE", "id": 1, "remote_path": chpath.strpath}
        )
        rcv = ch1.receive(10.0)
        assert {
            "msg": "DOWNLOAD_FILE",
            "channel_id": channel_id,
            "id": 1,
            "remote_path": chpath.strpath,
            "file_data": contents,
            "mode": os.stat(chpath.strpath).st_mode,
        } == rcv
    finally:
        ch1.send(None)
        ch1.waitclose(5)
