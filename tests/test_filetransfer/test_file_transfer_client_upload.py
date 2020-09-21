import pathlib

import pytest
from atsim.pro_fit.filetransfer import (ChannelException,
                                        DirectoryUploadException,
                                        UploadCancelledException,
                                        UploadChannels, UploadDirectory,
                                        UploadHandler)

from ._common import channel_id, cmpdirs
from ._common import create_dir_structure as _create_dir_structure
from ._common import execnet_gw

# pytestmark = pytest.mark.skip()

KEEP_ALIVE = 0.5


def create_dir_structure(tmpdir):
    _create_dir_structure(tmpdir)
    rpath = tmpdir.join("remote")
    lpath = tmpdir.join("local")
    rpath.rename(lpath)
    dpath = tmpdir.join("dest")
    dpath.rename(rpath)

    with lpath.join("0", "1", "hello_world.txt").open("w") as outfile:
        print("Hello World!", file=outfile)


def do_ul(tmpdir, channels, dl=None, do_cmp=True):
    spath = tmpdir.join("local")
    dpath = tmpdir.join("remote")
    if dl is None:
        dl = UploadDirectory(channels, spath.strpath, dpath.strpath)
    dl.upload()

    # Compare the remote and dest directories
    if do_cmp:
        cmpdirs(spath.strpath, dpath.strpath)


def testUploadChannel_BadStart_nonexistent_directory(execnet_gw, channel_id):
    badpath = "/this/is/not/a/path"
    assert not pathlib.Path(badpath).exists()
    ch = None
    try:
        ch = UploadChannels(
            execnet_gw, badpath, channel_id=channel_id, keepAlive=KEEP_ALIVE
        )
        assert False, "ChannelException should have been raised."
    except ChannelException as e:
        assert str(e).endswith("are existing directories.")
    finally:
        if ch:
            ch.broadcast(None)
            ch.waitclose(2)


def testDirectoryUpload_single_channel(tmpdir, execnet_gw, channel_id):
    create_dir_structure(tmpdir)
    # Create a upload channel.
    ch1 = UploadChannels(
        execnet_gw,
        tmpdir.join("remote").strpath,
        channel_id=channel_id,
        keepAlive=KEEP_ALIVE,
    )
    try:
        do_ul(tmpdir, ch1)
    finally:
        ch1.broadcast(None)
        ch1.waitclose(2)


def testDirectoryUpload_local_nonexistent(tmpdir, execnet_gw, channel_id):
    create_dir_structure(tmpdir)

    spath = tmpdir.join("local")
    dpath = tmpdir.join("remote")
    spath.remove(rec=True)

    assert not spath.exists()

    # Create a upload channel.
    ch1 = UploadChannels(
        execnet_gw,
        tmpdir.join("remote").strpath,
        channel_id=channel_id,
        keepAlive=KEEP_ALIVE,
    )
    try:
        UploadDirectory(ch1, spath.strpath, dpath.strpath)
        assert False, "OSError should have been raised, it wasn't."
    except OSError:
        pass
    finally:
        ch1.broadcast(None)
        ch1.waitclose(2)


def testUploadHandler_rewrite_remote_path():
    ulh = UploadHandler("/var/private/source", "remote")

    msg = {"remote_path": "/var/private/source/hello.txt"}
    assert {"remote_path": "remote/hello.txt"} == ulh.rewrite_file_path(msg)

    msg = {"remote_path": "/var/private/source/one/two"}
    assert {"remote_path": "remote/one/two"} == ulh.rewrite_directory_path(msg)

    ulh = UploadHandler(
        "/private/var/folders/t6/11n0g15d5hz5cwz83wt4zgm80000gn/T/pytest-of-mr498/pytest-478/testDirectoryUpload_create_mul0/source_1/",
        "/private/var/folders/t6/11n0g15d5hz5cwz83wt4zgm80000gn/T/pytest-of-mr498/pytest-478/testDirectoryUpload_create_mul0/dest_1/",
    )

    # Remote-root: /private/var/folders/t6/11n0g15d5hz5cwz83wt4zgm80000gn/T/pytest-of-mr498/pytest-478/testDirectoryUpload_create_mul0
    # Source: /private/var/folders/t6/11n0g15d5hz5cwz83wt4zgm80000gn/T/pytest-of-mr498/pytest-478/testDirectoryUpload_create_mul0/source_1/
    # Dest:   /private/var/folders/t6/11n0g15d5hz5cwz83wt4zgm80000gn/T/pytest-of-mr498/pytest-478/testDirectoryUpload_create_mul0/dest_1

    msg = {
        "remote_path": "/private/var/folders/t6/11n0g15d5hz5cwz83wt4zgm80000gn/T/pytest-of-mr498/pytest-478/testDirectoryUpload_create_mul0/source_1/file.txt"
    }
    assert {
        "remote_path": "/private/var/folders/t6/11n0g15d5hz5cwz83wt4zgm80000gn/T/pytest-of-mr498/pytest-478/testDirectoryUpload_create_mul0/dest_1/file.txt"
    } == ulh.rewrite_directory_path(msg)


def testUploadHandler_complete_callback(tmpdir, execnet_gw, channel_id):
    create_dir_structure(tmpdir)
    # Create a download channel.
    ch1 = UploadChannels(
        execnet_gw,
        tmpdir.join("remote").strpath,
        channel_id=channel_id,
        keepAlive=KEEP_ALIVE,
    )
    try:

        class ULH(UploadHandler):
            def __init__(self, local_path, remote_path):
                self.complete_called = False
                self.complete_exception = None
                super(ULH, self).__init__(local_path, remote_path)

            def finish(self, error=None):
                self.complete_called = True
                self.completion_exception = error

        remote_path = tmpdir.join("remote")
        local_path = tmpdir.join("local")

        ulh = ULH(local_path.strpath, remote_path.strpath)
        ul = UploadDirectory(ch1, local_path.strpath, remote_path.strpath, ulh)

        do_ul(tmpdir, ch1, ul)
        assert ulh.complete_called == True
        assert ulh.completion_exception is None

        # Now test what happens if no exception is passed to DownloadHandler.finish() but finish itself raises.
        class ThrowMe(Exception):
            pass

        class ThrowHandler(ULH):
            def finish(self, exception=None):
                super(ThrowHandler, self).finish(exception)
                raise ThrowMe()
                

        remote_path.remove(rec=True)
        remote_path.ensure_dir()
        ulh = ThrowHandler(local_path.strpath, remote_path.strpath)
        ul = UploadDirectory(ch1, local_path.strpath, remote_path.strpath, ulh)

        try:
            do_ul(tmpdir, ch1, ul, False)
            assert (
                False
            ), "ThrowMe exception should have been raised but wasn't"
        except ThrowMe:
            pass
        assert ulh.complete_called == True
        assert ulh.completion_exception is None

        # Now try again when an error-occurs - destination isn't writable should throw an error.
        remote_path.chmod(0o0)
        try:
            ulh = ULH(local_path.strpath, remote_path.strpath)
            ul = UploadDirectory(
                ch1, local_path.strpath, remote_path.strpath, ulh
            )

            try:
                do_ul(tmpdir, ch1, ul, False)
                assert (
                    False
                ), "DirectoryUploadException should have been raised."
            except DirectoryUploadException:
                pass

            assert ulh.complete_called == True
            assert type(ulh.completion_exception) == DirectoryUploadException

            # Now check supresssion of exception raising.
            class NoRaise(ULH):
                def finish(self, error=None):
                    super(NoRaise, self).finish(error)
                    return False

            ulh = NoRaise(local_path.strpath, remote_path.strpath)
            ul = UploadDirectory(
                ch1, local_path.strpath, remote_path.strpath, ulh
            )

            do_ul(tmpdir, [ch1], ul, False)
            assert ulh.complete_called == True
            assert type(ulh.completion_exception) == DirectoryUploadException

            # Test that DownloadHandler.complete can raise exception and this will be correctly propagated.
            ulh = ThrowHandler(local_path.strpath, remote_path.strpath)
            ul = UploadDirectory(
                ch1, local_path.strpath, remote_path.strpath, ulh
            )
        finally:
            local_path.chmod(0o700)
    finally:
        ch1.broadcast(None)
        ch1.waitclose(2)


def testDirectoryUpload_create_multiple_uploads(
    tmpdir, execnet_gw, channel_id
):
    source1 = tmpdir.join("source_1")
    source2 = tmpdir.join("source_2")

    source1.ensure_dir()
    source2.ensure_dir()

    with source1.join("file.txt").open("w") as outfile:
        print("Hello", file=outfile)

    with source2.join("file.txt").open("w") as outfile:
        print("Goodbye", file=outfile)

    dest1 = tmpdir.join("dest_1")
    dest2 = tmpdir.join("dest_2")

    dest1.ensure_dir()
    dest2.ensure_dir()

    ch1 = UploadChannels(execnet_gw, tmpdir.strpath, keepAlive=KEEP_ALIVE)
    try:
        dl1 = UploadDirectory(ch1, source1.strpath, dest1.strpath)
        dl2 = UploadDirectory(ch1, source2.strpath, dest2.strpath)

        dl1.upload()
        assert dest1.join("file.txt").isfile()
        line = next(dest1.join("file.txt").open())[:-1]
        assert line == "Hello"

        dl2.upload()
        assert dest2.join("file.txt").isfile()
        line = next(dest2.join("file.txt").open())[:-1]
        assert line == "Goodbye"
    finally:
        ch1.broadcast(None)
        ch1.waitclose(2)


def testDirectoryUpload_test_nonblocking(tmpdir, execnet_gw, channel_id):
    import threading

    source1 = tmpdir.join("source_1")

    source1.ensure_dir()

    with source1.join("file.txt").open("w") as outfile:
        print("Hello", file=outfile)

    dest1 = tmpdir.join("dest_1")

    dest1.ensure_dir()

    pause_event = threading.Event()
    upload_event = threading.Event()

    class PauseUploadHandler(UploadHandler):
        def upload(self, msg):
            upload_event.set()
            pause_event.wait()
            return super(PauseUploadHandler, self).upload(msg)

    class CallEventThread(threading.Thread):
        def __init__(self, dest1, paus_event):
            self.dest1 = dest1
            self.pause_event = pause_event
            super(CallEventThread, self).__init__()

        def run(self):
            upload_event.wait()
            import time

            time.sleep(1)
            self.dest1_state = self.dest1.join("file.txt").exists()
            self.pause_event.set()

    ct = CallEventThread(dest1, pause_event)
    ct.start()

    ch1 = UploadChannels(execnet_gw, tmpdir.strpath, keepAlive=KEEP_ALIVE)
    try:
        dl1 = UploadDirectory(
            ch1,
            source1.strpath,
            dest1.strpath,
            PauseUploadHandler(source1.strpath, dest1.strpath),
        )
        finished_event = dl1.upload(non_blocking=True)
        import time

        time.sleep(2)
        finished_event.wait(10)
        assert dest1.join("file.txt").isfile()
        line = next(dest1.join("file.txt").open())[:-1]
        assert line == "Hello"

        assert not ct.dest1_state
    finally:
        ch1.broadcast(None)
        ch1.waitclose(2)


def testDirectoryUpload_ensure_root(tmpdir, execnet_gw, channel_id):
    remote = tmpdir.join("remote")
    local = tmpdir.join("local")

    remote.ensure_dir()
    local.ensure_dir()

    with local.join("hello.txt").open("w") as outfile:
        print("Hello World!", file=outfile)

    local.join("blah").mkdir()
    dest = remote.join("Batch-0/0")

    ch1 = UploadChannels(
        execnet_gw, remote.strpath, channel_id=channel_id, keepAlive=KEEP_ALIVE
    )
    try:
        ud = UploadDirectory(ch1, local.strpath, dest.strpath)
        ud.upload()

        assert dest.isdir()
        assert dest.join("blah").isdir()
        assert dest.join("hello.txt").read()[:-1] == "Hello World!"
    finally:
        ch1.broadcast(None)
        ch1.waitclose(2)


def testDirectoryUpload_cancel(tmpdir, execnet_gw, channel_id):
    import threading

    source1 = tmpdir.join("source_1")

    source1.ensure_dir()

    with source1.join("file.txt").open("w") as outfile:
        print("Hello", file=outfile)

    dest1 = tmpdir.join("dest_1")

    dest1.ensure_dir()

    pause_event = threading.Event()

    class PauseUploadHandler(UploadHandler):
        def __init__(self, *args, **kwargs):
            super(PauseUploadHandler, self).__init__(*args, **kwargs)
            self.finishCalled = False
            self.exception = None

        def upload(self, msg):
            pause_event.wait(10)
            return super(PauseUploadHandler, self).upload(msg)

        def finish(self, exception=None):
            self.finishCalled = True
            self.exception = exception
            return None

    ulh = PauseUploadHandler(source1.strpath, dest1.strpath)
    ch1 = UploadChannels(execnet_gw, tmpdir.strpath, keepAlive=KEEP_ALIVE)
    try:
        ul1 = UploadDirectory(ch1, source1.strpath, dest1.strpath, ulh)
        finished_event = ul1.upload(non_blocking=True)
        cancel_event = ul1.cancel()

        assert cancel_event.wait(4)
        assert finished_event.wait(4)
        assert ulh.finishCalled

        try:
            raise ulh.exception
        except UploadCancelledException:
            pass
    finally:
        ch1.broadcast(None)
        ch1.waitclose(2)
