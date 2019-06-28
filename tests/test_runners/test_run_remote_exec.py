from atsim.pro_fit.runners import _run_remote_exec

# from assertpy import assert_that, fail, contents_of

import collections
import time

from ._runnercommon import execnet_gw, channel_id


def testKeepAlive(tmpdir, execnet_gw, channel_id):
    ch1 = execnet_gw.remote_exec(_run_remote_exec)
    try:
        ch1.send({"msg": "START_CHANNEL", "channel_id": channel_id})
        msg = ch1.receive(10.0)
        assert msg == dict(msg="READY", channel_id=channel_id)

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


def testJobPathDoesntExist(execnet_gw, tmpdir, channel_id):
    ch1 = execnet_gw.remote_exec(_run_remote_exec)
    try:
        ch1.send({"msg": "START_CHANNEL", "channel_id": channel_id})
        msg = ch1.receive(10.0)
        assert msg == dict(msg="READY", channel_id=channel_id)
        ch1.send(
            {
                "msg": "JOB_START",
                "job_path": tmpdir.strpath,
                "job_id": (1, 2, 3),
            }
        )
        msg = ch1.receive(10.0)
        assert msg["reason"].startswith("PATH_ERROR")
        del msg["reason"]
        assert msg == {
            "msg": "JOB_START_ERROR",
            "channel_id": channel_id,
            "job_id": (1, 2, 3),
        }
    finally:
        ch1.send(None)
        ch1.waitclose(5)


def testShellDoesntExist(execnet_gw, tmpdir, channel_id):
    ch1 = execnet_gw.remote_exec(_run_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_CHANNEL",
                "channel_id": channel_id,
                "shell": "/this/shell/doesnt/exist",
            }
        )
        msg = ch1.receive(10.0)
        assert msg == dict(
            msg="ERROR",
            channel_id=channel_id,
            reason="shell cannot be executed: '%s'"
            % "/this/shell/doesnt/exist",
        )
    finally:
        if not ch1.isclosed():
            ch1.send(None)
            ch1.waitclose(5)


def testStart(execnet_gw, tmpdir, channel_id):
    ch1 = execnet_gw.remote_exec(_run_remote_exec)
    try:
        ch1.send({"msg": "START_CHANNEL", "channel_id": channel_id})

        msg = ch1.receive(10.0)
        assert msg == dict(msg="READY", channel_id=channel_id)
        # Create a short job to run.
        jobpath = tmpdir.join("runjob")
        joboutput = tmpdir.join("joboutput")
        with jobpath.open("w") as outfile:
            print('echo "I\'m good" > joboutput', file=outfile)

        assert not joboutput.exists()

        ch1.send(
            {
                "msg": "JOB_START",
                "job_path": tmpdir.strpath,
                "job_id": (1, 2, 3),
            }
        )
        msg = ch1.receive(10.0)
        assert "pid" in msg
        del msg["pid"]
        assert msg == {
            "msg": "JOB_START",
            "channel_id": channel_id,
            "job_id": (1, 2, 3),
            "semaphore": 1,
        }

        msg = ch1.receive(20.0)
        assert joboutput.isfile()
        assert joboutput.readlines()[0][:-1] == "I'm good"
        assert msg == {
            "msg": "JOB_END",
            "channel_id": channel_id,
            "returncode": 0,
            "job_id": (1, 2, 3),
            "killed": False,
        }
    finally:
        ch1.send(None)
        ch1.waitclose(5)


def testEasyKill(execnet_gw, tmpdir, channel_id):
    ch1 = execnet_gw.remote_exec(_run_remote_exec)
    try:
        ch1.send({"msg": "START_CHANNEL", "channel_id": channel_id})

        msg = ch1.receive(10.0)
        assert msg == dict(msg="READY", channel_id=channel_id)

        # Create a short job to run.
        jobpath = tmpdir.join("runjob")
        joboutput = tmpdir.join("joboutput")
        with jobpath.open("w") as outfile:
            print('echo "I\'m good" > joboutput', file=outfile)
            print("sleep 10", file=outfile)

        assert not joboutput.exists()

        ch1.send(
            {
                "msg": "JOB_START",
                "job_path": tmpdir.strpath,
                "job_id": (1, 2, 3),
            }
        )
        msg = ch1.receive(10.0)
        assert "pid" in msg
        del msg["pid"]
        assert msg == {
            "msg": "JOB_START",
            "channel_id": channel_id,
            "job_id": (1, 2, 3),
            "semaphore": 1,
        }

        ch1.send({"msg": "JOB_KILL", "job_id": (1, 2, 3)})

        msg = ch1.receive(10.0)
        assert msg == {
            "msg": "JOB_END",
            "channel_id": channel_id,
            "returncode": -15,
            "job_id": (1, 2, 3),
            "killed": True,
        }
    finally:
        ch1.send(None)
        ch1.waitclose(5)


def testHardKill(execnet_gw, tmpdir, channel_id):
    ch1 = execnet_gw.remote_exec(_run_remote_exec)
    try:
        ch1.send(
            {
                "msg": "START_CHANNEL",
                "channel_id": channel_id,
                "hardkill_timeout": 2,
            }
        )

        msg = ch1.receive(10.0)
        assert msg == dict(msg="READY", channel_id=channel_id)

        # Create a short job to run.
        jobpath = tmpdir.join("runjob")
        joboutput = tmpdir.join("joboutput")
        with jobpath.open("w") as outfile:
            print('trap "" SIGINT SIGTERM', file=outfile)
            print('echo "I\'m good" > joboutput', file=outfile)
            print("sleep 20", file=outfile)

        assert not joboutput.exists()

        ch1.send(
            {
                "msg": "JOB_START",
                "job_path": tmpdir.strpath,
                "job_id": (1, 2, 3),
            }
        )
        time.sleep(1.0)
        msg = ch1.receive(10.0)
        assert "pid" in msg
        del msg["pid"]
        assert msg == {
            "msg": "JOB_START",
            "channel_id": channel_id,
            "job_id": (1, 2, 3),
            "semaphore": 1,
        }
        ch1.send({"msg": "JOB_KILL", "job_id": (1, 2, 3)})
        msg = ch1.receive(10.0)
        assert msg == {
            "msg": "JOB_END",
            "channel_id": channel_id,
            "returncode": -9,
            "job_id": (1, 2, 3),
            "killed": True,
        }
    finally:
        ch1.send(None)
        ch1.waitclose(5)


def testMultipleJobs(execnet_gw, tmpdir, channel_id):
    ch1 = execnet_gw.remote_exec(_run_remote_exec)
    try:
        ch1.send(
            {"msg": "START_CHANNEL", "channel_id": channel_id, "nprocesses": 2}
        )

        msg = ch1.receive(10.0)
        assert msg == dict(msg="READY", channel_id=channel_id)

        # Create a short job to run.
        jobdir1 = tmpdir.join("job1")
        jobdir1.ensure_dir()
        jobpath1 = jobdir1.join("runjob")
        joboutput1 = jobdir1.join("joboutput")
        with jobpath1.open("w") as outfile:
            print('echo "I\'m good1" > joboutput', file=outfile)
            print("sleep 10", file=outfile)
        assert not joboutput1.exists()

        jobdir2 = tmpdir.join("job2")
        jobdir2.ensure_dir()
        jobpath2 = jobdir2.join("runjob")
        joboutput2 = jobdir2.join("joboutput")
        with jobpath2.open("w") as outfile:
            print('echo "I\'m good2" > joboutput', file=outfile)
            print("sleep 5", file=outfile)
        assert not joboutput2.exists()

        jobdir3 = tmpdir.join("job3")
        jobdir3.ensure_dir()
        jobpath3 = jobdir3.join("runjob")
        joboutput3 = jobdir3.join("joboutput")
        with jobpath3.open("w") as outfile:
            print('echo "I\'m good3" > joboutput', file=outfile)
        assert not joboutput3.exists()

        ch1.send(
            {
                "msg": "JOB_START",
                "job_path": jobdir1.strpath,
                "job_id": (1, 2, 3),
            }
        )
        msg = ch1.receive(10.0)
        assert "pid" in msg
        del msg["pid"]
        assert msg == {
            "msg": "JOB_START",
            "channel_id": channel_id,
            "job_id": (1, 2, 3),
            "semaphore": 1,
        }

        ch1.send(
            {
                "msg": "JOB_START",
                "job_path": jobdir2.strpath,
                "job_id": (2, 3, 4),
            }
        )
        msg = ch1.receive(10.0)
        assert "pid" in msg
        del msg["pid"]
        assert msg == {
            "msg": "JOB_START",
            "channel_id": channel_id,
            "job_id": (2, 3, 4),
            "semaphore": 2,
        }

        ch1.send(
            {
                "msg": "JOB_START",
                "job_path": jobdir3.strpath,
                "job_id": (4, 5, 8),
            }
        )

        msg = ch1.receive(8.0)
        assert msg == {
            "msg": "JOB_END",
            "channel_id": channel_id,
            "returncode": 0,
            "job_id": (2, 3, 4),
            "killed": False,
        }

        msg = ch1.receive(2.0)
        assert "pid" in msg
        del msg["pid"]
        assert msg == {
            "msg": "JOB_START",
            "channel_id": channel_id,
            "job_id": (4, 5, 8),
            "semaphore": 2,
        }

        msg = ch1.receive(12.0)
        assert msg == {
            "msg": "JOB_END",
            "channel_id": channel_id,
            "returncode": 0,
            "job_id": (4, 5, 8),
            "killed": False,
        }

        msg = ch1.receive(12.0)
        assert msg == {
            "msg": "JOB_END",
            "channel_id": channel_id,
            "returncode": 0,
            "job_id": (1, 2, 3),
            "killed": False,
        }

        assert joboutput1.exists()
        assert joboutput1.read()[:-1] == "I'm good1"
        assert joboutput2.exists()
        assert joboutput2.read()[:-1] == "I'm good2"
        assert joboutput3.exists()
        assert joboutput3.read()[:-1] == "I'm good3"
    finally:
        ch1.send(None)
        ch1.waitclose(5)
