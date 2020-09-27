import logging
import os
import sys

import gevent
import pytest
from atsim import pro_fit
from atsim.pro_fit.runners._pbs_channel import PBSChannel
# from atsim.pro_fit.runners._pbsrunner_batch import PBSRunnerJobRecord
from atsim.pro_fit.runners._queueing_system_runner_batch import (
    QueueingSystemRunnerBatch, QueueingSystemRunnerJobRecord)

from ..testutil import vagrant_torque
from ._queueing_system_fixtures import (channel, channel_class, clearqueue,
                                        client, gw,
                                        queueing_system_test_module,
                                        runner_class, vagrant_box)
from ._runnercommon import DIR, FILE, runfixture, runnertestjob
from .test_queue_system_clients import chIsDir


def _createRunner(
    runner_class, runfixture, vagrantbox, sub_batch_size, pbsinclude=""
):
    username = vagrantbox.user()
    hostname = vagrantbox.hostname()
    port = vagrantbox.port()
    keyfilename = vagrantbox.keyfile()

    extraoptions = [("StrictHostKeyChecking", "no")]
    runner = runner_class(
        str(runner_class),
        "ssh://%s@%s:%s" % (username, hostname, port),
        pbsinclude,
        batch_size=sub_batch_size,
        poll_interval=1.0,
        identityfile=keyfilename,
        extra_ssh_options=extraoptions,
    )

    return runner


def _runBatch(runner, jobs):
    return runner.runBatch(jobs)


def _remoteIsDir(gw, path):
    ch = gw.remote_exec(chIsDir)
    ch.send(path)
    return ch.receive()


def waitcb(f):
    while not f._submittedQSRecords:
        gevent.sleep(1)


def makesleepy(jobs):
    for j in jobs:
        with open(os.path.join(j.path, "job_files", "runjob"), "a") as runjob:
            runjob.write("sleep 1200\n")


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testBatchTerminate(
    runfixture, gw, vagrant_box, channel_class, runner_class
):
    """Test batch .terminate() method."""
    # Make some sleepy jobs
    makesleepy(runfixture.jobs)
    runner = None
    indyrunner = None
    try:
        runner = _createRunner(runner_class, runfixture, vagrant_box, None)
        indyrunner = _createRunner(runner_class, runfixture, vagrant_box, None)

        f1 = runner.runBatch(runfixture.jobs[:6])
        f2 = runner.runBatch(runfixture.jobs[6:8])

        # Create a second runner to make sure that closing one runner doesn't affect the other.
        if3 = indyrunner.runBatch(runfixture.jobs[8:])

        assert gevent.wait(
            [
                gevent.spawn(waitcb, f1),
                gevent.spawn(waitcb, f2),
                gevent.spawn(waitcb, if3),
            ],
            60,
        )

        jr1 = f1._submittedQSRecords[0]
        jr2 = f2._submittedQSRecords[0]
        ij3 = if3._submittedQSRecords[0]

        assert jr1.submit_event.wait(60)
        assert jr1.jobId

        assert jr2.submit_event.wait(60)
        assert jr2.jobId

        assert ij3.submit_event.wait(60)
        assert ij3.jobId

        gevent.sleep(0)

        # Spin up a pbs_channel and check we can see the two jobs
        # ch = PBSChannel(gw, 'check_channel', nocb = True)
        ch = channel_class(gw, "check_channel", nocb=True)
        try:

            def qsel():
                ch.send({"msg": "QSELECT"})
                msg = next(ch)
                assert "QSELECT" == msg.get("msg", None)
                running_pbsids = set(msg["job_ids"])
                return running_pbsids

            pbsids = set([jr1.jobId, jr2.jobId])
            running_pbsids = qsel()
            assert pbsids.issubset(running_pbsids)
            assert pbsids != running_pbsids

            # Check the job directories exist
            for j in f1.jobs:
                assert _remoteIsDir(gw, j.remotePath)

            for j in f2.jobs:
                assert _remoteIsDir(gw, j.remotePath)

            for j in if3.jobs:
                assert _remoteIsDir(gw, j.remotePath)

            # Now close the second batch
            closevent = f2.terminate()
            assert closevent.wait(60)
            attempts = 5
            delay = 1
            for i in range(5):
                try:
                    assert qsel() == set([jr1.jobId, ij3.jobId])
                except AssertionError:
                    if i == attempts - 1:
                        raise
                    else:
                        gevent.sleep(delay)
                        delay *= 2.0

            # Check the job directories exist
            for j in f1.jobs:
                assert _remoteIsDir(gw, j.remotePath)

            for j in f2.jobs:
                assert not _remoteIsDir(gw, j.remotePath)

            for j in if3.jobs:
                assert _remoteIsDir(gw, j.remotePath)

            # Now close the first batch
            closevent = f1.terminate()
            assert closevent.wait(60)
            attempts = 5
            delay = 1
            for i in range(5):
                try:
                    assert qsel() == set([ij3.jobId])
                except AssertionError:
                    if i == attempts - 1:
                        raise
                    else:
                        gevent.sleep(delay)
                        delay *= 2.0

            # Check the job directories exist
            for j in f1.jobs:
                assert not _remoteIsDir(gw, j.remotePath)

            for j in f2.jobs:
                assert not _remoteIsDir(gw, j.remotePath)

            for j in if3.jobs:
                assert _remoteIsDir(gw, j.remotePath)

            # Now close the second runner's batch
            closevent = if3.terminate()
            assert closevent.wait(60)

            closevent = f1.terminate()
            assert closevent.wait(60)
            attempts = 5
            delay = 1
            for i in range(5):
                try:
                    assert qsel() == set()
                except AssertionError:
                    if i == attempts - 1:
                        raise
                    else:
                        gevent.sleep(delay)
                        delay *= 2.0

            # Check the job directories exist
            for j in f1.jobs:
                assert not _remoteIsDir(gw, j.remotePath)

            for j in f2.jobs:
                assert not _remoteIsDir(gw, j.remotePath)

            for j in if3.jobs:
                assert not _remoteIsDir(gw, j.remotePath)

        finally:
            ch.send(None)
    finally:
        if runner:
            runner.close()
        if indyrunner:
            indyrunner.close()


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testRunnerClose(runfixture, vagrant_box, runner_class, channel_class, gw):
    """Test batch .terminate() method."""

    # Make some sleepy jobs
    for j in runfixture.jobs:
        with open(os.path.join(j.path, "job_files", "runjob"), "a") as runjob:
            runjob.write("sleep 1200\n")

    runner = _createRunner(runner_class, runfixture, vagrant_box, None)
    indyrunner = _createRunner(runner_class, runfixture, vagrant_box, None)

    f1 = runner.runBatch(runfixture.jobs[:6])
    f2 = runner.runBatch(runfixture.jobs[6:8])

    # Create a second runner to make sure that closing one runner doesn't affect the other.
    if3 = indyrunner.runBatch(runfixture.jobs[8:])

    assert gevent.wait(
        [
            gevent.spawn(waitcb, f1),
            gevent.spawn(waitcb, f2),
            gevent.spawn(waitcb, if3),
        ],
        60,
    )

    jr1 = f1._submittedQSRecords[0]
    jr2 = f2._submittedQSRecords[0]
    ij3 = if3._submittedQSRecords[0]

    assert jr1.submit_event.wait(60)
    assert jr1.jobId

    assert jr2.submit_event.wait(60)
    assert jr2.jobId

    assert ij3.submit_event.wait(60)
    assert ij3.jobId

    gevent.sleep(0)


    # Spin up a pbs_channel and check we can see the two jobs
    ch = channel_class(gw, "check_channel", nocb=True)
    try:

        def qsel():
            ch.send({"msg": "QSELECT"})
            msg = next(ch)
            assert "QSELECT" == msg.get("msg", None)
            running_pbsids = set(msg["job_ids"])
            return running_pbsids

        pbsids = set([jr1.jobId, jr2.jobId])
        running_pbsids = qsel()
        assert pbsids.issubset(running_pbsids)
        assert pbsids != running_pbsids

        # Check the job directories exist
        for j in f1.jobs:
            assert _remoteIsDir(gw, j.remotePath)

        for j in f2.jobs:
            assert _remoteIsDir(gw, j.remotePath)

        for j in if3.jobs:
            assert _remoteIsDir(gw, j.remotePath)

        # Now close the runner
        closevent = runner.close()
        assert closevent.wait(60)
        attempts = 5
        delay = 5
        for i in range(5):
            try:
                assert qsel() == set([ij3.jobId])
            except AssertionError:
                if i == attempts - 1:
                    raise
                else:
                    gevent.sleep(delay)
                    delay *= 2.0

        # Check the job directories exist
        for j in f1.jobs:
            assert not _remoteIsDir(gw, j.remotePath)

        for j in f2.jobs:
            assert not _remoteIsDir(gw, j.remotePath)

        for j in if3.jobs:
            assert _remoteIsDir(gw, j.remotePath)

        # Now close the first batch
        closevent = f1.terminate()
        assert closevent.wait(60)
        attempts = 5
        delay = 1
        for i in range(5):
            try:
                assert qsel() == set([ij3.jobId])
            except AssertionError:
                if i == attempts - 1:
                    raise
                else:
                    gevent.sleep(delay)
                    delay *= 2.0

        # Check the job directories exist
        for j in f1.jobs:
            assert not _remoteIsDir(gw, j.remotePath)

        for j in f2.jobs:
            assert not _remoteIsDir(gw, j.remotePath)

        for j in if3.jobs:
            assert _remoteIsDir(gw, j.remotePath)

        try:
            runner._inner._pbschannel.send({"msg": "QSELECT"})
            assert False, "IOError not raised"
        except IOError:
            pass

    finally:
        ch.send(None)


def _tstSingleBatch(runner_class, runfixture, vagrant_box, sub_batch_size):
    # root = logging.getLogger()
    # root.setLevel(logging.DEBUG)

    # ch = logging.StreamHandler(sys.stdout)
    # ch.setLevel(logging.DEBUG)
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # ch.setFormatter(formatter)
    # root.addHandler(ch)
    runner = _createRunner(
        runner_class, runfixture, vagrant_box, sub_batch_size
    )
    try:
        _runBatch(runner, runfixture.jobs).join()
        for job in runfixture.jobs:
            runnertestjob(runfixture, job.variables.id, True)
    finally:
        runner.close()


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testAllInSingleBatch(runner_class, runfixture, vagrant_box):
    _tstSingleBatch(runner_class, runfixture, vagrant_box, None)


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testAllInSingleBatch_with_coexisting_jobs(
    runner_class, runfixture, vagrant_box
):
    # Make some sleepy jobs
    for j in runfixture.jobs[:6]:
        with open(os.path.join(j.path, "job_files", "runjob"), "a") as runjob:
            runjob.write("sleep 1200\n")

    runner = None
    indyrunner = None
    try:
        runner = _createRunner(runner_class, runfixture, vagrant_box, None)
        indyrunner = _createRunner(runner_class, runfixture, vagrant_box, None)

        f1 = runner.runBatch(runfixture.jobs[:6])

        assert gevent.wait([gevent.spawn(waitcb, f1)], 60)

        f2 = indyrunner.runBatch(runfixture.jobs[6:8])
        f2.join()

        for job in runfixture.jobs[6:8]:
            runnertestjob(runfixture, job.variables.id, True)

    finally:
        if runner:
            runner.close().wait(20)
        if indyrunner:
            indyrunner.close().wait(20)


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testAllInSingleBatch_sub_batch_size_1(
    runner_class, runfixture, vagrant_box
):
    _tstSingleBatch(runner_class, runfixture, vagrant_box, 1)


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testAllInSingleBatch_sub_batch_size_5(
    runner_class, runfixture, vagrant_box
):
    _tstSingleBatch(runner_class, runfixture, vagrant_box, 5)


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testAllInMultipleBatch(runner_class, runfixture, vagrant_box):
    runner = _createRunner(runner_class, runfixture, vagrant_box, None)
    try:
        f1 = _runBatch(runner, runfixture.jobs[:6])
        f2 = _runBatch(runner, runfixture.jobs[6:])
        f2.join()
        f1.join()
        for job in runfixture.jobs:
            runnertestjob(runfixture, job.variables.id, True)
    finally:
        runner.close()


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testAllInMultipleBatch_sub_batch_size_5(
    runner_class, runfixture, vagrant_box
):
    runner = _createRunner(runner_class, runfixture, vagrant_box, 5)
    try:
        f1 = _runBatch(runner, runfixture.jobs[:6])
        f2 = _runBatch(runner, runfixture.jobs[6:])
        f2.join()
        f1.join()
        for job in runfixture.jobs:
            runnertestjob(runfixture, job.variables.id, True)
    finally:
        runner.close()


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testAllInMultipleBatch_sub_batch_size_1(
    runner_class, runfixture, vagrant_box
):
    runner = _createRunner(runner_class, runfixture, vagrant_box, 1)
    try:
        f1 = _runBatch(runner, runfixture.jobs[:6])
        f2 = _runBatch(runner, runfixture.jobs[6:])
        f2.join()
        f1.join()
        for job in runfixture.jobs:
            runnertestjob(runfixture, job.variables.id, True)
    finally:
        runner.close()


def testPBSRunnerJobRecordIsFull():
    class Client:
        pass

    jr = QueueingSystemRunnerJobRecord("name", 2, Client(), None)
    assert not jr.isFull

    jr.append(None)
    assert not jr.isFull

    jr.append(None)
    assert jr.isFull

    try:
        jr.append(None)
        assert False, "IndexError not raised"
    except IndexError:
        pass


def qstatRemoteExec(channel):
    pbsId = channel.receive()
    import subprocess

    output = subprocess.check_output(["qstat", "-f", pbsId])
    channel.send(output)


@pytest.mark.slow
@pytest.mark.usefixtures("clearqueue")
def testPBSInclude(
    runfixture, queueing_system_test_module, vagrant_box, runner_class, gw
):
    if queueing_system_test_module.name != "PBS":
        pytest.skip("Test is only for PBSRunner")

    makesleepy(runfixture.jobs)
    runner = _createRunner(
        runner_class,
        runfixture,
        vagrant_box,
        1,
        "#PBS -l mem=10Mb\n#PBS -l walltime=5:00",
    )
    try:
        batch = _runBatch(runner, [runfixture.jobs[0]])
        gevent.wait([gevent.spawn(waitcb, batch)], 60)

        j = batch._submittedQSRecords[0]
        assert j.submit_event.wait(60)
        assert j.jobId

        pbsid = j.jobId

        ch = gw.remote_exec(qstatRemoteExec)
        ch.send(pbsid)
        qstat = ch.receive()
        qstat = qstat.split("\n")

        memline = [
            line
            for line in qstat
            if line.strip().startswith("Resource_List.mem")
        ]
        walltimeline = [
            line
            for line in qstat
            if line.strip().startswith("Resource_List.walltime")
        ]

        assert len(walltimeline) == 1
        walltimeline = walltimeline[0]
        assert walltimeline.endswith("00:05:00")

        assert len(memline) == 1
        memline = memline[0]
        assert memline.endswith("10mb")
    finally:
        runner.close()
