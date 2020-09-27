import os
import pathlib

import atsim.pro_fit.exceptions
import atsim.pro_fit.jobtasks
import atsim.pro_fit.tools.pprofit as pp
import pytest
from atsim.pro_fit.jobfactories import Job

from .common import MockJobFactory

from atsim.pro_fit.variables import Variables


def _make_tabulation_file(p: pathlib.Path, v: str):
    with (p).open("w") as outfile:
        outfile.write("[Tabulation]\n")
        outfile.write("target : LAMMPS\n")
        outfile.write("cutoff : 10.0\n")
        outfile.write("nr : 100\n")
        outfile.write("\n")
        outfile.write("[Pair]\n")
        outfile.write("O-O : as.constant {}\n".format(v))


def test_potable_createFromConfig(tmpdir):

    cfgitems = [
        ("type", "Potable"),
        ("input_filename", "infile"),
        ("output_filename", "outfile")
    ]

    potable_task = atsim.pro_fit.jobtasks.PotableJobTask.createFromConfig(
        "task_name", "job_path", cfgitems)

    assert potable_task.name == "task_name"
    assert potable_task.input_filename == "infile"
    assert potable_task.output_filename == "outfile"

    # Check that an exception is raised if outfile tries to escape the job directory
    test_out_files = ["/blah", "../", "blah/../../"]

    for test_out_file in test_out_files:
        with pytest.raises(atsim.pro_fit.exceptions.ConfigException):
            atsim.pro_fit.jobtasks.PotableJobTask.createFromConfig("task_name", tmpdir.strpath,
                                                                   [
                                                                       ("type",
                                                                        "Potable"),
                                                                       ("input_filename",
                                                                        "infile"),
                                                                       ("output_filename",
                                                                        test_out_file)
                                                                   ])


def test_potable_before_run(tmpdir):
    jobdir = tmpdir/"job_files"
    jobdir.ensure_dir()

    p = jobdir/"table.aspot"
    _make_tabulation_file(p, "10.0")

    job_factory = MockJobFactory("Local", "tabulate", [])
    job = Job(job_factory, tmpdir.strpath, Variables([('v', 5.0, True)]))

    out_table = "output_table"
    task = atsim.pro_fit.jobtasks.PotableJobTask("name", str(p), out_table)
    task.beforeRun(job)

    out_table = jobdir / out_table
    assert out_table.isfile()

    with out_table.open() as infile:
        line = next(infile)
        assert line.startswith('O-O')
        line = next(infile)
        assert line.startswith('N 99 R')


def test_potable_end_to_end(tmpdir):
    tmpdir = pathlib.Path(tmpdir.strpath)

    pproot = tmpdir / "root"
    job_tmpfiles = tmpdir / "tmp"
    os.makedirs(job_tmpfiles)
    keepfiles = tmpdir / "keep"
    os.makedirs(keepfiles)

    jobdir = pproot / "fit_files" / "table_job"
    os.makedirs(jobdir)

    with (pproot / "fit.cfg").open("w") as outfile:
        outfile.write("[FittingRun]\n")
        outfile.write("title : Table_Run\n")
        outfile.write("\n")
        outfile.write("[Minimizer]\n")
        outfile.write("type : SingleStep\n")
        outfile.write("keep-files-directory : {}\n".format(job_tmpfiles))
        outfile.write("\n")
        outfile.write("[Runner:Local]\n")
        outfile.write("type : Local\n")
        outfile.write("nprocesses : 1\n")
        outfile.write("\n")
        outfile.write("[Variables]\n")
        outfile.write("V = 5.0 *\n")

    header_regex = r"^N ([0-9]+?) R ([0-9]*\.?[0-9]*) ([0-9]*\.?[0-9]*)"
    line20_regex = r"^20 ([0-9]*\.?[0-9]*) ([0-9]*\.?[0-9]*) ([+-]?[0-9]*\.?[0-9]*)"

    # Create job.cfg
    job_cfg = jobdir / "job.cfg"

    with job_cfg.open("w") as outfile:
        outfile.write("[Job]\n")
        outfile.write("type : Template\n")
        outfile.write("runner : Local\n")
        outfile.write("\n")

        outfile.write("[Evaluator:regex]\n")
        outfile.write("type : Regex\n")
        outfile.write("filename : table.lmptab\n")
        outfile.write("nr : /{}/ 99 1.0 1\n".format(header_regex))
        outfile.write("dr : /{}/ 0.10101010 1.0 2\n".format(header_regex))
        outfile.write("cutoff : /{}/ 10.0 1.0 3\n".format(header_regex))
        outfile.write("r : /{}/ 2.02020202 1.0 1\n".format(line20_regex))
        outfile.write("v : /{}/ 5.0 1.0 2\n".format(line20_regex))
        outfile.write("dvdr : /{}/ 0.0 1.0 3\n".format(line20_regex))
        outfile.write("\n")

        outfile.write("[Task:table.lmptab]\n")
        outfile.write("type : Potable\n")
        outfile.write("input_filename : constant.aspot\n")
        outfile.write("output_filename : table.lmptab\n")

    _make_tabulation_file(jobdir/"constant.aspot.in", "@V@")

    with (jobdir/"runjob").open("w"):
        pass

    # Run fitting run.
    oldcwd = os.getcwd()
    try:
        os.chdir(pproot)
        cfg = pp._getSingleStepCfg(str(job_tmpfiles), str(keepfiles), [])
        merit = cfg.merit

        c_joblists = []

        def after_evaluation(candidate_joblists):
            c_joblists[:] = candidate_joblists

        merit.afterEvaluation.append(after_evaluation)

        minimizer = cfg.minimizer
        minimizer.minimize(merit)
    finally:
        os.chdir(oldcwd)

    assert len(c_joblists) == 1
    evaluator_records = c_joblists[0][1][0].evaluatorRecords[0]
    assert len(evaluator_records) == 6

    for er in evaluator_records:
        assert not er.errorFlag

    erdict = dict(
        zip([er.name for er in evaluator_records], evaluator_records))

    assert erdict["nr"].extractedValue == pytest.approx(99)
    assert erdict["dr"].extractedValue == pytest.approx(0.10101010)
    assert erdict["cutoff"].extractedValue == pytest.approx(10.0)
    assert erdict["r"].extractedValue == pytest.approx(2.02020202)
    assert erdict["v"].extractedValue == pytest.approx(5.0)
    assert erdict["dvdr"].extractedValue == pytest.approx(0.0)
