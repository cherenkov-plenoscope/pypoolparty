import pypoolparty
import importlib
import os


def test_slurm_filter_stderr_ends_with_newline():
    ierr = "one\ntwo\nthree\n"
    oerr = pypoolparty.slurm.filter_stderr(stderr=ierr)
    assert ierr == oerr


def test_slurm_filter_stderr_empty():
    ierr = ""
    oerr = pypoolparty.slurm.filter_stderr(stderr=ierr)
    assert ierr == oerr


def test_slurm_filter_stderr_only_newline():
    ierr = "\n"
    oerr = pypoolparty.slurm.filter_stderr(stderr=ierr)
    assert ierr == oerr


def test_slurm_filter_stderr_slurm_foo():
    ierr = [
        "one",
        "two",
        "three",
        "slurmstepd: error: blablabla ... DUE TO JOB NOT ENDING WITH SIGNALS",
    ]
    ierr = str.join("\n", ierr)
    oerr = pypoolparty.slurm.filter_stderr(stderr=ierr)
    assert oerr == "one\ntwo\nthree"
