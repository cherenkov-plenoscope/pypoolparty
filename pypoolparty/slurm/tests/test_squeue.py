import pypoolparty
import importlib
import os


def test_parse_squeue():
    pypoolparty_dir = pypoolparty.utils.resources_path()
    stdout_path = os.path.join(
        pypoolparty_dir,
        "slurm",
        "tests",
        "resources",
        "squeue_format_all.stdout",
    )

    with open(stdout_path) as f:
        o = f.read()

    d = pypoolparty.slurm.calling._parse_stdout_format_all(o)

    print(d[18])

    assert len(d) == 231
