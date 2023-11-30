import json
import subprocess
from . import utils


def dummy_init_queue_state(path, evil_jobs=[]):
    qstate = {"running": [], "pending": [], "evil_jobs": evil_jobs}
    utils.write_text(path=path, content=json.dumps(qstate))


def dummy_run_job(job):
    with open(job["_opath"], "wt") as o, open(job["_epath"], "wt") as e:
        subprocess.call(
            [job["_python_path"], job["_script_arg_0"], job["_script_arg_1"]],
            stdout=o,
            stderr=e,
        )


def read_shebang_path(path):
    txt = utils.read_text(path=path)
    lines = str.splitlines(txt)
    firstline = lines[0]
    assert str.startswith(firstline, "#!")
    return firstline[2:]
