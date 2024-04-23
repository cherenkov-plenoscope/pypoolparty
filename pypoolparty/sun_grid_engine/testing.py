"""
A dummy queue for testing qsub, qstat, and qdel.
"""
import os
from .. import utils


def dummy_paths():
    join = os.path.join
    pypoolparty_dir = utils.resources_path()
    ddir = join(pypoolparty_dir, "sun_grid_engine", "tests", "resources")
    out = {}
    out["queue_state"] = join(ddir, "dummy_queue_state.json")
    out["qsub"] = join(ddir, "dummy_qsub.py")
    out["qstat"] = join(ddir, "dummy_qstat.py")
    out["qdel"] = join(ddir, "dummy_qdel.py")
    return out


def dummy_init(path):
    os.makedirs(path, exist_ok=True)
    join = os.path.join
    pypoolparty_dir = utils.resources_path()
    rdir = join(pypoolparty_dir, "sun_grid_engine", "tests", "resources")

    queue_state_path = join(path, "queue_state.json")

    for sname in ["dummy_qsub.py", "dummy_qstat.py", "dummy_qdel.py"]:
        with open(join(rdir, sname), "rt") as fin:
            script = fin.read()
        outscript = script.replace(
            "queue_state_path = None  #  <- REQUIRED",
            'queue_state_path = "{:s}"'.format(queue_state_path),
        )
        with open(join(path, sname), "wt") as fout:
            fout.write(outscript)
        utils.make_path_executable(join(path, sname))

    out = {}
    out["queue_state"] = queue_state_path
    out["qsub"] = join(path, "dummy_qsub.py")
    out["qstat"] = join(path, "dummy_qstat.py")
    out["qdel"] = join(path, "dummy_qdel.py")
    return out
