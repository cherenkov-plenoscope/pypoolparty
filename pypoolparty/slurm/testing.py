import os
from .. import utils


def dummy_paths():
    join = os.path.join
    pypoolparty_dir = utils.resources_path()
    ddir = join(pypoolparty_dir, "slurm", "tests", "resources")
    out = {}
    out["queue_state"] = join(ddir, "dummy_queue_state.json")
    out["sbatch"] = join(ddir, "dummy_sbatch.py")
    out["squeue"] = join(ddir, "dummy_squeue.py")
    out["scancel"] = join(ddir, "dummy_scancel.py")
    return out
