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
