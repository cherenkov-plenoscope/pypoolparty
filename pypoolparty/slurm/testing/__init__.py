import os
from ... import utils


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


def dummy_init(path):
    os.makedirs(path, exist_ok=True)
    join = os.path.join
    pypoolparty_dir = utils.resources_path()
    rdir = join(pypoolparty_dir, "slurm", "tests", "resources")

    queue_state_path = join(path, "queue_state.json")

    for sname in ["dummy_sbatch.py", "dummy_squeue.py", "dummy_scancel.py"]:
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
    out["sbatch"] = join(path, "dummy_sbatch.py")
    out["squeue"] = join(path, "dummy_squeue.py")
    out["scancel"] = join(path, "dummy_scancel.py")
    return out
