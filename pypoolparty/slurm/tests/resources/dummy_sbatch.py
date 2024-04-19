#!/usr/bin/env python3
import argparse
import json
import datetime
import sys
import pypoolparty

qpaths = pypoolparty.slurm.testing.dummy_paths()

# dummy sbatch
# ============
parser = argparse.ArgumentParser(description="dummy slurm sbatch")
parser.add_argument("--array", type=str, help="array options")
parser.add_argument("--output", type=str, help="stdout path")
parser.add_argument("--error", type=str, help="stderr path")
parser.add_argument("--job-name", type=str, help="jobname")
parser.add_argument("script_args", nargs="*", default=None)
args = parser.parse_args()


with open(qpaths["queue_state"], "rt") as f:
    state = json.loads(f.read())

now = datetime.datetime.now()
jobid = str(int(now.timestamp() * 1e6))

worker_node_script_path = args.script_args[0]
python_path = pypoolparty.testing.read_shebang_path(
    path=worker_node_script_path
)


def make_job(jobid, python_path, args):
    job = {
        "STATE": "PENDING",
        "JOBID": jobid,
        "NAME": args.job_name,
        "REASON": "foobar",
        "PRIORITY": "0.999",
        "_opath": args.output,
        "_epath": args.error,
        "_python_path": python_path,
    }
    for ii, script_arg in enumerate(args.script_args):
        job["_script_arg_{:d}".format(ii)] = script_arg
    return job


if args.array is not None:
    array = pypoolparty.slurm.calling._parse_sbatch_array_task_id_str(
        task_id_str=args.array
    )
    if array["mode"] == "range":
        task_ids = np.arrange(
            array["start_task_id"],
            array["stop_task_id"],
        )
    elif array["mode"] == "list":
        task_ids = array["task_ids"]
    else:
        raise ValueError("bad mode in task_id_str.")

    for task_id in task_ids:
        job = make_job(
            jobid="{:s}_{:d}".format(jobid, task_id),
            python_path=python_path,
            args=args,
        )
        state["pending"].append(job)

else:
    assert len(args.script_args) == 2
    job = make_job(jobid=jobid, python_path=python_path, args=args)
    state["pending"].append(job)

with open(qpaths["queue_state"], "wt") as f:
    f.write(json.dumps(state, indent=4))

sys.exit(0)
