#!/usr/bin/env python3
import argparse
import json
import datetime
import sys
import pypoolparty

parser = argparse.ArgumentParser(description="dummy slurm sbatch")
parser.add_argument("--array", type=str, help="array options")
parser.add_argument("--output", type=str, help="stdout path")
parser.add_argument("--error", type=str, help="stderr path")
parser.add_argument("--job-name", type=str, help="jobname")
parser.add_argument("script_args", nargs="*", default=None)
args = parser.parse_args()

queue_state_path = None  #  <- REQUIRED

with open(queue_state_path, "rt") as f:
    state = json.loads(f.read())

now = datetime.datetime.now()
jobid = str(int(now.timestamp() * 1e6))

worker_node_script_path = args.script_args[0]
python_path = pypoolparty.testing.read_shebang_path(
    path=worker_node_script_path
)


def job_init_default(python_path):
    return {
        "STATE": "PENDING",
        "REASON": "foobar",
        "PRIORITY": "0.999",
        "_python_path": python_path,
    }


def job_update_script_args(job, args):
    for ii, script_arg in enumerate(args.script_args):
        job["_script_arg_{:d}".format(ii)] = script_arg
    return job


if args.array is not None:
    array = pypoolparty.slurm.calling._parse_sbatch_array_task_id_str(
        task_id_str=args.array
    )
    if array["mode"] == "range":
        array_task_ids = [
            ttt
            for ttt in range(array["start_task_id"], array["stop_task_id"] + 1)
        ]
    elif array["mode"] == "list":
        array_task_ids = array["task_ids"]
    else:
        raise ValueError("bad mode in task_id_str.")

    for array_task_id in array_task_ids:
        job = job_init_default(python_path=python_path)
        job[
            "JOBID"
        ] = pypoolparty.slurm.array.utils.join_job_id_and_array_task_id(
            job_id=jobid, array_task_id=array_task_id
        )
        job["NAME"] = args.job_name
        job[
            "_opath"
        ] = pypoolparty.slurm.array.utils.replace_array_task_id_format_with_integer_format(
            fmt=args.output
        ).format(
            array_task_id
        )
        job[
            "_epath"
        ] = pypoolparty.slurm.array.utils.replace_array_task_id_format_with_integer_format(
            fmt=args.error
        ).format(
            array_task_id
        )
        job["_additional_environment"] = {
            "SLURM_ARRAY_TASK_ID": str(array_task_id)
        }
        job_update_script_args(job=job, args=args)
        state["pending"].append(job)

else:
    assert len(args.script_args) == 2
    job = job_init_default(python_path=python_path)
    job["JOBID"] = jobid
    job["NAME"] = args.job_name
    job["_opath"] = args.output
    job["_epath"] = args.error
    job_update_script_args(job=job, args=args)
    state["pending"].append(job)

with open(queue_state_path, "wt") as f:
    f.write(json.dumps(state, indent=4))

sys.exit(0)
