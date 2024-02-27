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
parser.add_argument("--clusters", type=str, help="CSV of clusters")
parser.add_argument("--output", type=str, help="stdout path")
parser.add_argument("--error", type=str, help="stderr path")
parser.add_argument("--job-name", type=str, help="jobname")
parser.add_argument("script_args", nargs="*", default=None)

args = parser.parse_args()

assert len(args.script_args) == 2

with open(qpaths["queue_state"], "rt") as f:
    state = json.loads(f.read())

now = datetime.datetime.now()
jobid = str(int(now.timestamp() * 1e6))

_worker_node_script_path = args.script_args[0]
_python_path = pypoolparty.testing.read_shebang_path(
    path=_worker_node_script_path
)

job = {
    "STATE": "PENDING",
    "JOBID": jobid,
    "NAME": args.job_name,
    "REASON": "foobar",
    "PRIORITY": "0.999",
    "_opath": args.output,
    "_epath": args.error,
    "_python_path": _python_path,
    "_script_arg_0": args.script_args[0],
    "_script_arg_1": args.script_args[1],
}

state["pending"].append(job)

with open(qpaths["queue_state"], "wt") as f:
    f.write(json.dumps(state, indent=4))

sys.exit(0)
