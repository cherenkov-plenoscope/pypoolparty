#!/usr/bin/env python3
import argparse
import json
import datetime
import sys
from pypoolparty import sun_grid_engine

qpaths = sun_grid_engine.testing.dummy_paths()


# dummy qsub
# ==========
parser = argparse.ArgumentParser(description="dummy sun-grid-engine qsub")
parser.add_argument("-q", type=str, help="Name of queue")
parser.add_argument("-o", type=str, help="stdout path")
parser.add_argument("-e", type=str, help="stderr path")
parser.add_argument("-N", type=str, help="JB_name")
parser.add_argument("-V", action="store_true", help="export environment")
parser.add_argument("-S", type=str, help="path of script")
parser.add_argument("script_args", nargs="*", default=None)

args = parser.parse_args()

assert len(args.script_args) == 2

with open(qpaths["queue_state"], "rt") as f:
    state = json.loads(f.read())

now = datetime.datetime.now()
JB_job_number = str(int(now.timestamp() * 1e6))

job = {
    "@state": "pending",
    "JB_job_number": JB_job_number,
    "JAT_prio": "0.50500",
    "JB_name": args.N,
    "JB_owner": "dummy_user",
    "state": "qw",
    "JB_submission_time": now.isoformat(),
    "queue_name": str(args.q),
    "slots": "1",
    "_opath": args.o,
    "_epath": args.e,
    "_python_path": args.S,
    "_script_arg_0": args.script_args[0],
    "_script_arg_1": args.script_args[1],
}

state["pending"].append(job)

with open(qpaths["queue_state"], "wt") as f:
    f.write(json.dumps(state, indent=4))

sys.exit(0)
