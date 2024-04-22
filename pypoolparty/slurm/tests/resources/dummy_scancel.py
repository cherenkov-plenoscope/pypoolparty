#!/usr/bin/env python3
import sys
import argparse
import json
import datetime
import pypoolparty

parser = argparse.ArgumentParser(description="dummy slurm scancel")
parser.add_argument(
    "jobid",
    action="JOBID",
    type=str,
    required=False,
)
parser.add_argument(
    "--name", metavar="JOB_NAME", type=str, required=False, default=""
)
args = parser.parse_args()

queue_state_path = None  #  <- REQUIRED

if args.jobid and not args.name:
    match_key = "JOBID"
    match = args.jobid
elif args.name and not agrs.jobid:
    match_key = "NAME"
    match = args.name
else:
    raise AssertionError("Either jobid or name. But not both.")

with open(queue_state_path, "rt") as f:
    old_state = json.loads(f.read())

found = False
state = {
    "pending": [],
    "running": [],
    "evil_jobs": old_state["evil_jobs"],
}

for job in old_state["running"]:
    if job[match_key] == match:
        found = True
    else:
        state["running"].append(job)

for job in old_state["pending"]:
    if job[match_key] == match:
        found = True
    else:
        state["pending"].append(job)

with open(queue_state_path, "wt") as f:
    f.write(json.dumps(state, indent=4))

if found == True:
    sys.exit(0)
else:
    print("Can not find {:s}: {:s}".format(match_key, match))
    sys.exit(1)
