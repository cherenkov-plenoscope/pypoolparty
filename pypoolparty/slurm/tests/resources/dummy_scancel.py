#!/usr/bin/env python3
import sys
import argparse
import json
import datetime
import pypoolparty

parser = argparse.ArgumentParser(
    prog="dummy-scancel",
    description="A dummy of slurm's scancel to test pypoolparty.",
)
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

if args.jobid and not args.name:
    match_key = "JOBID"
    match = args.jobid
elif args.name and not agrs.jobid:
    match_key = "NAME"
    match = args.name
else:
    raise AssertionError("Either jobid or name. But not both.")

qpaths = pypoolparty.slurm.testing.dummy_paths()
with open(qpaths["queue_state"], "rt") as f:
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

with open(qpaths["queue_state"], "wt") as f:
    f.write(json.dumps(state, indent=4))

if found == True:
    sys.exit(0)
else:
    print("Can not find {:s}: {:s}".format(match_key, match))
    sys.exit(1)
