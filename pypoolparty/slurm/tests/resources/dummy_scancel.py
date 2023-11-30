#!/usr/bin/env python3
import sys
import json
import datetime
import pypoolparty

qpaths = pypoolparty.slurm.testing.dummy_paths()

# dummy scancel
# =============
assert len(sys.argv) == 3
assert sys.argv[1] == "--name"
jobname = sys.argv[2]

with open(qpaths["queue_state"], "rt") as f:
    old_state = json.loads(f.read())

found = False
state = {
    "pending": [],
    "running": [],
    "evil_jobs": old_state["evil_jobs"],
}
for job in old_state["running"]:
    if job["NAME"] == jobname:
        found = True
    else:
        state["running"].append(job)

for job in old_state["pending"]:
    if job["NAME"] == jobname:
        found = True
    else:
        state["pending"].append(job)

with open(qpaths["queue_state"], "wt") as f:
    f.write(json.dumps(state, indent=4))

if found == True:
    sys.exit(0)
else:
    print("Can not find ", jobname)
    sys.exit(1)
