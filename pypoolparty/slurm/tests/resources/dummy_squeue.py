#!/usr/bin/env python3
import sys
import json
import datetime
import subprocess
import pypoolparty

qpaths = pypoolparty.slurm.testing.dummy_paths()


def job_head():
    return str.split(job_head_to_line(), "|")


def job_head_to_line():
    return "NAME|JOBID|STATE"


def job_to_line(job, delimiter="|"):
    line = ""
    head = job_head()
    for i, key in enumerate(head):
        line += job[key]
        if (i + 1) < len(head):
            line += "|"
    return line


def state_to_table(state):
    lines = []
    lines.append(job_head_to_line())
    for job in state["running"]:
        lines.append(job_to_line(job=job))
    for job in state["pending"]:
        lines.append(job_to_line(job=job))
    return str.join("\n", lines)


# dummy squeue
# ============
# Every time this is called, it runs one job.
MAX_NUM_RUNNING = 10

assert len(sys.argv) == 3
assert sys.argv[1] == "--format"
assert sys.argv[2] == "%all"

with open(qpaths["queue_state"], "rt") as f:
    state = json.loads(f.read())

evil_ichunks_num_fails = {}
evil_ichunks_max_num_fails = {}
for evil in state["evil_jobs"]:
    evil_ichunks_num_fails[evil["ichunk"]] = evil["num_fails"]
    evil_ichunks_max_num_fails[evil["ichunk"]] = evil["max_num_fails"]


if len(state["running"]) >= MAX_NUM_RUNNING:
    run_job = state["running"].pop(0)
    pypoolparty.testing.dummy_run_job(run_job)
elif len(state["pending"]) > 0:
    job = state["pending"].pop(0)
    ichunk = pypoolparty.pooling.make_ichunk_from_jobname(jobname=job["NAME"])
    if ichunk in evil_ichunks_num_fails:
        if evil_ichunks_num_fails[ichunk] < evil_ichunks_max_num_fails[ichunk]:
            job["STATE"] = "ERROR"
            state["pending"].append(job)
            evil_ichunks_num_fails[ichunk] += 1
        else:
            job["STATE"] = "RUNNING"
            state["running"].append(job)
    else:
        job["STATE"] = "RUNNING"
        state["running"].append(job)
elif len(state["running"]) > 0:
    run_job = state["running"].pop(0)
    pypoolparty.testing.dummy_run_job(run_job)


evil_jobs = []
for ichunk in evil_ichunks_num_fails:
    evil_jobs.append(
        {
            "ichunk": ichunk,
            "num_fails": evil_ichunks_num_fails[ichunk],
            "max_num_fails": evil_ichunks_max_num_fails[ichunk],
        }
    )
state["evil_jobs"] = evil_jobs


with open(qpaths["queue_state"], "wt") as f:
    f.write(json.dumps(state, indent=4))

out_table = state_to_table(state)
print(out_table)

sys.exit(0)
