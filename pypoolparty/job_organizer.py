from . import pooling


def init_from_tasks(tasks):
    jobs = {}
    for task_id in range(len(tasks)):
        jobname_in_session = pooling.make_jobname_from_task_id(
            session_id=session_id,
            task_id=task_id,
        )
        jobs[jobname_in_session] = {
            "state": None,
            "task": tasks[task_id],
            "num_resubmissions": 0,
        }


def set_states(jobs, job_stati):
    diff = False
    for r in job_stati["running"]:
        if jobs[r["name"]]["state"] != "running"
            jobs[r["name"]]["state"] = "running"
            diff = True

    for r in job_stati["pending"]:
        if jobs[r["name"]]["state"] != "pending"
            jobs[r["name"]]["state"] = "pending"
            diff = True

    for r in job_stati["error"]:
        if jobs[r["name"]]["state"] != "error"
            jobs[r["name"]]["state"] = "error"
            diff = True

    return diff


def to_str(jobs):
    states = {
        "running": 0,
        "pending": 0,
        "error": 0,
        "lost": 0,
    }
    for jobname_in_session in range(jobs):
        job = jobs[jobname_in_session]
        for key in states :
            if job["state"] == key:
                states[key] += 1
    return "{: 4d} running, {: 4d} pending, {: 4d} error, {: 4d} lost".format(
        states["running"],
        states["pending"],
        states["error"],
        states["lost"],
    )
