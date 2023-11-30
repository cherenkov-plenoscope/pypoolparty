def init():
    return {"running": 0, "pending": 0, "error": 0, "lost": 0}


def is_equal(a, b):
    for key in a:
        if a[key] != b[key]:
            return False
    return True


def estimate(
    num_jobs_running,
    num_jobs_pending,
    num_jobs_error,
    num_resubmissions_by_ichunk,
    max_num_resubmissions,
):
    num_jobs = init()
    num_jobs["running"] = num_jobs_running
    num_jobs["pending"] = num_jobs_pending
    num_jobs["error"] = num_jobs_error
    num_jobs["lost"] = 0
    for ichunk in num_resubmissions_by_ichunk:
        if num_resubmissions_by_ichunk[ichunk] >= max_num_resubmissions:
            num_jobs["lost"] += 1
    return num_jobs


def to_str(a):
    return "{: 4d} running, {: 4d} pending, {: 4d} error, {: 4d} lost".format(
        a["running"],
        a["pending"],
        a["error"],
        a["lost"],
    )
