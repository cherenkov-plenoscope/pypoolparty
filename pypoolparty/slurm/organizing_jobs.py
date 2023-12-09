import json_line_logger


def filter_jobs_by_jobnames(jobs, jobnames):
    jobnames = set(jobnames)
    outjobs = []
    for job in jobs:
        if job["name"] in jobnames:
            outjobs.append(job)
    return outjobs


def split_jobs_in_running_pending_error(jobs, logger=None):
    if logger is None:
        logger = json_line_logger.LoggerStdout()

    running = []
    pending = []
    error = []

    for job in jobs:
        if any([e in job["reason"] for e in ["err", "bad", "fail", "halt"]]):
            error.append(job)
            logger.debug(
                "job {:s} has reason: {:s}.".format(job["name"], job["reason"])
            )
        elif "err" in str.lower(job["state"]):
            error.append(job)
        elif job["state"] == "RUNNING":
            running.append(job)
        elif job["state"] == "PENDING":
            pending.append(job)
        else:
            logger.debug(
                "job {:s} is in state {:s}.".format(job["name"], job["state"])
            )

    return running, pending, error
