import json_line_logger
import json


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
        resub_might_help = job_is_in_state_which_might_be_solved_by_resubmission(
            job=job, logger=logger
        )

        if job["state"] == "RUNNING" or job["state"] == "COMPLETING":
            running.append(job)
        elif job["state"] == "PENDING" and not resub_might_help:
            pending.append(job)
        elif resub_might_help:
            error.append(job)
            logger.info(make_log_msg_simple_job_state(job=job))
        else:
            logger.debug(make_log_msg_full_job_state(job=job))

    return running, pending, error


def job_is_in_state_which_might_be_solved_by_resubmission(job, logger):
    # according to 'sman squeue'
    states_for_resubmission = [
        "BOOT_FAIL",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "TIMEOUT",
    ]

    reasons_for_resubmission = [
        "admin",
        "err",
        "bad",
        "fail",
        "halt",
        "held",
    ]

    low = str.lower
    job_should_be_resubmitted = False

    if any([low(e) in low(job["state"]) for e in states_for_resubmission]):
        job_should_be_resubmitted = True
        logger.info(
            "job '{:s}' has STATE '{:s}'.".format(job["name"], job["state"])
        )

    if any([low(e) in low(job["reason"]) for e in reasons_for_resubmission]):
        job_should_be_resubmitted = True
        logger.info(
            "job '{:s}' has REASON '{:s}'.".format(job["name"], job["reason"])
        )

    job_priority_indicates_resubmission = False
    if str_can_be_converted_to_float(job["priority"]):
        priority = float(job["priority"])
        if priority == 0.0:
            job_priority_indicates_resubmission = True
    else:
        job_priority_indicates_resubmission = True

    if job_priority_indicates_resubmission:
        job_should_be_resubmitted = True
        logger.info(
            "job '{:s}' has PRIORITY '{:s}''.".format(
                job["name"], job["priority"]
            )
        )

    return job_should_be_resubmitted


def str_can_be_converted_to_float(s):
    try:
        _ = float(s)
        return True
    except ValueError as e:
        return False


def job_to_str(job):
    try:
        job_str = json.dumps(job, indent=None)
        return job_str
    except Exception:
        return ">Can not dump job to json<"


def make_log_msg_simple_job_state(job):
    msg = "job NAME:'{name:s}', STATE:'{state:s}', REASON:'{reason:s}', PRIORITY:'{priority:s}'.".format(
        name=job["name"],
        state=job["state"],
        reason=job["reason"],
        priority=job["priority"],
    )
    return msg


def make_log_msg_full_job_state(job):
    msg = "job '{:s}' is odd: {:s}.".format(job["name"], job_to_str(job=job))
    return msg
