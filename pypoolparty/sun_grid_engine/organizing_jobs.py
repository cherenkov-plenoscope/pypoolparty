def get_jobs_running_pending_error(
    JB_names_set, error_state_indicator, all_jobs_running, all_jobs_pending
):
    jobs_running = filter_jobs_by_JB_name(all_jobs_running, JB_names_set)
    jobs_pending = filter_jobs_by_JB_name(all_jobs_pending, JB_names_set)
    return extract_error_from_running_pending(
        jobs_running=jobs_running,
        jobs_pending=jobs_pending,
        error_state_indicator=error_state_indicator,
    )


def extract_error_from_running_pending(
    jobs_running, jobs_pending, error_state_indicator
):
    # split into runnning, pending, and error
    _running = []
    _pending = []
    _error = []

    for job in jobs_running:
        if error_state_indicator in job["state"]:
            _error.append(job)
        else:
            _running.append(job)

    for job in jobs_pending:
        if error_state_indicator in job["state"]:
            _error.append(job)
        else:
            _pending.append(job)

    return _running, _pending, _error


def filter_jobs_by_JB_name(jobs, JB_names_set):
    my_jobs = []
    for job in jobs:
        if job["JB_name"] in JB_names_set:
            my_jobs.append(job)
    return my_jobs
