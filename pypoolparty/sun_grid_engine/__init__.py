from . import testing
from . import call
from . import organizing_jobs
from .. import proto_pool
from .. import utils


def Pool(
    num_chunks=None,
    python_path=None,
    polling_interval=5.0,
    work_dir=None,
    keep_work_dir=False,
    max_num_resubmissions=0,
    # sge specific
    # ------------
    qsub_path="qsub",
    queue_name=None,
    qstat_path="qstat",
    error_state_indicator="E",
    qdel_path="qdel",
):
    if python_path is None:
        python_path = utils.default_python_path()

    return proto_pool.Pool(
        num_chunks=num_chunks,
        python_path=python_path,
        polling_interval=polling_interval,
        work_dir=work_dir,
        keep_work_dir=keep_work_dir,
        max_num_resubmissions=max_num_resubmissions,
        job_submission_function=submit,
        job_submission_function_kwargs={
            "qsub_path": qsub_path,
            "queue_name": queue_name,
            "script_exe_path": python_path,
        },
        job_query_status_function=status,
        job_query_status_function_kwargs={
            "qstat_path": qstat_path,
            "error_state_indicator": error_state_indicator,
        },
        job_deletion_function=delete,
        job_deletion_function_kwargs={"qdel_path": qdel_path},
    )


def submit(
    jobname,
    script_path,
    script_arguments,
    stdout_path,
    stderr_path,
    logger,
    # sge specific
    # ------------
    qsub_path,
    queue_name,
    script_exe_path,
):
    return call.qsub(
        qsub_path=qsub_path,
        queue_name=queue_name,
        script_exe_path=script_exe_path,
        script_path=script_path,
        arguments=script_arguments,
        JB_name=jobname,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        logger=logger,
    )


def status(
    jobnames,
    logger,
    # sge specific
    # ------------
    qstat_path,
    error_state_indicator,
):
    all_jobs_running, all_jobs_pending = call.qstat(
        qstat_path=qstat_path,
        logger=logger,
    )
    running, pending, error = organizing_jobs.get_jobs_running_pending_error(
        JB_names_set=jobnames,
        all_jobs_running=all_jobs_running,
        all_jobs_pending=all_jobs_pending,
        error_state_indicator=error_state_indicator,
    )
    out = {
        "running": [],
        "pending": [],
        "error": [],
    }
    for job in running:
        out["running"].append(_make_job(sge_job=job))
    for job in pending:
        out["pending"].append(_make_job(sge_job=job))
    for job in error:
        out["error"].append(_make_job(sge_job=job))
    return out


def delete(
    job,
    logger,
    # sge specific
    # ------------
    qdel_path,
):
    return call.qdel(
        JB_job_number=job["JB_job_number"],
        qdel_path=qdel_path,
        logger=logger,
    )


def _make_job(sge_job):
    return {
        "name": sge_job["JB_name"],
        "JB_job_number": sge_job["JB_job_number"],
    }
