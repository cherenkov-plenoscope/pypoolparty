from . import call
from . import organizing_jobs
from . import testing
from .. import proto_pool
from .. import utils


def Pool(
    num_chunks=None,
    python_path=None,
    polling_interval=5.0,
    work_dir=None,
    keep_work_dir=False,
    max_num_resubmissions=0,
    # slurm specific
    # --------------
    sbatch_path="sbatch",
    clusters=None,
    squeue_path="squeue",
    scancel_path="scancel",
):
    return proto_pool.Pool(
        num_chunks=num_chunks,
        python_path=python_path,
        polling_interval=polling_interval,
        work_dir=work_dir,
        keep_work_dir=keep_work_dir,
        max_num_resubmissions=max_num_resubmissions,
        submit_func=submit,
        submit_func_kwargs={
            "sbatch_path": sbatch_path,
            "clusters": clusters,
        },
        status_func=status,
        status_func_kwargs={
            "squeue_path": squeue_path,
        },
        delete_func=delete,
        delete_func_kwargs={"scancel_path": scancel_path},
    )


def submit(
    jobname,
    script_path,
    script_arguments,
    stdout_path,
    stderr_path,
    logger,
    # slurm specific
    # --------------
    sbatch_path="sbatch",
    clusters=None,
):
    return call.sbatch(
        script_path=script_path,
        script_arguments=script_arguments,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        jobname=jobname,
        logger=logger,
        clusters=clusters,
        sbatch_path=sbatch_path,
    )


def status(
    jobnames,
    logger,
    # slurm specific
    # --------------
    squeue_path,
):
    all_jobs = call.squeue(
        squeue_path=squeue_path,
        logger=logger,
    )
    our_jobs = organizing_jobs.filter_jobs_by_jobnames(
        jobs=all_jobs,
        jobnames=jobnames,
    )
    (
        running,
        pending,
        error,
    ) = organizing_jobs.split_jobs_in_running_pending_error(
        jobs=our_jobs,
        logger=logger,
    )
    out = {
        "running": [],
        "pending": [],
        "error": [],
    }
    for job in running:
        out["running"].append(_make_job(slurm_job=job))
    for job in pending:
        out["pending"].append(_make_job(slurm_job=job))
    for job in error:
        out["error"].append(_make_job(slurm_job=job))
    return out


def delete(
    job,
    logger,
    # slurm specific
    # --------------
    scancel_path,
):
    return call.scancel(
        jobname=job["name"],
        scancel_path=scancel_path,
        logger=logger,
    )


def _make_job(slurm_job):
    return {
        "name": slurm_job["name"],
    }
