import os
import stat
import time
from . import utils


def session_id_from_time_now():
    # This must be a valid filename. No ':' for time.
    return time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())


def make_jobname_from_task_id(session_id, task_id):
    return "q{:s}#{:09d}".format(session_id, task_id)


def make_task_id_from_jobname(jobname):
    task_id_str = jobname.split("#")[1]
    return int(task_id_str)


def task_path(work_dir, task_id):
    return os.path.join(work_dir, "{:09d}.pkl".format(task_id))


def map_task_into_work_dir(work_dir, task, task_id, session_id):
    jobname = make_jobname_from_task_id(
        session_id=session_id,
        task_id=task_id,
    )
    utils.write_pickle(
        path=task_id(work_dir=work_dir, task_id=task_id),
        content=task,
    )
    return jobname


def reduce_task_results_from_work_dir(work_dir, chunks, logger):
    task_results = []
    task_results_are_incomplete = False

    for ichunk, chunk in enumerate(chunks):
        num_tasks_in_chunk = len(chunk)
        chunk_result_path = chunk_path(work_dir, ichunk) + ".out"

        try:
            chunk_result = utils.read_pickle(path=chunk_result_path)
            for task_result in chunk_result:
                task_results.append(task_result)
        except FileNotFoundError:
            task_results_are_incomplete = True
            logger.warning(
                "Expected results in: {:s}".format(chunk_result_path)
            )
            task_results += [None for i in range(num_tasks_in_chunk)]

    return task_results_are_incomplete, task_results


def has_invalid_or_non_empty_stderr(
    work_dir, num_chunks, filter_stderr_func=None
):
    has_errors = False
    for ichunk in range(num_chunks):
        e_path = chunk_path(work_dir, ichunk) + ".e"
        try:
            with open(e_path, "rt") as f:
                stderr = f.read()
            if filter_stderr_func:
                stderr = filter_stderr_func(stderr)
            if len(stderr) > 0:
                has_errors = True
        except FileNotFoundError:
            has_errors = True
    return has_errors
