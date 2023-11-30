import os
import stat
import time
from . import utils


def session_id_from_time_now():
    # This must be a valid filename. No ':' for time.
    return time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())


def make_jobname_from_ichunk(session_id, ichunk):
    return "q{:s}#{:09d}".format(session_id, ichunk)


def make_ichunk_from_jobname(jobname):
    ichunk_str = jobname.split("#")[1]
    return int(ichunk_str)


def chunk_path(work_dir, ichunk):
    return os.path.join(work_dir, "{:09d}.pkl".format(ichunk))


def map_tasks_into_work_dir(work_dir, tasks, chunks, session_id):
    jobnames_in_session = set()
    for ichunk, chunk in enumerate(chunks):
        jobname = make_jobname_from_ichunk(
            session_id=session_id,
            ichunk=ichunk,
        )
        jobnames_in_session.add(jobname)
        chunk_payload = [tasks[itask] for itask in chunk]
        utils.write_pickle(
            path=chunk_path(work_dir, ichunk),
            content=chunk_payload,
        )
    return jobnames_in_session


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


def has_invalid_or_non_empty_stderr(work_dir, num_chunks):
    has_errors = False
    for ichunk in range(num_chunks):
        e_path = chunk_path(work_dir, ichunk) + ".e"
        try:
            if os.stat(e_path).st_size != 0:
                has_errors = True
        except FileNotFoundError:
            has_errors = True
    return has_errors
