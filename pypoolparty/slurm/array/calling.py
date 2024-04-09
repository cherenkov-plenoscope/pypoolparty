from ... import utils

import subprocess
import os


def sbatch_array(
    work_dir,
    jobname,
    start_task_id,
    stop_task_id,
    num_simultaneously_running_tasks=None,
    sbatch_path="sbatch",
    timeout=None,
    timecooldown=1.0,
    max_num_retry=25,
    logger=None,
):
    logger = utils.make_logger_to_stdout_if_none(logger)
    assert start_task_id >= 0
    assert stop_task_id >= start_task_id

    cmd = [sbatch_path]
    if num_simultaneously_running_tasks is None:
        cmd += ["--array", "{:d}-{:d}".format(start_task_id, stop_task_id)]
    else:
        num_simultaneously_running_tasks = int(
            num_simultaneously_running_tasks
        )
        assert num_simultaneously_running_tasks > 0
        cmd += [
            "--array",
            "{:d}-{:d}%{:d}".format(
                start_task_id, stop_task_id, num_simultaneously_running_tasks
            ),
        ]

    logger.debug("Call: " + str.join(" ", cmd))

    cmd += ["--output", os.path.join(work_dir, "%a.stdout")]
    cmd += ["--error", os.path.join(work_dir, "%a.stderr")]
    cmd += ["--job-name", jobname]
    cmd += [os.path.join(work_dir, "script.py")]

    numtry = 0
    while True:
        utils.raise_if_too_often(
            numtry=numtry, max_num_retry=max_num_retry, logger=logger
        )
        try:
            numtry += 1
            subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, timeout=timeout
            )
            break
        except Exception as bad:
            logger.warning(
                "Problem calling sbatch, num. tries = {:d}".format(numtry)
            )
            logger.warning(str(bad))
            utils.random_sleep(timecooldown=timecooldown, logger=logger)
