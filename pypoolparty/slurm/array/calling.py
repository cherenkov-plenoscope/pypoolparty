from ... import utils
from ..call import _parse_stdout_format_all

import subprocess
import tempfile
import os
import pwd


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


def squeue_array(
    jobname,
    squeue_path="squeue",
    timeout=None,
    timecooldown=1.0,
    max_num_retry=25,
    logger=None,
    debug_dump_path=None,
):
    logger = utils.make_logger_to_stdout_if_none(logger)

    numtry = 0
    while True:
        utils.raise_if_too_often(
            numtry=numtry, max_num_retry=max_num_retry, logger=logger
        )
        try:
            numtry += 1
            logger.debug("calling squeue, num. tries = {:d}".format(numtry))
            stdout = _squeue_array_stdout(
                jobname=jobname,
                squeue_path=squeue_path,
                timeout=timeout,
                logger=logger,
            )
            break
        except Exception as bad:
            logger.warning("problem in _squeue_format_all_stdout()")
            logger.warning(str(bad))
            utils.random_sleep(timecooldown=timecooldown, logger=logger)

    logger.debug("parsing stdout into list of dicts")
    try:
        list_of_dicts = _parse_stdout_format_all(
            stdout=stdout,
            delimiter="|",
            logger=logger,
        )
        logger.debug("num. jobs in squeue = {:d}".format(len(list_of_dicts)))
    except Exception as err:
        logger.critical("Can not parse squeue's stdout.")
        if debug_dump_path:
            utils.write(path=debug_dump_path, content=stdout, mode="t")
            logger.critical("Dump stdout to {:s}.".format(debug_dump_path))
        raise err

    return list_of_dicts


def _squeue_array_stdout(
    jobname,
    squeue_path="squeue",
    timeout=None,
    logger=None,
):
    logger = utils.make_logger_to_stdout_if_none(logger)

    cmd = [squeue_path]
    cmd += ["--me"]
    cmd += ["--format", "'%j|%i|%T|%p|%R'"]
    cmd += ["--array"]
    cmd += ["--name", jobname]

    with tempfile.TemporaryDirectory(prefix="slurmpypoolurm") as tmp:
        stdout_path = os.path.join(tmp, "stdout.txt")
        logger.debug("stdout in {:s}".format(stdout_path))
        if timeout:
            logger.debug("timeout = {:f}s".format(float(timeout)))
        with open(stdout_path, "wt") as f:
            p = subprocess.Popen(cmd, stdout=f)
            p.wait(timeout=timeout)
        with open(stdout_path, "rt") as f:
            stdout = f.read()
    logger.debug("len(stdout) = {:d}".format(len(stdout)))
    return stdout
