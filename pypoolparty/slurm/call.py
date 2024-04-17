import json_line_logger
import subprocess
import tempfile
import random
import os
import time
from .. import utils


def sbatch(
    script_path,
    script_arguments,
    stdout_path,
    stderr_path,
    jobname,
    logger=None,
    clusters=None,
    sbatch_path="sbatch",
    timeout=None,
    timecooldown=1.0,
    max_num_retry=25,
):
    if logger is None:
        logger = json_line_logger.LoggerStdout()

    cmd = [sbatch_path]
    if clusters:
        cmd += ["--clusters", str.join(",", clusters)]

    cmd += ["--job-name", jobname]
    cmd += ["--output", stdout_path]
    cmd += ["--error", stderr_path]
    cmd += [script_path]
    for argument in script_arguments:
        cmd += [argument]

    numtry = 0
    while True:
        utils.raise_if_too_often(
            numtry=numtry, max_num_retry=max_num_retry, logger=logger
        )
        try:
            numtry += 1
            logger.debug("calling sbatch, num. tries = {:d}".format(numtry))
            subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, timeout=timeout
            )
            break
        except Exception as bad:
            logger.warning("Problem in sbatch()")
            logger.warning(str(bad))
            utils.random_sleep(timecooldown=timecooldown, logger=logger)


def scancel(
    jobname=None,
    jobid=None,
    scancel_path="scancel",
    timeout=None,
    timecooldown=1.0,
    max_num_retry=25,
    logger=None,
):
    if logger is None:
        logger = json_line_logger.LoggerStdout()

    cmd = [scancel_path]
    if jobid is not None:
        cmd += [jobid]
    if jobname is not None:
        cmd += ["--name", str(jobname)]

    numtry = 0
    while True:
        utils.raise_if_too_often(
            numtry=numtry, max_num_retry=max_num_retry, logger=logger
        )
        try:
            numtry += 1
            logger.debug("calling scancel, num. tries = {:d}".format(numtry))
            subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, timeout=timeout
            )
            break
        except Exception as bad:
            logger.warning("Problem in scancel()")
            logger.warning(str(bad))
            utils.random_sleep(timecooldown=timecooldown, logger=logger)


def squeue(
    squeue_path="squeue",
    jobname=None,
    array=False,
    timeout=None,
    timecooldown=1.0,
    max_num_retry=25,
    logger=None,
    debug_dump_path=None,
):
    """
    Call slurm's squeue.

    Parameters
    ----------
    squeue_path : str, (default: "squeue")
        Path to the squeue executable
    timeout : float or None
        Time to wait for squeue to return.
    timecooldown : float
        Time to wait before calling squeue again in case of a problem.

    Returns
    -------
    squeue : list of dicts
        The jobs and their attributes
    """
    if logger is None:
        logger = json_line_logger.LoggerStdout()

    numtry = 0
    while True:
        utils.raise_if_too_often(
            numtry=numtry, max_num_retry=max_num_retry, logger=logger
        )
        try:
            numtry += 1
            logger.debug("calling squeue, num. tries = {:d}".format(numtry))
            stdout = _squeue_format_all_stdout(
                squeue_path=squeue_path,
                jobname=jobname,
                array=array,
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


def _parse_stdout_format_all(stdout, delimiter="|", logger=None):
    if logger is None:
        logger = json_line_logger.LoggerStdout()

    lines = str.splitlines(stdout)
    logger.debug("num. lines = {:d}".format(len(lines)))
    header_line = lines[0]

    keys = str.split(header_line, delimiter)
    keys = [str.lower(key) for key in keys]
    logger.debug("header line has {:d} keys".format(len(keys)))

    out = []
    # print("---lines---")
    for i in range(1, len(lines)):
        line = lines[i]
        # print("line: '{:s}'.".format(line))
        values = str.split(line, delimiter)
        if len(values) != len(keys):
            logger.debug("line {:d} has not expected num. of tokens".format(i))
        line_dict = {}
        for j in range(len(keys)):
            jkey = keys[j]
            jval = values[j]
            line_dict[jkey] = jval
        out.append(line_dict)
    return out


def _squeue_format_all_stdout(
    squeue_path="squeue", jobname=None, array=False, timeout=None, logger=None
):
    if logger is None:
        logger = json_line_logger.LoggerStdout()

    cmd = [squeue_path]
    cmd += ["--me"]
    cmd += ["--format", "%all"]
    if array:
        cmd += ["--array"]
    if jobname is not None:
        cmd += ["--name", jobname]

    with tempfile.TemporaryDirectory(prefix="slurmpypoolurm") as tmp:
        tmp_stdout_path = os.path.join(tmp, "stdout.txt")

        logger.debug("stdout in {:s}".format(tmp_stdout_path))
        if timeout:
            logger.debug("timeout = {:f}s".format(float(timeout)))

        with open(tmp_stdout_path, "wt") as f:
            p = subprocess.Popen(cmd, stdout=f)
            p.wait(timeout=timeout)

        with open(tmp_stdout_path, "rt") as f:
            stdout = f.read()

    logger.debug("len(stdout) = {:d}".format(len(stdout)))
    return stdout
