import subprocess
import qstat as external_qstat_call
import time


def qsub(
    qsub_path,
    queue_name,
    script_exe_path,
    script_path,
    arguments,
    JB_name,
    stdout_path,
    stderr_path,
    logger,
):
    cmd = [qsub_path]
    if queue_name:
        cmd += ["-q", queue_name]
    cmd += ["-o", stdout_path]
    cmd += ["-e", stderr_path]
    cmd += ["-N", JB_name]
    cmd += ["-S", script_exe_path]
    cmd += [script_path]
    for argument in arguments:
        cmd += [argument]

    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logger.critical("Error in qsub()")
        logger.critical("qsub() returncode: {:d}".format(e.returncode))
        logger.critical(e.output)
        raise


def _qdel(JB_job_number, qdel_path, logger):
    try:
        _ = subprocess.check_output(
            [qdel_path, str(JB_job_number)],
            stderr=subprocess.STDOUT,
        )
    except subprocess.CalledProcessError as e:
        logger.critical("qdel returncode: {:s}".format(e.returncode))
        logger.critical("qdel stdout: {:s}".format(e.output))
        raise


def qdel(JB_job_number, qdel_path, logger):
    while True:
        try:
            _qdel(JB_job_number, qdel_path, logger=logger)
            break
        except KeyboardInterrupt:
            raise
        except Exception as bad:
            logger.warning("Problem in qdel()")
            logger.warning(str(bad))
            time.sleep(1)


def qstat(qstat_path, logger):
    """
    Return lists of running and pending jobs.
    Try again in case of Failure.
    Only except KeyboardInterrupt to stop.
    """
    while True:
        try:
            running, pending = external_qstat_call.qstat(qstat_path=qstat_path)
            break
        except KeyboardInterrupt:
            raise
        except Exception as bad:
            logger.warning("Problem in qstat()")
            logger.warning(str(bad))
            time.sleep(1)
    return running, pending
