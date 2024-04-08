from . import making_script
from . import mapping
from . import reducing
from ... import utils

import subprocess
import json_line_logger
import os
import time
import shutil


class Pool:
    def __init__(
        self,
        processes=None,
        python_path=None,
        polling_interval=5.0,
        work_dir=None,
        keep_work_dir=False,
        verbose=False,
    ):
        if python_path is None:
            self.python_path = utils.default_python_path()
        else:
            self.python_path = python_path

        self.work_dir = work_dir
        self.polling_interval = float(polling_interval)
        assert self.polling_interval > 0.0
        self.keep_work_dir = bool(keep_work_dir)
        self.processes = processes
        self.verbose = bool(verbose)

    def __repr__(self):
        return self.__class__.__name__ + "()"

    def print(self, msg):
        print("[pypoolparty]", utils.time_now_iso8601(), msg)

    def map(self, func, iterable):
        tasks = iterable
        session_id = utils.session_id_from_time_now()
        if self.work_dir is None:
            work_dir = os.path.abspath(
                os.path.join(".", ".pypoolparty_slurm_array_" + session_id)
            )
        else:
            work_dir = os.path.abspath(self.work_dir)

        os.makedirs(work_dir)
        if self.verbose:
            self.print("start: {:s}".format(work_dir))

        logger = json_line_logger.LoggerFile(
            path=os.path.join(work_dir, "log.jsonl")
        )

        logger.debug("Starting map()")
        logger.debug("python path: {:s}".format(self.python_path))
        logger.debug("polling-interval: {:f}s".format(self.polling_interval))

        logger.debug("Making script.")
        script_path = os.path.join(work_dir, "script.py")
        script_content = making_script.make(
            func_module=func.__module__,
            func_name=func.__name__,
            shebang="#!{:s}".format(self.python_path),
            work_dir=work_dir,
        )
        utils.write_text(path=script_path, content=script_content)
        utils.make_path_executable(path=script_path)

        mapping.write_tasks_to_work_dir(work_dir=work_dir, tasks=tasks)
        logger.debug("Wrote {:d} tasks into work_dir.".format(len(tasks)))

        logger.debug("Calling sbatch --array.")
        call_sbatch_array(
            work_dir=work_dir,
            jobname=session_id,
            start_task_id=0,
            stop_task_id=len(iterable) - 1,
            num_simultaneously_running_tasks=self.processes,
            logger=logger,
        )

        logger.debug("Prepare reducing of results.")
        reducer = reducing.Reducer(work_dir=work_dir)
        num_tasks_returned_last_poll = -1

        logger.debug("Wait for tasks to finish.")
        while True:
            reducer.reduce()

            poll_msg = "returned: {: 6d} / {:d}".format(
                len(reducer.tasks_returned), len(tasks)
            )
            if len(reducer.tasks_exceptions) or len(reducer.tasks_with_stderr):
                poll_msg += ", exceptions: {: 6d}, stderr: {: 6d}".format(
                    len(reducer.tasks_exceptions),
                    len(reducer.tasks_with_stderr),
                )

            logger.debug(poll_msg)
            if self.verbose:
                if len(reducer.tasks_returned) > num_tasks_returned_last_poll:
                    self.print(poll_msg)

            if len(reducer.tasks_returned) == len(tasks):
                logger.debug("complete")
                break

            num_tasks_returned_last_poll = int(len(reducer.tasks_returned))
            time.sleep(self.polling_interval)

        reducer.close()

        remove_this_work_dir = True

        if self.keep_work_dir:
            remove_this_work_dir = False

        if len(reducer.tasks_exceptions) > 0:
            remove_this_work_dir = False

        if len(reducer.tasks_with_stderr) > 0:
            remove_this_work_dir = False

        out = reducing.read_task_results(
            work_dir=work_dir, len_tasks=len(tasks), logger=logger,
        )

        utils.shutdown_logger(logger=logger)
        del logger

        if remove_this_work_dir:
            shutil.rmtree(work_dir)

        return out


def call_sbatch_array(
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
    assert stop_task_id >= stop_task_id

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
