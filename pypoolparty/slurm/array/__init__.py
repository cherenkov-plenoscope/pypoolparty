from . import making_script
from . import mapping
from . import reducing
from ... import utils

import subprocess
import json_line_logger
import os
import time


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
        self.polling_interval = polling_interval
        self.work_dir = work_dir
        self.keep_work_dir = keep_work_dir
        self.processes = processes
        self.verbose = verbose

    def __repr__(self):
        return self.__class__.__name__ + "()"

    def print(self, msg):
        print("[pypoolparty]", utils.time_now_iso8601(), msg)

    def map(self, func, iterable):
        tasks = iterable
        session_id = utils.session_id_from_time_now()
        if self.work_dir is None:
            swd = os.path.abspath(
                os.path.join(".", ".pypoolparty_slurm_array_" + session_id)
            )
        else:
            swd = os.path.abspath(self.work_dir)

        os.makedirs(swd)
        if self.verbose:
            self.print("start: {:s}".format(swd))

        sl = json_line_logger.LoggerFile(path=os.path.join(swd, "log.jsonl"))

        sl.debug("Starting map()")
        sl.debug("python path: {:s}".format(self.python_path))
        sl.debug("polling-interval: {:f}s".format(self.polling_interval))

        sl.debug("Making script.")
        script_path = os.path.join(swd, "script.py")
        script_content = making_script.make(
            func_module=func.__module__,
            func_name=func.__name__,
            shebang="#!{:s}".format(self.python_path),
            work_dir=swd,
        )
        utils.write_text(path=script_path, content=script_content)
        utils.make_path_executable(path=script_path)

        mapping.write_tasks_to_work_dir(work_dir=swd, tasks=tasks)
        sl.debug("Wrote {:d} tasks into work_dir.".format(len(tasks)))

        sl.debug("Calling sbatch --array.")
        call_sbatch_array(
            work_dir=swd,
            jobname=session_id,
            start_task_id=0,
            stop_task_id=len(iterable) - 1,
            num_simultaneously_running_tasks=self.processes,
        )

        sl.debug("Prepare reducing of results.")
        reducer = reducing.Reducer(work_dir=swd)
        num_task_results_last_poll = reducer.num_task_results

        sl.debug("Wait for tasks to finish.")
        while True:
            reducer.reduce()

            if self.verbose:
                if reducer.num_task_results > num_task_results_last_poll:
                    self.print(
                        "{: 6d} of {: 6d}".format(
                            reducer.num_task_results, len(tasks)
                        )
                    )

            if reducer.num_task_results == len(tasks):
                sl.debug("complete")
                break

            num_task_results_last_poll = int(reducer.num_task_results)
            time.sleep(self.polling_interval)

        reducer.close()

        task_results = reducing.read_task_results(
            path=os.path.join(swd, "tasks.results.tar")
        )
        out = []
        for task_id in range(len(tasks)):
            if task_id in task_results:
                out.append(task_results.pop(task_id))
            else:
                out.append(None)
                sl.error("No result found for task_id {:d}.".format(task_id))
        return out


def call_sbatch_array(
    work_dir,
    jobname,
    start_task_id,
    stop_task_id,
    num_simultaneously_running_tasks=None,
    sbatch_path="sbatch",
    timeout=None,
):
    assert start_task_id >= 0
    assert stop_task_id >= stop_task_id

    cmd = [sbatch_path]
    if num_simultaneously_running_tasks is None:
        cmd += ["--array", "{:d}-{:d}".format(start_task_id, stop_task_id)]
    else:
        assert num_simultaneously_running_tasks > 0
        cmd += [
            "--array",
            "{:d}-{:d}%{:d}".format(
                start_task_id, stop_task_id, num_simultaneously_running_tasks
            ),
        ]

    cmd += ["--output", os.path.join(work_dir, "%a.stdout")]
    cmd += ["--error", os.path.join(work_dir, "%a.stderr")]
    cmd += ["--job-name", jobname]
    cmd += [os.path.join(work_dir, "script.py")]

    subprocess.check_output(
        cmd,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
