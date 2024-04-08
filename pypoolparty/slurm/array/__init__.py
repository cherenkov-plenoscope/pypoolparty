from . import making_script
from . import mapping
from . import reducing
from . import calling
from ... import utils

import subprocess
import json_line_logger
import os
import time
import shutil


class Pool:
    """
    A pool of compute resources on a distributed compute cluster
    using slurm's sbatch --array.
    """

    def __init__(
        self,
        num_simultaneously_running_tasks=None,
        python_path=None,
        polling_interval=5.0,
        work_dir=None,
        keep_work_dir=False,
        verbose=False,
    ):
        """
        Parameters
        ----------
        num_simultaneously_running_tasks : int or None
            Up to this many tasks will run in parallel.
        python_path : str or None
            The python path to be used on the computing-cluster's worker-nodes
            to execute the worker-node's python-script.
        polling_interval : float or None
            The time in seconds to wait before polling squeue again while
            waiting for the jobs to finish.
        work_dir : str
            The directory path where the tasks, the results and the
            worker-node-script is stored.
        keep_work_dir : bool
            When True, the working directory will not be removed.
        verbose : bool
            If true, the pool will print the state of its jobs to stdout.

        Returns
        -------
        pool : pypoolparty.slurm.array.Pool
            A pool instance with a map() function.
        """

        if python_path is None:
            self.python_path = utils.default_python_path()
        else:
            self.python_path = python_path

        self.work_dir = work_dir
        self.polling_interval = float(polling_interval)
        assert self.polling_interval > 0.0
        self.keep_work_dir = bool(keep_work_dir)
        self.num_simultaneously_running_tasks = (
            num_simultaneously_running_tasks
        )
        self.verbose = bool(verbose)

    def __repr__(self):
        return self.__class__.__name__ + "()"

    def print(self, msg):
        print("[pypoolparty]", utils.time_now_iso8601(), msg)

    def map(self, func, iterable):
        """
        Apply `func` to each element in `iterable`, collecting the results
        in a list that is returned.

        Parameters
        ----------
        func : function-pointer
            Pointer to a function in a python-module. It must have both:
            func.__module__
            func.__name__
        iterable : list
            List of tasks. Each task must be a valid input to 'func'.

        Returns
        -------
        results : list
            Results. One result for each task.

        Example
        -------
        results = pool.map(sum, [[1, 2], [2, 3], [4, 5], ])
            [3, 5, 9]
        """

        tasks = iterable  # to be consistent with multiprocessing's pool.map.

        session_id = utils.session_id_from_time_now()
        if self.work_dir is None:
            work_dir = os.path.abspath(
                os.path.join(".", ".pypoolparty_slurm_array_" + session_id)
            )
        else:
            work_dir = os.path.abspath(self.work_dir)

        os.makedirs(work_dir)
        logger = json_line_logger.LoggerFile(
            path=os.path.join(work_dir, "log.jsonl")
        )

        _start_msg = "start: {:s}".format(work_dir)
        logger.debug(_start_msg)
        if self.verbose:
            self.print(_start_msg)

        logger.debug("Making script...")
        script_path = os.path.join(work_dir, "script.py")
        script_content = making_script.make(
            func_module=func.__module__,
            func_name=func.__name__,
            shebang="#!{:s}".format(self.python_path),
            work_dir=work_dir,
        )
        utils.write_text(path=script_path, content=script_content)
        utils.make_path_executable(path=script_path)
        logger.debug("Making script: done.")

        logger.debug("Mapping {:d} tasks...".format(len(tasks)))
        mapping.write_tasks_to_work_dir(work_dir=work_dir, tasks=tasks)
        logger.debug("Mapping {:d} tasks: done.".format(len(tasks)))

        logger.debug("Calling sbatch --array...")
        calling.sbatch_array(
            work_dir=work_dir,
            jobname=session_id,
            start_task_id=0,
            stop_task_id=len(iterable) - 1,
            num_simultaneously_running_tasks=self.num_simultaneously_running_tasks,
            logger=logger,
        )
        logger.debug("Calling sbatch --array: done.")

        logger.debug("Preparing reduction of results...")
        reducer = reducing.Reducer(work_dir=work_dir)
        num_tasks_returned_last_poll = -1
        logger.debug("Preparing reduction of results: done.")

        logger.debug("Waiting for tasks to return...")
        while True:
            reducer.reduce()

            poll_msg = "tasks: {: 6d} / {:d}".format(
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
                logger.debug("All tasks returned.")
                break

            num_tasks_returned_last_poll = int(len(reducer.tasks_returned))
            time.sleep(self.polling_interval)

        logger.debug("Waiting for tasks to return: done.")

        logger.debug("Closing files to reduce results...")
        reducer.close()
        logger.debug("Closing files to reduce results: done.")

        remove_this_work_dir = True

        if self.keep_work_dir:
            remove_this_work_dir = False

        if len(reducer.tasks_exceptions) > 0:
            logger.warning("Found exceptions, will keep work_dir.")
            remove_this_work_dir = False

        if len(reducer.tasks_with_stderr) > 0:
            logger.warning("Found non empty stderr, will keep work_dir.")
            remove_this_work_dir = False

        logger.debug("Reading results to return them...")
        out = reducing.read_task_results(
            work_dir=work_dir,
            len_tasks=len(tasks),
            logger=logger,
        )
        logger.debug("Reading results to return them: done.")

        logger.debug("Shuting down logger.")
        utils.shutdown_logger(logger=logger)
        del logger

        if remove_this_work_dir:
            shutil.rmtree(work_dir)

        return out
