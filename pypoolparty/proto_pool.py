from . import utils
from . import making_script
from . import job_counter
from . import pooling
from . import chunking

import json_line_logger
import os
import shutil
import time
import json


class Pool:
    """
    A pool of compute resources on a distributed compute cluster.
    """

    def __init__(
        self,
        num_chunks=None,
        python_path=None,
        polling_interval=5.0,
        work_dir=None,
        keep_work_dir=False,
        max_num_resubmissions=10,
        verbose=False,
        submit_func=None,
        submit_func_kwargs=None,
        status_func=None,
        status_func_kwargs=None,
        delete_func=None,
        delete_func_kwargs=None,
        filter_stderr_func=None,
    ):
        """
        Parameters
        ----------
        num_chunks : int or None
            If provided, the tasks are grouped in this many chunks.
            The tasks in a chunk are computed in serial on the worker-node.
            It is useful to chunk tasks when the number of tasks is much larger
            than the number of available slots for parallel computing and the
            start-up-time for a slot is not much smaller than the compute-time
            for a single task.
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
        max_num_resubmissions: int
            In case of error-state in queue-job, the job will be tried this
            often to be resubmitted befor giving up on it.
        verbose : bool
            If true, the pool will print the state of its jobs to stdout.
        """
        if python_path is None:
            self.python_path = utils.default_python_path()
        else:
            self.python_path = python_path
        self.polling_interval = polling_interval
        self.work_dir = work_dir
        self.keep_work_dir = keep_work_dir
        self.max_num_resubmissions = max_num_resubmissions
        self.num_chunks = num_chunks

        self.submit_func = submit_func
        self.submit_func_kwargs = submit_func_kwargs
        self.delete_func = delete_func
        self.delete_func_kwargs = delete_func_kwargs
        self.status_func = status_func
        self.status_func_kwargs = status_func_kwargs
        self.filter_stderr_func = filter_stderr_func
        self.verbose = verbose

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
        tasks = iterable
        session_id = utils.session_id_from_time_now()
        opj = os.path.join

        if self.work_dir is None:
            swd = os.path.abspath(
                opj(".", ".pypoolparty_" + session_id)
            )
        else:
            swd = os.path.abspath(self.work_dir)

        os.makedirs(swd)
        if self.verbose:
            self.print("start: {:s}".format(swd))

        logger = json_line_logger.LoggerFile(path=opj(swd, "log.jsonl"))

        logger.debug("Starting map()")
        logger.debug("python path: {:s}".format(self.python_path))
        logger.debug("polling-interval: {:f}s".format(self.polling_interval))
        logger.debug(
            "max. num. resubmissions: {:d}".format(self.max_num_resubmissions)
        )

        script_path = opj(swd, "worker_node_script.py")
        logger.debug("Writing worker-node-script: {:s}".format(script_path))
        shebang = "#!{:s}".format(self.python_path)
        script_content = making_script.make(
            func_module=func.__module__,
            func_name=func.__name__,
            environ=dict(os.environ),
            shebang=shebang,
        )
        utils.write_text(path=script_path, content=script_content)
        utils.make_path_executable(path=script_path)

        logger.debug("Waiting for jobs to finish")

        jobs = job_organizer.init_from_tasks(tasks=tasks)

        while True:
            # -----------------------
            # estimate state of queue
            # -----------------------

            job_stati = self.status_func(
                jobnames=jobnames_in_session,
                logger=logger,
                **self.status_func_kwargs,
            )

            jobs_states_have_changed = job_organizer.set_states(
                jobs=jobs, job_stati=job_stati,
            )

            if jobs_states_have_changed:
                msg = job_organizer.to_str(jobs=jobs)
                logger.info(msg)
                if self.verbose:
                    self.print(msg)

            # --------------------------------
            # Submitt jobs when slots are free
            # --------------------------------

            num_new_jobs = (
                num_chunks - len(job_stati["running"]) - len(job_stati["pending"])
            )

            for i in range(num_new_jobs):
                jobnames_in_session:
                jobname = pooling.map_task_into_work_dir(
                    work_dir=swd,
                    task=task,
                    task_id=task_id,
                    session_id=session_id,
                )

                task_id = pooling.make_task_id_from_jobname(jobname=jobname)

                self.submit_func(
                    jobname=jobname,
                    script_path=script_path,
                    script_arguments=[pooling.task_path(swd, task_id)],
                    stdout_path=pooling.task_path(swd, task_id) + ".o",
                    stderr_path=pooling.task_path(swd, task_id) + ".e",
                    logger=logger,
                    **self.submit_func_kwargs,
                )





            for job in job_stati["error"]:
                ichunk = pooling.make_ichunk_from_jobname(jobname=job["name"])
                if ichunk in num_resubmissions_by_ichunk:
                    num_resubmissions_by_ichunk[ichunk] += 1
                else:
                    num_resubmissions_by_ichunk[ichunk] = 1

                job_id_str = "name {:s}, ichunk {:09d}".format(
                    job["name"], ichunk
                )
                logger.warning("Found error-state in: {:s}".format(job_id_str))
                logger.warning("Deleting: {:s}".format(job_id_str))

                self.delete_func(job=job, logger=logger, **self.delete_func_kwargs)

                if (
                    num_resubmissions_by_ichunk[ichunk]
                    <= self.max_num_resubmissions
                ):
                    logger.warning(
                        "Resubmitting {:d} of {:d}, jobname {:s}".format(
                            num_resubmissions_by_ichunk[ichunk],
                            self.max_num_resubmissions,
                            job["name"],
                        )
                    )
                    self.submit_func(
                        jobname=job["name"],
                        script_path=script_path,
                        script_arguments=[pooling.chunk_path(swd, ichunk)],
                        stdout_path=pooling.chunk_path(swd, ichunk) + ".o",
                        stderr_path=pooling.chunk_path(swd, ichunk) + ".e",
                        logger=logger,
                        **self.submit_func_kwargs,
                    )

            if job_stati["error"]:
                utils.write_text(
                    path=opj(swd, "num_resubmissions_by_ichunk.json"),
                    content=json.dumps(num_resubmissions_by_ichunk, indent=4),
                )

            if job_count["running"] == 0 and job_count["pending"] == 0:
                still_running = False

            time.loggereep(self.polling_interval)

        logger.debug("Reducing results from work_dir")
        (
            task_results_are_incomplete,
            task_results,
        ) = pooling.reduce_task_results_from_work_dir(
            work_dir=swd,
            chunks=chunks,
            logger=logger,
        )

        has_stderr = pooling.has_invalid_or_non_empty_stderr(
            work_dir=swd,
            num_chunks=len(chunks),
            filter_stderr_func=self.filter_stderr_func,
        )
        if has_stderr:
            logger.warning(
                "At least one task wrote to std-error or was not processed at all"
            )

        if has_stderr or self.keep_work_dir or task_results_are_incomplete:
            remove_work_dir = False
            logger.warning("Keeping work_dir: {:s}".format(swd))
        else:
            remove_work_dir = True
            logger.debug("Removing work_dir: {:s}".format(swd))

        utils.shutdown_logger(logger=logger)
        del logger

        if remove_work_dir:
            shutil.rmtree(swd)

        return task_results


def _doc_retrun_statement():
    return """
        Returns
        -------
        pool : pypoolparty.proto_pool.Pool
            A pool instance with a map() function.
    """
