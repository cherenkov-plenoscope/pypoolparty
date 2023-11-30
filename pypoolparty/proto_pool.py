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
        submit_func=None,
        submit_func_kwargs=None,
        status_func=None,
        status_func_kwargs=None,
        delete_func=None,
        delete_func_kwargs=None,
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

    def __repr__(self):
        return self.__class__.__name__ + "()"

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

        if self.work_dir is None:
            swd = os.path.abspath(
                os.path.join(".", ".pypoolparty_" + session_id)
            )
        else:
            swd = os.path.abspath(self.work_dir)

        os.makedirs(swd)
        sl = json_line_logger.LoggerFile(path=os.path.join(swd, "log.jsonl"))

        sl.debug("Starting map()")
        sl.debug("python path: {:s}".format(self.python_path))
        sl.debug("polling-interval: {:f}s".format(self.polling_interval))
        sl.debug(
            "max. num. resubmissions: {:d}".format(self.max_num_resubmissions)
        )

        script_path = os.path.join(swd, "worker_node_script.py")
        sl.debug("Writing worker-node-script: {:s}".format(script_path))
        shebang = "#!{:s}".format(self.python_path)
        script_content = making_script.make(
            func_module=func.__module__,
            func_name=func.__name__,
            environ=dict(os.environ),
            shebang=shebang,
        )
        utils.write_text(path=script_path, content=script_content)
        utils.make_path_executable(path=script_path)

        sl.debug("Make chunks of tasks")

        chunks = chunking.assign_tasks_to_chunks(
            num_tasks=len(tasks),
            num_chunks=self.num_chunks,
        )

        sl.debug("Mapping chunks of tasks into work_dir")

        jobnames_in_session = pooling.map_tasks_into_work_dir(
            work_dir=swd,
            tasks=tasks,
            chunks=chunks,
            session_id=session_id,
        )

        sl.debug("Submitting jobs")

        for jobname in jobnames_in_session:
            ichunk = pooling.make_ichunk_from_jobname(jobname=jobname)
            self.submit_func(
                jobname=jobname,
                script_path=script_path,
                script_arguments=[pooling.chunk_path(swd, ichunk)],
                stdout_path=pooling.chunk_path(swd, ichunk) + ".o",
                stderr_path=pooling.chunk_path(swd, ichunk) + ".e",
                logger=sl,
                **self.submit_func_kwargs,
            )

        sl.debug("Waiting for jobs to finish")

        still_running = True
        num_resubmissions_by_ichunk = {}
        last_job_count = job_counter.init()

        while still_running:
            job_stati = self.status_func(
                jobnames=jobnames_in_session,
                logger=sl,
                **self.status_func_kwargs,
            )
            job_count = job_counter.estimate(
                num_jobs_running=len(job_stati["running"]),
                num_jobs_pending=len(job_stati["pending"]),
                num_jobs_error=len(job_stati["error"]),
                num_resubmissions_by_ichunk=num_resubmissions_by_ichunk,
                max_num_resubmissions=self.max_num_resubmissions,
            )

            if not job_counter.is_equal(job_count, last_job_count):
                sl.info(job_counter.to_str(job_count))
            last_job_count = job_count

            for job in job_stati["error"]:
                ichunk = pooling.make_ichunk_from_jobname(jobname=job["name"])
                if ichunk in num_resubmissions_by_ichunk:
                    num_resubmissions_by_ichunk[ichunk] += 1
                else:
                    num_resubmissions_by_ichunk[ichunk] = 1

                job_id_str = "name {:s}, ichunk {:09d}".format(
                    job["name"], ichunk
                )
                sl.warning("Found error-state in: {:s}".format(job_id_str))
                sl.warning("Deleting: {:s}".format(job_id_str))

                self.delete_func(job=job, logger=sl, **self.delete_func_kwargs)

                if (
                    num_resubmissions_by_ichunk[ichunk]
                    <= self.max_num_resubmissions
                ):
                    sl.warning(
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
                        logger=sl,
                        **self.submit_func_kwargs,
                    )

            if job_stati["error"]:
                utils.write_text(
                    path=os.path.join(swd, "num_resubmissions_by_ichunk.json"),
                    content=json.dumps(num_resubmissions_by_ichunk, indent=4),
                )

            if job_count["running"] == 0 and job_count["pending"] == 0:
                still_running = False

            time.sleep(self.polling_interval)

        sl.debug("Reducing results from work_dir")
        (
            task_results_are_incomplete,
            task_results,
        ) = pooling.reduce_task_results_from_work_dir(
            work_dir=swd,
            chunks=chunks,
            logger=sl,
        )

        has_stderr = pooling.has_invalid_or_non_empty_stderr(
            work_dir=swd, num_chunks=len(chunks)
        )
        if has_stderr:
            sl.warning(
                "At least one task wrote to std-error or was not processed at all"
            )

        if has_stderr or self.keep_work_dir or task_results_are_incomplete:
            remove_work_dir = False
            sl.warning("Keeping work_dir: {:s}".format(swd))
        else:
            remove_work_dir = True
            sl.debug("Removing work_dir: {:s}".format(swd))

        utils.shutdown_logger(logger=sl)
        del sl

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
