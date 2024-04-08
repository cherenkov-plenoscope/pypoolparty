import os
import sequential_tar
import pickle
import glob
import re
from ... import utils


class Reducer:
    def __init__(self, work_dir):
        self.work_dir = work_dir
        self.tar_results = sequential_tar.open(
            os.path.join(work_dir, "tasks.results.tar"), "w"
        )
        self.tar_stdout = sequential_tar.open(
            os.path.join(work_dir, "tasks.stdout.tar"), "w"
        )
        self.tar_stderr = sequential_tar.open(
            os.path.join(work_dir, "tasks.stderr.tar"), "w"
        )
        self.tar_exceptions = sequential_tar.open(
            os.path.join(work_dir, "tasks.exceptions.tar"), "w"
        )
        self.tasks_results = []
        self.tasks_exceptions = []
        self.tasks_with_stdout = []
        self.tasks_with_stderr = []

    @property
    def tasks_returned(self):
        return self.tasks_results + self.tasks_exceptions

    def reduce(self):
        result_paths = glob.glob(os.path.join(self.work_dir, "*.pickle"))
        for path in result_paths:
            task_id = get_task_id_from_basename(os.path.basename(path))
            self._reduce_result_of_task(task_id=task_id)
            self._reduce_stdout_of_task(task_id=task_id)
            self._reduce_stderr_of_task(task_id=task_id)

        exception_paths = glob.glob(os.path.join(self.work_dir, "*.exception"))
        for path in exception_paths:
            task_id = get_task_id_from_basename(os.path.basename(path))
            self._reduce_exception_of_task(task_id=task_id)
            self._reduce_stdout_of_task(task_id=task_id)
            self._reduce_stderr_of_task(task_id=task_id)

    def _reduce_stderr_of_task(self, task_id):
        basename = "{:d}.stderr".format(task_id)
        path = os.path.join(self.work_dir, basename)
        with open(path, "rb") as f:
            content = f.read()
            if len(content) > 0:
                self.tasks_with_stderr.append(task_id)
            self.tar_stdout.write(
                name=basename,
                payload=content,
                mode="wb",
            )
        os.remove(path)

    def _reduce_stdout_of_task(self, task_id):
        basename = "{:d}.stdout".format(task_id)
        path = os.path.join(self.work_dir, basename)
        with open(path, "rb") as f:
            content = f.read()
            if len(content) > 0:
                self.tasks_with_stdout.append(task_id)
            self.tar_stdout.write(
                name=basename,
                payload=content,
                mode="wb",
            )
        os.remove(path)

    def _reduce_result_of_task(self, task_id):
        basename = "{:d}.pickle".format(task_id)
        path = os.path.join(self.work_dir, basename)
        with open(path, "rb") as f:
            self.tar_results.write(
                name=basename,
                payload=f.read(),
                mode="wb",
            )
        os.remove(path)
        self.tasks_results.append(task_id)

    def _reduce_exception_of_task(self, task_id):
        basename = "{:d}.exception".format(task_id)
        path = os.path.join(self.work_dir, basename)
        with open(path, "rt") as f:
            self.tar_exceptions.write(
                name=basename,
                payload=f.read(),
                mode="wt",
            )
        os.remove(path)
        self.tasks_exceptions.append(task_id)

    def close(self):
        self.tar_results.close()
        self.tar_stdout.close()
        self.tar_stderr.close()
        self.tar_exceptions.close()


def get_task_id_from_basename(basename):
    return int(re.findall(r"\d+", basename)[0])


def read_task_results_from_tar(path):
    task_results = {}
    with sequential_tar.open(path, "r") as tar:
        for item in tar:
            task_id = get_task_id_from_basename(item.name)
            task_result = pickle.loads(item.read(mode="rb"))
            task_results[task_id] = task_result
    return task_results


def read_task_results(work_dir, len_tasks, logger=None):
    logger = utils.make_logger_to_stdout_if_none(logger)

    task_results = read_task_results_from_tar(
        path=os.path.join(work_dir, "tasks.results.tar")
    )
    out = []
    for task_id in range(len_tasks):
        if task_id in task_results:
            out.append(task_results.pop(task_id))
        else:
            out.append(None)
            logger.error("No result found for task_id {:d}.".format(task_id))
