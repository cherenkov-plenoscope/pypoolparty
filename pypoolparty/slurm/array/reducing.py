import os
import sequential_tar
import pickle
import glob
import re
import gzip
import numpy as np


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
        self.num_task_results = 0

    def reduce(self):
        result_paths = glob.glob(os.path.join(self.work_dir, "*.pickle"))
        for result_path in result_paths:
            result_basename = os.path.basename(result_path)
            task_id = int(re.findall(r"\d+", result_basename)[0])
            result_stdout_basename = "{:d}.stdout".format(task_id)
            result_stderr_basename = "{:d}.stderr".format(task_id)
            result_stdout_path = os.path.join(self.work_dir, result_stdout_basename)
            result_stderr_path = os.path.join(self.work_dir, result_stderr_basename)

            with open(result_path, "rb") as f:
                self.tar_results.write(
                    name=result_basename,
                    payload=f.read(),
                    mode="wb",
                )
            with open(result_stdout_path, "rb") as f:
                self.tar_stdout.write(
                    name=result_stdout_basename,
                    payload=f.read(),
                    mode="wb",
                )
            with open(result_stderr_path, "rb") as f:
                self.tar_stderr.write(
                    name=result_stderr_basename,
                    payload=f.read(),
                    mode="wb",
                )
            self.num_task_results += 1
            os.remove(result_path)
            os.remove(result_stdout_path)
            os.remove(result_stderr_path)

    def close(self):
        self.tar_results.close()
        self.tar_stdout.close()
        self.tar_stderr.close()


def read_task_results(path):
    task_results = {}
    with sequential_tar.open(path, "r") as tar:
        for item in tar:
            task_id = int(re.findall(r"\d+", item.name)[0])
            task_result = pickle.loads(item.read(mode="rb"))
            task_results[task_id] = task_result
    return task_results


def write_task_result(path, task_result, mode="wb"):
    assert mode == "wb"
    tmp_path = path + ".part"
    with open(tmp_path, "wb") as f:
        f.write(pickle.dumps(task_result))
    os.rename(tmp_path, path)
