import os
import zipfile
import pickle


def read_task_from_work_dir(work_dir, task_id):
    tasks_path = os.path.join(work_dir, "tasks.zip")
    task_filename = "{:d}.pickle".format(task_id)
    with zipfile.ZipFile(file=tasks_path, mode="r") as zin:
        with zin.open(task_filename, "r") as f:
            task = pickle.loads(f.read())
    return task


def write_tasks_to_work_dir(work_dir, tasks):
    tasks_path = os.path.join(work_dir, "tasks.zip")
    with zipfile.ZipFile(file=tasks_path + ".part", mode="w") as zout:
        for task_id in range(len(tasks)):
            with zout.open(name="{:d}.pickle".format(task_id), mode="w") as f:
                f.write(pickle.dumps(tasks[task_id]))
    os.rename(tasks_path + ".part", tasks_path)
