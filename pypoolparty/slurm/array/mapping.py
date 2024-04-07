import os
import sequential_tar
import pickle
import glob
import re


def read_task_from_work_dir(work_dir, task_id):
    task_block_path = _find_task_block_in_work_dir(
        work_dir=work_dir, task_id=task_id
    )

    with sequential_tar.open(task_block_path, "r") as tf:
        for item in tf:
            item_task_id = int(re.findall(r"\d+", item.filename)[0])
            if item_task_id == task_id:
                task_pkl = item.read(mode="rb|gz")
                task = pickle.loads(task_pkl)
                return task


def _find_task_block_in_work_dir(work_dir, task_id):
    paths = glob.glob(
        os.path.join(work_dir, _task_block_filename_wildcard_glob())
    )
    for path in paths:
        basename = os.path.basename(path)
        start_task_id_str, stop_task_id_str = re.findall(r"\d+", basename)
        start_task_id = int(start_task_id_str)
        stop_task_id = int(stop_task_id_str)
        if start_task_id <= task_id <= stop_task_id:
            return path
    return None


def write_tasks_to_work_dir(
    work_dir,
    tasks,
    block_max_filesize=2**24,
):
    tf_tmp_path = os.path.join(work_dir, "tasks.tar.part")
    tf = sequential_tar.open(tf_tmp_path, "w")
    tf_filesize = 0
    tf_task_id_start = 0

    for task_id in range(len(tasks)):
        tf_filesize += tf.write(
            filename="{:d}.pickle.gz".format(task_id),
            payload=pickle.dumps(tasks[task_id]),
            mode="wb|gz",
        )

        if tf_filesize > block_max_filesize:
            tf.close()
            tf_final_path = os.path.join(
                work_dir,
                _task_block_filename_wildcard().format(
                    start_task_id=tf_task_id_start,
                    stop_task_id=task_id,
                ),
            )
            os.rename(tf_tmp_path, tf_final_path)

            tf = sequential_tar.open(tf_tmp_path, "w")
            tf_filesize = 0
            tf_task_id_start = task_id + 1

    if tf_filesize == 0:
        tf.close()
        os.remove(tf_tmp_path)
    else:
        tf.close()
        tf_final_path = os.path.join(
            work_dir,
            _task_block_filename_wildcard().format(
                start_task_id=tf_task_id_start,
                stop_task_id=task_id,
            ),
        )
        os.rename(tf_tmp_path, tf_final_path)


def _task_block_filename_wildcard_glob():
    s = _task_block_filename_wildcard()
    s = s.replace("{start_task_id:d}", "*")
    s = s.replace("{stop_task_id:d}", "*")
    return s


def _task_block_filename_wildcard():
    return "tasks_{start_task_id:d}_to_{stop_task_id:d}.tar"
