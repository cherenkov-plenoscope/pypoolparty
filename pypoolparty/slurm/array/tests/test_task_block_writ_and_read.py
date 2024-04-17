import pypoolparty
import tempfile
import glob
import os


def test_one_big_task():
    tasks = [2000 * "0"]
    with tempfile.TemporaryDirectory() as tmp:
        pypoolparty.slurm.array.mapping.write_tasks_to_work_dir(
            work_dir=tmp,
            tasks=tasks,
        )
        task_0 = pypoolparty.slurm.array.mapping.read_task_from_work_dir(
            work_dir=tmp,
            task_id=0,
        )
        assert tasks[0] == task_0


def test_many_big_tasks():
    tasks = [2000 * "0" for i in range(10)]
    with tempfile.TemporaryDirectory() as tmp:
        pypoolparty.slurm.array.mapping.write_tasks_to_work_dir(
            work_dir=tmp,
            tasks=tasks,
        )

        for task_id in range(len(tasks)):
            task = pypoolparty.slurm.array.mapping.read_task_from_work_dir(
                work_dir=tmp,
                task_id=task_id,
            )
            assert tasks[task_id] == task


def test_many_small_tasks():
    tasks = ["Hello {:d}".format(i) for i in range(100)]
    with tempfile.TemporaryDirectory() as tmp:
        pypoolparty.slurm.array.mapping.write_tasks_to_work_dir(
            work_dir=tmp,
            tasks=tasks,
        )

        for task_id in range(len(tasks)):
            task = pypoolparty.slurm.array.mapping.read_task_from_work_dir(
                work_dir=tmp,
                task_id=task_id,
            )
            assert tasks[task_id] == task
