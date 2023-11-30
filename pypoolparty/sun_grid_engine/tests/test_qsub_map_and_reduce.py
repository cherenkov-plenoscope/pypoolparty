from pypoolparty import sun_grid_engine
import numpy
import tempfile
import os
import subprocess


NUM_TASKS = 10
GOOD_FUNC = numpy.sum
GOOD_TASKS = []
for i in range(NUM_TASKS):
    work = numpy.arange(i, i + 100)
    GOOD_TASKS.append(work)
BAD_FUNC = os.path.join


def test_full_chain():
    qpath = sun_grid_engine.testing.dummy_paths()
    func = GOOD_FUNC
    tasks = GOOD_TASKS

    with tempfile.TemporaryDirectory(prefix="pypoolparty") as tmp:
        sun_grid_engine.testing.init_queue_state(path=qpath["queue_state"])
        pool = sun_grid_engine.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            polling_interval=1e-3,
            qsub_path=qpath["qsub"],
            qstat_path=qpath["qstat"],
            qdel_path=qpath["qdel"],
        )
        results = pool.map(func=func, iterable=tasks)

        assert len(results) == NUM_TASKS
        for i in range(NUM_TASKS):
            assert results[i] == func(tasks[i])


def test_force_dump_tmp_dir():
    qpath = sun_grid_engine.testing.dummy_paths()
    with tempfile.TemporaryDirectory(prefix="pypoolparty") as tmp:
        sun_grid_engine.testing.init_queue_state(path=qpath["queue_state"])
        pool = sun_grid_engine.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            keep_work_dir=True,
            polling_interval=1e-3,
            qsub_path=qpath["qsub"],
            qstat_path=qpath["qstat"],
            qdel_path=qpath["qdel"],
        )
        results = pool.map(
            func=GOOD_FUNC,
            iterable=GOOD_TASKS,
        )
        assert os.path.exists(os.path.join(tmp, "my_work_dir"))


def test_BAD_FUNC_creating_stderr():
    qpath = sun_grid_engine.testing.dummy_paths()
    with tempfile.TemporaryDirectory(prefix="pypoolparty") as tmp:
        sun_grid_engine.testing.init_queue_state(path=qpath["queue_state"])
        pool = sun_grid_engine.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            polling_interval=1e-3,
            qsub_path=qpath["qsub"],
            qstat_path=qpath["qstat"],
            qdel_path=qpath["qdel"],
        )
        results = pool.map(func=BAD_FUNC, iterable=GOOD_TASKS)
        assert len(results) == NUM_TASKS
        for r in results:
            assert r is None
        assert os.path.exists(os.path.join(tmp, "my_work_dir"))


def test_one_bad_task_creating_stderr():
    qpath = sun_grid_engine.testing.dummy_paths()
    with tempfile.TemporaryDirectory(prefix="pypoolparty") as tmp:
        bad_tasks = GOOD_TASKS.copy()
        bad_tasks.append("np.sum will not work for me.")

        sun_grid_engine.testing.init_queue_state(path=qpath["queue_state"])
        pool = sun_grid_engine.Pool(
            work_dir=os.path.join(tmp, "my_work_dir"),
            polling_interval=1e-3,
            qsub_path=qpath["qsub"],
            qstat_path=qpath["qstat"],
            qdel_path=qpath["qdel"],
        )
        results = pool.map(func=GOOD_FUNC, iterable=bad_tasks)

        assert len(results) == NUM_TASKS + 1
        for itask in range(NUM_TASKS):
            assert results[itask] == GOOD_FUNC(GOOD_TASKS[itask])
        assert results[itask + 1] is None
        assert os.path.exists(os.path.join(tmp, "my_work_dir"))


def test_bundling_many_tasks():
    qpath = sun_grid_engine.testing.dummy_paths()
    with tempfile.TemporaryDirectory(prefix="pypoolparty") as tmp:
        sun_grid_engine.testing.init_queue_state(path=qpath["queue_state"])

        num_many_tasks = 120

        tasks = []
        for i in range(num_many_tasks):
            task = [i, i + 1, i + 2]
            tasks.append(task)

        pool = sun_grid_engine.Pool(
            polling_interval=1e-3,
            qsub_path=qpath["qsub"],
            qstat_path=qpath["qstat"],
            qdel_path=qpath["qdel"],
            work_dir=os.path.join(tmp, "my_work_dir"),
            num_chunks=7,
        )
        results = pool.map(func=numpy.sum, iterable=tasks)

        assert len(results) == num_many_tasks
        for i in range(len(results)):
            assert results[i] == numpy.sum(tasks[i])
