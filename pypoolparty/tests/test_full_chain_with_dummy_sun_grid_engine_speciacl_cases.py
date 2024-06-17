import pypoolparty
import tempfile
import os
import subprocess
import pytest


@pytest.fixture()
def debug_dir(pytestconfig):
    return pytestconfig.getoption("debug_dir")


NUM_TASKS = 10
GOOD_FUNC = sum
GOOD_TASKS = []
for i in range(NUM_TASKS):
    work = pypoolparty.utils.arange(start=i, stop=i + 100)
    GOOD_TASKS.append(work)
BAD_FUNC = os.path.join


def test_good(debug_dir):
    func = GOOD_FUNC
    tasks = GOOD_TASKS

    with pypoolparty.testing.DebugDirectory(
        debug_dir=debug_dir, suffix="_sun-grid-engine_good"
    ) as tmp_dir:
        work_dir = os.path.join(tmp_dir, "work_dir")
        dummy_dir = os.path.join(tmp_dir, "dummy")

        qpaths = pypoolparty.sun_grid_engine.testing.dummy_init(path=dummy_dir)
        pypoolparty.testing.dummy_init_queue_state(path=qpaths["queue_state"])

        pool = pypoolparty.sun_grid_engine.Pool(
            work_dir=work_dir,
            polling_interval=1e-3,
            qsub_path=qpaths["qsub"],
            qstat_path=qpaths["qstat"],
            qdel_path=qpaths["qdel"],
            verbose=True,
        )
        results = pool.map(func=func, iterable=tasks)

        assert len(results) == NUM_TASKS
        for i in range(NUM_TASKS):
            assert results[i] == func(tasks[i])


def test_force_dump_tmp_dir(debug_dir):
    with pypoolparty.testing.DebugDirectory(
        debug_dir=debug_dir, suffix="_sun-grid-engine_force_dump_tmp_dir"
    ) as tmp_dir:
        work_dir = os.path.join(tmp_dir, "work_dir")
        dummy_dir = os.path.join(tmp_dir, "dummy")

        qpaths = pypoolparty.sun_grid_engine.testing.dummy_init(path=dummy_dir)
        pypoolparty.testing.dummy_init_queue_state(path=qpaths["queue_state"])

        pool = pypoolparty.sun_grid_engine.Pool(
            work_dir=work_dir,
            keep_work_dir=True,
            polling_interval=1e-3,
            qsub_path=qpaths["qsub"],
            qstat_path=qpaths["qstat"],
            qdel_path=qpaths["qdel"],
            verbose=True,
        )
        results = pool.map(
            func=GOOD_FUNC,
            iterable=GOOD_TASKS,
        )
        assert os.path.exists(work_dir)


def test_BAD_FUNC_creating_stderr(debug_dir):
    with pypoolparty.testing.DebugDirectory(
        debug_dir=debug_dir, suffix="_sun-grid-engine_BAD_FUNC_creating_stderr"
    ) as tmp_dir:
        work_dir = os.path.join(tmp_dir, "work_dir")
        dummy_dir = os.path.join(tmp_dir, "dummy")

        qpaths = pypoolparty.sun_grid_engine.testing.dummy_init(path=dummy_dir)
        pypoolparty.testing.dummy_init_queue_state(path=qpaths["queue_state"])

        pool = pypoolparty.sun_grid_engine.Pool(
            work_dir=work_dir,
            polling_interval=1e-3,
            qsub_path=qpaths["qsub"],
            qstat_path=qpaths["qstat"],
            qdel_path=qpaths["qdel"],
            verbose=True,
        )
        results = pool.map(func=BAD_FUNC, iterable=GOOD_TASKS)
        assert len(results) == NUM_TASKS
        for r in results:
            assert r is None
        assert os.path.exists(work_dir)


def test_one_bad_task_creating_stderr(debug_dir):
    with pypoolparty.testing.DebugDirectory(
        debug_dir=debug_dir,
        suffix="_sun-grid-engine_one_bad_task_creating_stderr",
    ) as tmp_dir:
        work_dir = os.path.join(tmp_dir, "work_dir")
        dummy_dir = os.path.join(tmp_dir, "dummy")

        qpaths = pypoolparty.sun_grid_engine.testing.dummy_init(path=dummy_dir)
        pypoolparty.testing.dummy_init_queue_state(path=qpaths["queue_state"])

        bad_tasks = GOOD_TASKS.copy()
        bad_tasks.append("np.sum will not work for me.")

        pypoolparty.testing.dummy_init_queue_state(path=qpaths["queue_state"])
        pool = pypoolparty.sun_grid_engine.Pool(
            work_dir=work_dir,
            polling_interval=1e-3,
            qsub_path=qpaths["qsub"],
            qstat_path=qpaths["qstat"],
            qdel_path=qpaths["qdel"],
            verbose=True,
        )
        results = pool.map(func=GOOD_FUNC, iterable=bad_tasks)

        assert len(results) == NUM_TASKS + 1
        for itask in range(NUM_TASKS):
            assert results[itask] == GOOD_FUNC(GOOD_TASKS[itask])
        assert results[itask + 1] is None
        assert os.path.exists(work_dir)


def test_bundling_many_tasks(debug_dir):
    with pypoolparty.testing.DebugDirectory(
        debug_dir=debug_dir, suffix="_sun-grid-engine_bundling_many_tasks"
    ) as tmp_dir:
        work_dir = os.path.join(tmp_dir, "work_dir")
        dummy_dir = os.path.join(tmp_dir, "dummy")

        qpaths = pypoolparty.sun_grid_engine.testing.dummy_init(path=dummy_dir)
        pypoolparty.testing.dummy_init_queue_state(path=qpaths["queue_state"])

        num_many_tasks = 120

        tasks = []
        for i in range(num_many_tasks):
            task = [i, i + 1, i + 2]
            tasks.append(task)

        pool = pypoolparty.sun_grid_engine.Pool(
            polling_interval=1e-3,
            qsub_path=qpaths["qsub"],
            qstat_path=qpaths["qstat"],
            qdel_path=qpaths["qdel"],
            work_dir=work_dir,
            num_chunks=7,
            verbose=True,
        )
        results = pool.map(func=sum, iterable=tasks)

        assert len(results) == num_many_tasks
        for i in range(len(results)):
            assert results[i] == sum(tasks[i])
