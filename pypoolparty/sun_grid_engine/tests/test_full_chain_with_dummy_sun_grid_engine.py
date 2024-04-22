import pypoolparty
import numpy as np
import tempfile
import os
import pytest


@pytest.fixture()
def debug_dir(pytestconfig):
    return pytestconfig.getoption("debug_dir")


def test_dummys_exist():
    qpath = pypoolparty.sun_grid_engine.testing.dummy_paths()
    assert os.path.exists(qpath["qsub"])
    assert os.path.exists(qpath["qstat"])
    assert os.path.exists(qpath["qdel"])


def test_run_with_failing_job(debug_dir):
    """
    The dummy_qsub will run the jobs.
    It will intentionally bring ichunk == 13 into error-state 'E' five times.
    This tests if qmr.map can recover this error using 10 trials.
    """
    qpath = pypoolparty.sun_grid_engine.testing.dummy_paths()

    with pypoolparty.testing.DebugDirectory(
        debug_dir=debug_dir, suffix="-sun-grid-engine"
    ) as tmp_dir:
        work_dir = os.path.join(tmp_dir, "work_dir")

        pypoolparty.testing.dummy_init_queue_state(
            path=qpath["queue_state"],
            evil_jobs=[{"ichunk": 13, "num_fails": 0, "max_num_fails": 5}],
        )

        NUM_JOBS = 30

        tasks = []
        for i in range(NUM_JOBS):
            task = np.arange(0, 100)
            tasks.append(task)

        pool = pypoolparty.sun_grid_engine.Pool(
            polling_interval=0.1,
            work_dir=work_dir,
            keep_work_dir=True,
            max_num_resubmissions=10,
            qsub_path=qpath["qsub"],
            qstat_path=qpath["qstat"],
            qdel_path=qpath["qdel"],
            error_state_indicator="E",
        )

        results = pool.map(func=np.sum, iterable=tasks)

        for i in range(NUM_JOBS):
            assert results[i] == np.sum(tasks[i])
