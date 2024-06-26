import pypoolparty
import tempfile
import os
import pytest
import operator


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

    with pypoolparty.testing.DebugDirectory(
        debug_dir=debug_dir, suffix="-sun-grid-engine"
    ) as tmp_dir:
        work_dir = os.path.join(tmp_dir, "work_dir")
        dummy_dir = os.path.join(tmp_dir, "dummy")

        qpaths = pypoolparty.sun_grid_engine.testing.dummy_init(path=dummy_dir)

        pypoolparty.testing.dummy_init_queue_state(
            path=qpaths["queue_state"],
            evil_jobs=[{"ichunk": 13, "num_fails": 0, "max_num_fails": 5}],
        )

        NUM_JOBS = 30

        tasks = []
        for i in range(NUM_JOBS):
            task = pypoolparty.utils.arange(start=0, stop=100)
            tasks.append(task)

        pool = pypoolparty.sun_grid_engine.Pool(
            polling_interval=0.1,
            work_dir=work_dir,
            keep_work_dir=True,
            max_num_resubmissions=10,
            qsub_path=qpaths["qsub"],
            qstat_path=qpaths["qstat"],
            qdel_path=qpaths["qdel"],
            error_state_indicator="E",
            verbose=True,
        )

        results = pool.map(func=sum, iterable=tasks)

        for i in range(NUM_JOBS):
            assert results[i] == sum(tasks[i])

        # starmap
        # -------
        results = pool.starmap(
            func=operator.eq, iterable=zip([1, 1, 1, 1], [1, 0, 1, 1])
        )
        assert results[0]
        assert not results[1]
        assert results[2]
        assert results[3]
