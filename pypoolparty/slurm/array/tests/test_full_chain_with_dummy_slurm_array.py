import pypoolparty
import numpy as np
import tempfile
import pytest
import os


@pytest.fixture()
def debug_dir(pytestconfig):
    return pytestconfig.getoption("debug_dir")


def test_run_with_failing_job(debug_dir):
    """
    The dummy will run the jobs.
    It will intentionally bring jobid == "13" into error-state 'E' five times.
    This tests if pool.map can recover this error using 10 trials.
    """
    with pypoolparty.testing.DebugDirectory(
        suffix="-slurm-array", debug_dir=debug_dir
    ) as tmp_dir:
        work_dir = os.path.join(tmp_dir, "work_dir")
        dummy_dir = os.path.join(tmp_dir, "dummy")

        qpaths = pypoolparty.slurm.testing.dummy_init(path=dummy_dir)

        pypoolparty.testing.dummy_init_queue_state(
            path=qpaths["queue_state"],
            evil_jobs=[
                {"array_task_id": 13, "num_fails": 0, "max_num_fails": 5}
            ],
        )

        NUM_JOBS = 30

        tasks = []
        for i in range(NUM_JOBS):
            task = np.arange(0, 100)
            tasks.append(task)

        pool = pypoolparty.slurm.array.Pool(
            polling_interval=0.1,
            work_dir=work_dir,
            keep_work_dir=True,
            max_num_resubmissions=10,
            sbatch_path=qpaths["sbatch"],
            squeue_path=qpaths["squeue"],
            scancel_path=qpaths["scancel"],
            verbose=True,
        )

        results = pool.map(func=np.sum, iterable=tasks)

        for i in range(NUM_JOBS):
            assert results[i] == np.sum(tasks[i])
