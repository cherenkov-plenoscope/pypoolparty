import pypoolparty
import numpy as np
import tempfile
import os


def test_dummys_exist():
    qpath = pypoolparty.slurm.testing.dummy_paths()
    assert os.path.exists(qpath["sbatch"])
    assert os.path.exists(qpath["squeue"])
    assert os.path.exists(qpath["scancel"])


def test_run_with_failing_job():
    """
    The dummy will run the jobs.
    It will intentionally bring ichunk == 13 into error-state 'E' five times.
    This tests if qmr.map can recover this error using 10 trials.
    """
    qpath = pypoolparty.slurm.testing.dummy_paths()

    with tempfile.TemporaryDirectory(prefix="pypoolparty-slurm") as tmp_dir:
        qsub_tmp_dir = os.path.join(tmp_dir, "qsub_tmp")

        pypoolparty.testing.init_queue_state(
            path=qpath["queue_state"],
            evil_jobs=[{"ichunk": 13, "num_fails": 0, "max_num_fails": 5}],
        )

        NUM_JOBS = 30

        tasks = []
        for i in range(NUM_JOBS):
            task = np.arange(0, 100)
            tasks.append(task)

        pool = pypoolparty.slurm.Pool(
            polling_interval=0.1,
            work_dir=qsub_tmp_dir,
            keep_work_dir=True,
            max_num_resubmissions=10,
            sbatch_path=qpath["sbatch"],
            squeue_path=qpath["squeue"],
            scancel_path=qpath["scancel"],
        )

        results = pool.map(func=np.sum, iterable=tasks)

        for i in range(NUM_JOBS):
            assert results[i] == np.sum(tasks[i])
