#################
Python Pool Party
#################
|TestStatus| |PyPiStatus| |BlackStyle| |BlackPackStyle| |MITLicenseBadge|

A python package for job pools (as in ``multiprocessing.Pool()``) which makes
use of workload managers on distributed compute clusters.

The ``pypoolparty`` provides a ``Pool()`` with a ``map()`` function which aims
to be a drop-in-replacement for ``multiprocessing.Pool()``'s ``map()``.
This way you can always fall back to the builtin pools and map-functions
in case a distributed compute cluster is not available.

This package respects the concept of 'fair share' what is commonly found
in scientific environments, but is not common in commercial environments.
Here, fair share means that compute resources are only requested when they
are needed. Compute resources are not requested to just idle and wait for
jobs to be submitted.

A consequence of this fair sharing is, that this package expects your jobs
to randomly die in conflicts for resources with jobs submitted by other users,
such as conflicts for limited disk space on temporary drives. If your jobs run
into error states, they will be resubmitted until a predefined limit is
reached.


Installing
==========

.. code:: bash

    pip install pypoolparty


Basic Usage
===========

.. code:: python

    import pypoolparty

    pool = pypoolparty.slurm.array.Pool()
    results = pool.map(sum, [[1, 2], [2, 3], [4, 5], ])


The ``pool`` implements two functions ``map`` and ``starmap``.

.. code:: python

    import operator

    results = pool.starmap(operator.eq, zip([1, 2, 3], [1, "nope", 3]))


For more details, see the ``Pool()'s`` docs, e.g. ``pypoolparty.slurm.array.Pool?``.
Options to the ``Pool()s`` are defined in therir constructors e.g.


.. code:: python

    pool = pypoolparty.slurm.array.Pool(
        num_simultaneously_running_tasks=200,
        python_path="/path/to/python/interpreter/to/be/called/on/the/worker/nodes",
        polling_interval=5.0,
        work_dir="/path/to/the/pools/work_dir/where/the/map/and/reduce/happens",
        keep_work_dir=True,  # e.g for debugging
        verbose=True,  # Talk to me!
        slurm_call_timeout=60.0,
        max_num_resubmissions=3,
    )


Pools
=====

``pypoolparty.slurm.array.Pool()``
----------------------------------
Uses slurm's ``--array`` option.
It will call ``sbatch --array``, ``squeue`` and ``scancel``.

``pypoolparty.sun_grid_engine.Pool()``
--------------------------------------
It will call ``qsub``, ``qstat`` and ``qdel``.

``pypoolparty.slurm.Pool()``
----------------------------
It will call ``sbatch``, ``squeue`` and ``scancel``.
Uses the same inner workings as ``pypoolparty.sun_grid_engine.Pool()``.

Testing
=======
The ``pypoolparty`` comes with ist own dummys for slurm and the sun grid engine.
This allows to test the full chain without the actual workload managers to be installed.

.. code:: bash

    pytest -s pypoolparty


Workload managers
=================
We tested:

- SLURM, version 22.05.6
- Sun Grid Engine (SGE), version 8.1.9


Alternatives
============
When you do not share resources with other users, when you do not need to respect fair share, and when you have some administrative power you might want to use one of these:

- Dask_ has a ``job_queue`` which also supports other flavors such as PBS, SLURM.

- pyABC.sge_ has a ``pool.map()`` very much like the one in this package.

- ipyparallel_


Inner Workings
==============
The maaping and reducing takes place in a ``work_dir`` in the filesystem.
The ``work_dir`` can be defined manually and must be reachable by all
compute notes.

``slurm.array.Pool``
--------------------
- Makes a ``work_dir`` where it creates a zip-file named ``tasks.zip`` in which it dumps all ``tasks`` using ``pickle``.

- Starts a logger which logs into a file named ``log.jsonl`` in the ``work_dir``.

- Makea a script which will execute the tasks on the compute nodes and dumps the script named ``script.py`` into the ``work_dir``. The script contains the path to the ``work_dir`` and queries the environment variable ``SLURM_ARRAY_TASK_ID`` to determine which ``task`` it shall process. It will write its result, ``stdout`` and ``stderr``, and potentially a report of raised ``exceptions`` into the ``work_dir``.

- Calls ``sbatch --array``

- After the initial call of ``sbatch``, we wait for the jobs to return (to write their results) or to get stuck in some error state. With a polling interval of 5s (can be adjusted), the ``work_dir`` is searched for results and ``squeue`` is searched for jobs in error states. When results are found in the ``work_dir``, they are read and appended into the four zip-files named ``tasks.results.zip``, ``tasks.stdout.zip``, ``tasks.stderr.zip``, and ``tasks.exceptions.zip``. When the individual files writen by a job got appended to the zip-files, the individual files are removed to keep the number of files low.

- If the poll of ``squeue`` indicates ``tasks`` with error like flags, these specific ``tasks`` will be removed from the queue by calling ``scancel`` and then added again by calling ``sbatch --array`` until a predefined limit of resubmissions is reached.

- Finally, either all ``tasks`` returned results or got finally stuck in errors and exceptions. The results are read into memory from ``work_dir/tasks_results.zip`` and returned by the ``map()`` function. If there was non zero ``stderr`` or an exception, the ``work_dir`` will not be removed after the call of ``map()``, but will stay for potential debugging.


``sun_grid_engine.Pool`` and ``slurm.Pool``
-------------------------------------------

- ``map()`` makes a ``work_dir`` because the mapping and reducing takes place in the filesystem. You can set ``work_dir`` manually to make sure both the worker nodes and the process node can reach it.

- ``map()`` serializes your ``tasks`` using ``pickle`` into separate files in ``work_dir/{ichunk:09d}.pkl``.

- ``map()`` reads all environment variables in its process.

- ``map()`` creates the worker-node script in ``work_dir/worker_node_script.py``. It contains and exports the process' environment variables into the batch job's context. It reads the chunk of tasks in ``work_dir/{ichunk:09d}.pkl``, imports and runs your ``func(task)``, and finally writes the result back to ``work_dir/{ichunk:09d}.pkl.out``.

- ``map()`` submits queue jobs. The ``stdout`` and ``stderr`` of the tasks are written to ``work_dir/{ichunk:09d}.pkl.o`` and ``work_dir/{ichunk:09d}.pkl.e`` respectively. By default, ``shutil.which("python")`` is used to process the worker-node-script.

- When all queue jobs are submitted, ``map()`` monitors their progress. In case a queue-job runs into an error-state, the job will be deleted and resubmitted until a maximum number of resubmissions is reached.

- When no more queue jobs are running or pending, ``map()`` will reduce the results from ``work_dir/{ichunk:09d}.pkl.out``.

- In case of non-zero ``stderr`` in any task, a missing result, or on the user's request, the ``work_dir`` will be kept for inspection. Otherwise its removed.


Environment Variables
=====================
All the user's environment variables in the process where ``map()`` is called
will be exported in the queue job's context.

The worker-node script explicitly sets the environment variables.
This package does not rely on the batch system's ability (``slurm``/``sge``)
to do so.


Wording
=======

- ``task`` is a valid input to ``func``. The ``tasks`` are the actual payload to be processed.

- ``iterable`` is an iterable (list) of ``tasks``. It is the naming adopted from ``multiprocessing.Pool.map``.

- ``itask`` is the index of a ``task`` in ``iterable``.

- ``chunk`` is a chunk of ``tasks`` which is processed on a worker-node in serial.

- ``ichunk`` is the index of a chunk. It is used to create the chunks's filenames such as ``work_dir/{ichunk:09d}.pkl``.

- `queue-job` is what we submit into the queue. Each queue-job processes the tasks in a single chunk in series.

- ``jobname`` or ``job["name"]`` is assigned to a queue job by our ``map()``. It is composed of our ``map()``'s session-id, and ``ichunk``. E.g. ``"q"%Y-%m-%dT%H:%M:%S"#{ichunk:09d}"``


Testing for developers
======================
The tests have an option ``--debug_dir`` which allows to make the otherwise
temporary output and working directories to remain after the tests have run.

.. code:: bash

    pytest -s --debug_dir path/to/do/debugging pypoolparty


dummy queue
-----------
To test our ``map()`` we provide a dummy ``qsub``, ``qstat``, and ``qdel``
for the sun-grid-engine, and a dummy ``sbatch``, ``squeue``, and ``scancel``
for slurm.
These are individual ``python`` scripts which all act on a common state file
named ``queue_state.json`` in order to imitate the workload managers.

- ``qsub``/``sbatch`` only append pening jobs to the list of jobs in ``queue_state.json``.

- ``qdel``/``scancel`` only remove jobs from the list of jobs in ``queue_state.json``.

- ``qstat``/``squeue`` changes  the state of jobs from pending to running, and triggers the actual processing of the jobs. Each time ``qstat.py`` is called it performs a single action on ``queue_state.json``. So it must be called multiple times to process all jobs. It can intentionally bring jobs into error states when this is set accordingly in the ``queue_state.json``.


.. |TestStatus| image:: https://github.com/cherenkov-plenoscope/pypoolparty/actions/workflows/test.yml/badge.svg?branch=main
    :target: https://github.com/cherenkov-plenoscope/pypoolparty/actions/workflows/test.yml

.. |PyPiStatus| image:: https://img.shields.io/pypi/v/pypoolparty
    :target: https://pypi.org/project/pypoolparty

.. |BlackStyle| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. |BlackPackStyle| image:: https://img.shields.io/badge/pack%20style-black-000000.svg
    :target: https://github.com/cherenkov-plenoscope/black_pack

.. |MITLicenseBadge| image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: https://opensource.org/licenses/MIT

.. _Dask: https://docs.dask.org/en/latest/

.. _pyABC.sge: https://pyabc.readthedocs.io/en/latest/api_sge.html

.. _ipyparallel: https://ipyparallel.readthedocs.io/en/latest/index.html
