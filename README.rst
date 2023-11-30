#################
Python Pool Party
#################
|TestStatus| |PyPiStatus| |BlackStyle| |BlackPackStyle| |MITLicenseBadge|

A python package for job pools (as in ``multiprocessing.Pool()``) which acts
on distributed compute clusters.


Basic Usage
===========
A drop-in-replacement for builtins' ``map()``, and ``multiprocessing.Pool()``'s ``map()``.

.. code:: python

    import pypoolparty as ppp

    pool = ppp.slurm.Pool()
    results = pool.map(sum, [[1, 2], [2, 3], [4, 5], ])


Currently, there is ``ppp.slurm.Pool()`` and ``ppp.sun_grid_engine.Pool()``.

Alternatives
============
When you do not share resources with other users, and when you have some administrative power you might want to use one of these:

- Dask_ has a ``job_queue`` which also supports other flavors such as PBS, SLURM.

- pyABC.sge_ has a ``pool.map()`` very much like the one in this package.

- ipyparallel_


Queue Flavors
=============

- SLURM, version 22.05.6
- Sun Grid Engine (SGE), version 8.1.9


Inner Workings
==============
- ``map()`` makes a ``work_dir`` because the mapping and reducing takes place in the filesystem. You can set ``work_dir`` manually to make sure both the worker nodes and the process node can reach it.

- ``map()`` serializes your ``tasks`` using ``pickle`` into separate files in ``work_dir/{ichunk:09d}.pkl``.

- ``map()`` reads all environment variables in its process.

- ``map()`` creates the worker-node script in ``work_dir/worker_node_script.py``. It contains and exports the process' environment variables into the batch job's context. It reads the chunk of tasks in ``work_dir/{ichunk:09d}.pkl``, imports and runs your ``func(task)``, and finally writes the result back to ``work_dir/{ichunk:09d}.pkl.out``.

- ``map()`` submits queue jobs. The ``stdout`` and ``stderr`` of the tasks are written to ``work_dir/{ichunk:09d}.pkl.o`` and ``work_dir/{ichunk:09d}.pkl.e`` respectively. By default, ``shutil.which("python")`` is used to process the worker-node-script.

- When all queue jobs are submitted, ``map()`` monitors their progress. In case a queue-job runs into an error-state, the job will be deleted and resubmitted until a maximum number of resubmissions is reached.

- When no more queue jobs are running or pending, ``map()`` will reduce theresults from ``work_dir/{ichunk:09d}.pkl.out``.

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

- `queue-job` is what we submitt into the queue. Each queue-job processes the tasks in a single chunk in series.

- ``jobname`` or ``job["name"]`` is assigned to a queue job by our ``map()``. It is composed of our ``map()``'s session-id, and ``ichunk``. E.g. ``"q"%Y-%m-%dT%H:%M:%S"#{ichunk:09d}"``


Testing
=======

.. code:: bash

    pytest -s .

dummy queue
-----------
To test our ``map()`` we provide a dummy ``qsub``, ``qstat``, and ``qdel``
for the sun-grid-engine.
These are individual ``python`` scripts which all act on a common state file
in ``tests/resources/dummy_queue_state.json`` in order to fake the
sun-grid-engine's queue.

- ``dummy_qsub.py`` only appends queue jobs to the list of pending jobs in the state-file.

- ``dummy_qdel.py`` only removes queue jobs from the state-file.

- ``dummy_qstat.py`` does move the queue jobs from the pending to the running list, and does trigger the actual processing of the jobs. Each time ``dummy_qstat.py`` is called it performs a single action on the state file. So it must be called multiple times to process all jobs. It can intentionally bring jobs into the error-state when this is set in the state-file.

Before running the dummy queue, its state file must be initialized:

.. code:: python

    from pypoolparty import sun_grid_engine

    sun_grid_engine.testing.init_queue_state(
        path="tests/resources/dummy_queue_state.json"
    )

When testing our ``map()`` you set its arguments ``qsub_path``, ``qdel_path``,
and ``qstat_path`` to point to the dummy queue.

See ``tests/test_full_chain_with_dummy_qsub.py``.

Because of the global state file, only one instance of dummy_queue must run
at a time.


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
