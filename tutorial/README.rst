###################
Tutorial 2024-06-17
###################

The ``pypoolparty`` provides ``Pool.map`` which is a drop-in-replacement for
python's ``multiprocessing.Pool.map``.


********************************
An embarrassingly simple Problem
********************************

For example, we want to compute the standard deviation of multiple arrays.
We can list the arrays into ``jobs``.

.. code:: python

    jobs = [
        [3,4,56,7],
        [22,5,4,5,7],
        [11,2,545,235,6,3],
        [22,45,6,4]
    ]

To compute the standard deviation, we use ``numpy.std()``

.. code:: python

    import numpy

    numpy.std(jobs[0])

    np.float64(22.276669409945463)

To compute the results, one result for each job, we can loop over the jobs

.. code:: python

    results = []
    for job in jobs:
        result = numpy.std(job)
        results.append(result)

While correct, this computation can be done quicker in an easy way becasue the
individual processings of the jobs are independent of each other.
This is a so called `embarrassingly simple` problem in parallelisation.

************************
Python's multiprocessing
************************

One way of computing the results in parallel is to use pythons builtin multiprocessing
library.

.. code:: python

    import multiprocessing

    pool = multiprocessing.Pool(3)

This creates a compute ``pool`` which will run in up to ``3`` threads in parallel.
The ``pool`` got a ``map`` function which takes two arguments.

.. code:: python

    results_using_multiprocessing = pool.map(numpy.std, jobs)

This simple call will yield the exact same results as the loop in a single thread.

.. code:: python
    
    assert results == results_using_multiprocessing

While the call looks simple, ``multiprocessing.Pool.map`` is actually rather advanced
and smart. It has smart scheduler which assigns the individual jobs to different threads
in order to minimize the ideling of threads.

****************
Smart scheduling
****************

A smart assignment of jobs to threads is important when the jobs have different compute
times or when threads have different compute speeds. A non trivial scheduler will split
jobs into bunches of equal size and assign them to the threads right at the beginning of
the ``map()`` call. But this can lead to poor performance.

.. code::

    jobs: |.1.|..2..|.3.|...4...|......5......|...6...|..7..|...8...|

    thread 1: |.1.|..2..|.3.|...4...|
    thread 2: |......5......|...6...|..7..|...8...|

A smart scheduler submits individual jobs to the threads and assigns the next one when the
thread is done with the former.

.. code::

    jobs: |.1.|..2..|.3.|...4...|......5......|...6...|..7..|...8...|

    thread 1: |.1.|.3.|......5......|..7..|
    thread 2: |..2..|...4...|...6...|...8...|


***********
Pypoolparty
***********

The multiprocessing pool is great but it can only use the threads of the local machine.
To use the compute threads many machines in parallel, we have distributed computing with
tools such as the ``sun-grid-engine`` or ``SLURM``.

The ``pypoolparty`` allows us to make use of this with the same interface we are used from
``multiprocessing.Pool``.

.. code:: python

    import pypoolparty

    slurm_pool = pypoolparty.slurm.array.Pool(verbose=True)

    results_using_pypoolparty = slurm_pool.map(numpy.std, jobs)

Again, this will satisfy:

.. code:: python
    
    assert results == results_using_pypoolparty


*********************
What do we got so far
*********************

This allows you to write your simulations/analysis independent of the compute hardware.
You do not need to mix the physics code with the bookkeeping code for distributed or local
parallel computing.

.. code:: python

    import very_important_simulations
    import pypoolparty

    pool = pypoolparty.slurm.array.Pool()

    very_important_simulations.do_horrid_things_with_numbers(pool=pool)

In the example above, ``pool`` could either be from ``pypoolparty`` or from ``multiprocessing``.








