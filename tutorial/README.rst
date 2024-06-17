###################
Tutorial 2024-06-17
###################

The ``pypoolparty`` provides ``Pool.map`` which is a drop-in-replacement for
python's ``multiprocessing.Pool.map``.


*****************************
Minimal example for ``map()``
*****************************

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

To compute the results, one for each job, we can loop over the jobs

.. code:: python

    results = []
    for job in jobs:
        result = numpy.std(job)
        results.append(result)

While correct, this is computation can be done quicker in an easy way becasue the
individual processings of the jobs are independent of each other.
This is a so called `embarresingly simple` problem for parallelisation.

