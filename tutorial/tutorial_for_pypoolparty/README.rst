#######################################
Muon ring demonstration for pypoolparty
#######################################
|BlackStyle| |BlackPackStyle| |MITLicenseBadge|


A demonstration for parallel compute. The objective is to parameterize
rings in many, independent images.

Installing
==========

.. code:: bash

    pip install -e tutorial_for_pypoolparty/


Basic Usage
===========

.. code:: python

    import tutorial_for_pypoolparty as tut

    tut.init(work_dir="crazy_muons")


This will make a directory with 'images' of so called muon events as they
are common on atmospheric Cherenkov telescopes. The special signature of
the images it a ring.

The images can be looked at in a text editor, for example:

.. code:: bash

    cd crazy_muons/
    less 000.image.txt

    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ..........................................##########............
    ........................................####......####..........
    ......................................####..........####........
    ......................................##..............##........
    ......................................##..............##........
    ......................................##..............##........
    ......................................####............##........
    ........................................####........####........
    ..........................................############..........
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................
    ................................................................



By means of parallel compute using a ''map()'' function from a parallel compute
pool.

To reconstruct an individual event call:

.. code:: python

    import tutorial_for_pypoolparty as tut

    recon = tut.reconstruct_event(work_dir="crazy_muons/", event_number=3)


To go parallel, there is ''tut.make_jobs()'' and ''tut.run_job()''.

.. code:: python

    import multiprocessing

    pool = multiprocessing.Pool(6)

    jobs = tut.make_jobs(work_dir="crazy_muons/")

    results = pool.map(tut.run_job, jobs)


The ''pool'' can be replaced by e.g. ''pypoolparty.slurm.array.Pool()''.
Finally, the confusion of the reconstruction (true vs. reco) can be printed:


.. code:: python

    tut.print_summary("crazy_muons/")
    x-position true vs. reco
    ------------------------
    ................................................
    ................................................
    ................................................
    ................................................
    ......................................##........
    ................................................
    ................................................
    ................................##..............
    ..............................##................
    ............................##..................
    ................................................
    ........................##......................
    ......................##........................
    ................................................
    ................................................
    ................................................
    ..............##................................
    ............##..................................
    ................................................
    ........##......................................
    ................................................
    ................................................
    ................................................
    ................................................

    y-position true vs. reco
    ------------------------
    ................................................
    ................................................
    ................................................
    ................................................
    ................................................
    ..................................##............
    ................................####............
    ................................................
    ............................####................
    ................................................
    ..........................##....................
    ......................##........................
    ................................................
    ..................##............................
    ................................................
    ..............##................................
    ............####................................
    ................................................
    ................................................
    ........##......................................
    ................................................
    ................................................
    ................................................
    ................................................

    radius true vs. reco
    ------------------------
    ..............................
    ..............................
    ........................##....
    ..............................
    ..................##..........
    ..............................
    ................##............
    ..............................
    ............##................
    ..........##..................
    ..............................
    ....##........................
    ..............................
    ##............................
    ##............................



.. |BlackStyle| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. |BlackPackStyle| image:: https://img.shields.io/badge/pack%20style-black-000000.svg
    :target: https://github.com/cherenkov-plenoscope/black_pack

.. |MITLicenseBadge| image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: https://opensource.org/licenses/MIT

