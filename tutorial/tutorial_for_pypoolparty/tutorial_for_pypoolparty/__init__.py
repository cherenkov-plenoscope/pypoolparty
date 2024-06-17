from .version import __version__
import os
import glob
import json
import numpy as np


def init(work_dir, seed=815, number_runs=96, number_events_per_run=100):
    """
    make the work_dir and put some dummy runs with dummy events in it.
    """
    os.makedirs(work_dir, exist_ok=True)
    with open(os.path.join(work_dir, "config.json"), "wt") as fout:
        fout.write(
            json.dumps(
                {
                    "seed": seed,
                    "number_runs": number_runs,
                    "number_events_per_run": number_events_per_run,
                }
            )
        )

    prng = np.random.Generator(np.random.PCG64(seed))
    for run_id in range(number_runs):
        run_path = os.path.join(work_dir, "{:06d}.txt".format(run_id))
        with open(run_path, "wt") as fout:
            for event_id in range(number_events_per_run):
                number_items_in_event = prng.integers(low=5, high=25)
                items = prng.integers(
                    low=0, high=100, size=number_items_in_event
                )
                items_str = str.join(", ", [str(i) for i in items.tolist()])
                fout.write("{:06d}:".format(event_id))
                fout.write(items_str)
                fout.write("\n")


def run_full_analysis(work_dir, pool):
    """
    The full analysis might call pool.map() several times.
    """
    jobs = make_jobs(
        work_dir=work_dir,
        threshold_size=10,
        result_suffix="pass-10",
    )
    rc = pool.map(run_job, jobs)

    # second pass ...
    jobs = make_jobs(
        work_dir=work_dir,
        threshold_size=20,
        result_suffix="pass-20",
    )
    rc = pool.map(run_job, jobs)


def make_jobs(work_dir, threshold_size=10, result_suffix="pass1"):
    with open(os.path.join(work_dir, "config.json"), "rt") as fin:
        config = json.loads(fin.read())
    prng = np.random.Generator(np.random.PCG64(config["seed"]))

    jobs = []

    run_paths = glob.glob(os.path.join(work_dir, "*.txt"))
    for run_path in run_paths:
        # we pretend that some events are broken and need to be skipped
        num_broken_events = prng.integers(low=0, high=5)
        broken_event_ids = prng.integers(
            low=0,
            high=config["number_events_per_run"],
            size=num_broken_events,
        )

        job = {
            "work_dir": work_dir,
            "basename": os.path.basename(run_path),
            "broken_events_to_be_skipped": broken_event_ids,
            "threshold_size": threshold_size,
            "result_suffix": result_suffix,
        }
        jobs.append(job)
    return jobs


def very_intense_and_complex_compute_of_event(event, threshold_size):
    result = {}
    result["size"] = int(len(event["data"]))
    if result["size"] >= threshold_size:
        result["mean"] = float(np.mean(event["data"]))
        result["std"] = float(np.std(event["data"]))
    return result


class EventReader:
    def __init__(self, path):
        self.path = path
        self.stream = open(path, mode="rt")

    def __next__(self):
        line = self.stream.readline()
        if not line:
            raise StopIteration

        event_id_str, items_str = str.split(line, ":")

        item_strs = str.split(items_str, ",")
        data = [int(i) for i in item_strs]
        return {"event_id": int(event_id_str), "data": data}

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stream.close()

    def __repr__(self):
        return "{:s}(path={:s})".format(
            self.__class__.__name__, repr(self.path)
        )


def run_job(job):
    ipath = os.path.join(job["work_dir"], job["basename"])
    opath = ipath + ".{:s}.jsonl".format(job["result_suffix"])
    broken_events_to_be_skipped = set(job["broken_events_to_be_skipped"])

    number_events_processed = 0
    with EventReader(path=ipath) as run:
        with open(opath, "wt") as fout:
            for event in run:
                if event["event_id"] not in broken_events_to_be_skipped:
                    number_events_processed += 1
                    result = {}
                    result["event_id"] = event["event_id"]
                    compute = very_intense_and_complex_compute_of_event(
                        event=event,
                        threshold_size=job["threshold_size"],
                    )
                    result.update(compute)
                    out_line = json.dumps(result, indent=None)
                    fout.write(out_line)
                    fout.write("\n")
    return number_events_processed
