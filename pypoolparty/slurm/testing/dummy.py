"""
def _job_init_default(python_path):
    return {
        "STATE": "PENDING",
        "REASON": "foobar",
        "PRIORITY": "0.999",
        "_python_path": python_path,
    }


def _job_update_script_args(job, args):
    for ii, script_arg in enumerate(args.script_args):
        job["_script_arg_{:d}".format(ii)] = script_arg
    return job



def sbatch(
    queue_state,
    name,
    script_args,
    opath,
    epath,
    python_path,
):
    assert len(script_args) == 2
    job = _job_init_default(python_path=python_path)
    job["JOBID"] = jobid
    job["NAME"] = args.job_name
    job["_opath"] = args.output
    job["_epath"] = args.error
    _job_update_script_args(job=job, args=args)
    queue_state["pending"].append(job)
    return queue_state


def sbatch_array(

):
"""
