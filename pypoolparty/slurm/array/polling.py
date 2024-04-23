from ... import utils as general_utils


def init_negative_ones():
    p = {}
    p["len_tasks"] = -1
    p["returned"] = -1
    p["running"] = -1
    p["pending"] = -1
    p["error"] = -1
    p["exceptions"] = -1
    p["stderr"] = -1
    p["resubmissions"] = -1
    return p


def init(
    len_tasks, reducer=None, jobs=None, num_resubmissions_by_array_task_id=None
):
    p = init_negative_ones()
    p["len_tasks"] = len_tasks

    if reducer is None:
        assert jobs is None
        assert num_resubmissions_by_array_task_id is None
    else:
        p["returned"] = len(reducer.tasks_returned)
        p["running"] = len(jobs["running"])
        p["pending"] = len(jobs["pending"])
        p["error"] = len(jobs["error"])
        p["exceptions"] = len(reducer.tasks_exceptions)
        p["stderr"] = len(reducer.tasks_with_stderr)
        p["resubmissions"] = general_utils.dict_sum(
            num_resubmissions_by_array_task_id
        )
    return p


def is_eual(a, b):
    for key in a:
        if a[key] != b[key]:
            return False
    return True


def to_str(poll):
    msg = "{: 6d} of {:d} complete, ".format(
        poll["returned"], poll["len_tasks"]
    )
    msg += "{: 6d} running, {: 6d} pending, {: 6d} error, ".format(
        poll["running"], poll["pending"], poll["error"]
    )
    msg += "{: 6d} exceptions, {: 6d} stderr, ".format(
        poll["exceptions"],
        poll["stderr"],
    )
    msg += "{: 6d} resubmissions".format(poll["resubmissions"])
    return msg
