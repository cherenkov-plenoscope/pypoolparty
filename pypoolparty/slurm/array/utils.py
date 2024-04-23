def replace_array_task_id_format_with_integer_format(
    fmt,
    slurm_array_task_id_format="%a",
    python_integer_format="{:d}",
):
    return fmt.replace(
        slurm_array_task_id_format,
        python_integer_format,
    )


def split_job_id_and_array_task_id(job_id_str):
    tokens = job_id_str.split("_")
    return (tokens[0], tokens[1])


def join_job_id_and_array_task_id(job_id, array_task_id):
    return str(int(job_id)) + "_" + str(int(array_task_id))


def array_task_shall_be_resubmitted(
    array_task_id,
    num_resubmissions_by_array_task_id,
    max_num_resubmissions,
):
    resubmit = True
    if max_num_resubmissions is not None:
        if array_task_id in num_resubmissions_by_array_task_id:
            if (
                num_resubmissions_by_array_task_id[array_task_id]
                >= max_num_resubmissions
            ):
                resubmit = False
    return resubmit
