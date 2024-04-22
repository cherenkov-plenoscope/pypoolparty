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
    job_id_str.split("_")
    return (job_id_str[0], job_id_str[1])


def join_job_id_and_array_task_id(job_id, array_task_id):
    return str(int(job_id)) + "_" + str(int(array_task_id))
