import pypoolparty


def test_split_job_id_and_array_task_id():
    job_id_str = "123_45"
    (
        job_id,
        array_task_id,
    ) = pypoolparty.slurm.array.utils.split_job_id_and_array_task_id(
        job_id_str=job_id_str
    )
    assert job_id == "123"
    assert array_task_id == "45"


def test_join_job_id_and_array_task_id():
    job_id_str = pypoolparty.slurm.array.utils.join_job_id_and_array_task_id(
        job_id="123", array_task_id="45"
    )
    assert job_id_str == "123_45"


def test_replace_array_task_id_format_with_integer_format():
    fmt = pypoolparty.slurm.array.utils.replace_array_task_id_format_with_integer_format(
        fmt="/some/abs/path/prefix-%a.suffix"
    )
    assert fmt == "/some/abs/path/prefix-{:d}.suffix"
