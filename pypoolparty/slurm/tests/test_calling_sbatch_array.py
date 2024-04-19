import pypoolparty
import pytest


def test_list_mode_normal():
    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=None,
        stop_task_id=None,
        task_ids=[1, 2, 3, 4],
        num_simultaneously_running_tasks=None,
    )
    assert task_id_str == "1,2,3,4"

    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=None,
        stop_task_id=None,
        task_ids=[10, 100, 1, 0],
        num_simultaneously_running_tasks=None,
    )
    assert task_id_str == "10,100,1,0"


def test_list_mode_normal_str():
    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=None,
        stop_task_id=None,
        task_ids=["1", "2", "3", "4"],
        num_simultaneously_running_tasks=None,
    )
    assert task_id_str == "1,2,3,4"

    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=None,
        stop_task_id=None,
        task_ids=["10", "100", "1", "0"],
        num_simultaneously_running_tasks=None,
    )
    assert task_id_str == "10,100,1,0"


def test_list_mode_normal_with_num_simultaneously_running():
    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=None,
        stop_task_id=None,
        task_ids=[1, 2, 3, 4],
        num_simultaneously_running_tasks=33,
    )
    assert task_id_str == "1,2,3,4%33"


def test_list_mode_empty_list():
    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=None,
            stop_task_id=None,
            task_ids=[],
            num_simultaneously_running_tasks=None,
        )


def test_list_mode_negative_task_id():
    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=None,
            stop_task_id=None,
            task_ids=[0, 1, 2, -500],
            num_simultaneously_running_tasks=None,
        )


def test_range_mode_normal():
    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=0,
        stop_task_id=100,
        task_ids=None,
        num_simultaneously_running_tasks=None,
    )
    assert task_id_str == "0-100"

    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=412,
        stop_task_id=567,
        task_ids=None,
        num_simultaneously_running_tasks=None,
    )
    assert task_id_str == "412-567"


def test_range_mode_normal_with_num_simultaneously_running():
    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=0,
        stop_task_id=100,
        task_ids=None,
        num_simultaneously_running_tasks=12,
    )
    assert task_id_str == "0-100%12"


def test_range_mode_equal():
    task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
        start_task_id=1,
        stop_task_id=1,
        task_ids=None,
        num_simultaneously_running_tasks=None,
    )
    assert task_id_str == "1-1"


def test_range_mode_start_gt_stop():
    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=10,
            stop_task_id=1,
            task_ids=None,
            num_simultaneously_running_tasks=None,
        )


def test_range_mode_negative():
    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=1,
            stop_task_id=-6,
            task_ids=None,
            num_simultaneously_running_tasks=None,
        )

    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=-5,
            stop_task_id=100,
            task_ids=None,
            num_simultaneously_running_tasks=None,
        )


def test_bad_combinations():
    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=None,
            stop_task_id=None,
            task_ids=None,
            num_simultaneously_running_tasks=None,
        )

    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=1,
            stop_task_id=None,
            task_ids=None,
            num_simultaneously_running_tasks=None,
        )

    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=None,
            stop_task_id=None,
            task_ids=None,
            num_simultaneously_running_tasks=10,
        )

    with pytest.raises(AssertionError):
        task_id_str = pypoolparty.slurm.calling._make_sbatch_array_task_id_str(
            start_task_id=1,
            stop_task_id=10,
            task_ids=[1, 2, 3],
            num_simultaneously_running_tasks=10,
        )


def test_parsing_sbatch_array_id_str_mode_range():
    o = pypoolparty.slurm.calling._parse_sbatch_array_task_id_str("1-10")
    assert o["mode"] == "range"
    assert o["start_task_id"] == 1
    assert o["stop_task_id"] == 10
    assert "num_simultaneously_running_tasks" not in o


def test_parsing_sbatch_array_id_str_mode_range_num_simul():
    o = pypoolparty.slurm.calling._parse_sbatch_array_task_id_str("1-10%3")
    assert o["mode"] == "range"
    assert o["start_task_id"] == 1
    assert o["stop_task_id"] == 10
    assert o["num_simultaneously_running_tasks"] == 3


def test_parsing_sbatch_array_id_str_mode_list():
    o = pypoolparty.slurm.calling._parse_sbatch_array_task_id_str(
        "1,3,4,10%12"
    )
    assert o["mode"] == "list"
    assert o["num_simultaneously_running_tasks"] == 12
    assert o["task_ids"] == [1, 3, 4, 10]
