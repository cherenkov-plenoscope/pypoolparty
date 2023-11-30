import pypoolparty as ppp
import pytest


def _flatten(chunks):
    flat = []
    for b in chunks:
        flat += b
    return flat


def test_zero_tasks_and_valid_num_chunks():
    for num_chunks in [None, 1, 100]:
        chunks = ppp.chunking.assign_tasks_to_chunks(
            num_tasks=0, num_chunks=num_chunks
        )
        assert len(chunks) == 0


def test_zero_tasks_and_bad_num_chunks():
    with pytest.raises(AssertionError):
        chunks = ppp.chunking.assign_tasks_to_chunks(num_tasks=0, num_chunks=0)

    with pytest.raises(AssertionError):
        chunks = ppp.chunking.assign_tasks_to_chunks(
            num_tasks=0, num_chunks=-1
        )


def test_many_tasks_one_bundle():
    chunks = ppp.chunking.assign_tasks_to_chunks(num_tasks=100, num_chunks=1)
    assert len(chunks) == 1

    itasks = _flatten(chunks)

    for j in range(100):
        assert itasks[j] == j


def test_many_tasks_many_chunks():
    chunks = ppp.chunking.assign_tasks_to_chunks(num_tasks=1000, num_chunks=10)
    assert len(chunks) == 10

    itasks = _flatten(chunks)

    for j in range(1000):
        assert itasks[j] == j


def test_few_tasks_many_chunks():
    chunks = ppp.chunking.assign_tasks_to_chunks(num_tasks=10, num_chunks=100)
    assert len(chunks) == 10

    itasks = _flatten(chunks)

    for j in range(10):
        assert itasks[j] == j
