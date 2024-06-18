import pypoolparty as ppp
import tempfile
import os


def test_read_write_text():
    with tempfile.TemporaryDirectory(prefix="pypoolparty") as tmp:
        tmp_path = os.path.join(tmp, "stuff.txt")
        content = "my text\ngoes in here."
        ppp.utils.write_text(path=tmp_path, content=content)
        content_back = ppp.utils.read_text(path=tmp_path)
        assert content_back == content


def test_read_write_pickle():
    with tempfile.TemporaryDirectory(prefix="pypoolparty") as tmp:
        tmp_path = os.path.join(tmp, "stuff.txt")
        content = {"a_pytho_object": 1337, "more": ["1", "a", "?"]}
        ppp.utils.write_pickle(path=tmp_path, content=content)
        content_back = ppp.utils.read_pickle(path=tmp_path)
        assert content == content_back


def test_int_ceil_division():
    assert 2 == ppp.utils.int_ceil_division(10, 5)
    assert 3 == ppp.utils.int_ceil_division(10, 4)
    assert 4 == ppp.utils.int_ceil_division(10, 3)
    assert 5 == ppp.utils.int_ceil_division(10, 2)
