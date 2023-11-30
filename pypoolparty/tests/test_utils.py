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
