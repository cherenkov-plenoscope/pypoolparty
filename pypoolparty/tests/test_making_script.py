import pypoolparty as ppp
import tempfile
import os
import subprocess
import numpy


def test_make_worker_node_script():
    with tempfile.TemporaryDirectory(prefix="pypoolparty") as tmp:
        bundle = [numpy.arange(100)]
        func = numpy.sum
        ppp.utils.write_pickle(
            path=os.path.join(tmp, "bundle.pkl"),
            content=bundle,
        )
        script_str = ppp.making_script.make(
            func_module=func.__module__,
            func_name=func.__name__,
            environ={},
            shebang=None,
        )
        ppp.utils.write_text(
            path=os.path.join(tmp, "worker_node_script.py"),
            content=script_str,
        )
        rc = subprocess.call(
            [
                "python",
                os.path.join(tmp, "worker_node_script.py"),
                os.path.join(tmp, "bundle.pkl"),
            ]
        )
        assert rc == 0
        assert os.path.exists(os.path.join(tmp, "bundle.pkl") + ".out")
        result = ppp.utils.read_pickle(
            path=os.path.join(tmp, "bundle.pkl.out")
        )
        assert result == func(bundle[0])


def test_make_environ_str():
    s = ppp.making_script.make_os_environ_string(environ={"a": "b"})
    assert s == 'os.environ["a"] = "b"\n'

    s = ppp.making_script.make_os_environ_string(environ={"a'b": "b"})
    assert s == 'os.environ["a\'b"] = "b"\n'

    s = ppp.making_script.make_os_environ_string(environ={'a"b': "b"})
    assert s == 'os.environ[\'a"b\'] = "b"\n'

    s = ppp.making_script.make_os_environ_string(environ={'a"b': "b'c"})
    assert s == "os.environ['a\"b'] = \"b'c\"\n"
