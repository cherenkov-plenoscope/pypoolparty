import os
import stat
import shutil
import time
import rename_after_writing
import pickle


def make_path_executable(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def default_python_path():
    return os.path.abspath(shutil.which("python"))


def session_id_from_time_now():
    # This must be a valid filename. No ':' for time.
    return time.strftime("%Y-%m-%dT%H-%M-%S", time.gmtime())


def read(path, mode="t"):
    with open(path, mode + "r") as f:
        content = f.read()
    return content


def write(path, content, mode="t"):
    with rename_after_writing.open(file=path, mode=mode + "w") as f:
        f.write(content)


def read_text(path):
    return read(path=path, mode="t")


def write_text(path, content):
    write(path=path, content=content, mode="t")


def read_pickle(path):
    return pickle.loads(read(path=path, mode="b"))


def write_pickle(path, content):
    write(path=path, content=pickle.dumps(content), mode="b")
