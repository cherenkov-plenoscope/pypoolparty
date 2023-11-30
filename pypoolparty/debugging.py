from . import utils
import glob
import os


def list_ids(work_dir, wildcard, num_digits_jobid=9):
    paths = glob.glob(os.path.join(work_dir, wildcard))
    basenames = [os.path.basename(path) for path in paths]
    list_ids = [int(basename[0:num_digits_jobid]) for basename in basenames]
    return list_ids


def make_summary(work_dir, max_osize=1000 * 1000, max_esize=1000 * 1000):
    col = {}
    print("make list started")
    col["started"] = set(list_ids(work_dir, "*.pkl"))
    print("make list completed")
    col["completed"] = set(list_ids(work_dir, "*.pkl.out"))
    col["incomplete"] = col["started"].difference(col["completed"])
    col["stdout"] = {}
    col["stderr"] = {}
    print("read stdout and stderr")

    for c, jid in enumerate(col["started"]):
        if c % (79 - 13) == 0:
            print("{:6d}/{:6d}".format(c + 1, len(col["started"])))
        print(".", end="", flush=True)
        try:
            opath = os.path.join(work_dir, "{:09d}.pkl.o".format(jid))
            epath = os.path.join(work_dir, "{:09d}.pkl.e".format(jid))
            osize = os.stat(opath).st_size
            osize_read = min([osize, max_osize])
            with open(opath, "rb") as f:
                col["stdout"][jid] = f.read(osize_read)
            esize = os.stat(epath).st_size
            esize_read = min([esize, max_esize])
            with open(epath, "rb") as f:
                col["stderr"][jid] = f.read(esize_read)
        except Exception as error:
            print("Failed to read stdout and stderr")
            print(error)

    print("read jobs of 'incomplete'")
    col["incomplete_jobs"] = {}

    for jid in col["incomplete"]:
        try:
            jpath = os.path.join(work_dir, "{:09d}.pkl".format(jid))
            job = utils.read_pickle(path=jpath)
            col["incomplete_jobs"][jid] = job
        except Exception as error:
            print("Failed to read incomplete job's *.pkl.")
            print(error)

    return col
