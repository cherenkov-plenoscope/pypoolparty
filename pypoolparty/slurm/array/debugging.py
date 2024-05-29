import zipfile
import os
import glob
import pickle
import json_utils


class Debugging:
    def __init__(self, work_dir):
        opj = os.path.join
        self.work_dir = work_dir

        self.stderr = read_items(
            path=opj(work_dir, "tasks.stderr.zip"),
            pattern=".stderr",
        )
        self.stdout = read_items(
            path=opj(work_dir, "tasks.stdout.zip"),
            pattern=".stdout",
        )
        self.exceptions = read_items(
            path=opj(work_dir, "tasks.exceptions.zip"),
            pattern=".exception",
        )
        with open(opj(work_dir, "scripy.py"), "rt") as f:
            self.script = f.read()

        self.log = []
        with json_utils.lines.open(opj(work_dir, "log.jsonl"), "r") as jlin:
            for log_item in jlin:
                self.log.append(log_item)

        self.tasks = read_items(
            path=opj(work_dir, "tasks.zip"),
            pattern=".pickle",
        )
        for i in self.tasks:
            self.tasks[i] = pickle.loads(self.tasks[i])

        self.results = read_items(
            path=opj(work_dir, "tasks.results.zip"),
            pattern=".pickle",
        )
        for i in self.results:
            self.results[i] = pickle.loads(self.results[i])


def read_items(path, pattern):
    out = {}
    with zipfile.ZipFile(path, "r") as zin:
        for fileitem in zin.filelist:
            if pattern in fileitem.filename:
                nnn = str.replace(fileitem.filename, pattern, "")
                nnn = int(nnn)
                out[nnn] = zin.read(name=fileitem.filename)
    return out
