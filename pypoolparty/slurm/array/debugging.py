import zipfile
import os
import glob
import pickle
import difflib
import json_utils
import io

from . import reducing


class Debugging:
    def __init__(self, work_dir):
        opj = os.path.join
        self.work_dir = work_dir

        self.stderr = read_items(
            path=path_fallback(opj(work_dir, "tasks.stderr.zip"), ".part"),
            pattern=".stderr",
        )
        self.stdout = read_items(
            path=path_fallback(opj(work_dir, "tasks.stdout.zip"), ".part"),
            pattern=".stdout",
        )
        self.exceptions = read_items(
            path=path_fallback(opj(work_dir, "tasks.exceptions.zip"), ".part"),
            pattern=".exception",
        )
        with open(opj(work_dir, "script.py"), "rt") as f:
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
            path=path_fallback(opj(work_dir, "tasks.results.zip"), ".part"),
            pattern=".pickle",
        )
        for i in self.results:
            self.results[i] = pickle.loads(self.results[i])

        # determine the completion status
        self.completion = {}
        for task_id in range(len(self.tasks)):
            self.completion[task_id] = "unknown"

        for task_id in self.results:
            if task_id in self.stderr and task_id in self.stderr:
                self.completion[task_id] = "complete"
            else:
                self.completion[task_id] = "incomplete"

        left_over_stdout = read_glob(os.path.join(work_dir, "*.stdout"))
        for task_id in left_over_stdout:
            self.completion[task_id] = "incomplete"
        self.stdout.update(left_over_stdout)

        left_over_stderr = read_glob(os.path.join(work_dir, "*.stderr"))
        for task_id in left_over_stderr:
            self.completion[task_id] = "incomplete"
        self.stderr.update(left_over_stderr)

        left_over_results = read_glob(os.path.join(work_dir, "*.pickle"))
        for task_id in left_over_results:
            self.completion[task_id] = "incomplete"
        self.results.update(left_over_results)

        left_over_exceptions = read_glob(os.path.join(work_dir, "*.exception"))
        for task_id in left_over_exceptions:
            self.completion[task_id] = "incomplete"
        self.exceptions.update(left_over_exceptions)

    def has_non_zero_stdout(self):
        out = set()
        for task_id in self.stdout:
            if len(self.stdout[task_id]) > 0:
                out.add(task_id)
        return out

    def has_non_zero_stderr(self):
        out = set()
        for task_id in self.stderr:
            if len(self.stderr[task_id]) > 0:
                out.add(task_id)
        return out

    def has_exceptions(self):
        out = set()
        for task_id in self.exceptions:
            out.add(task_id)
        return out

    def is_not_complete(self):
        out = set()
        for task_id in self.completion:
            if self.completion[task_id] != "complete":
                out.add(task_id)
        return out


def path_fallback(path, extention):
    if os.path.exists(path):
        return path
    else:
        return path + extention


def read_items(path, pattern):
    out = {}
    with zipfile.ZipFile(path, "r") as zin:
        for fileitem in zin.filelist:
            if pattern in fileitem.filename:
                nnn = str.replace(fileitem.filename, pattern, "")
                nnn = int(nnn)
                out[nnn] = zin.read(name=fileitem.filename)
    return out


def read_glob(path):
    out = {}
    paths = glob.glob(path)
    for ppp in paths:
        try:
            task_id = reducing.get_task_id_from_basename(os.path.basename(ppp))
            with open(ppp, "rb") as fin:
                out[task_id] = fin.read()
        except Exception as err:
            print(str(err))
    return out


def histogram_loglines(logtexts, match_ratio_threshold=0.8):
    out = {}

    for task_id in logtexts:
        logtext = bytes.decode(logtexts[task_id])
        for line in str.splitlines(logtext):
            if len(out) == 0:
                out[line] = {}
                out[line][task_id] = 1
            else:
                known_lines = list(out.keys())
                line_matches = False
                for known_line in known_lines:
                    s = str_similarity(line, known_line)
                    if s >= match_ratio_threshold:
                        if task_id in out[known_line]:
                            out[known_line][task_id] += 1
                        else:
                            out[known_line][task_id] = 1
                        line_matches = True
                        break

                if not line_matches:
                    out[line] = {}
                    out[line][task_id] = 1
    return out


def histogram_loglines_intensity(hist):
    keys = []
    inte = []
    for line in hist:
        keys.append(line)
        inte.append(len(hist[line]))

    okeys = []
    ointe = []
    for a in reversed(argsort(inte)):
        okeys.append(keys[a])
        ointe.append(inte[a])
    return okeys, ointe


def histogram_loglines_print(hist):
    lines, intensity = histogram_loglines_intensity(hist=hist)

    o = io.StringIO()
    for i in range(len(lines)):
        o.write("{:d}: {:d}\n".format(i + 1, intensity[i]))
        o.write("    '{:s}'\n".format(lines[i]))
        task_ids = list(hist[lines[i]].keys())
        task_ids = sorted(task_ids)
        o.write("    [")
        fill = 0
        for task_id in task_ids:
            fill += o.write("{:d},".format(task_id))
            if fill > 75:
                o.write("\n")
                o.write("    ")
                fill = 0
        o.write("]\n")

    o.seek(0)
    return o.read()


def str_similarity(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()


def argsort(seq):
    return sorted(range(len(seq)), key=seq.__getitem__)
