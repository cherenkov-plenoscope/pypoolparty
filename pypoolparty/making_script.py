import io


def make(func_module, func_name, environ, shebang=None):
    """
    Returns a string that is a python-script.
    This python-script will be executed on the worker-node.
    In here, the environment variables are set explicitly.
    It reads the chunk of tasks, runs result = func(task), and writes the
    results. The script is called on the worker-node with a single argument:

    python worker_node_script.py /path/to/work_dir/{ichunk:09d}.pkl

    Parameters
    ----------
    func_module : str
        The name of the python module containing the function to be executed.
    func_name : str
        The name of the function to be executed.
    environ : dict
        The envirionment variables to be set when the script is executed on the
        worker node.
    shebang : str (optional)
        The first line string pointing to the executable for this script.
        Example: '#!/path/to/executable'
    """
    scr = io.StringIO()
    if shebang:
        scr.write(shebang + "\n")
    scr.write("# I was generated automatically by pypoolparty.\n")
    scr.write("# I will be executed on the worker-nodes.\n")
    scr.write("import os\n")
    scr.write("import sys\n")
    scr.write("import pickle\n")
    scr.write("import pypoolparty as ppp\n")
    scr.write("import {:s}\n".format(func_module))
    scr.write("\n")
    scr.write(make_os_environ_string(environ=environ))
    scr.write("\n")
    scr.write("assert(len(sys.argv) == 2)\n")
    scr.write("chunk = ppp.utils.read_pickle(path=sys.argv[1])\n")
    scr.write("task_results = []\n")
    scr.write("for j, task in enumerate(chunk):\n")
    scr.write("    try:\n")
    scr.write(
        "        task_result = {func_module:s}.{func_name:s}(task)\n".format(
            func_module=func_module,
            func_name=func_name,
        )
    )
    scr.write("    except Exception as bad:\n")
    scr.write('        print("[task ", j, ", in chunk]", file=sys.stderr)\n')
    scr.write("        print(bad, file=sys.stderr)\n")
    scr.write("        task_result = None\n")
    scr.write("    task_results.append(task_result)\n")
    scr.write("\n")
    scr.write("ppp.utils.write_pickle(\n")
    scr.write('    path=sys.argv[1]+".out",\n')
    scr.write("    content=task_results,\n")
    scr.write(")\n")
    scr.seek(0)
    return scr.read()


def make_os_environ_string(environ):
    """
    Returns a python-code string which sets the environment variables in
    'environ'.

    Parameters
    ----------
    environ : dict
        The envirionment variables to be set when the script is executed on the
        worker node.
    """
    env = io.StringIO()
    for key in environ:
        value = environ[key]
        keydec = key.encode("unicode_escape").decode()

        if '"' in keydec:
            start = "os.environ['{key:s}']".format(key=keydec)
        else:
            start = 'os.environ["{key:s}"]'.format(key=keydec)

        iiivalue = [int(b) for b in bytes(value, "utf8")]
        stop = "str(bytes(" + str(iiivalue) + "))" + "\n"
        line = start + " = " + stop
        env.write(line)
    env.seek(0)
    return env.read()
