import io


def make(func_module, func_name, work_dir, shebang=None):
    """
    Parameters
    ----------
    func_module : str
        The name of the python module containing the function to be executed.
    func_name : str
        The name of the function to be executed.
    shebang : str (optional)
        The first line string pointing to the executable for this script.
        Example: '#!/path/to/executable'
    """
    scr = io.StringIO()
    if shebang:
        scr.write(shebang + "\n")
    scr.write("# I was generated automatically by pypoolparty.slurm.array.\n")
    scr.write("# I will be executed on the worker nodes.\n")
    scr.write("import os\n")
    scr.write("import pickle\n")
    scr.write("import pypoolparty as ppp\n")
    scr.write("import {:s}\n".format(func_module))
    scr.write("\n")
    scr.write('work_dir = "{:s}"\n'.format(work_dir))
    scr.write("env = os.environ\n")
    scr.write('task_id = int(env["SLURM_ARRAY_TASK_ID"])\n')
    scr.write("task = ppp.slurm.array.mapping.read_task_from_work_dir(\n")
    scr.write("    work_dir=work_dir,\n")
    scr.write("    task_id=task_id,\n")
    scr.write(")\n")
    scr.write(
        "task_result = {func_module:s}.{func_name:s}(task)\n".format(
            func_module=func_module,
            func_name=func_name,
        )
    )
    scr.write("ppp.slurm.array.reducing.write_task_result(\n")
    scr.write('    path="{:d}.pickle.gz".format(task_id),\n')
    scr.write("    content=task_result,\n")
    scr.write('    mode="wb|gz",\n')
    scr.write(")\n")
    scr.seek(0)
    return scr.read()
