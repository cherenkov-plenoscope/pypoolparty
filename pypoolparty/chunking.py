import math


def assign_tasks_to_chunks(num_tasks, num_chunks):
    """
    When you have too many tasks for your parallel processing queue this
    function chunks multiple tasks into fewer chunks.

    Parameters
    ----------
    num_tasks : int
        Number of tasks.
    num_chunks : int (optional)
        The maximum number of chunks. Your tasks will be spread over
        these many chunks. If None, each chunk contains a single task.

    Returns
    -------
        A list of chunks where each chunk is a list of task-indices `itask`.
        The lengths of the list of chunks is <= num_chunks.
    """
    if num_chunks is None:
        num_tasks_in_chunk = 1
    else:
        assert num_chunks > 0
        num_tasks_in_chunk = int(math.ceil(num_tasks / num_chunks))

    chunks = []
    current_chunk = []
    for j in range(num_tasks):
        if len(current_chunk) < num_tasks_in_chunk:
            current_chunk.append(j)
        else:
            chunks.append(current_chunk)
            current_chunk = []
            current_chunk.append(j)
    if len(current_chunk):
        chunks.append(current_chunk)
    return chunks
