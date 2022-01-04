#!/usr/bin/env python
"""Stau job executor

This module contains all the required functions for the job executor/worker. The executor is the
process that listens for work on the queue and is able to execute it locally. It also handles
dependency resolving and general queue maintenance.

The module is designed as a continuously running process (the main task on each worker) that executes
jobs on a separate process. The executor provides the two endpoints /_status and /readiness to allow
kubernetes information about the executor health.
"""

# Core libs
import importlib
import time
from datetime import datetime, timedelta
import os
import math
import random
import itertools
from threading import Event, Thread
import traceback
import logging as lg

# Memory debugging libraries
import gc

# HTTP libs
import urllib
from http.server import BaseHTTPRequestHandler
import socketserver

# DB libs
import mysql.connector

# Parallel execution libs
from concurrent.futures import ProcessPoolExecutor

# Stau queue libs
from stau_db_interface import StauQueue as Stau, range_to_work_ids, get_mysql_config
from stau_schedule_utils import StauPeriodicJobsScheduler
from stau_utils import ModuleLoader

# Graph libs
import networkx as nx

# Setup local logger
LOG = lg.getLogger("stau")
# LOG.setLevel("DEBUG")

# The maximum number of allowed recursions before
# raising an error
MAX_RECURSION_LEVEL = 10
# Seconds between each scan of the queue for work
QUEUE_SCAN_FREQUENCY = 15
# Seconds until we alert in #data-science-alerts that the job has been running for too long
MAX_RUNTIME_BEFORE_ALERT = 60 * 60
sleep_between_queue_scan = Event()


class JobCanceledInterrupt(Exception):
    """A exception to use if the current job was canceled"""

    pass


def get_git_commit_sha(short=True):
    """Read the current (short-hand) git commit sha from file written during Docker build stage

    Args:
        short (bool): If True, only returns the first seven characters of the SHA
    """
    git_commit_file_name = "/app/.git_commit_sha"

    try:
        with open(git_commit_file_name) as git_commit_file:
            git_commit_sha = git_commit_file.read().strip()

        if short:
            git_commit_sha = git_commit_sha[:7]
    except FileNotFoundError:
        git_commit_sha = None

    return git_commit_sha


def get_latest_stau_execution(
    job_type, as_epoch=False, time_column="execEnd", before=None
):
    """Get the latest execution time for a Stau job type

    Args:
        job_type (str): Name of the job (normally the NAME parameter from the script)
        db_conn
        as_epoch (bool): Return elapsed seconds since Jan 1th 1970
        time_column (str): Which time column from StauQueue to use. Default: execEnd
        before (datetime.datetime): Only look for jobs executed before this datetime. Default: latest job

    Notes:
        RuntimeError is raised by design as the function oterwhise has no meaning if the user requests a report that
        doesn't exist in stau. The alternative would be to return a very large number (since the report has not executed
        and we don't know if it will) but that would not catch typos (e.g. "report_initions") and is probably more
        dangerous.

    Returns:
        execEnd (datetime.datetime or int): Time when the Stau master job finished executing

    Raises:
        RuntimeError if no executions found for input job_type
    """
    where_before = ""
    if before is not None:
        where_before = f"AND {time_column} < '{before.replace(microsecond=0)}'"

    sql = f"""
    SELECT UNIX_TIMESTAMP({time_column}) AS execTime
    FROM StauQueue
    WHERE jobType = "{job_type}"
    AND masterJobId IS NULL  -- Only find master jobs
    AND status = 'DONE'      -- Ignore running or canceled jobs
    { where_before }
    ORDER BY {time_column} DESC    -- Latest job first
    LIMIT 1
    """
    with Stau() as queue:
        df_latest_exec = queue._exec(sql, {})

    # df_latest_exec = dblib.read_sql(sql, dblib.connection_qual_string)

    if df_latest_exec.empty:
        raise RuntimeError(f"No executions found for job type {job_type}")

    # A single row and column is returned. Get the nested value.
    exec_end = df_latest_exec.values[0][0]
    if not as_epoch:
        exec_end = datetime.datetime.utcfromtimestamp(exec_end)
    return exec_end


def lookup_last_report_execution(job_type, work_ids=None):
    """Lookup in the database when the report/job chunk last executed

    This is the expected table schema from the database (id and timestamp columns
    are omitted),

    ---------------------------------------------------
    |  work_id   |  history                           |
    ---------------------------------------------------
    |  1000      | {"report_A": 2019-01-11 11:22:33,
                    "report_B": 2020-01-12 02:03:44}  |
    |  2000      | {"report_A": 2012-01-11 12:23:33}  |
    ---------------------------------------------------

    The work_id parameter is expected to be work ids. The reason for naming the
    parameter work_ids is to support future changes.

    Args:
        job_type (str): The name of the job to check execution time for
        work_ids (list): Specific work ids to check execution time for

    Returns:
        last_exec_min (int or None): Largest number of minutes since the last
                                     execution for any of the work ids. None
                                     if never executed

    Examples:
        Looking up the greatest time since work id 1000 executed report_B
        should be 2 minutes

        >>> str(datetime.utcnow())
            2020-01-12 02:05:44
        >>> lookup_last_report_execution("report_B", [1000])
            2

        Looking up the greatest time since work id 1234 executed report_B
        should be None, as it was never executed

        >>> print(lookup_last_report_execution("report_B", [1234]))
            None
    """
    # Create string ready for SQL
    work_ids_string = ", ".join([str(c) for c in work_ids])

    # Query database
    # This returns a single number that is the latest execution for any of
    # the work_ids in minutes or a single row containing 99999999
    sql = f"""
    SELECT
        MAX(IFNULL(MINUTES_SINCE_LAST_EXEC, 99999999)) AS last_exec
    FROM (
      -- Calculate the time since last execution
      SELECT
          TIMESTAMPDIFF(
              MINUTE,
              STR_TO_DATE(
                  JSON_UNQUOTE(
                      JSON_EXTRACT(
                          history,
                          '$."{job_type}"')
               ), "%Y-%m-%d %H:%i:%s"),
               CURRENT_TIMESTAMP()
        ) AS MINUTES_SINCE_LAST_EXEC
        FROM StauLatestExecution
        WHERE workId IN ({work_ids_string})
    ) as subq
    """
    with Stau() as queue:
        rtn = queue._exec(sql, {})
    return rtn.get("last_exec", None)


def exception_to_string(raised_exception):
    """Reformat an exception to string

    The default __repr__ for exceptions strips away the traceback information. This
    is fine for logging as it accepts the exc_info flag. Normal formatting `str(exception)`
    doesn't support that. This formatter ensures the full stacktrace is returned an not just
    the exception text.

    Args:
        raised_exception (Exception): Exception to format

    Returns:
        message (str): Representation of the exception as a string with traceback

    Examples:
        >>> exception_to_string(RuntimeError("Well this happend"))

            File "somefile.py", line 1443, in test_exception_to_string
                raise RuntimeError("Well, this happend")
    """
    try:
        msg = f"{type(raised_exception)}: {raised_exception}\n\n"
    except AttributeError:
        msg = ""

    try:
        msg += "\n".join(traceback.format_tb(raised_exception.__traceback__))
    except Exception:
        # Fallback to default formatting if traceback isn't available
        msg += str(raised_exception)
    return msg


def work_ids_to_range(work_ids):
    """Convert a list of work ids to ranges

    This function takes a list of work ids (ints) and converts any ranges
    within the list to tuples. This is a more compressed way to store a lot
    of ids in the database.

    Args:
        work_ids (list): List of work ids

    Returns:
        work_range (iterator): Tuple of range pairs from the list

    Examples:
        > list(work_ids_to_range([0, 1, 2, 3, 4, 7, 8, 9, 11]))
          [(0, 4), (7, 9), (11,)]
    """
    unique_ids = sorted(list(set(work_ids)))

    # The groupby below matches consecutive integers that are only a single whole number apart into the
    # same group. Once the next and previous integer are more than 1 apart a new group is formed.
    for a, b in itertools.groupby(
        enumerate(unique_ids), key=(lambda pair: pair[1] - pair[0])
    ):
        b = list(b)
        if b[0][1] == b[-1][1]:
            # If the current range only contains a single number just return that number. E.g. (12, )
            yield (b[0][1],)
        else:
            # If the range contains multiple numbers return the first and list. E.g. (10, 12)
            yield b[0][1], b[-1][1]


def get_default_work_ids(*args, **kwargs):
    """Return list of default work ids

    This function should return the a Python list with default work IDs that is used in case the function doesn't define
    the select_work function.

    Returns:
        work_ids (list): Unique identifiers for work ids as ranged tuples
    """
    raise RuntimeError("You have to implement this function!")

    return []


def is_cyclic_util(node, visited, recStack, graph):
    """Test a sub-branch for cyclic behaviour

    A directed graph is cyclic if you can follow the graph along its
    direction and return to a node you have already visited. This behavior
    will lead to circular dependencies (i.e. a unsolvable dependency tree)
    if the graph describes dependencies.
    For more read: http://mathworld.wolfram.com/CyclicGraph.html

    Note:
        This function is designed to analyse cyclic behavior from a specific
        node. See the `is_cyclic` function to analyse an entire graph.

    Args:
        node (str): Name of current node
        visited (dict): Dict of visited nodes
        recStack (dict): Dict of nodes in the recursive search
        graph (NX Graph): The full graph object

    Returns:
        is_cyclic (bool): Is the sub-graph cyclic
    """
    # Mark current node as visited and
    # adds to recursion stack
    visited[node] = True
    recStack[node] = True

    # Recur for all neighbours
    # if any neighbour is visited and in
    # recStack then graph is cyclic
    for neighbour in graph[node]:
        if visited[neighbour] is False:
            if is_cyclic_util(neighbour, visited, recStack, graph) is True:
                return True
        elif recStack[neighbour] is True:
            return True

    # The node needs to be poped from
    # recursion stack before function ends
    recStack[node] = False
    return False


def is_cyclic(G):
    """Test if the graph is cyclic

    A directed graph is cyclic if you can follow the graph along its
    direction and return to a node you have already visited. This behavior
    will lead to circular dependencies (i.e. a unsolvable dependency tree)
    if the graph describes dependencies.
    For more read: http://mathworld.wolfram.com/CyclicGraph.html

    Args:
        G (NX Graph): The graph object to test

    Returns
        is_cyclic (bool): If the graph is cyclic or not
    """
    visited = {}
    recStack = {}

    for n in G.nodes:
        visited[n] = False
        recStack[n] = False

    for node in G.nodes:
        if visited[node] is False:
            if is_cyclic_util(node, visited, recStack, G) is True:
                return True
    return False


def _run_report_main_func(job_type, **work_kwargs):
    """Run modules main function

    This function imports the module `job_type` into the current memory scope. Due to Pythons handling of modules it's
    highly suggested to run this function from within a ProcessPoolExecutor to ensure the module is unloaded once the
    function ends.

    Args:
        job_type (str): Name of module (e.g. report_distance)
        work_kwargs (dict): Keyword arguments to parse directly to the main function
    """
    try:
        report_module = importlib.import_module(job_type)
        report_config = report_module.STAU_CONFIG
    except ImportError:
        raise ImportError(f"The job type {job_type} does not exist.")
    except AttributeError:
        raise AttributeError(
            f"The report {job_type} does not follow the config standard"
        )

    # Execute waiting job
    main_func = getattr(report_module, report_config.main_func)

    # Transform range of work ids to full list
    try:
        work_kwargs["kwargs"]["work_ids"] = range_to_work_ids(
            work_kwargs["kwargs"]["work_ids"]
        )
    except TypeError:
        raise TypeError(
            f"Cannot convert work_ids of job '{work_kwargs['jobType']}' to list, was {type(work_kwargs['kwargs']['work_ids'])}."
        )

    # Start the actual work.
    main_func(**work_kwargs["kwargs"])


def resolve_dependencies(queue, waiting):
    """Resolve jobs with missing dependencies on the queue

    A job in the QUEUED status is preliminary work and cannot be executed
    directly since no work ids nor dependencies are known for the job. When a
    new job is added to the queue it will have its dependencies set to None.
    This indicates that it is a preliminary/placeholder job.

    For the job to become executable two things needs to happen,

    * The job must be split into multiple sub-jobs dependent on the chunk_size
      configuration variable and how many work ids are in the job.
    * The N new chunks that each will become their own job needs dependencies
      computed

    Once this function terminates all subtasks and dependent jobs are available
    on the queue. The grandfather/original job is then moved to the MASTER
    state to indicate it should not execute. Only its children need to execute for
    all the work to complete.

    Notes:
        This function operates directly on the queue when adding and updating jobs.

    Args:
        queue (Stau queue object): A object to communicate with the queue
        waiting (list): List of jobs to resolve dependencies for

    Raises:
        RuntimeError: If the resulting graph is cyclic
    """
    for job in waiting:
        # Resolve jobs if dependencies are not calculated
        if job["dependencies"] is None:
            try:
                module = ModuleLoader(job["jobType"])
                # Load main report and lookup its dependencies
                main_configs = module.load_report_config()

                # Delay dependency resolution for this job if any of it's dependencies are already in the queue
                #
                # Example: Report A was added to the queue 1 hour ago but hasn't executed yet since the queue was busy.
                #          Now report B, that depends upon report A, is added. Since report A hasn't executed yet
                #          report B will resubmit report A and add the dependency. Now two copies of report A are waiting
                #          to execute. That is a waste. The logic below scans the queue when report B is added and sees
                #          that it's dependency report A is already in the queue. It then keeps report B in the QUEUED
                #          status until report A has executed. Once that happens report B is "unlocked" and the normal
                #          dependency resolution logic kicks in.
                #
                # Technically, this logic can keep jobs QUEUED forever if two jobs depends upon each other. In that case,
                # the queue will simply fill-up until Grafana alerts that the number of jobs isn't decreasing. This isn't
                # optimal but the alternative logic becomes very complex. So it's a lot easier to simply alert a human
                # and get them to solve the underlying problem, as this is simply a scheduling problem.
                skip_job = False
                for dep in main_configs.dependencies:
                    if queue.is_job_type_in_queue(dep.job_type):
                        # Yes, skip this job and break the current loop
                        skip_job = dep.job_type
                        break

                if skip_job:
                    # Check if the job has been skipped too many times
                    # We only want to alert about this issue once. Not on every queue scan after the job has waited for
                    # too long. Therefore, we only log the warning of the wait is between 0 <-> QUEUE_SCAN_FREQUENCY
                    # seconds. On the next queue scan time_until_alert is above QUEUE_SCAN_FREQUENCY and the issue is
                    # not mentioned again. The default value in the `get` is to ensure this doesn't log or fail if
                    # createdAt doesn't exist.
                    waited_for_sec = datetime.utcnow() - job.get(
                        "createdAt", datetime.utcnow()
                    )
                    if isinstance(waited_for_sec, timedelta):
                        waited_for_sec = waited_for_sec.total_seconds()
                    time_until_alert = waited_for_sec - MAX_RUNTIME_BEFORE_ALERT
                    if (
                        time_until_alert >= 0
                        and time_until_alert <= QUEUE_SCAN_FREQUENCY
                    ):
                        LOG.warning(
                            f"{job['jobType']} (id {job['id']}) has waited {MAX_RUNTIME_BEFORE_ALERT}s for {skip_job} to run and isn't resolved yet"
                        )
                    else:
                        LOG.info(
                            f"{job['jobType']} (id {job['id']}) is waiting for {skip_job} to finish and cannot resolve"
                        )

                    # Skip the job this time and wait for the dependencies to finish
                    continue

                # Build the directed dependency graph
                G, is_cyclic = build_dependency_graph(main_configs)
                G.graph["main_job"] = job["jobType"]

                # Add job information to the arguments supplied to each chunk
                job["kwargs"]["job_created_at"] = str(job.get("createdAt", None))
                job["kwargs"]["job_id"] = job.get("job_id", 0)

                # Store the orginale kwargs to each chunk - this variable may be modified later to limit the key
                # each dependent job sees
                chunk_kwargs = job["kwargs"]

                # Append work_ids as attribute - The naming convention changes to indicate the variable
                # is a local value and not a database reference
                # WorkIds are sorted to improve work chunking performance
                try:
                    if job["kwargs"]["work_ids"] is None:
                        # If work_ids is None, remove them. Jobs without `work_ids` are handled in the expection below
                        job["kwargs"].pop("work_ids")
                    G.graph["work_ids"] = job["kwargs"]["work_ids"]
                except KeyError:
                    # If the job does not contain any work ids then,
                    #   - Check if the report implements the select_work function and use that
                    #   - Else submit all default work_ids
                    try:
                        work_list = module.call_report_select_work(**job["kwargs"])

                        if isinstance(work_list, tuple):
                            # The select_work function returned multiple arguments. The first is work_list which is the
                            # only return value we care about here.
                            work_list = work_list[0]

                        if not isinstance(work_list, list):
                            raise RuntimeError(
                                f"select_work function must return list in report {job['jobType']}"
                            )
                        if len(work_list) == 0:
                            # No work are available to run this report - cancel it and add a note to the database
                            msg = f"JobId {job['id']} canceled since select_work function returned an empty list"
                            queue.update_job(
                                job["id"], {"status": "CANCELED", "message": msg}
                            )

                            LOG.warning(msg)
                            continue  # Skip to the next job

                        G.graph["work_ids"] = work_list
                        LOG.info("Got work_ids from select_work function")
                    except AttributeError:
                        # IMPORTANT: Some kwargs are only relevant when resolving the work_ids. Any keys that aren't
                        # required to execute the work are stripped in the function below.
                        G.graph["work_ids"], chunk_kwargs = get_default_work_ids(
                            **job["kwargs"]
                        )
                G.graph["work_ids"].sort()

                if is_cyclic:
                    msg = "The dependency graph is cyclic. Cannot continue."
                    queue.update_job(job["id"], {"status": "FAILED", "message": msg})
                    raise RuntimeError(msg)

                # Chunk job graph in parallel
                chunks = chunk_graph(G)

                # Submit the graph
                main_dependency_ids = []
                for chunk in chunks:
                    submit_job = submit_graph_to_queue(
                        queue, chunk, chunk_kwargs, masterJobId=job["id"]
                    )

                    # Add the source nodes (i.e. main dependencies of the master job) to the
                    # list of master dependencies
                    main_dependency_ids += [
                        submit_job[j]["job_id"] for j in submit_job["source nodes"]
                    ]
                    LOG.debug(
                        f"Submitted {len(submit_job)} jobs to queue: {submit_job}"
                    )

                # Mark original job as a master
                queue.update_job(
                    job["id"], {"status": "MASTER", "dependencies": main_dependency_ids}
                )

            except Exception as e:
                # Report module error to API
                LOG.error(e, exc_info=e)
                msg = exception_to_string(e)
                queue.update_job(job["id"], {"status": "FAILED", "message": msg})


def submit_graph_to_queue(
    queue, graph, kwargs, cur_node=None, jobs=None, masterJobId=None
):
    """Add graph as jobs on queue

    This function converts the NetworkX graph supplied in the `graph` object
    to a formatted dictionary ready for the queue. The algorithm visits each node
    recursively and submits node with resolved dependencies to the queue. If the
    `cur_node` argument is empty the source nodes (node belonging to the original job)
    is used as the starting point. Once a node with no dependencies is located that node
    is added to the queue. If the node has dependent nodes it waits for the child nodes
    to get a job_id and is then added to the queue.

    Examples:
        In the directed graph below,

                A       B
                |\     /|
                | \   / |
                v  \ /  v
                C   D   E
                \   |   /
                 \  |  /
                  \ v /
                    F

        All jobs depends on F. There F is added to the queue first. Then jobs C, D and E
        are added with F as their only dependency. Finally job A is added with C and D as
        dependencies and B is added with D and E as dependencies.
        If `cur_node` is None the algorithm will find the first nodes without any parents
        A and B). It will then visit node C (the first child of A) and continue along the
        graph recursively until all nodes/jobs are added.
        The master node is not shown as it will never execute but is the first node in
        the graph with A and B as its dependencies.

    Args:
        queue: Stau queue instance
            Queue object with a connection to the API
        graph: NetworkX graph
            Dependency graph to submit
        kwargs: dict
            Keyword arguments to pass to all jobs
        cur_node: str
            Name of current node being processed
        jobs: dict
            Overview of which jobs have already been submitted
        masterJobId: int
            Job id for the main/master job

    Returns:
        jobs: dict
            Overview of all jobs submitted to the queue with parameters

    Raises:
        RuntimeError: If the graph has no source nodes
    """

    jobs = jobs or {}

    if cur_node is None:
        # Find source nodes (nodes that only has edges away from it)
        # Source nodes are always submitted last as they execution depends on all other nodes
        source_nodes = [n for n in graph.nodes if len(list(graph.predecessors(n))) == 0]
        if len(source_nodes) == 0:
            # Mathematically this should never happen
            raise RuntimeError("Graph has no source nodes. Cannot continue")

        # Store reference to source nodes in jobs
        jobs["source nodes"] = source_nodes

        # Transverse the graph from the main job
        for sn in source_nodes:
            jobs[sn] = {}
            jobs = {
                **jobs,
                **submit_graph_to_queue(queue, graph, kwargs, sn, jobs, masterJobId),
            }
    else:
        dependencies = []
        for itt, (parent_node, child_node) in enumerate(graph.edges(cur_node)):
            if child_node not in jobs:
                jobs[child_node] = {}
                jobs = {
                    **jobs,
                    **submit_graph_to_queue(
                        queue, graph, kwargs, child_node, jobs, masterJobId
                    ),
                }
            dependencies.append(jobs[child_node]["job_id"])

        # All edge nodes visited add dependencies to job
        jobs[cur_node]["dependencies"] = dependencies
        jobs[cur_node]["jobType"] = graph.nodes[cur_node]["job_type"]
        jobs[cur_node]["masterJobId"] = masterJobId
        # Ensure the node is run against the correct list of work_ids
        jobs[cur_node]["kwargs"] = {
            **kwargs,
            # It is important to transform the work_ids to list as the range function
            # returns a iterator
            "work_ids": list(work_ids_to_range(graph.nodes[cur_node]["work_ids"])),
            # Add any custom keyword arguments associated with the node - These will overwrite any existing variable
            # with the same key name from kwargs
            **graph.nodes[cur_node]["custom_kwargs"],
        }

        # Submit to queue to get the job id
        jobs[cur_node]["job_id"] = queue.add_job(jobs[cur_node])

    return jobs


def chunk_graph(G):
    """Chunk a dependency graph

    The graph produced by the build_dependency_graph function has all the correct nodes and is
    ordered correctly. The execution size of each node is, however, not scaled to the requirements
    from the `chunk_size` variable in the jobs `configs` variable. This processing step computes the
    most optimal way to split the graph into N independent executions. Dependent on the requested
    work items the graph can either be split into independent graphs and the nodes in each graph
    can be split into independent nodes.

    For this function to work the Graph must have the chunk_size attribute set on all nodes. It
    must also contain work_ids and main_job attributes set on the graph itself.

    Examples:
        The following dependency graph is received,

                    A
                    |
                    v
                    B
                    |
                    v
                    C

        Three nodes A, B and C that has a simple relationship. Each node has the same chunk size
        and the work_ids to distribute is 3 times the chunk size. This would result in the following
        three independent graphs,

                X   Y   Z
                |   |   |
                E   F   G
                |   |   |
                H   I   J

        Each subgraph is dependent only on work completed within the graph. The graphs are therefore
        executable in parallel.
        If node `C` had 3 times the chunk size the same would result in the following dependencies,

                X   Y   Z
                |   |   |
                E   F   G
                \   |   /
                 \  |  /
                  \ | /
                    H

        Now only a single graph is produced with all nodes waiting for `H` to finish before anything
        else can execute.

    Args:
        G (NetworkX Graph):
            The graph to chunk. The graph must contain the attribute 'work_ids' and all nodes
            must have a chunk_size attribute.

    Raises:
        RuntimeError: If G is not a directed graph
        ValueError: If the chunk size or work ids are malformed
    """
    if not G.is_directed():
        raise RuntimeError("This function only works for directed graphs")

    num_work_ids = len(G.graph["work_ids"])

    # Find largest chunk_size of any node
    max_chunk_size = 0
    for node, csize in G.nodes(data="chunk_size"):
        if csize > max_chunk_size:
            max_chunk_size = csize

    # Compute number of parallel graphs by splitting the total work load
    # against the largest possible chunk size of any job
    try:
        num_graphs = math.ceil(num_work_ids / max_chunk_size)
    except ZeroDivisionError:
        raise ValueError("Chunk sizes or work ids are malformed in graph")
    parallel_graphs = [G.copy() for n in range(num_graphs)]

    # Distribute work to every graph based on the global work_ids
    for graph_num, graph in enumerate(parallel_graphs):
        first_work_item = graph_num * max_chunk_size
        last_work_item = graph_num * max_chunk_size + max_chunk_size

        # Work ids on graph level are lists to allow indexing
        if last_work_item > num_work_ids:
            graph.graph["work_ids"] = G.graph["work_ids"][first_work_item:]
        else:
            graph.graph["work_ids"] = G.graph["work_ids"][
                first_work_item:last_work_item
            ]

    # Start building each subgraph
    #
    # For each graph the nodes with a chunk size smaller than the largest chunk size in the graph
    # should be split into N replicas of the same node with different work ids
    for itt, cur_graph in enumerate(parallel_graphs):
        replicate_branch(cur_graph, cur_graph.graph["main_job"])
        # Update the main/entrypoint job to point to the chunk specific node
        cur_graph.graph["main_job"] += f"_{itt}"

        # New nodes (replicas) have a different naming convention than the orginal node names
        # so we can remove the old graph
        cur_graph.remove_nodes_from(G.nodes())

    return parallel_graphs


def chunks(lst, n):
    """Yield successive n-sized chunks from a list

    This function is designed to iterate though the list
    using a generator syntax.

    Examples:
        >>> list(chunks([1, 2, 3, 4], 2))
            [[1, 2], [3, 4]]

    Args:
        lst (list): The list to split
        n (int): Number of chunks

    Returns:
        subset (set): The Nth chunk of the list
    """
    for i in range(0, len(lst), n):
        yield set(lst[i : i + n])


def replicate_branch(cur_graph, cur_node, level=0, custom_kwargs=None):
    """Replicate branch based on graph workload

    This function is designed for recursive execution. The primary way to execute is through
    the `chunk_graph` function. Once the number of parallel graphs is known this function
    visits each node in the newly created graph and creates the required number of copies for
    each node.

    The new nodes will be similar to the existing single node but have the attributes set,

    * work_ids: The individual work items that node needs to execute
    * job_type: The job type inherit from the mother node

    New nodes will follow the naming scheme job_type_replication_number. The reason to add the
    job type to every node is so they can easily be added to the queue later.

    This function is called recursively to transverse the graph.

    This function will result in two seperate graphs. The orginale graph is preserved and a new
    graph is added (without shared edges) containing replicated nodes depending on chunk size.
    The old graph can simply be deleted.

    Examples:
        The following 3 node chunked dependency graph is received,

                    X
                    |
                    v
                    E
                    |
                    v
                    H

        If node `E` only has half the node chunk size compared to `X` and `H` then the following
        graph is produced by the replication,

                   X_1
                   / \
                  /   \
                E_1  E_2
                  \   /
                   \ /
                   H_1

        This is a different computation to the result of `chunk_graph` as this method will produce
        multiple copies of each node but always only return a single graph.

    Notes:
        New nodes are added directly to the `cur_graph` and the original nodes are kept.

    Args:
        cur_graph: directed graph
            The graph to replicate nodes on
        cur_node: str
            Name of the node to process
        level: int
            Recursion level
        custom_kwargs: dict
            Custom keyword arguments to supply to each node within the current graph/subgraph. The values may be
            overwritten if the node edge has a matching key.

    Returns:
        node_replicas: list of str
            The name of the newly created nodes

    Raises:
        RuntimeError: If level is above the maximum set in MAX_RECURSION_LEVEL
    """
    if level > MAX_RECURSION_LEVEL:
        raise RuntimeError("Maximum recursion level reached")
    cur_edges = cur_graph.edges(cur_node, data=True)
    num_subgraph_work_items = len(cur_graph.graph["work_ids"])
    chunk_size = cur_graph.nodes[cur_node]["chunk_size"]
    job_type = cur_graph.nodes[cur_node]["job_type"]
    custom_kwargs = custom_kwargs or {}

    # Note: Work ids on node/vertex level are sets to improve comparison operations
    node_work_items = [l for l in chunks(cur_graph.graph["work_ids"], chunk_size)]

    # Split the current job node if required
    required_node_replicas = math.ceil(num_subgraph_work_items / chunk_size)
    node_replicas = [
        f"{cur_node}_{replica}" for replica in range(required_node_replicas)
    ]

    # Create required copies of current node without edges
    for node_no in range(required_node_replicas):
        cur_graph.add_node(
            node_replicas[node_no],
            job_type=job_type,
            work_ids=node_work_items[node_no],
            custom_kwargs=custom_kwargs,
        )

    for itt, (parent_node, child_node, edge_data) in enumerate(cur_edges):
        max_lag = edge_data["max_lag"]
        allow_missing_work = edge_data.get("allow_missing_work_ids", False)

        # Expand any custom keyword arguments listed on the edge to all nodes below this point. The keys from the
        # current edge will overwrite any generic arguments from previous custom arguments.
        child_kwargs = {**custom_kwargs, **edge_data.get("custom_kwargs", {})}

        # Is this dependency allowed to miss work? In that case, how old it the latest execution of the dependency.
        if allow_missing_work:
            # NOTE: The `if` is split since we don't want to compute `minutes_since_execution` if we don't need it
            minutes_since_execution = (
                time.time() - get_latest_stau_execution(child_node, as_epoch=True)
            ) / 60
            if minutes_since_execution < max_lag:
                # The dependency already executed within max_lag and the job doesn't care about which work IDs were part
                # of that execution - only that it executed. In this case, skip the dependency and move on to the next.
                continue

        # Add edge if the child node requires recomputation
        last_execution = lookup_last_report_execution(
            child_node, cur_graph.graph["work_ids"]
        )
        if last_execution is None or max_lag < last_execution:
            # Recurse down the branch and add any custom kwargs to all nodes along this edge
            branch_nodes = replicate_branch(
                cur_graph, child_node, level + 1, child_kwargs
            )

            # Join current replication nodes with nodes from the branch
            for rep_node in node_replicas:
                for branch_node in branch_nodes:
                    node_work = cur_graph.nodes[rep_node]["work_ids"]
                    branch_work = cur_graph.nodes[branch_node]["work_ids"]

                    if not node_work.isdisjoint(branch_work):
                        # Join node if they share work items (i.e. the branch node
                        # computes work ids the replication node depends upon)
                        cur_graph.add_edge(rep_node, branch_node, max_lag=max_lag)

    return node_replicas


def build_dependency_graph(report_config, level=0):
    """Build the dependency graph

    Based on the information provided in the `dependencies` parameter for the report/job
    this function builds a dependency graph where each node is a report to execute.

    Each dependent report loaded and checked for dependencies of its own. This process is
    continued recursively until all dependent reports have been visited.

    Args:
        report_config (dict):
            The report config from a report class
        level (int):
            Current level of the recursion

    Returns:
        graph (Directed Graph):
            The resulting graph
        is_cyclic (bool):
            If the graph is cyclic
    """
    G = nx.DiGraph()
    parent_node_name = report_config.job_type
    G.add_node(
        parent_node_name, chunk_size=report_config.chunk_size, job_type=parent_node_name
    )

    if level > MAX_RECURSION_LEVEL:
        # The level of recursion is to high
        LOG.warning("Maximum recursion level reached when building graph.")
        return G, False

    if report_config.dependencies is None:
        # Report has no dependencies
        return G, is_cyclic(G)

    for dep in report_config.dependencies:

        # Load module
        try:
            module = ModuleLoader(dep.job_type)
            dependent_configs = module.load_report_config()
        except ImportError:
            # Re-raise error to indicate which dependent report produced the error
            raise ImportError(
                f"Failed to import the dependent job of type {dep.job_type}"
            )
        except AttributeError:
            # Re-raise error to indicate which dependent report produced the error
            raise AttributeError(
                f"The dependent job of type {dep.job_type} did not contain a configs parameter"
            )

        # Add edge to dependent report with all parameters from the ReportDependencies class without the job_type
        # parameter
        edge_data = {k: v for k, v in dep.__dict__.items() if k != "job_type"}

        # Resolve the dependency name - The dependency can either link directly to the current node or any node that
        # depends upon the same job type. E.g. the job type A depends upon the job type B. Job type C also depends upon
        # job type B. If both job A and job C depend on job B with the same parameters then only a single copy of B is
        # added to the graph with A and C as parent/dependent jobs:
        # A   C
        #  \ /
        #   B
        # However, if the parameters don't match (e.g. different work_ids or kwargs) then two copies of job type B
        # should exist and only link to their respective parent jobs:
        # A  C
        # |  |
        # B  B
        # In this case, `unique` is True.
        child_node_name = dep.job_type
        if dep.unique:
            child_node_name = f"{report_config.job_type}->{dep.job_type}"

        G.add_edge(report_config.job_type, child_node_name, **edge_data)

        G.nodes[child_node_name]["job_type"] = dep.job_type
        if dependent_configs.dependencies == []:
            # Report has no dependencies but ensure it gets the chunk_size attribute set
            G.nodes[child_node_name]["chunk_size"] = dependent_configs.chunk_size
        else:
            # Follow dependency - Ignore cyclic graphs at this stage
            G_node, _ = build_dependency_graph(dependent_configs, level + 1)

            # Merge the branch onto the main graph
            G = nx.compose(G, G_node)

        # Unload module
        del dependent_configs

    return G, is_cyclic(G)


def groom_queue(queue):
    """Maintain the queue state

    This function maintains the consistency of the queue. It moves jobs between
    states and ensures stale jobs are cleaned up.

    Args:
        queue (StauQueue): Reference to the queue
    """

    # Init two function variables used to log failed jobs to Slack, if they don't already exist. Function variables
    # are used since they keep their state even after the function returns.
    groom_queue.last_queue_scan_datetime = getattr(
        groom_queue, "last_queue_scan_datetime", datetime.utcnow()
    )
    groom_queue.failed_master_job_ids = getattr(
        groom_queue, "failed_master_job_ids", []
    )

    LOG.info("Checking for QUEUED jobs")
    for job in queue.iter_jobs("QUEUED"):
        LOG.info(f"Resolving dependencies for job {job['id']}")
        resolve_dependencies(queue, [job])

        # Check if any queued work should move to PENDING
        # The logic to move jobs is handled here and not in the
        # backend to keep all logic about job status in one place
        if queue.check_dependencies(job["id"]):
            # Move job to pending state since it is ready to execute
            LOG.info(
                f"The job id {job['id']} is ready to execute. Moving it to PENDING status."
            )
            queue.change_job_status(job["id"], "PENDING")

    # Check if any running jobs have not checked-in to indicate that they are still running.
    # This will indicate that a container has crashed
    LOG.info("Checking for RUNNING jobs")
    for job in queue.iter_jobs("RUNNING"):
        # The iterator only returns jobs with a lock that this executor holds. If the iterator returns
        # a running job that is not running on this executor the job is stale and is reset.
        if (
            datetime.utcnow() - job["updatedAt"]
        ).total_seconds() >= QUEUE_SCAN_FREQUENCY * 2:

            # No check-in within two scan cycles - the job has stopped running without failing. Most often this is
            # caused by deploying new code. In that case, the git SHA has changed and we don't need to inform the user.
            # The job is automatically reset. However, if the SHA is the same the pod likely died without warning.
            # In that case, we would like to know about.
            exec_env = get_git_commit_sha(short=True)
            if job.get("execEnvironment", exec_env) == exec_env:
                msg = f"It appears the job {job['id']} ({job['jobType']}) stopped running without warning - Rerunning the job on SHA {exec_env}"
                LOG.warning(msg)

            if queue.rerun_job(job["id"]) is None:
                LOG.error(
                    f"Unable to rerun job {job['id']} ({job['jobType']}). Please investigate."
                )

    # Check if any MASTER work should move to DONE
    # MASTER jobs are only dependent upon their source nodes. Once their status changes to
    # DONE the master is also DONE.
    LOG.info("Checking for MASTER jobs")
    for job in queue.iter_jobs("MASTER"):
        # When a master job fails it is added to the list of `failed_master_job_ids` to ensure failures are
        # only reported once. However, if the same job is rerun and fails the job will fail silently. Any failed jobs
        # that move back to the master status are reset to ensure future failures are recorded as expected.
        try:
            groom_queue.failed_master_job_ids.remove(job["id"])
        except ValueError:
            pass

        if queue.check_dependencies(job["id"]):
            # Move job to DONE state since the dependent jobs have finished
            LOG.info(f"The master job id {job['id']} is done.")
            queue.update_job(
                job["id"],
                {
                    "status": "DONE",
                    "execEnd": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                },
            )

    # Report any jobs that has failed since the last queue scan. A small buffer of 5 seconds is added to ensure no jobs
    # fails without an error being raised.
    LOG.info("Checking for FAILED jobs")
    max_job_age = (
        datetime.utcnow() - groom_queue.last_queue_scan_datetime
    ).total_seconds() + 5  # sec

    for job in queue.get_jobs("FAILED", max_age_seconds=max_job_age):
        job_id = job["id"]
        if (
            job["masterJobId"] is None
            and job_id not in groom_queue.failed_master_job_ids
        ):
            msg = f"One or more chunks from {job['jobType']} (id {job_id}) has failed - Check Grafana and Stackdriver for more"
            LOG.error(msg)
            groom_queue.failed_master_job_ids.append(
                job_id
            )  # Record job as failed so it's not logged again

    # Last datetime the queue was scanned
    groom_queue.last_queue_scan_datetime = datetime.utcnow()


def execution_complete(process):
    """This function is called once the current job is done processing

    Sending the interupt will break the executor from the current wait and
    trigger a new scan of the queue for find more work.

    Args:
        process (Futures object): The process that just completed

    Also See:
        https://docs.python.org/3/library/asyncio-future.html#asyncio.Future.add_done_callback
    """
    LOG.info("Current work has finished processing - Will send interupt")
    sleep_between_queue_scan.set()


def process_queue(queue, pool_executor, process_sleep=QUEUE_SCAN_FREQUENCY):
    """Check the queue to resolve or start work

    This function contains all logic to investigate the queue for jobs
    in different stats that could start running or needs dependencies resolved.

    Args:
        queue: Stau queue object
            Instance of the queue
        process_sleep: int
            Number of seconds to sleep between touching the database
    """
    LOG.info("Starting a new scan of the queue for PENDING jobs")
    work = queue.get_job_to_run(exec_env=get_git_commit_sha(short=True))
    if work is not None:
        # Claim work to current process
        try:
            LOG.info(f"Found work with ID {work['id']}")

            # Start the actual work
            active_job = pool_executor.submit(
                _run_report_main_func, work["jobType"], **work
            )
            active_job.add_done_callback(execution_complete)

            # Store job parameters
            active_job_start_time = time.time()
            last_touch_time = (
                active_job_start_time  # Indicate that the job was just updated
            )
            active_job_config = work

            # Store job info so it is available to other scripts
            os.environ["ACTIVE_STAU_JOB_NAME"] = str(work["jobType"])
            os.environ["ACTIVE_STAU_JOB_ID"] = str(work["id"])

            LOG.info(f"Work started on {work['jobType']} with ID {work['id']}")

            # Update job information
            while active_job.running():
                # Reset any interupts that might be set once the job has finished executing
                sleep_between_queue_scan.clear()

                # Sleep for N seconds or until the current job has finished
                sleep_between_queue_scan.wait(process_sleep)

                # Check the database to ensure the job should keep running
                active_job_config = queue.get_job(active_job_config["id"])
                if active_job_config["status"] != "RUNNING":
                    # It is not possible to cancel a running process in Python. The cancel method for futures only
                    # works for processes that have not started executing. The only option to kill a running job is to
                    # restart the container. This is not a problem since containers are cheap and the job will not execute
                    # again since it is already marked as CANCELED or FAILED in the database at this point.
                    LOG.info(
                        f"Job was running but queue status was {active_job_config['status']}. Attempting to cancel job."
                    )
                    raise JobCanceledInterrupt()

                # Check-in with the database to show the job is still running. Only do so if we didn't just update the
                # job within the last second.
                if time.time() - last_touch_time > 1.0:
                    queue.touch_job(active_job_config["id"])
                last_touch_time = time.time()
                runtime = last_touch_time - active_job_start_time

                # Check the job runtime. We only want to alert about this issue once. Not on every queue scan. Therefore,
                # we only log the warning once between 0 - QUEUE_SCAN_FREQUENCY seconds of the incident. On the next
                # queue scan time_until_alert is above QUEUE_SCAN_FREQUENCY and the issue is not mentioned again.
                time_until_alert = runtime - MAX_RUNTIME_BEFORE_ALERT
                if time_until_alert >= 0 and time_until_alert <= QUEUE_SCAN_FREQUENCY:
                    msg = f"""
                    A chunk in {active_job_config['jobType']} (Id {active_job_config['id']}) has been running for {runtime:.2f}s.
                    This might indicate a problem! If not, you can increase the `MAX_RUNTIME_BEFORE_ALERT` constant.
                    """
                    LOG.warning(msg)

                LOG.info(
                    f"Waiting for current job to finish. Runtime is {runtime:.2f}s"
                )

            LOG.debug(f"Finished {active_job}")

            # Record previous job as completed
            # exitcode will be None if the job never started
            active_exception = active_job.exception()
            if active_exception:
                queue.change_job_status(active_job_config["id"], "FAILED")
                # Store error message in database
                err_msg = exception_to_string(active_exception)
                queue.update_job(
                    active_job_config["id"],
                    {
                        "status": "FAILED",
                        "message": err_msg,
                        "execEnd": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    },
                )

                # Any exceptions from a future that fails are re-raised so the LOG can access the
                # exception message and forward it
                msg = f"The job {active_job_config['id']} has failed while executing {active_job_config['jobType']}"
                try:
                    raise RuntimeError(msg) from active_exception
                except Exception:
                    # Don't log this message to Slack as each chunk may spam Slack with identical messages. The master
                    # executor is responsible for alerting this issue in the `groom_queue()` function.
                    LOG.error(
                        msg, exc_info=active_exception, extra={"send_to_slack": False}
                    )
            else:
                queue.change_job_status(active_job_config["id"], "DONE")

            LOG.info(f"Finished processing job id {active_job_config['id']}")

        except (ImportError, AttributeError, TypeError) as e:
            # Errors related to the load_report function or execution
            msg = f"The job {work['id']} has failed while executing {work['jobType']}"
            LOG.exception(msg)
            queue.update_job(work["id"], {"status": "FAILED", "message": str(e)})
        except Exception as e:
            msg = f"The job {work['id']} has failed while executing {work['jobType']}"
            LOG.exception(msg)
            queue.update_job(work["id"], {"status": "FAILED", "message": str(e)})

        # Job has finished - remove global reference to job
        try:
            del os.environ["ACTIVE_STAU_JOB_NAME"]
            del os.environ["ACTIVE_STAU_JOB_ID"]
            del active_job
            del report_module
            del report_configs
        except (KeyError, UnboundLocalError):
            pass  # Ignore any errors if the variables don't exist
        gc.collect()

    else:
        LOG.info("No work found on queue")

        # The connection is no longer needed and could lock other executor from R/W. Close it and a new connection is
        # opened on the scan.
        queue.close()
        sleep_between_queue_scan.wait(process_sleep)


def status_check(port=8080):
    """Host job health check endpoints

    Kubernetes expects containers to host `/_status` and `/readiness` over HTTP
    to ensure container health. This function starts a new process that ensures
    the endpoints are available. **Note**: This functions is blocking and should
    be executed in a seperate thread.

    Args:
        port (int): Which network port to listen on

    Returns:
        server: TCPServer object
            The server object
        main_thread: futures oject
            Reference to the main server thread
    """
    LOG.info(f"Starting HTTP server")

    max_port_retries = 5
    port_retries = 0
    while port_retries <= max_port_retries:
        try:
            http_server = socketserver.TCPServer(("", port), SimpleHTTPRequestHandler)
        except OSError as err:
            # Fails with Address already in use - Try a different port
            port_retries += 1
            port = random.randint(8100, 8900)
        else:
            break

    if port_retries >= max_port_retries:
        raise RuntimeError("Unable to bind web server to available port")

    web_server_thread = Thread(target=http_server.serve_forever, daemon=True)
    web_server_thread.start()

    LOG.info(f"serving pod information on port {port}")
    return http_server, web_server_thread


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    """Simple processor for HTTP requests

    This function accepts GET requests through the status_check funtion
    and returns a response on the following endpoints,

    * /readiness
    * /_status

    The endpoints are important when running the executor through the
    kubernetes environment.
    """

    def do_GET(self):

        self.send_response(200)
        self.end_headers()

        url = urllib.parse.urlparse(self.path)
        LOG.info(url.path)
        if url.path == "/readiness":
            self.wfile.write(b"readiness")
        elif url.path == "/_status":
            self.wfile.write(b"status")
        else:
            self.wfile.write(b"Path not supported")


def is_lock_free(db_conn, lock_name):
    """Check the database if a named lock exists

    Args:
        db_conn (database connection): Open connection object from mysql-connector
        lock_name (str): Name of the lock to check

    Returns:
        lock_free (bool): True if free otherwise False.
    """
    cursor = db_conn.cursor()
    cursor.execute(f"SELECT IS_FREE_LOCK('{lock_name}')")
    lock_status = cursor.fetchone()
    if 1 in lock_status:
        # Lock is released
        return True
    # Lock in place
    return False


def release_lock(db_conn, lock_name):
    """Release an existing database lock

    A lock release request has three states:
        - True (1): The lock was released with success
        - False (0): The lock was not released because it was not acquired by the current executor
        - None: No lock with that name exists in the database

    Args:
        db_conn (database connection): Open connection object from mysql-connector
        lock_name (str): Name of the lock to release

    Return:
        released (bool): True/False/None - See function description for more information.

    Notes:
        Also see: https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html#function_release-lock
    """
    cursor = db_conn.cursor()
    cursor.execute(f"SELECT RELEASE_LOCK('{lock_name}')")
    lock_status = cursor.fetchone()
    if 1 in lock_status:
        # Lock is released
        return True
    elif 0 in lock_status:
        # You don't own this lock so it isn't released
        return False
    else:
        # No lock with this name
        return None


def acquire_lock(db_conn, lock_name="stauSchedulerProcess"):
    """Attempt to acquire lock

    Args:
        db_conn (database connection):
            Open connection object from mysql-connector
        lock_name (str): Name of the lock if acquired

    Returns:
        lock_status (bool):
            If the lock was acquired
    """
    LOG.debug("Checking locks")

    cursor = db_conn.cursor()
    cursor.execute(f"SELECT IS_USED_LOCK('{lock_name}') = CONNECTION_ID()")
    lock_status = cursor.fetchone()

    if None in lock_status:
        # No lock - let's get it
        LOG.info(f"Lock '{lock_name}' was created in database")
        cursor.execute(f"SELECT GET_LOCK('{lock_name}', 30)")
        if 1 not in cursor.fetchone():
            LOG.error("Unable to get free lock...")
            return False
        return True
    elif 0 in lock_status:
        # Someone else has the lock - Let's bail
        LOG.debug(f"The database is locked by a different process")
        return False
    elif 1 in lock_status:
        # I already have the lock - Let's boogie
        LOG.debug(f"The database is already locked by me")
        return True
    else:
        LOG.error(f"Unknown lock status {lock_status}")
        return False


def schedule_jobs(db_conn, scheduler):
    """Start the scheduler for recurring jobs

    If the scheduler is None and a lock is acquired then a
    new scheduler is started.

    Args:
        db_conn (Database connection object):
            Existing database connection or None. If None a connection is opened.
        scheduler (Instance of StauPeriodicJobsScheduler):
            Existing scheduler or None.

    Notes:
        This function does not have default arguments to force users to
        declare variables before calling this function.
    """
    LOG.debug("Checking scheduler status")
    try:
        # Connect to database if required
        db_conn = connect_to_database(db_conn)

        # Ping the database to keep the connection alive
        db_conn.ping(reconnect=True, attempts=3, delay=30)

        if acquire_lock(db_conn) and not scheduler:
            # If the scheduler lock is free and don't are have a scheduler running then
            # start the process.
            LOG.info("Starting the scheduling process")
            return db_conn, StauPeriodicJobsScheduler()

        # Temporary fix of ch46676 - Multiple schedulers
        if scheduler and not acquire_lock(db_conn):
            # You have a scheduler but not the lock? Then terminate the scheduler
            LOG.info("Shutting down scheduler since I don't have the lock")
            if scheduler.scheduler.running:
                scheduler.scheduler.shutdown()

    except (mysql.connector.errors.Error) as err:
        # If there was an error with the database connection simply ignore
        # the problem and retry the scheduler next time.
        LOG.error(f"Database connection failed with error {str(err)}")

    LOG.debug(f"Finished scheduler check. Schedule is now {scheduler}")
    return db_conn, scheduler


def connect_to_database(db_conn=None):
    """Get a database connection to Data Quality

    If running locally, the default connection is to the local database.

    Args:
        db_conn (Database connection object):
            Existing database connection or None. If None a connection is opened.

    Returns:
        db_conn (Database connection object):
            Existing database connection (input argument) or new connection if None
    """
    if not db_conn:
        creds = get_mysql_config()
        if not all(creds.values()):
            raise RuntimeError(
                f"Unable to find all database setting in environment variables. Found {creds}"
            )
        db_conn = mysql.connector.connect(**creds)
    return db_conn


def launch_stau_executor(queue_scan_interval=None):
    """Launch a new generic job executor

    The general executor will connect to the Stau API and listen for
    work added to the queue. All jobs are executed in a seperate process
    allowing the executor to continue processing the queue even if a job
    is already executing.

    The executor will start a new thread to run a HTTP server hosting health endpoints. It also starts a new process
    that performs the actual work.

    **Note**: The executor is a blocking process that will run until termination.

    Args:
        queue_scan_interval: int
            Seconds between each scan through the queue. If none the QUEUE_SCAN_FREQUENCY is used

    Raises:
        Excption: Any error from processing the queue
    """
    LOG.info(f"Started general executor")

    process_sleep = queue_scan_interval or QUEUE_SCAN_FREQUENCY

    # Starting new thread to host runtime information
    web_server, web_server_thread = status_check()
    while not web_server_thread.is_alive():
        # Wait for thread to start
        pass
    LOG.debug("Web server started moving to executor")

    queue = None
    db_conn = connect_to_database()
    scheduler = None

    try:
        while True:
            # Start the recurring/periodic jobs scheduler if this process can acquire the database lock
            LOG.debug("Process checking if it is the scheduler")
            db_conn, scheduler = schedule_jobs(db_conn, scheduler)
            LOG.debug(f"Process finished checking if it is the scheduler: {scheduler}")

            with Stau() as queue:
                # Once the Event.wait() method returns, Event.set() is called and all subsequent waits are skipped since the
                # wait already happened. The call to Event.clear() clears the Event such that Event.wait() doesn't return
                # immediately.
                sleep_between_queue_scan.clear()

                # The scheduler process is responsible for checking if any newly added jobs should
                # have their dependencies resolved
                if scheduler is not None:
                    LOG.info("Scheduler is about to groom the queue")
                    groom_queue(queue)
                    LOG.info("Scheduler finished grooming the queue")

                    # Update the latest execution information for each work id for all jobs that have finished
                    # Only one process can do this since multiple jobs might have completed for the same work_id
                    # Doing this in parallel cannot ensure the database state
                    LOG.info("Updating execution information for reports")
                    queue.jobs_missing_latest_execution_update()

                    LOG.info("Scheduler is about to sleep")
                    # Sleep for N seconds or until the interupt is send once the job
                    # has finished executing
                    sleep_between_queue_scan.wait(process_sleep)
                    LOG.info("Scheduler woke from sleep")
                else:
                    # All executors who are not scheduling jobs are responsible for performing the actual work
                    with ProcessPoolExecutor(max_workers=1) as process_executor:
                        # Process queue in a seperate thread such that the main thread is able to update runtime
                        # information to the database.
                        # Note: ThreadPoolExecutor calls the Threading library under the hood.
                        process_queue(queue, process_executor, process_sleep)

    except (JobCanceledInterrupt, KeyboardInterrupt):
        # Shutdown without logging since this is not an error
        web_server_thread.join(timeout=1)  # Terminate webserver with a 1s wait
        if db_conn:
            db_conn.close()
    except Exception as e:
        LOG.error("Worker failed with execution a job", exc_info=e)
        # Close the webserver
        web_server_thread.join(timeout=1)  # Terminate webserver with a 1s wait
        if db_conn:
            db_conn.close()


if __name__ == "__main__":
    try:
        """
        # Example of debugging exactly which work_ids two reports executed on
        # This is useful if a job triggeres a dependency it shouldn't
        # NOTE: If testing against the production or staging environemnts you have to edit in the __init__ func in Stau()
        with Stau() as queue:
            # Only check this job type - useful if one job has this job type as a dependency
            jobType = "report_service"
            checked_ids = set()

            # This function iterates a master job and extract all work ids for chunks that match the jobType
            def resursive_resolve_workids(job, jobType):
                if job['dependencies'] is not None and any(job['dependencies']):
                    all_ids = set()

                    # Check all leave nodes
                    for dep in job['dependencies']:
                        if dep not in checked_ids:
                            jtmp = queue.get_job(dep)
                            chunk_ids = resursive_resolve_workids(jtmp, jobType)
                            all_ids.update(chunk_ids)

                    # Check the job ifself
                    if job["jobType"] == jobType:
                        try:
                            w = set(range_to_work_ids(job['kwargs']['work_ids']))
                            all_ids.update(w)
                        except KeyError:
                            # The master job doesn't have any work_ids
                            pass

                    return all_ids
                else:
                    checked_ids.add(job["id"])
                    if job["jobType"] == jobType:
                        return set(range_to_work_ids(job['kwargs']['work_ids']))
                    else:
                        return set()

            # Get all work_ids for chunks that match the jobType
            j1 = queue.get_job(1196355)
            j2 = queue.get_job(1196073)
            w1 = resursive_resolve_workids(j1, jobType)
            w2 = resursive_resolve_workids(j2, jobType)

            # Are the work_ids different between the two jobs?
            print(w1.difference(w2))
            breakpoint()
        """
        launch_stau_executor()
    except Exception:
        raise RuntimeError("Failure while running job executor")
    exit()
