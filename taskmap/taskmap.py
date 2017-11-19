from itertools import chain, repeat
from collections import namedtuple
from functools import partial

import os
import time
import asyncio
import logging
import datetime as dt
import multiprocess as mp
import multiprocessing_logging as mplogging

# dependencies: map from each task to a list of tasks on which it depends
# done: set of names of functions that have been complete
# results: map from task to result of function call
# in_progress: tasks currently in progress (for async and parallel)
Graph = namedtuple('graph', [
    'funcs', 'dependencies', 'done', 'results', 'in_progress', 'lock',
    'io_bound'
])

logger = logging.getLogger('taskmap')
logger.setLevel(logging.DEBUG)

now = dt.datetime.now()
log_frmt = 'taskmap{}.log'.format(now.strftime('%m-%d-%Y--%H.%M.%S'))
fh = logging.FileHandler(log_frmt)

fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter(log_frmt)
ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)
mplogging.install_mp_handler(logger)


def reset_failed_tasks(graph):
    """
    create a new graph based on the outcomes of a previous run.
    if there were errors - only the failed tasks and their children will
    be included in the new graph. otherwise the new graph will be empty
    """
    failed_tasks = set([
        task for task, res in graph.results.items()
        if isinstance(res, Exception)
    ])

    return reset_tasks(graph, failed_tasks)


def reset_tasks(graph, tasks):
    children = set(chain(* [get_all_children(graph, task) for task in tasks]))
    rerun = children | tasks

    for task in rerun:
        graph.results[task] = None
        graph.done.remove(task)

    return create_graph(graph.funcs, graph.dependencies, graph.io_bound,
                        graph.done, graph.results)


def create_graph(funcs, dependencies, io_bound=None, done=None, results=None):
    dependencies = {task: list(deps) for task, deps in dependencies.items()}
    io_bound = io_bound or []
    done = done or []
    results = results or {}

    check_all_tasks_present(dependencies)
    check_cyclic_dependency(dependencies)
    check_all_keys_are_funcs(funcs, dependencies)

    return Graph(
        funcs=funcs,
        dependencies=dependencies,
        in_progress=[],
        done=list(done),
        results=results,
        lock=0,
        io_bound=io_bound)


def check_cyclic_dependency(dependencies):
    ancestry = dict()

    for task, parents in dependencies.items():
        already_seen = set()
        ancestry[task] = set()

        while parents:
            if task in parents:
                raise ValueError('Cyclic dependency: task %s' % task)

            already_seen.update(parents)
            ancestry[task].update(parents)

            new_parents = set()
            for parent in parents:
                new_parents.update(ancestry.get(parent, dependencies[parent]))

            parents = new_parents - already_seen


def check_all_tasks_present(deps):
    absent_tasks = set(chain(*deps.values())) - set(deps.keys())

    if absent_tasks:
        msg = ' '.join([
            'Tasks {} are depended upon, but are not present as',
            'keys in dependencies dictionary.'
        ])
        raise ValueError(msg.format(absent_tasks))


def check_all_keys_are_funcs(funcs, dependencies):
    vacuous_names = set(dependencies.keys()) - set(funcs.keys())
    if vacuous_names:
        msg = ' '.join([
            'Tasks {} are listed in the dependencies dict, but do',
            'not correspond to functions in the funcs dict.'
        ])
        raise ValueError(msg.format(vacuous_names))


def run_task(graph, task, args=None):
    args = args or []
    func = graph.funcs[task]

    try:
        logger.info('pid {}: starting task {}'.format(os.getpid(), task))
        result = func(*args)
        logger.info('pid {}: finished task {}'.format(os.getpid(), task))
    except Exception as error:
        kwargs = {'exc_info': error}
        logger.exception('pid {}: failed task {}'.format(os.getpid(), task),
                         kwargs)
        result = error
        graph = mark_children_as_incomplete(graph, task)

    graph.results[task] = result
    graph = mark_as_done(graph, task)
    return graph


def get_all_children(graph, task):
    all_children = set()
    new_children = {k for k, v in graph.dependencies.items() if task in v}
    while new_children:
        all_children.update(new_children)
        new_children = {
            k
            for child in new_children for k, v in graph.dependencies.items()
            if child in v
        }
        new_children = new_children - all_children

    return all_children


def get_ready_tasks(graph):
    done = graph.done or set()
    in_progress = graph.in_progress or set()
    ready = set()
    for task, deps in graph.dependencies.items():
        if not set(deps) - set(done):
            ready.add(task)
    return ready - set(done) - set(in_progress)


def mark_as_done(graph, task):
    graph.done.append(task)
    return graph


def mark_as_in_progress(graph, task):
    graph.in_progress.append(task)
    return graph


def all_done(graph):
    if graph.done:
        return set(graph.done) == set(graph.dependencies.keys())
    return False


def run(graph):
    while not all_done(graph):
        ready = get_ready_tasks(graph)
        for task in ready:
            args = get_args(graph, task)

            graph = run_task(graph, task, args)
    return graph


def run_parallel(graph, ncores=None, sleep=.1):
    with mp.Pool(ncores or mp.cpu_count() // 2) as pool:
        while not all_done(graph):
            ready = get_ready_tasks(graph)
            for task in ready:
                mark_as_in_progress(graph, task)
                logger.info(
                    'pid {}: claimed task {}'.format(os.getpid(), task))
                args = get_args(graph, task)

                def callback(result, task):
                    graph.results[task] = result
                    mark_as_done(graph, task)
                    logger.info(
                        'pid {}: finished task {}'.format(os.getpid(), task))

                def error_callback(result, task):
                    kwargs = {'exc_info': result}
                    logger.exception('pid {}: failed task {}'.format(
                        os.getpid(), task), kwargs)
                    mark_children_as_incomplete(graph, task)
                    graph.results[task] = result
                    mark_as_done(graph, task)

                call = partial(callback, task=task)
                error_call = partial(error_callback, task=task)
                logger.info(
                    'pid {}: starting task {}'.format(os.getpid(), task))
                pool.apply_async(
                    graph.funcs[task],
                    args=args,
                    callback=call,
                    error_callback=error_call)

            time.sleep(sleep)
    return graph


async def scheduler(graph, sleep, loop):
    while not all_done(graph):
        ready = get_ready_tasks(graph)

        # run io bound tasks first
        overlap = set(ready) & set(graph.io_bound)
        if overlap:
            ready = overlap

        for task in ready:
            mark_as_in_progress(graph, task)
            logger.info('pid {}: claimed task {}'.format(os.getpid(), task))
            args = get_args(graph, task)

            asyncio.ensure_future(run_task_async(graph, task, args), loop=loop)
        await asyncio.sleep(sleep)


def run_async(graph, sleep=.1, scheduler=scheduler):
    """ sleep: how long to wait before checking if a task is done """
    loop = asyncio.new_event_loop()
    loop.run_until_complete(scheduler(graph, sleep, loop))
    loop.close()
    return graph


def create_parallel_compatible_graph(graph, manager):
    deps = manager.dict(graph.dependencies)
    funcs = manager.dict(graph.funcs)
    done = manager.list(graph.done)
    io_bound = manager.list(graph.io_bound)
    results = manager.dict(graph.results)
    return Graph(
        funcs=funcs,
        dependencies=deps,
        done=done,
        results=results,
        in_progress=manager.list(),
        lock=manager.Value(int, 0),
        io_bound=io_bound)


def recover_values_from_manager(graph):
    return Graph(
        dependencies=dict(graph.dependencies),
        in_progress=[],
        funcs=dict(graph.funcs),
        done=list(graph.done),
        lock=0,
        results=dict(graph.results),
        io_bound=list(graph.io_bound))


def run_parallel_async(graph, ncores=None, sleep=.05):
    ncores = ncores or mp.cpu_count() // 2

    with mp.Manager() as manager:
        graph = create_parallel_compatible_graph(graph, manager)

        with mp.Pool(ncores) as pool:
            pool.starmap(run_async,
                         repeat([graph, sleep, parallel_scheduler], ncores))

        return recover_values_from_manager(graph)


async def parallel_scheduler(graph, sleep, loop):
    while not all_done(graph):
        if graph.lock.value == 0:
            graph.lock.value = os.getpid()

        # wait to see if some other pid got assigned due to race condition
        # i.e. if the conditional check above passed 'simultaneously' in two
        # processes, the second one wins (the value itself is processes safe)
        asyncio.sleep(.2)

        if graph.lock.value != os.getpid():
            await asyncio.sleep(sleep)
            continue

        ready = get_ready_tasks(graph)

        if ready:
            # start io bound tasks first
            overlap = set(ready) & set(graph.io_bound)
            if overlap:
                ready = overlap

            task = list(ready)[0]
            mark_as_in_progress(graph, task)
            logger.info('pid {}: claimed task {}'.format(os.getpid(), task))
        else:
            graph.lock.value = 0
            await asyncio.sleep(sleep)
            continue

        args = get_args(graph, task)

        asyncio.ensure_future(run_task_async(graph, task, args), loop=loop)

        graph.lock.value = 0
        await asyncio.sleep(sleep)


async def run_task_async(graph, task, args):
    func = graph.funcs[task]

    try:
        logger.info('pid {}: starting task {}'.format(os.getpid(), task))
        result = await func(*args)
        logger.info('pid {}: finished task {}'.format(os.getpid(), task))
    except Exception as error:
        kwargs = {'exc_info': error}
        logger.exception('pid {}: failed task {}'.format(os.getpid(), task),
                         kwargs)
        result = error
        graph = mark_children_as_incomplete(graph, task)

    graph.results[task] = result
    graph = mark_as_done(graph, task)
    return graph


def mark_children_as_incomplete(graph, task):
    children = get_all_children(graph, task)

    if not children:
        return graph

    logger.info('pid {}: marking children {} of failed task {}'.format(
        os.getpid(), children, task))

    msg = 'Ancestor task {} failed; task not run'.format(task)
    for child in children:
        graph.results[child] = msg
        mark_as_done(graph, child)
    return graph


def get_args(graph, task):
    return [
        graph.results.get(dep) for dep in graph.dependencies[task]
        if graph.results.get(dep) is not None
    ]
