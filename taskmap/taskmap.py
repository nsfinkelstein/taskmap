from taskmap import tgraph

import os
import time
import queue
import asyncio
import logging
import datetime as dt
import multiprocess as mp
import multiprocessing_logging as mplogging

from operator import contains
from functools import partial

logger = logging.getLogger('taskmap')
logger.setLevel(logging.DEBUG)

now = dt.datetime.now()
logname_frmt = 'taskmap{}.log'.format(now.strftime('%m-%d-%Y:%H.%M.%S'))
fh = logging.FileHandler(logname_frmt)

fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)
mplogging.install_mp_handler(logger)


def run_task(graph, task):
    try:
        graph = tgraph.mark_as_in_progress(graph, task)
        args = get_task_args(graph, task)
        logger.info('pid {}: starting task {}'.format(os.getpid(), task))
        result = graph.funcs[task](*args)
        return task_success(graph, task, result)
    except Exception as error:
        return task_error(graph, task, error)


async def run_task_async(graph, task):
    try:
        graph = tgraph.mark_as_in_progress(graph, task)
        args = get_task_args(graph, task)
        logger.info('pid {}: starting task {}'.format(os.getpid(), task))
        result = await graph.funcs[task](*args)
        return task_success(graph, task, result)
    except Exception as error:
        return task_error(graph, task, error)


def task_success(graph, task, result):
    logger.info('pid {}: finished task {}'.format(os.getpid(), task))
    graph.results[task] = result
    return tgraph.mark_as_done(graph, task)


def task_error(graph, task, error):
    msg = 'pid {}: failed task {}'.format(os.getpid(), task)
    logger.exception(msg, {'exc_info': error})
    graph.results[task] = error
    graph = tgraph.mark_as_done(graph, task)
    return mark_children_as_incomplete(graph, task)


def run(graph):
    while not tgraph.all_done(graph):
        ready = tgraph.get_ready_tasks(graph)
        for task in ready:
            graph = run_task(graph, task)
    return graph


def run_parallel(graph, nprocs=None, sleep=0.01):
    nprocs = nprocs or mp.cpu_count() - 1
    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)
        with mp.Pool(nprocs) as pool:
            while not tgraph.all_done(graph):
                for task in tgraph.get_ready_tasks(graph):
                    pool.apply_async(run_task, args=(graph, task))
                time.sleep(sleep)
        return tgraph.recover_values_from_manager(graph)


def run_parallel_async(graph, nprocs=None, sleep=0.01):
    nprocs = nprocs or mp.cpu_count() // 2

    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)

        q = mp.Queue(len(graph.funcs.keys()))
        for _ in range(nprocs):
            proc = mp.Process(target=run_scheduler, args=(graph, sleep, q))
            proc.start()

        while not tgraph.all_done(graph):
            key = partial(contains, graph.io_bound)
            ready = sorted(
                tgraph.get_ready_tasks(graph), key=key, reverse=True)
            for task in ready:
                graph = tgraph.mark_as_in_progress(graph, task)
                q.put(task)

            time.sleep(sleep)

        return tgraph.recover_values_from_manager(graph)


async def scheduler(graph, sleep, q, loop):
    while not tgraph.all_done(graph):
        try:
            task = q.get_nowait()
            asyncio.ensure_future(run_task_async(graph, task), loop=loop)
        except queue.Empty:
            await asyncio.sleep(sleep)
        except asyncio.QueueEmpty:
            await asyncio.sleep(sleep)


def mark_children_as_incomplete(graph, task):
    children = tgraph.get_all_children(graph, task)

    if not children:
        return graph

    logger.info('pid {}: marking children {} of failed task {}'.format(
        os.getpid(), children, task))

    msg = 'Ancestor task {} failed; task not run'.format(task)
    for child in children:
        graph.results[child] = msg
        tgraph.mark_as_done(graph, child)
    return graph


def get_task_args(graph, task):
    return [
        graph.results.get(dep) for dep in graph.dependencies[task]
        if graph.results.get(dep) is not None
    ]


def run_async(graph=None, sleep=0.01, coro=None):
    " either graph or coro has to be non-none"
    q = asyncio.Queue(len(graph.funcs.keys()))
    loop = asyncio.new_event_loop()
    coros = asyncio.gather(
        queue_loader(graph, q, sleep),
        scheduler(graph, sleep, q, loop),
        loop=loop
    )
    loop.run_until_complete(coros)
    loop.close()
    return graph


def run_scheduler(graph, sleep, q):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(scheduler(graph, sleep, q, loop))
    loop.close()


async def queue_loader(graph, q, sleep):
    while not tgraph.all_done(graph):
        key = partial(contains, graph.io_bound)
        ready = sorted(tgraph.get_ready_tasks(graph), key=key, reverse=True)
        for task in ready:
            graph = tgraph.mark_as_in_progress(graph, task)
            await q.put(task)
        await asyncio.sleep(sleep)
