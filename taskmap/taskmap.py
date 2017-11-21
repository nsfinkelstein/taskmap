from taskmap import tgraph
from itertools import repeat

import os
import time
import asyncio
import logging
import datetime as dt
import multiprocess as mp
import multiprocessing_logging as mplogging

logger = logging.getLogger('taskmap')
logger.setLevel(logging.DEBUG)

now = dt.datetime.now()
logname_frmt = 'taskmap{}.log'.format(now.strftime('%m-%d-%Y:%H.%M.%S'))
fh = logging.FileHandler(logname_frmt)

fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)
mplogging.install_mp_handler(logger)


def run_task(graph, task):
    graph = tgraph.mark_as_in_progress(graph, task)
    args = get_task_args(graph, task)
    try:
        logger.info('pid {}: starting task {}'.format(os.getpid(), task))
        result = graph.funcs[task](*args)
        logger.info('pid {}: finished task {}'.format(os.getpid(), task))
    except Exception as error:
        kwargs = {'exc_info': error}
        logger.exception('pid {}: failed task {}'.format(os.getpid(), task),
                         kwargs)
        result = error
        graph = mark_children_as_incomplete(graph, task)

    graph.results[task] = result
    graph = tgraph.mark_as_done(graph, task)
    return graph


async def run_task_async(graph, task):
    args = get_task_args(graph, task)
    try:
        logger.info('pid {}: starting task {}'.format(os.getpid(), task))
        result = await graph.funcs[task](*args)
        logger.info('pid {}: finished task {}'.format(os.getpid(), task))
    except Exception as error:
        kwargs = {'exc_info': error}
        logger.exception('pid {}: failed task {}'.format(os.getpid(), task),
                         kwargs)
        result = error
        graph = mark_children_as_incomplete(graph, task)

    graph.results[task] = result
    graph = tgraph.mark_as_done(graph, task)
    return graph


def run(graph):
    while not tgraph.all_done(graph):
        ready = tgraph.get_ready_tasks(graph)
        for task in ready:
            graph = run_task(graph, task)
    return graph


def run_parallel(graph, sleep=.1):
    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)
        while not tgraph.all_done(graph):
            for task in tgraph.get_ready_tasks(graph):
                proc = mp.Process(target=run_task, args=(graph, task))
                proc.start()
            time.sleep(sleep)
        return tgraph.recover_values_from_manager(graph)


def run_parallel_async(graph, nprocs=None, sleep=.05):
    nprocs = nprocs or mp.cpu_count() // 2

    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)

        for _ in range(nprocs):
            proc = mp.Process(target=run_async, args=(graph, sleep, parallel_scheduler))
            proc.start()

        while True:
            if tgraph.all_done(graph):
                return tgraph.recover_values_from_manager(graph)
            time.sleep(sleep)


async def parallel_scheduler(graph, sleep, loop):
    while not tgraph.all_done(graph):
        if graph.lock.value == 0:
            graph.lock.value = os.getpid()

        # wait to see if some other pid got assigned due to race condition
        # i.e. if the conditional check above passed 'simultaneously' in two
        # processes, the second one wins (the value itself is processes safe)
        asyncio.sleep(.2)

        if graph.lock.value != os.getpid():
            await asyncio.sleep(sleep)
            continue

        ready = tgraph.get_ready_tasks(graph)

        if ready:
            # start io bound tasks first
            overlap = set(ready) & set(graph.io_bound)
            if overlap:
                ready = overlap

            task = list(ready)[0]
            tgraph.mark_as_in_progress(graph, task)
            logger.info('pid {}: claimed task {}'.format(os.getpid(), task))
        else:
            graph.lock.value = 0
            await asyncio.sleep(sleep)
            continue

        asyncio.ensure_future(run_task_async(graph, task), loop=loop)

        graph.lock.value = 0
        await asyncio.sleep(sleep)


async def scheduler(graph, sleep, loop):
    while not tgraph.all_done(graph):
        ready = tgraph.get_ready_tasks(graph)

        # run io bound tasks first
        overlap = set(ready) & set(graph.io_bound)
        if overlap:
            ready = overlap

        for task in ready:
            tgraph.mark_as_in_progress(graph, task)
            logger.info('pid {}: claimed task {}'.format(os.getpid(), task))
            asyncio.ensure_future(run_task_async(graph, task), loop=loop)

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


def run_async(graph, sleep=.1, scheduler=scheduler):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(scheduler(graph, sleep, loop))
    loop.close()
    return graph
