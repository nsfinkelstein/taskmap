from taskmap import tgraph

import os
import time
import asyncio
import logging
import datetime as dt
import multiprocess as mp
import multiprocessing_logging as mplogging


mlogger = logging.getLogger('taskmap-manager')
mlogger.setLevel(logging.DEBUG)

logger = logging.getLogger('taskmap-worker')
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

mlogger.addHandler(ch)
mlogger.addHandler(fh)


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
            logger.info('pid {}: claiming task {}'.format(os.getpid(), task))
            graph = run_task(graph, task)
    return graph


def run_parallel(graph, nprocs=None, sleep=0.2):
    nprocs = nprocs or mp.cpu_count() - 1
    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)
        with mp.Pool(nprocs) as pool:
            while not tgraph.all_done(graph):
                for task in tgraph.get_ready_tasks(graph, reverse=False):
                    graph = tgraph.mark_as_in_progress(graph, task)
                    mlogger.info('pid {}: assigning task {}'.format(os.getpid(), task))
                    pool.apply_async(run_task, args=(graph, task))
                time.sleep(sleep)
        return tgraph.recover_values_from_manager(graph)


def run_async(graph, sleep=0.2, coro=None):
    ioq = asyncio.Queue(len(graph.funcs.keys()))
    cpuq = asyncio.Queue(len(graph.funcs.keys()))
    loop = asyncio.new_event_loop()
    coros = asyncio.gather(
        queue_loader(graph, ioq, cpuq, sleep),
        scheduler(graph, sleep, ioq, cpuq, loop),
        loop=loop
    )
    loop.run_until_complete(coros)
    loop.close()
    return graph


def run_parallel_async(graph, nprocs=None, sleep=0.2):
    if nprocs == 1:
        return run_async(graph)

    nprocs = nprocs or mp.cpu_count() // 2

    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)

        ioq = mp.Queue(len(graph.funcs.keys()))
        cpuq = mp.Queue(len(graph.funcs.keys()))

        for _ in range(nprocs):
            proc = mp.Process(target=run_scheduler, args=(graph, sleep, ioq, cpuq))
            proc.start()

        while not tgraph.all_done(graph):
            for task in tgraph.get_ready_tasks(graph):
                graph = tgraph.mark_as_in_progress(graph, task)
                mlogger.info('pid {}: queueing task {}'.format(os.getpid(), task))
                if task in graph.io_bound:
                    ioq.put(task)
                else:
                    cpuq.put(task)

            time.sleep(sleep)

        return tgraph.recover_values_from_manager(graph)


def run_scheduler(graph, sleep, ioq, cpuq):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(scheduler(graph, sleep, ioq, cpuq, loop))
    loop.close()


# TODO: scheduler can be improved
async def scheduler(graph, sleep, ioq, cpuq, loop):
    while not tgraph.all_done(graph):
        try:
            task = ioq.get_nowait()
            logger.info('pid {}: dequeueing task {}'.format(os.getpid(), task))
            asyncio.ensure_future(run_task_async(graph, task), loop=loop)
        except Exception:
            try:
                task = cpuq.get_nowait()
                logger.info('pid {}: dequeueing task {}'.format(os.getpid(), task))
                asyncio.ensure_future(run_task_async(graph, task), loop=loop)
                # don't put two cpu intensive tasks on the same core without waiting
                await asyncio.sleep(sleep)
            except:
                await asyncio.sleep(sleep)


async def queue_loader(graph, ioq, cpuq, sleep):
    while not tgraph.all_done(graph):
        for task in tgraph.get_ready_tasks(graph):
            graph = tgraph.mark_as_in_progress(graph, task)
            logger.info('pid {}: queueing task {}'.format(os.getpid(), task))

            if task in graph.io_bound:
                await ioq.put(task)
            else:
                await cpuq.put(task)

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
