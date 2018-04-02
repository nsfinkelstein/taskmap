from taskmap import tgraph

import os
import time
import asyncio
import logging
import traceback
import multiprocess as mp


def log(graph):
    return logging.getLogger('{}-worker'.format(graph.name))


def mlog(graph):
    return logging.getLogger('{}-manager'.format(graph.name))


def run_task(graph, task, raise_errors=False):
    graph = tgraph.mark_as_in_progress(graph, task)
    args = get_task_args(graph, task)
    log(graph).info('pid {}: starting task {}'.format(os.getpid(), task))

    try:
        result = graph.funcs[task](*args)
        return task_success(graph, task, result)

    except Exception as error:
        graph = task_error(graph, task, error)
        if raise_errors:
            raise
        return graph


async def run_task_async(graph, task, raise_errors=False):
    graph = tgraph.mark_as_in_progress(graph, task)
    args = get_task_args(graph, task)
    log(graph).info('pid {}: starting task {}'.format(os.getpid(), task))

    try:
        result = await graph.funcs[task](*args)
        return task_success(graph, task, result)

    except Exception as error:
        graph = task_error(graph, task, error)
        if raise_errors:
            raise
        return graph


def task_success(graph, task, result):
    log(graph).info('pid {}: finished task {}'.format(os.getpid(), task))
    graph.results[task] = result
    return tgraph.mark_as_done(graph, task)


def task_error(graph, task, error):
    tb = traceback.format_exc()
    msg = 'pid {}: failed task {}: stack {}'.format(os.getpid(), task, tb)
    log(graph).exception(msg, {'exc_info': error})
    graph.results[task] = error
    graph = tgraph.mark_as_done(graph, task)
    return mark_children_as_incomplete(graph, task)


def run(graph, raise_errors=False):
    while not tgraph.all_done(graph):
        ready = tgraph.get_ready_tasks(graph)
        for task in ready:
            log(graph).info('pid {}: claiming task {}'.format(os.getpid(), task))
            graph = run_task(graph, task, raise_errors)
    return graph


def run_parallel(graph, nprocs=None, sleep=0.2, raise_errors=False):
    nprocs = nprocs or mp.cpu_count() - 1
    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)
        with mp.Pool(nprocs) as pool:

            exception_q = mp.Queue(10)

            def error_callback(exception):
                exception_q.put_nowait(exception)
                pool.terminate()

            while not tgraph.all_done(graph):
                for task in tgraph.get_ready_tasks(graph, reverse=False):
                    graph = tgraph.mark_as_in_progress(graph, task)
                    mlog(graph).info(
                        'pid {}: assigning task {}'.format(os.getpid(), task))
                    pool.apply_async(
                        run_task, args=(graph, task, raise_errors),
                        error_callback=error_callback
                    )
                time.sleep(sleep)

                if not exception_q.empty():
                    raise exception_q.get()

        return tgraph.recover_values_from_manager(graph)


def exception_handler(loop, context):
    # workaround for the fact that asyncio will not let you stop on exceptions
    # for tasks added to the loop after it has already started running
    loop.stop()


def run_async(graph, sleep=0.2, coro=None, raise_errors=False):
    ioq = asyncio.Queue(len(graph.funcs.keys()))
    cpuq = asyncio.Queue(len(graph.funcs.keys()))
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(exception_handler)
    coros = asyncio.gather(
        queue_loader(graph, ioq, cpuq, sleep),
        scheduler(graph, sleep, ioq, cpuq, loop, raise_errors),
        loop=loop)

    try:
        loop.run_until_complete(coros)
    except Exception as error:
        raise RuntimeError('An async task has failed. Please check your logs')
    finally:
        loop.close()

    return graph


def run_parallel_async(graph, nprocs=None, sleep=0.2, raise_errors=False):
    if nprocs == 1:
        return run_async(graph, sleep=sleep, raise_errors=raise_errors)

    nprocs = nprocs or mp.cpu_count() // 2

    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)

        ioq = mp.Queue(len(graph.funcs.keys()))
        cpuq = mp.Queue(len(graph.funcs.keys()))

        procs = [mp.Process(target=run_scheduler,
                            args=(graph, sleep, ioq, cpuq, raise_errors))
                            for _ in range(nprocs)]
        for proc in procs:
            proc.start()

        while not tgraph.all_done(graph):
            for task in tgraph.get_ready_tasks(graph):
                graph = tgraph.mark_as_in_progress(graph, task)
                mlog(graph).info(
                    'pid {}: queueing task {}'.format(os.getpid(), task))
                if task in graph.io_bound:
                    ioq.put(task)
                else:
                    cpuq.put(task)

            time.sleep(sleep)

            if raise_errors and sum(not p.is_alive() for p in procs):
                raise RuntimeError('An async task has failed. Please check your logs')

        return tgraph.recover_values_from_manager(graph)


def run_scheduler(graph, sleep, ioq, cpuq, raise_errors=False):
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(exception_handler)
    try:
        loop.run_until_complete(
            scheduler(graph, sleep, ioq, cpuq, loop, raise_errors))
    except Exception as error:
        raise RuntimeError('An async task has failed. Please check your logs')
    finally:
        loop.close()


# TODO: scheduler can be improved
async def scheduler(graph, sleep, ioq, cpuq, loop, raise_errors):
    while not tgraph.all_done(graph):
        try:
            task = ioq.get_nowait()
            log(graph).info(
                'pid {}: dequeueing task {}'.format(os.getpid(), task))
            asyncio.ensure_future(
                run_task_async(graph, task, raise_errors), loop=loop)
        except Exception:
            try:
                task = cpuq.get_nowait()
                log(graph).info(
                    'pid {}: dequeueing task {}'.format(os.getpid(), task))
                asyncio.ensure_future(
                    run_task_async(graph, task, raise_errors), loop=loop)
                # don't put two cpu intensive tasks on the same core without waiting
                await asyncio.sleep(sleep)
            except Exception:
                await asyncio.sleep(sleep)


async def queue_loader(graph, ioq, cpuq, sleep):
    while not tgraph.all_done(graph):
        for task in tgraph.get_ready_tasks(graph):
            graph = tgraph.mark_as_in_progress(graph, task)
            log(graph).info(
                'pid {}: queueing task {}'.format(os.getpid(), task))

            if task in graph.io_bound:
                await ioq.put(task)
            else:
                await cpuq.put(task)

        await asyncio.sleep(sleep)


def mark_children_as_incomplete(graph, task):
    children = tgraph.get_all_children(graph, task)

    if not children:
        return graph

    log(graph).info('pid {}: marking children {} of failed task {}'.format(
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
