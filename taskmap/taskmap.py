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



def run_task(graph, task, trap_exceptions=True):
    graph = tgraph.mark_as_in_progress(graph, task)
    args = get_task_args(graph, task)
    log(graph).info('pid {}: starting task {}'.format(os.getpid(), task))
    if trap_exceptions:
        try:
            result = graph.funcs[task](*args)
            return task_success(graph, task, result)
        except Exception as error:
            return task_error(graph, task, error)
    else:
        result = graph.funcs[task](*args)
        return task_success(graph, task, result)

async def run_task_async(graph, task, trap_exceptions=True):
    graph = tgraph.mark_as_in_progress(graph, task)
    args = get_task_args(graph, task)
    log(graph).info('pid {}: starting task {}'.format(os.getpid(), task))
    if trap_exceptions:
        try:
            result = await graph.funcs[task](*args)
            return task_success(graph, task, result)
        except Exception as error:
            return task_error(graph, task, error)
    else:
        result = await graph.funcs[task](*args)
        return task_success(graph, task, result)


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


def run(graph, trap_exceptions=True):
    while not tgraph.all_done(graph):
        ready = tgraph.get_ready_tasks(graph)
        for task in ready:
            log(graph).info('pid {}: claiming task {}'.format(os.getpid(), task))
            graph = run_task(graph, task, trap_exceptions)
    return graph


def run_parallel(graph, nprocs=None, sleep=0.2, trap_exceptions=True):
    nprocs = nprocs or mp.cpu_count() - 1
    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)
        with mp.Pool(nprocs) as pool:
            results = []
            while not tgraph.all_done(graph):
                for task in tgraph.get_ready_tasks(graph, reverse=False):
                    graph = tgraph.mark_as_in_progress(graph, task)
                    mlog(graph).info('pid {}: assigning task {}'.format(os.getpid(), task))
                    results += [pool.apply_async(run_task, args=(graph, task, trap_exceptions))]


                time.sleep(sleep)
                if trap_exceptions == False:
                    is_alive = sum([p.successful() for p in results]) == len(results)
                    if not is_alive:
                        failed = [p for p in results if not p.successful()]
                        msg = 'pid {}: failed tasks {}'.format(os.getpid(), failed)
                        mlog(graph).error(msg)
                        pool.terminate()
                        raise Exception(msg)

        return tgraph.recover_values_from_manager(graph)


def run_async(graph, sleep=0.2, coro=None, trap_exceptions=True):
    ioq = asyncio.Queue(len(graph.funcs.keys()))
    cpuq = asyncio.Queue(len(graph.funcs.keys()))
    loop = asyncio.new_event_loop()
    coros = asyncio.gather(
        queue_loader(graph, ioq, cpuq, sleep),
        scheduler(graph, sleep, ioq, cpuq, loop, trap_exceptions),
        loop=loop
    )
    loop.run_until_complete(coros)
    loop.close()
    return graph


def run_parallel_async(graph, nprocs=None, sleep=0.2, trap_exceptions=True):
    if nprocs == 1:
        return run_async(graph)

    nprocs = nprocs or mp.cpu_count() // 2

    with mp.Manager() as manager:
        graph = tgraph.create_parallel_compatible_graph(graph, manager)

        ioq = mp.Queue(len(graph.funcs.keys()))
        cpuq = mp.Queue(len(graph.funcs.keys()))

        processes = []
        for _ in range(nprocs):
            proc = mp.Process(target=run_scheduler, args=(graph, sleep, ioq, cpuq, trap_exceptions))
            proc.start()
            processes += [proc]

        while not tgraph.all_done(graph):
            for task in tgraph.get_ready_tasks(graph):
                graph = tgraph.mark_as_in_progress(graph, task)
                mlog(graph).info('pid {}: queueing task {}'.format(os.getpid(), task))
                if task in graph.io_bound:
                    ioq.put(task)
                else:
                    cpuq.put(task)

            if trap_exceptions == False:
                is_alive = sum([p.is_alive() for p in processes]) == len(processes)
                if not is_alive:
                    failed = [p for p in processes if not p.is_alive()]
                    msg = 'pid {}: failed tasks {}'.format(os.getpid(), failed)
                    mlog(graph).error(msg)
                    map(lambda x: x.terminate(), processes)
                    raise Exception(msg)
            time.sleep(sleep)

        return tgraph.recover_values_from_manager(graph)


def run_scheduler(graph, sleep, ioq, cpuq, trap_exceptions=True):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(scheduler(graph, sleep, ioq, cpuq, loop, trap_exceptions))
    loop.close()


# TODO: scheduler can be improved
async def scheduler(graph, sleep, ioq, cpuq, loop, trap_exceptions):
    if trap_exceptions:
        while not tgraph.all_done(graph):
            try:
                task = ioq.get_nowait()
                log(graph).info('pid {}: dequeueing task {}'.format(os.getpid(), task))
                asyncio.ensure_future(run_task_async(graph, task, True), loop=loop)
            except Exception:
                try:
                    task = cpuq.get_nowait()
                    log(graph).info('pid {}: dequeueing task {}'.format(os.getpid(), task))
                    asyncio.ensure_future(run_task_async(graph, task, True), loop=loop)
                    # don't put two cpu intensive tasks on the same core without waiting
                    await asyncio.sleep(sleep)
                except:
                    await asyncio.sleep(sleep)
    else:
        while not tgraph.all_done(graph):
            task = ioq.get_nowait()
            log(graph).info('pid {}: dequeueing task {}'.format(os.getpid(), task))
            asyncio.ensure_future(run_task_async(graph, task, False), loop=loop)


async def queue_loader(graph, ioq, cpuq, sleep):
    while not tgraph.all_done(graph):
        for task in tgraph.get_ready_tasks(graph):
            graph = tgraph.mark_as_in_progress(graph, task)
            log(graph).info('pid {}: queueing task {}'.format(os.getpid(), task))

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
