from itertools import chain, repeat
from collections import namedtuple
from functools import partial

import os
import time
import asyncio
import multiprocess as mp

# TODO: Error handling
# create an 'error' property
# then move all children to 'done' with 'results' value 'not done; parent error'

# dependencies: map from each task to a list of tasks on which it depends
# done: set of names of functions that have been complete
# results: map from task to result of function call
# in_progress: tasks currently in progress (for async and parallel)
Graph = namedtuple('graph', ['funcs', 'dependencies', 'done', 'results',
                             'in_progress', 'lock', 'io_bound'])


def create_graph(funcs, dependencies, io_bound=None):
    dependencies = {task: list(deps) for task, deps in dependencies.items()}
    io_bound = set(io_bound) if io_bound else set()

    check_all_tasks_present(dependencies)
    check_cyclic_dependency(dependencies)
    check_all_keys_are_funcs(funcs, dependencies)

    marked_funcs = {}
    for name, func in funcs.items():
        if not hasattr(func, 'bottleneck'):
            func.bottleneck = None
        marked_funcs[name] = func

    return Graph(funcs=marked_funcs, dependencies=dependencies, done=[],
                 results={}, in_progress=[], lock=0, io_bound=io_bound)


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
        msg = ' '.join(['Tasks {} are depended upon, but are not present as',
                        'keys in dependencies dictionary.'])
        raise ValueError(msg.format(absent_tasks))


def check_all_keys_are_funcs(funcs, dependencies):
    vacuous_names = set(dependencies.keys()) - set(funcs.keys())
    if vacuous_names:
        msg = ' '.join(['Tasks {} are listed in the dependencies dict, but do',
                        'not correspond to functions in the funcs dict.'])
        raise ValueError(msg.format(vacuous_names))


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
    try:
        graph.in_progress.remove(task)
    except ValueError:
        pass

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
            args = [graph.results[dep] for dep in graph.dependencies[task]
                    if graph.results[dep] is not None]
            graph.results[task] = graph.funcs[task](*args)
            graph = mark_as_done(graph, task)
    return graph.results


def run_parallel(graph, ncores=None, sleep=.1):
    with mp.Pool(ncores or mp.cpu_count() // 2) as pool:
        while not all_done(graph):
            ready = get_ready_tasks(graph)
            for task in ready:
                mark_as_in_progress(graph, task)
                args = [graph.results[dep] for dep in graph.dependencies[task]
                        if graph.results[dep] is not None]

                def callback(result, task):
                    graph.results[task] = result
                    mark_as_done(graph, task)

                call = partial(callback, task=task)
                pool.apply_async(graph.funcs[task], args=args, callback=call)

            time.sleep(sleep)
    return graph.results


async def scheduler(graph, sleep, loop):
    while not all_done(graph):
        ready = get_ready_tasks(graph)

        # run io bound tasks first
        overlap = set(ready) & set(graph.io_bound)
        if overlap:
            ready = overlap

        for task in ready:
            mark_as_in_progress(graph, task)
            args = [graph.results[dep] for dep in graph.dependencies[task]
                    if graph.results[dep] is not None]

            async def coro(task):
                result = await graph.funcs[task](*args)
                graph.results[task] = result
                mark_as_done(graph, task)

            asyncio.ensure_future(coro(task), loop=loop)
        await asyncio.sleep(sleep)


def run_async(graph, sleep=.1, scheduler=scheduler):
    """ sleep: how long to wait before checking if a task is done """
    loop = asyncio.new_event_loop()
    loop.run_until_complete(scheduler(graph, sleep, loop))
    loop.close()
    return graph.results


def create_parallel_compatible_graph(graph, manager):
    deps = manager.dict(graph.dependencies)
    funcs = manager.dict(graph.funcs)
    io_bound = manager.list(graph.io_bound)
    return Graph(funcs=funcs, dependencies=deps, done=manager.list(),
                 results=manager.dict(), in_progress=manager.list(),
                 lock=manager.Value(int, 0), io_bound=io_bound)


def run_parallel_async(graph, ncores=None, sleep=.05):
    ncores = ncores or mp.cpu_count() // 2

    with mp.Manager() as manager:
        graph = create_parallel_compatible_graph(graph, manager)

        with mp.Pool(ncores) as pool:
            pool.starmap(run_async, repeat([graph, sleep, parallel_scheduler], ncores))

        return dict(graph.results)


async def parallel_scheduler(graph, sleep, loop):
    while not all_done(graph):
        if graph.lock.value == 0:
            graph.lock.value = os.getpid()

        # wait to see if some other pid got assigned due to race condition
        # i.e. if the conditional check above passed 'simultaneously' in two
        # processes, the second one wins (the value itself is processes safe)
        time.sleep(sleep)

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
        else:
            graph.lock.value = 0
            await asyncio.sleep(sleep)
            continue

        args = [graph.results[dep] for dep in graph.dependencies[task]
                if graph.results[dep] is not None]

        async def coro(task):
            result = await graph.funcs[task](*args)
            graph.results[task] = result
            mark_as_done(graph, task)

        asyncio.ensure_future(coro(task), loop=loop)

        graph.lock.value = 0
        await asyncio.sleep(sleep)
