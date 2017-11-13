from itertools import chain, repeat
from collections import namedtuple
from bunch import Bunch

import time
import asyncio
import multiprocessing as mp

# dependencies: map from each task to a list of tasks on which it depends
# done: set of names of functions that have been complete
# results: map from task to result of function call
# in_progress: tasks currently in progress (for async and parallel)
Graph = namedtuple('graph', ['dependencies', 'done', 'results', 'in_progress'])


def create_graph(dependencies):
    dependencies = {task: list(deps) for task, deps in dependencies.items()}
    check_all_tasks_present(dependencies)
    check_cyclic_dependency(dependencies)
    return Graph(dependencies=dependencies, done=[], results={}, in_progress=[])


def check_cyclic_dependency(dependencies):
    ancestry = dict()

    for task, parents in dependencies.items():
        ancestry[task] = set()

        while parents:
            if task in parents:
                raise ValueError('Cyclic dependency: task %s' % task.__name__)

            ancestry[task].update(parents)

            new_parents = set()
            for parent in parents:
                new_parents.update(ancestry.get(parent, dependencies[parent]))

            parents = new_parents


def check_all_tasks_present(deps):
    absent_tasks = set(chain(*deps.values())) - set(deps.keys())

    if absent_tasks:
        task_names = [task.__name__ for task in absent_tasks]
        msg = ' '.join(['Tasks {} are depended upon, but are not present as',
                        'keys in dependencies dictionary.'])
        raise ValueError(msg.format(task_names))


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
            args = [graph.results[dep] for dep in graph.dependencies[task]]
            graph.results[task] = task(*args)
            graph = mark_as_done(graph, task)
    return graph.results


def run_parallel(graph, ncores=None, sleep=.01):
    with mp.Pool(ncores or mp.cpu_count() // 2) as pool:
        while not all_done(graph):
            ready = get_ready_tasks(graph)
            for task in ready:
                mark_as_in_progress(graph, task)
                args = [graph.results[dep] for dep in graph.dependencies[task]
                        if graph.results[dep] is not None]

                def callback(result):
                    graph.results[task] = result
                    mark_as_done(graph, task)

                pool.apply_async(task, args=args, callback=callback)

            time.sleep(sleep)
    return graph.results


async def scheduler(graph, sleep, loop):
    while not all_done(graph):
        ready = get_ready_tasks(graph)

        if ready:
            task = list(ready)[0]
            mark_as_in_progress(graph, task)
        else:
            await asyncio.sleep(sleep)
            continue

        args = [graph.results[dep] for dep in graph.dependencies[task]
                if graph.results[dep] is not None]

        async def coro():
            result = await task(*args)
            graph.results[task] = result
            mark_as_done(graph, task)

        asyncio.ensure_future(coro(), loop=loop)

        await asyncio.sleep(sleep)


def run_async(graph, sleep=.01, scheduler=scheduler):
    """ sleep: how long to wait before checking if a task is done """
    loop = asyncio.new_event_loop()
    loop.run_until_complete(scheduler(graph, sleep, loop))
    loop.close()
    return graph.results


def create_parallel_compatible_graph(graph, manager):
    deps = manager.dict(graph.dependencies)
    return Bunch(dependencies=deps, done=manager.list(),
                 results=manager.dict(), in_progress=manager.list(),
                 lock=manager.Value(int, 0))


def run_parallel_async(graph, ncores=None, sleep=.01):
    ncores = ncores or mp.cpu_count() // 2

    with mp.Manager() as manager:
        graph = create_parallel_compatible_graph(graph, manager)

        with mp.Pool(ncores) as pool:
            pool.starmap(run_async, repeat([graph, sleep, parallel_scheduler], ncores))

        return dict(graph.results)


async def parallel_scheduler(graph, sleep, loop):
    while not all_done(graph):
        if graph.lock.value == 1:
            await asyncio.sleep(sleep)
            continue

        graph.lock.value = 1

        ready = get_ready_tasks(graph)

        if ready:
            task = list(ready)[0]
            mark_as_in_progress(graph, task)
        else:
            await asyncio.sleep(sleep)
            graph.lock.value = 0
            continue

        args = [graph.results[dep] for dep in graph.dependencies[task]
                if graph.results[dep] is not None]

        async def coro():
            result = await task(*args)
            graph.results[task] = result
            mark_as_done(graph, task)

        asyncio.ensure_future(coro(), loop=loop)

        graph.lock.value = 0
        await asyncio.sleep(sleep)
