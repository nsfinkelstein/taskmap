from collections import namedtuple
from itertools import chain

import multiprocessing as mp

# dependencies: map from each task to a list of tasks on which it depends
# done: set of names of functions that have been complete
# results: map from task to result of function call
# in_progress: tasks currently in progress (for async and parallel)
graph = namedtuple('graph', ['dependencies', 'done', 'results', 'in_progress'])


def create_graph(dependencies):
    dependencies = {task: set(deps) for task, deps in dependencies.items()}
    check_all_tasks_present(dependencies)
    check_cyclic_dependency(dependencies)
    return graph(dependencies=dependencies, done=set(), results=dict(),
                 in_progress=set())


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
    ready = set()
    for task, deps in graph.dependencies.items():
        if not deps - graph.done:
            ready.add(task)
    return ready - graph.done - graph.in_progress


def mark_as_done(graph, task):
    graph.done.add(task)
    graph.in_progress.discard(task)
    return graph


def mark_as_in_progress(graph, task):
    graph.in_progress.add(task)
    return graph


def all_done(graph):
    return graph.done == graph.dependencies.keys()


def run(graph):
    while not all_done(graph):
        ready = get_ready_tasks(graph)
        for task in ready:
            args = [graph.results[dep] for dep in graph.dependencies[task]]
            graph.results[task] = task(*args)
            graph = mark_as_done(graph, task)
    return graph


def run_parallel(graph, ncores=None):
    with mp.Pool(ncores or mp.cpu_count() // 2) as pool:
        while not all_done(graph):
            ready = get_ready_tasks(graph)
            for task in ready:
                args = [graph.results[dep] for dep in graph.dependencies[task]]

                def callback(result):
                    mark_as_done(graph, task)
                    graph.results[task] = result

                pool.apply_async(task, args=args, callback=callback)
                mark_as_in_progress(graph, task)
    return graph
