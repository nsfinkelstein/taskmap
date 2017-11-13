from collections import namedtuple
from itertools import chain

# dependencies: map from each task to the set of tasks on which it depends
# done: set of names of functions that have been complete
graph = namedtuple('graph', ['dependencies', 'done'])


def create_graph(dependencies):
    dependencies = {task: set(deps) for task, deps in dependencies.items()}
    check_all_tasks_present(dependencies)
    check_cyclic_dependency(dependencies)
    return graph(dependencies=dependencies, done=set())


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
    return ready - graph.done


def mark_as_done(graph, func):
    graph.done.add(func)
    return graph
