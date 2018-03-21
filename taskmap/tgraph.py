import logging
import datetime as dt
import multiprocessing_logging as mplogging

from itertools import chain
from operator import contains
from functools import partial
from collections import namedtuple

Graph = namedtuple('graph', [
    'funcs', 'dependencies', 'done', 'results', 'in_progress', 'lock',
    'io_bound', 'name'
])


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
    rerun = children | set(tasks)

    for task in rerun:
        if task in graph.done:
            graph.results[task] = None
            graph.done.remove(task)

    return graph


def create_graph(funcs, dependencies, io_bound=None, done=None, results=None,
                 name='taskmap', logging_config=None):
    """
    logging_config is expected to be a dictionary. the keys can be 'name',
    which names the loggers to be used, and 'write', which specificies whether
    the log is written to disk. Note if two graphs with the same name are
    created, only the logging config from the first will be used.
    """
    defaults = {'name': name, 'write': False}
    setup_loggers({**defaults, **(logging_config or {})})

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
        io_bound=io_bound,
        name=name
    )


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


def get_ready_tasks(graph, reverse=True):
    done = set(graph.done) or set()
    in_progress = graph.in_progress or set()
    ready = set()
    for task, deps in graph.dependencies.items():
        if not set(deps) - done:
            ready.add(task)
    ready = list(ready - done - set(in_progress))
    key = partial(contains, graph.io_bound)
    return sorted(ready, key=key, reverse=reverse)


def mark_as_done_except(graph, task):
    if type(task) == str:
        task = [task]

    all_tasks = graph.dependencies.keys()

    for t in set(all_tasks) - set(graph.done) - set(task):
        graph.done.append(t)

    return graph


def mark_as_done(graph, task):
    if type(task) == str:
        task = [task]

    for t in set(task) - set(graph.done):
        graph.done.append(t)

    return graph


def mark_as_in_progress(graph, task):
    graph.in_progress.append(task)
    return graph


def all_done(graph):
    return set(graph.done) == set(graph.dependencies.keys())


def setup_loggers(config):
    name = config.get('name', 'taskmap')
    level = config.get('level', logging.DEBUG)

    if logging.getLogger('{}-manager'.format(name)).handlers:
        # we've already configured these loggers
        return

    mlogger = logging.getLogger('{}-manager'.format(name))
    mlogger.setLevel(level)

    logger = logging.getLogger('{}-worker'.format(name))
    logger.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(level)
    logger.addHandler(ch)
    mlogger.addHandler(ch)

    if config.get('write', True):
        now = dt.datetime.now()
        logname_frmt = '{}{}.log'.format(name, now.strftime('%m-%d-%Y:%H.%M.%S'))
        fh = logging.FileHandler(logname_frmt)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        mlogger.addHandler(fh)

    mplogging.install_mp_handler(logger)


def create_parallel_compatible_graph(graph, manager):
    return Graph(
        funcs=manager.dict(graph.funcs),
        dependencies=manager.dict(graph.dependencies),
        done=manager.list(graph.done),
        results=manager.dict(graph.results),
        in_progress=manager.list(),
        lock=manager.Value(int, 0),
        io_bound=manager.list(graph.io_bound),
        name=graph.name)


def recover_values_from_manager(graph):
    return Graph(
        lock=0,
        in_progress=[],
        done=list(graph.done),
        funcs=dict(graph.funcs),
        results=dict(graph.results),
        io_bound=list(graph.io_bound),
        dependencies=dict(graph.dependencies),
        name=graph.name)
