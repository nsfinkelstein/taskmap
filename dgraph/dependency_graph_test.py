import asyncio
import pytest
import time

import dgraph


def a():
    return 5


def b(x):
    return x + 10


def c(x, y):
    return x + y + 20


def test_graph_ready():
    # given
    dependencies = {
        'a': {'b', 'c'},
        'b': {'c'},
        'c': set(),
    }

    funcs = {
        'a': a,
        'b': b,
        'c': c,
    }

    graph = dgraph.create_graph(funcs, dependencies)

    # when
    results = dgraph.get_ready_tasks(graph)

    # then
    assert results == {'c'}


def test_graph_ready_after_task_completed():
    # given
    dependencies = {
        'a': {'b', 'c'},
        'b': {'c'},
        'c': set(),
    }

    funcs = {
        'a': a,
        'b': b,
        'c': c,
    }

    graph = dgraph.create_graph(funcs, dependencies)
    ready = dgraph.get_ready_tasks(graph)

    # when
    for func in ready:
        dgraph.mark_as_done(graph, func)

    results = dgraph.get_ready_tasks(graph)

    # then
    assert results == {'b'}


def test_cyclic_dependency():
    # given
    dependencies = {
        'a': {'b'},
        'b': {'c'},
        'c': {'a'},
    }

    funcs = {
        'a': a,
        'b': b,
        'c': c,
    }

    # then
    with pytest.raises(ValueError):

        # when
        dgraph.create_graph(funcs, dependencies)


def test_absent_tasks():
    # given
    dependencies = {
        'a': {'b', 'c'},
    }

    funcs = {
        'a': a,
        'b': b,
        'c': c,
    }

    # then
    with pytest.raises(ValueError):

        # when
        dgraph.create_graph(funcs, dependencies)


def test_all_names_are_funcs():
    # given
    dependencies = {'d': ['a'], 'a': []}

    funcs = {'a': a, 'b': b, 'c': c}

    # then
    with pytest.raises(ValueError):

        # when
        dgraph.create_graph(funcs, dependencies)


def test_run_pass_args():
    # given
    dependencies = {
        'c': ['a', 'b'],
        'b': ['a'],
        'a': [],

    }

    funcs = {
        'a': a,
        'b': b,
        'c': c,
    }

    graph = dgraph.create_graph(funcs, dependencies)

    # when
    results = dgraph.run(graph)

    # then
    assert results == {'a': 5, 'b': 15, 'c': 40}


def long_task():
    time.sleep(.02)
    return 5


def test_run_parallel():
    # given
    dependencies = {
        'c': ['long_task', 'b'],
        'b': ['long_task'],
        'long_task': [],

    }

    funcs = {
        'long_task': long_task,
        'b': b,
        'c': c,
    }

    graph = dgraph.create_graph(funcs, dependencies)

    # when
    results = dgraph.run_parallel(graph)

    # then
    assert results == {'long_task': 5, 'b': 15, 'c': 40}


async def ab(x):
    return x + 10


async def ac(x, y):
    return x + y + 20


async def along_task():
    await asyncio.sleep(.02)
    return 5


def test_run_async():
    # given
    dependencies = {
        'ac': ['along_task', 'ab'],
        'ab': ['along_task'],
        'along_task': [],

    }

    funcs = {
        'along_task': along_task,
        'ab': ab,
        'ac': ac,
    }

    graph = dgraph.create_graph(funcs, dependencies)

    # when
    results = dgraph.run_async(graph)

    # then
    assert results == {'along_task': 5, 'ab': 15, 'ac': 40}


def test_run_parllel_async():
    # given
    dependencies = {
        'ac': ['along_task', 'ab'],
        'ab': ['along_task'],
        'along_task': [],

    }

    funcs = {
        'along_task': along_task,
        'ab': ab,
        'ac': ac,
    }

    graph = dgraph.create_graph(funcs, dependencies)

    # when
    results = dgraph.run_parallel_async(graph)

    # then
    assert results == {'along_task': 5, 'ab': 15, 'ac': 40}


async def x():
    await asyncio.sleep(.5)
    return 5


async def y():
    await asyncio.sleep(.5)
    return 5


def test_async_speed():
    # given
    funcs = {'x': x, 'y': y}
    dependencies = {'x': [], 'y': []}
    graph = dgraph.create_graph(funcs, dependencies)

    # when
    start = time.time()
    dgraph.run_async(graph)
    end = time.time()

    # then
    assert end - start < 1


def v():
    time.sleep(.5)
    return 5


def u():
    time.sleep(.5)
    return 5


def test_parallel_speed():
    # given
    funcs = {'x': u, 'y': v}
    dependencies = {'x': [], 'y': []}
    graph = dgraph.create_graph(funcs, dependencies)

    # when
    start = time.time()
    dgraph.run_parallel(graph)
    end = time.time()

    # then
    assert end - start < 1


async def r():
    await asyncio.sleep(1)


async def t():
    await asyncio.sleep(1)


async def w():
    time.sleep(1)


async def p():
    time.sleep(1)


def test_async_parallel_speed():
    # given
    funcs = {'r': r, 't': t, 'w': w, 'p': p}
    dependencies = {'r': [], 't': [], 'w': [], 'p': []}
    graph = dgraph.create_graph(funcs, dependencies, io_bound=['r', 't'])

    # when
    start = time.time()
    dgraph.run_parallel_async(graph, ncores=2)
    end = time.time()

    # then
    assert end - start < 2
