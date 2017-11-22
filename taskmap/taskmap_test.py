import asyncio
import logging
import taskmap
import pytest
import time

# disable logging during tests
logging.disable(logging.CRITICAL)


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

    graph = taskmap.create_graph(funcs, dependencies)

    # when
    results = taskmap.get_ready_tasks(graph)

    # then
    assert results == ['c']


def test_graph_ordered_ready():
    # given
    dependencies = {'a': set(), 'b': set()}
    funcs = {'a': a, 'b': b}
    io_bound = ['a']
    graph = taskmap.create_graph(funcs, dependencies, io_bound=io_bound)

    # when
    results = taskmap.get_ready_tasks(graph)

    # then
    assert results == ['a', 'b']


def test_tasks_can_be_marked_done():
    # given
    funcs = {'a': a, 'b': b}
    dependencies = {'a': ['b'], 'b': []}

    # when
    graph = taskmap.create_graph(funcs, dependencies, done=['b'])

    # then
    assert taskmap.get_ready_tasks(graph) == ['a']


def test_cached_results_are_used():
    # given
    funcs = {'a': a, 'b': b}
    dependencies = {'b': ['a'], 'a': []}
    results = {'a': 5}

    graph = taskmap.create_graph(
        funcs, dependencies, done=['a'], results=results)

    # when
    graph = taskmap.run(graph)

    # then
    assert graph.results['b'] == 15


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

    graph = taskmap.create_graph(funcs, dependencies)
    ready = taskmap.get_ready_tasks(graph)

    # when
    for func in ready:
        taskmap.mark_as_done(graph, func)

    results = taskmap.get_ready_tasks(graph)

    # then
    assert results == ['b']


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
        taskmap.create_graph(funcs, dependencies)


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
        taskmap.create_graph(funcs, dependencies)


def test_all_names_are_funcs():
    # given
    dependencies = {'d': ['a'], 'a': []}

    funcs = {'a': a, 'b': b, 'c': c}

    # then
    with pytest.raises(ValueError):

        # when
        taskmap.create_graph(funcs, dependencies)


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

    graph = taskmap.create_graph(funcs, dependencies)

    # when
    graph = taskmap.run(graph)

    # then
    assert graph.results == {'a': 5, 'b': 15, 'c': 40}


error = RuntimeError('some error')


def d():
    raise error


def test_sync_error_handling():
    # given
    dependencies = {
        'c': ['d'],
        'd': [],
    }

    funcs = {
        'd': d,
        'c': c,
    }

    # when
    graph = taskmap.create_graph(funcs.copy(), dependencies.copy())
    graph = taskmap.run(graph)

    graph_parallel = taskmap.create_graph(funcs.copy(), dependencies.copy())
    graph_parallel = taskmap.run_parallel(graph, nprocs=2, sleep=.001)

    # then
    expected = {
        'd': error,
        'c': 'Ancestor task d failed; task not run',
    }
    assert graph.results['c'] == expected['c']
    assert graph.results['d'].__class__ == expected['d'].__class__
    assert graph.results['d'].args == expected['d'].args

    assert graph_parallel.results['c'] == expected['c']
    assert graph_parallel.results['d'].__class__ == expected['d'].__class__
    assert graph_parallel.results['d'].args == expected['d'].args


async def control():
    return 5


async def e():
    raise error


def test_async_error_handling():
    # given
    dependencies = {
        'c': ['e'],
        'e': [],
        'control': [],
    }

    funcs = {
        'e': e,
        'c': c,
        'control': control,
    }

    # when
    graph = taskmap.create_graph(funcs.copy(), dependencies.copy())
    graph = taskmap.run_async(graph, sleep=.001)

    graph_parallel = taskmap.create_graph(funcs.copy(), dependencies.copy())
    graph_parallel = taskmap.run_parallel_async(graph_parallel, nprocs=2, sleep=.001)

    # then
    expected = {
        'e': error,
        'control': 5,
        'c': 'Ancestor task e failed; task not run',
    }

    assert graph.results['c'] == expected['c']
    assert graph.results['e'].__class__ == expected['e'].__class__
    assert graph.results['e'].args == expected['e'].args
    assert graph.results['control'] == 5

    assert graph_parallel.results['c'] == expected['c']
    assert graph_parallel.results['e'].__class__ == expected['e'].__class__
    assert graph_parallel.results['e'].args == expected['e'].args
    assert graph.results['control'] == 5


def test_rebuilding_graph_from_failure():
    # given
    dependencies = {
        'c': ['e'],
        'e': [],
        'w': [],
    }

    funcs = {
        'e': e,
        'c': c,
        'w': w,
    }

    graph = taskmap.create_graph(funcs.copy(), dependencies.copy())
    graph = taskmap.run_parallel_async(graph, nprocs=2, sleep=.001)

    # when
    new_graph = taskmap.reset_failed_tasks(graph)

    # then
    assert new_graph.done == ['w']


def test_get_all_children():
    # given
    # given
    dependencies = {
        'd': ['a'],
        'c': ['b'],
        'b': ['a'],
        'a': [],
    }

    funcs = {
        'a': a,
        'b': b,
        'c': c,
        'd': d,
    }

    graph = taskmap.create_graph(funcs, dependencies)

    # when
    a_children = taskmap.get_all_children(graph, 'a')
    b_children = taskmap.get_all_children(graph, 'b')
    c_children = taskmap.get_all_children(graph, 'c')

    # then
    assert a_children == {'b', 'c', 'd'}
    assert b_children == {'c'}
    assert c_children == set()


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

    graph = taskmap.create_graph(funcs, dependencies)

    # when
    graph = taskmap.run_parallel(graph, nprocs=2, sleep=.001)

    # then
    assert graph.results == {'long_task': 5, 'b': 15, 'c': 40}


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

    graph = taskmap.create_graph(funcs, dependencies)

    # when
    graph = taskmap.run_async(graph, sleep=0.001)

    # then
    assert graph.results == {'along_task': 5, 'ab': 15, 'ac': 40}


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

    graph = taskmap.create_graph(funcs, dependencies)

    # when
    graph = taskmap.run_parallel_async(graph, nprocs=2, sleep=.001)

    # then
    assert graph.results == {'along_task': 5, 'ab': 15, 'ac': 40}


async def x():
    await asyncio.sleep(.4)
    return 5


async def y():
    await asyncio.sleep(.4)
    return 5


def test_async_speed():
    # given
    funcs = {'x': x, 'y': y}
    dependencies = {'x': [], 'y': []}
    graph = taskmap.create_graph(funcs, dependencies)

    # when
    start = time.time()
    taskmap.run_async(graph, sleep=0.001)
    end = time.time()

    # then
    assert end - start < .8


def v():
    time.sleep(.4)
    return 5


def u():
    time.sleep(.4)
    return 5


def test_parallel_speed():
    # given
    funcs = {'x': u, 'y': v}
    dependencies = {'x': [], 'y': []}
    graph = taskmap.create_graph(funcs, dependencies)

    # when
    start = time.time()
    taskmap.run_parallel(graph, nprocs=2, sleep=.001)
    end = time.time()

    # then
    assert end - start < .8


async def r():
    await asyncio.sleep(.4)


async def t():
    await asyncio.sleep(.4)


async def w():
    time.sleep(.4)


async def p():
    time.sleep(.4)


def test_async_parallel_speed():
    # given
    funcs = {'r': r, 't': t, 'w': w, 'p': p}
    dependencies = {'r': [], 't': [], 'w': [], 'p': []}
    graph = taskmap.create_graph(funcs, dependencies, io_bound=['r', 't'])

    # when
    start = time.time()
    taskmap.run_parallel_async(graph, nprocs=2, sleep=.0001)
    end = time.time()

    # then
    assert end - start < .8


async def io_bound_a(): await asyncio.sleep(.4); return 'io_a'
async def io_bound_b(x): await asyncio.sleep(.4); return x + ' io_b'
async def cpu_bound_a(x): time.sleep(.4); return x + ' cpu_a'
async def cpu_bound_b(): time.sleep(.4); return 'cpu_b'


def test_async_parallel_demo():
    # given
    funcs = {
        'io_bound_a': io_bound_a,
        'io_bound_b': io_bound_b,
        'cpu_bound_a': cpu_bound_a,
        'cpu_bound_b': cpu_bound_b,
    }

    dependencies = {
        'io_bound_a': [],
        'io_bound_b': ['cpu_bound_b'],
        'cpu_bound_a': ['io_bound_a'],
        'cpu_bound_b': [],
    }

    io_bound = ['io_bound_a', 'io_bound_b']
    graph = taskmap.create_graph(funcs, dependencies, io_bound=io_bound)

    # when
    start = time.time()
    graph = taskmap.run_parallel_async(graph, nprocs=2, sleep=.001)
    end = time.time()

    # then
    assert end - start < 1.2
    assert graph.results['io_bound_a'] == 'io_a'
    assert graph.results['io_bound_b'] == 'cpu_b io_b'
    assert graph.results['cpu_bound_a'] == 'io_a cpu_a'
    assert graph.results['cpu_bound_b'] == 'cpu_b'
