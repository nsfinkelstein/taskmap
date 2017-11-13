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
        a: {b, c},
        b: {c},
        c: set(),
    }
    graph = dgraph.create_graph(dependencies)

    # when
    results = dgraph.get_ready_tasks(graph)

    # then
    assert results == {c}


def test_graph_ready_after_task_completed():
    # given
    dependencies = {
        a: {b, c},
        b: {c},
        c: set(),
    }
    graph = dgraph.create_graph(dependencies)
    ready = dgraph.get_ready_tasks(graph)

    # when
    for func in ready:
        dgraph.mark_as_done(graph, func)

    results = dgraph.get_ready_tasks(graph)

    # then
    assert results == {b}


def test_cyclic_dependency():
    # given
    dependencies = {
        a: {b},
        b: {c},
        c: {a},
    }

    # then
    with pytest.raises(ValueError):

        # when
        dgraph.create_graph(dependencies)


def test_absent_tasks():
    # given
    dependencies = {
        a: {b, c},
    }

    # then
    with pytest.raises(ValueError):

        # when
        dgraph.create_graph(dependencies)


def test_run_pass_args():
    # given
    dependencies = {
        c: [a, b],
        b: [a],
        a: [],

    }
    graph = dgraph.create_graph(dependencies)

    # when
    results = dgraph.run(graph)

    # then
    assert results == {a: 5, b: 15, c: 40}


def long_task():
    time.sleep(.02)
    return 5


def test_run_parallel():
    # given
    dependencies = {
        c: [long_task, b],
        b: [long_task],
        long_task: [],

    }
    graph = dgraph.create_graph(dependencies)

    # when
    results = dgraph.run_parallel(graph)

    # then
    assert results == {long_task: 5, b: 15, c: 40}


async def ab(x):
    return x + 10


async def ac(x, y):
    return x + y + 20


async def along_task():
    time.sleep(.02)
    return 5


def test_run_async():
    # given
    dependencies = {
        ac: [along_task, ab],
        ab: [along_task],
        along_task: [],

    }
    graph = dgraph.create_graph(dependencies)

    # when
    results = dgraph.run_async(graph)

    # then
    assert results == {along_task: 5, ab: 15, ac: 40}


def test_run_parllel_async():
    # given
    dependencies = {
        ac: [along_task, ab],
        ab: [along_task],
        along_task: [],

    }
    graph = dgraph.create_graph(dependencies)

    # when
    results = dgraph.run_parallel_async(graph)

    # then
    assert results == {along_task: 5, ab: 15, ac: 40}
