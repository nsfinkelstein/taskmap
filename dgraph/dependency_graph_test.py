import pytest

import dependency_graph


def a():
    return 5


def b():
    return 10


def c():
    return 20


def test_graph_ready():
    # given
    dependencies = {
        a: {b, c},
        b: {c},
        c: set(),
    }
    graph = dependency_graph.create_graph(dependencies)

    # when
    results = dependency_graph.get_ready_tasks(graph)

    # then
    assert results == {c}


def test_graph_ready_after_task_completed():
    # given
    dependencies = {
        a: {b, c},
        b: {c},
        c: set(),
    }
    graph = dependency_graph.create_graph(dependencies)
    ready = dependency_graph.get_ready_tasks(graph)

    # when
    for func in ready:
        dependency_graph.done(graph, func)

    results = dependency_graph.get_ready_tasks(graph)

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
        dependency_graph.create_graph(dependencies)


def test_absent_tasks():
    # given
    dependencies = {
        a: {b, c},
    }

    # then
    with pytest.raises(ValueError):

        # when
        dependency_graph.create_graph(dependencies)
