# taskmap

This library facilitates keeping track of dependencies between python functions,
and running them asyncronously and/or in parallel.

## Overview

There are many libraries in python that help run ETL pipelines in parallel while
keeping track of dependencies between tasks, notably
[joblib](https://pythonhosted.org/joblib/) and 
[luigi](https://github.com/spotify/luigi).

taskmap provides a way to easily handle coroutines in ETL pipelines. Most ETL
pipelines will have a significant number of tasks that are IO bound. Running
these tasks in parallel will still leave lots of unused processing time.

That's where taskmap comes in. It's designed to help you get the most out of a
single machine. You specify the dependency graph for you tasks (which are just
native python functions or coroutines), and optionally which ones are IO bound. 
The tasks can then be asynchronously and in parallel, making sure that no core
sits unused while there are tasks it could be running.

Because taskmap keeps track of the dependency graph, it is easy to only rerun
failed tasks. It's also possible to change the functions corresponding to tasks
and rerun only those changed tasks and their children.

## Installation

```
pip install taskmap
```

## Quick Start

This example demonstrates the major use case of the taskmap library.

```.py
import taskmap
import asyncio
import time

# simulates io waits with asyncio.sleep
async def io_bound_a(): await asyncio.sleep(1); return 'io_a'
async def io_bound_b(x): await asyncio.sleep(1); return x + ' io_b'

# simulates cpu usage with time.sleep
async def cpu_bound_a(x): time.sleep(1); return x + ' cpu_a'
async def cpu_bound_b(): time.sleep(1); return 'cpu_b'

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
    graph = taskmap.run_parallel_async(graph, ncores=2)

    # then
    assert graph.results['io_bound_a'] == 'io_a'
    assert graph.results['io_bound_b'] == 'cpu_b io_b'
    assert graph.results['cpu_bound_a'] == 'io_a cpu_a'
    assert graph.results['cpu_bound_b'] == 'cpu_b'
```

More examples can be found in the tests.

## API

#### create_graph(funcs, dependencies, io_bound=None, done=None, results=None)

Creates the dependency graph.

The `dependencies` argument is a dictionary that maps task names to a collection
of the names of the tasks they depend on. The keys in the dictionary are
functions and the values are list of functions. The order matters because the
the results of the functions that a function depends on will be fed in to that
function in the order that the functions are listed in the dependencies dict.
Functions that return None will not have their results fed into functions that
depend on them.

The `funcs` argument is a dictionary that maps the names of the tasks to
functions. Each function should accept the same number of arguments as it has
dependencies that return non `None` values.

The `io_bound` argument is a list of the names of the tasks that are io bound.
These will be picked up first, so that the cpu bound tasks can be executed while
waiting on results from e.g. network or database calls.

The `done` argument is a list of the names of tasks that are already done. These
tasks will not be run if any of the `run*(graph)` functions are called with this
graph. This is a way to run only part of a dependency graph without changing the
code that creates the `dependencies` or `funcs` arguments.

The `results` argument is a dictionary mapping the names of tasks to their
results. This is useful if the tasks listed in the `done` arguments have results
that their children will need passed to them.

The `create_graph` function will throw for a dependency dictionary with cyclic
dependencies, or if there are functions that are depended on but are not present
as keys in the dependencies dictionary.

Note that for coroutines, `functools.partial` will not work. If you need to
create partial functions to use as tasks, you can use `paco.partial` from the
`paco` library.

#### taskmap.run_parallel_async(graph, sleep=0.1, ncores=None)

Runs the graph in a single thread. All tasks must be python coroutines. This is
especially useful when tasks are bottlenecked by both io and cpu.

`sleep` determines how long each process waits between checks to see if a new
task has become available.

`ncores` is how many cores are used in parallel. Defaults to half of available
cores.

#### taskmap.run_async(graph, sleep=.01)

Runs all coroutines on a single core. This can be used if all tasks are
bottlenecked by io.

#### taskmap.run_parallel(graph, sleep=.01, ncores=None)

The tasks must be normal python functions, and are not run asynchronously. This
can be used if all tasks are cpu bottlenecked.

#### taskmap.run(graph)

All tasks must be normal python functions and are run synchronously in a single
process.

#### taskmap.reset_failed_tasks

This function allows you to rebuild a graph to only run the tasks that have
failed and their children. A common pattern is:

```.py
result_graph = taskmap.run_parallel_async(graph)
# failures abound

new_graph = taskmap.reset_failed_tasks(result_graph)

# make a fix (e.g. make sure DB is available)
new_result_graph = taskmap.run_parallel_async(new_graph)
```

#### taskmap.reset_tasks

This function allows you to rebuild a graph to only run a subset of tasks, and
their children. This is useful if you change some of the tasks in the `'funcs'`
and want to rerun those tasks and the tasks that depend on their outcomes.

Here's an example.

```.py
result_graph = taskmap.run_parallel_async(graph)

# change the function corresponding to some task name
result_graph.funcs['some_func'] = new_task

new_graph = taskmap.reset_tasks(graph, ['some_func'])
new_result_graph = taskmap.run_parallel_async(new_graph)
```
