[![Build Status](https://travis-ci.org/n-s-f/taskmap.svg?branch=master)](https://travis-ci.org/n-s-f/taskmap)

# taskmap

This library facilitates keeping track of dependencies between python functions,
and running them asyncronously and/or in parallel.

## Overview

There are many libraries in python that help run pipelines in parallel while
keeping track of dependencies between tasks, notably
[joblib](https://pythonhosted.org/joblib/) and 
[luigi](https://github.com/spotify/luigi).

taskmap provides a way to easily handle coroutines in task pipelines. Many kinds
of pipelines will have a significant number of tasks that are IO bound. Running
these tasks in parallel will still leave lots of unused processing time.

That's where taskmap comes in. It's designed to help you get the most out of a
single machine. You specify the dependency graph for your tasks (which are just
native python functions or coroutines), and optionally which ones are IO bound. 
The tasks can then be run asynchronously and in parallel, making sure that no core
sits unused while there are tasks it could be running.

Because taskmap keeps track of the dependency graph, it is easy to only rerun
failed tasks. It's also possible to change the functions corresponding to tasks
and rerun only those changed tasks and their children. You can then cache your
results, so that later you can pick up where you left off.

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

### Creating and Running the Graph

#### create_graph(funcs, dependencies, io_bound=None, done=None, results=None)

Creates the dependency graph.

`dependencies`: a dictionary that maps task names to a list of dependencies. The
results of those dependencies will be fed into the function in the order in
which they appear. Tasks that return `None` will not have the results fed into
the tasks that depend on them.

`funcs`: a dictionary that maps the names of the tasks to functions. Each
function should accept the same number of arguments as it has dependencies that
return non `None` values.

`io_bound`: a list of the names of the tasks that are io bound. These will be
picked up first, so that the cpu bound tasks can be executed while waiting on
results from e.g. network or database calls.

`done`: a list of the names of tasks that are already done. These tasks will not
be run if any of the `run*(graph)` functions are called with this graph. This is
a way to run only part of a dependency graph without changing the code that
creates the `dependencies` or `funcs` arguments.

`results`: a dictionary mapping the names of tasks to their results. This is
useful if the tasks listed in the `done` arguments have results that their
children will need passed to them.

This function will throw for a dependency dictionary with cyclic dependencies,
or if there are functions that are depended on but are not present as keys in
the dependencies dictionary.

Note that for coroutines, `functools.partial` will not work. If you need to
create partial functions to use as tasks, you can use `partial` from the `paco`
library.

#### taskmap.run_parallel_async(graph, sleep=0.1, ncores=None)

Runs the graph asynchronously across multiple cores. All tasks must be python
coroutines. This can be used when tasks are bottlenecked by both io and cpu.

`sleep` determines how long each process waits between checks to see if a new
task has become available.

`ncores` is how many cores are used in parallel. Defaults to half of available
cores.

#### taskmap.run_async(graph, sleep=.01)

Runs all coroutines on a single core. This can be used if all tasks are
bottlenecked by io.

#### taskmap.run_parallel(graph, sleep=.01, ncores=None)

The tasks must be normal python functions, and are not run in parallel but not
asynchronously. This can be used if all tasks are cpu bottlenecked.

#### taskmap.run(graph)

All tasks must be normal python functions and are run synchronously in a single
process.

### Handling Failed Tasks

#### taskmap.reset_failed_tasks

taskmap marks tasks that throw an exception as 'failed'. This function allows
you to rebuild a graph to only run the tasks that have failed and their
children. A common pattern is:

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
and want to rerun those tasks and the tasks that depend on their outcomes. This
can be because there was an bug in the task, or simply because you want to alter
the behavior.

```.py
result_graph = taskmap.run_parallel_async(graph)

# change the function corresponding to some task name
result_graph.funcs['some_func'] = new_task

new_graph = taskmap.reset_tasks(result_graph, ['some_func'])
new_result_graph = taskmap.run_parallel_async(new_graph)
```
