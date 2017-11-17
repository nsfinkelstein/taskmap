## taskmap

This library facilitates keeping track of dependencies between python functions,
and running them asyncronously and/or in parallel.

### Installation

```
pip install taskmap
```

### API

This library exports three functions.

##### create_graph(funcs, dependencies)

Creates the dependency graph.

The `funcs` argument is a dictionary that maps the names of the tasks to
functions.

The `dependencies` argument is a dictionary that maps task names to a collection
of the names of the tasks they depend on. The keys in the dictionary are
functions and the values are list of functions. The order matters because the
the results of the functions that a function depends on will be fed in to that
function in the order that the functions are listed in the dependencies dict.
Functions that return None will not have their results fed into functions that
depend on them.

This will throw for a dependency dictionary with cyclic dependencies, or if
there are functions that are depended on but are not present as keys in the
dependencies dictionary.

You can set a `bottleneck` property on the actual function in the `funcs` dict.
Functions that have an io bottleneck should be run first for efficiency reasons
(we can use the processors while waiting for them to finish), so functions with
a `bottleneck` property set to `'io'` will be run first.

Note that for coroutines, `functools.partial` will not work. If you need to
create functions that do not take arguments, you can use `paco.partial` from the
`paco` library.

##### taskmap.run(graph)

Runs the graph in a single thread.


# TODO: update examples to use funcs argument
```.py
import taskmap

def a(): return 5
def b(x): return x + 10
def c(x, y): return x + y + 20

dependencies = {
    c: [a, b],
    b: [a],
    a: [],

}

graph = taskmap.create_graph(dependencies)

results = taskmap.run(graph)

assert results == {a: 5, b: 15, c: 40}
```

Note that, for example, the result of `a` is fed to `b`, and the results of `a`
and `b` are fed to `c`.

##### taskmap.run_async(graph, sleep=.01)

Similar to `taskmap.run`, but runs functions asyncronously. All functions must be
coroutines. This is useful when many of the functions are bottlenecked by io.

`sleep` here and elsewhere is how long to wait before checking if a new function
can be run.


##### taskmap.run_parallel(graph, ncores=None, sleep=.01)

Similar to `taskmap.run`, but runs each new function in a new process for up to
`ncores` processes as new functions become available. This is useful when many
of the functions are bottlenecked by cpu.


##### taskmap.run_parallel_async(graph, ncores=None, sleep=.01)

Similar to `taskmap.run`, but runs each new function asynchronously on one of up
to `ncores` cores. All functions must be coroutines. This is useful when
functions are bottlenecked by both cpu and io.

```.py
import time
import taskmap

async def ab(x): return x + 10
async def ac(x, y): return x + y + 20
async def along_task(): time.sleep(.02); return 5

dependencies = {
    ac: [along_task, ab],
    ab: [along_task],
    along_task: [],

}
graph = taskmap.create_graph(dependencies)

results = taskmap.run_parallel_async(graph)

assert results == {along_task: 5, ab: 15, ac: 40}

```


##### taskmap.build_graph_for_failed_tasks

This function allows you to rebuild a graph to only run the tasks that have
failed. A common pattern is something like:

```.py
result_graph = taskmap.run_parallel_async(graph)
# failures abound

new_graph = taskmap.build_graph_for_failed_tasks(result_graph)
new_result_grpah = taskmpa.run_parallel_async(new_graph)
```


