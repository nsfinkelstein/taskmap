## dgraph

This library facilitates keeping track of dependencies between python functions,
and running them asyncronously and in parallel.

### Installation

```
pip install dgraph
```

### API

This library exports three functions.

##### create_graph(dependencies)

Creates the dependency graph. This is a named tuple comprised of the
dependencies argument and a list called `done` which is used to keep track of
which tasks have been completed. 

The `dependencies` argument is a dictionary that maps functions to the functions
they depend on. The keys in the dictionary are functions and the values are list
of functions. The order matters because the the results of the functions that a
function depends on will be fed in to that function in the order that the
functions are listed in the dependencies dict. Functions that return None will
not have their results fed into functions that depend on them.

This will throw for a dependency dictionary with cyclic dependencies, or if
there are functions that are depended on but are not present as keys in the
dependencies dictionary.

##### dgraph.run(graph)

Runs the graph in a single thread.


```.py
import dgraph

def a(): return 5
def b(x): return x + 10
def c(x, y): return x + y + 20

dependencies = {
    c: [a, b],
    b: [a],
    a: [],

}

graph = dgraph.create_graph(dependencies)

results = dgraph.run(graph)

assert results == {a: 5, b: 15, c: 40}
```

##### dgraph.run_async(graph, sleep=.01)

Similar to `dgraph.run`, but runs functions asyncronously. All functions must be
coroutines. This is useful when many of the functions are bottlenecked by io.

`sleep` here and elsewhere is how long to wait before checking if a new function
can be run.


##### dgraph.run_parallel(graph, ncores=None, sleep=.01)

Similar to `dgraph.run`, but runs each new function in a new process for up to
`ncores` processes as new functions become available. This is useful when many
of the functions are bottlenecked by cpu.


##### dgraph.run_parallel_async(graph, ncores=None, sleep=.01)

Similar to `dgraph.run`, but runs each new function asynchronously on one of up
to `ncores` cores. All functions must be coroutines. This is useful when
functions are bottlenecked by both cpu and io.

```.py
import time
import dgraph

async def ab(x): return x + 10
async def ac(x, y): return x + y + 20
async def along_task(): time.sleep(.02); return 5

dependencies = {
    ac: [along_task, ab],
    ab: [along_task],
    along_task: [],

}
graph = dgraph.create_graph(dependencies)

results = dgraph.run_async(graph)

assert results == {along_task: 5, ab: 15, ac: 40}

```

