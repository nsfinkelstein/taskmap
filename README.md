## dgraph

This library facilitates keeping track of dependencies between python functions.

### Installation

```
pip install dgraph
```

### API

This library exports three functions.

##### create_graph(dependencies)

Creates the dependency graph. This is a named tuple comprised of the
dependencies argument and a set called `done` which is used to keep track of
which tasks have been completed. 

The `dependencies` argument is a dictionary that maps functions to the functions
they depend on. The keys in the dictionary are functions and the values are sets
of functions. 

This will throw for a dependency dictionary with cyclic dependencies, or if
there are functions that are depended on but are not present as keys in the
dependencies dictionary.


##### mark_as_done(graph, func)

Takes a graph object (which is a namedtuple returned from `create_graph`) and
marks the function as completed


##### get_ready_tasks(graph)

Returns all tasks that are ready to be completed based on the tasks that have
been marked as done.


### Example

```.py
import dgraph

def a(): return 5
def b(): return 5
def c(): return 5
def d(): return 5

dependencies = {
  a: {b, c},
  b: {c, d},
  c: set(),
  d: set(),
}

graph = dgraph.create_graph(dependencies)
ready = dgraph.get_ready_tasks(graph)
print(ready)  # {c, d}

for func in ready:
    func()
    dgraph.mark_as_done(graph, func)

print(graph.done)  # {c, d}

ready = dgraph.get_ready_tasks(graph)
print(ready)  # {b}
```
