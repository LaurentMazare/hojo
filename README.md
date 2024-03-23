# hojo
A small python library to run iterators in a separate process.
This uses the dill package for serialization of the closure and iterator values.

## Building

In order to compile the wheel, you need to have
[maturin](https://github.com/PyO3/maturin) installed, and then run `make build`.

## Example

Below is an example where a simple iterator gets wrapped so as to run in a
separate process.

```python
def f():
    for i in range(40):
        yield i * i

iter = hojo.run_in_worker(f)
for elem in iter:
  print(elem)
```

Note that if `f` uses some python packages, they might not be imported properly.
If this happens you may want to have an import declaration at the beggining of
`f`, e.g.:

```python
def f():
    import numpy as np
    for i in range(40):
        i = np.array([i, i+1])
        yield i * i
```
