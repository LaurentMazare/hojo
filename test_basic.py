import numpy as np
import hojo

print(dir(hojo))

def f():
    import numpy as np
    print("Hello, World!")
    for i in range(40):
        i = np.array([i, i+1])
        yield i * i

# worker = hojo.run_in_worker(f, binary="./target/release/hojo")
worker = hojo.run_in_worker(f)
print(dir(worker))
print(worker.status())

for elem in worker:
    print(elem)
print("DONE")
