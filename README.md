# pyMapReduce
A lightweight distributed MapReduce implementation on Python.

## Feature

1. **Distributed.** Master, slave and client can run on different machines.
2. **Muti-Tasks Supported.** Tasks can be executed simultaneously.

## Example

This is a character counting example testing on localhost.

```python
from pyMapReduce import Master, Slave, Job
from threading import Thread
import time
import random

start_port = random.randint(50000, 65000)


def start_slave(port):
    slave = Slave(port)
    slave.run()


def start_master(port):
    master = Master(port)
    for i in range(0, slave_num):
        master.register_slave('slave_' + str(i), 'localhost', start_port + i)
    master.run()


# Run slaves
slave_num = 3
for i in range(0, slave_num):
    thread = Thread(target=start_slave, args=(start_port + i,))
    thread.daemon = True
    thread.start()

time.sleep(3)

# Run Master
thread = Thread(target=start_master, args=(start_port + slave_num,))
thread.daemon = True
thread.start()


class CountCharacters(Job):
    def __init__(self):
        Job.__init__(self, 'localhost', start_port + slave_num)
        self.File = [
            {'1': 'MapReduce implemented with Python'},
            {'2': 'This is a Python package named pyMapReduce'},
            {'3': 'MapReduce is a programming model and an associated implementation for processing and generating large data sets. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key. Many real world tasks are expressible in this model, as shown in the paper.'},
            {'4': 'Programs written in this functional style are automatically parallelized and executed on a large cluster of commodity machines. The runtime system takes care of the details of partitioning the input data, scheduling the program’s execution across a set of machines, handling machine failures, and managing the required intermachine communication. This allows programmers without any experience with parallel and distributed systems to easily utilize the resources of a large distributed system.'},
            {'5': 'Our implementation of MapReduce runs on a large cluster of commodity machines and is highly scalable: a typical MapReduce computation processes many terabytes of data on thousands of machines. Programmers find the system easy to use: hundreds of MapReduce programs have been implemented and upwards of one thousand MapReduce jobs are executed on Google’s clusters every day.'},
            {'6': 'The computation takes a set of input key/value pairs, and produces a set of output key/value pairs. The user of the MapReduce library expresses the computation as two functions: Map and Reduce.'},
            {'7': 'Map, written by the user, takes an input pair and produces a set of intermediate key/value pairs. The MapReduce library groups together all intermediate values associated with the same intermediate key I and passes them to the Reduce function.'}
        ]

    def map(self, key, value):
        for char in value:
            yield char, 1

    def reduce(self, key, values):
        return sum(values)


job = CountCharacters()
res = job.run()
print('The result of CountCharacters:', res)
```

```json
The result of CountCharacters: {'M': 12, 'a': 149, 'p': 53, 'R': 11, 'e': 198, 'd': 64, 'u': 61, 'c': 54, ' ': 265, 'i': 97, 'm': 54, 'l': 52, 'n': 89, 't': 122, 'w': 12, 'h': 47, 'P': 4, 'y': 32, 'o': 81, 'T': 6, 's': 108, 'k': 12, 'g': 26, 'r': 83, 'f': 25, '.': 12, 'U': 1, '/': 5, 'v': 9, ',': 8, 'x': 6, 'b': 10, 'z': 2, '’': 2, 'q': 1, 'O': 1, ':': 3, 'j': 1, 'G': 1, 'I': 1}
```

## To be Implemented

- [ ] Server fault tolerance
- [ ] Effective and flexible task scheduling
- [ ] Authentication
