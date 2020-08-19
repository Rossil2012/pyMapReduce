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
        statistics = dict()
        for char in value:
            if char != ' ':
                if char not in statistics:
                    statistics[char] = 1
                else:
                    statistics[char] += 1

        for k, v in statistics.items():
            yield k, v

    def reduce(self, key, values):
        return sum(values)


job = CountCharacters()
job.run()
