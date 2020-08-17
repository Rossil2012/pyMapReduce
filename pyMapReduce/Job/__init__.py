import inspect
from Exception import UndefException
from abc import ABCMeta, abstractmethod


class Job(metaclass=ABCMeta):
    def __init__(self, url: str, port: int):
        self.master_url = url
        self.master_port = port

    @abstractmethod
    def map(self, key, value):
        raise UndefException('Map')

    @abstractmethod
    def reduce(self, key, values: list):
        raise UndefException('Reduce')

    def run(self):
        code = inspect.getsource(self.map)
        pass
