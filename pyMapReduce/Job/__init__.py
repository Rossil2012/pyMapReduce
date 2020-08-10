import inspect
from Exception import UndefException
from abc import ABCMeta, abstractmethod


def parse_code_to_bytes(source: str):
    parsed_code = str()
    lines = source.splitlines(False)

    def_line = lines[0].replace('(self)', '()', 1)
    stripped_def_line = def_line.lstrip()
    lines.pop(0)
    indent = len(def_line) - len(stripped_def_line)
    parsed_code += stripped_def_line + '\n'

    for line in lines:
        stripped_line = line.rstrip()
        if len(stripped_line) != 0:
            parsed_code += stripped_line[indent:] + '\n'

    code_bytes = compile(parsed_code, '<string>', 'exec')
    return code_bytes.co_consts[0]


class Job(metaclass=ABCMeta):
    def __init__(self, url, port):
        self.master_url = url
        self.master_port = port

    @abstractmethod
    def map(self):
        raise UndefException('Map')

    @abstractmethod
    def reduce(self):
        raise UndefException('Reduce')

    def run(self):
        code_bytes = parse_code_to_bytes(inspect.getsource(self.map))
        pass
