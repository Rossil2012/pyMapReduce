import socket
import threading
import socketserver
from enum import Enum
from types import FunctionType


def parse_code_to_func(source: str) -> FunctionType:
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
    return FunctionType(code_bytes.co_consts[0], {'__builtins__': __builtins__})


class MsgType(Enum):
    HeartBeat = 0
    HeartBeat_Res = 1

    File = 2
    File_Res = 3

    MapFunc = 4
    MapFunc_Res = 5

    ReduceFunc = 6
    ReduceFunc_Res = 7


class ThreadedMasterTCPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        msg_type = int.from_bytes(self.request.recv(1), 'big')
        # print(type(msg_type), MsgType(msg_type))

        tot_data = bytes()
        while True:
            data = self.request.recv(1024)
            if not data:
                break
            tot_data += data

        if msg_type == MsgType.MapFunc.value:
            print(str(tot_data))
            map_func = parse_code_to_func(bytes.decode(tot_data))
            map_func()
        # data = str(self.request.recv(1024), 'ascii')
        # cur_thread = threading.current_thread()
        # response = bytes("{}: {}".format(cur_thread.name, data), 'ascii')
        # print(response)


class ThreadedSlaveTCPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        data = str(self.request.recv(1024), 'ascii')
        cur_thread = threading.current_thread()
        response = bytes("{}: {}".format(cur_thread.name, data), 'ascii')
        print(response)

    def finish(self):
        print(threading.current_thread())


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


class Server:

    def __init__(self, server: ThreadedTCPServer):
        self.__server = server
        self.__server_running = False

    def run(self):
        if self.__server_running:
            print('Master server has been started.')
        else:
            server_thread = threading.Thread(target=self.__server.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            self.__server_running = True
            print("Server loop running in thread:", server_thread.name)

    def close(self):
        if not self.__server_running:
            print('Master server has not been started.')
        else:
            self.__server.shutdown()
            self.__server_running = False

    def server_address(self):
        return self.__server.server_address


class Master(Server):

    def __init__(self):
        Server.__init__(self, ThreadedTCPServer(('', 0), ThreadedMasterTCPRequestHandler))


class Slave(Server):

    def __init__(self):
        Server.__init__(self, ThreadedTCPServer(('', 0), ThreadedSlaveTCPRequestHandler))


def client(ip, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((ip, port))
        sock.sendall(MsgType.MapFunc.value.to_bytes(1, 'big'))
        sock.sendall(str.encode(message))

