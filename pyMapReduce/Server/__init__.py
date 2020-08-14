import socket
import threading
import socketserver
import hashlib
import inspect
import sys
from enum import Enum
from types import FunctionType
from multiprocessing import Lock, Process

__all__ = [
    'Master',
    'Slave'
]


class _MsgType(Enum):

    Heartbeat = 0
    Heartbeat_Res = 1

    MapTask = 2
    MapTask_Res = 3

    ReduceTask = 4
    ReduceTask_Res = 5


def _parse_code_to_func(source: str) -> FunctionType:
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


def _recv_exact_n_bytes(sock: socket.socket, n: int):
    cur_bytes = 0
    ret = bytes()
    while cur_bytes < n:
        fetch = sock.recv(n - cur_bytes)
        ret += fetch
        cur_bytes += len(fetch)

    return ret


def _receive_and_decode_Msg(sock: socket.socket):
    try:
        msg_type = _MsgType(int.from_bytes(_recv_exact_n_bytes(sock, 1), 'big'))

        fingerprint = None

        if msg_type == _MsgType.Heartbeat or msg_type == _MsgType.Heartbeat_Res:
            body_bytes = int.from_bytes(_recv_exact_n_bytes(sock, 8), 'big')
            body = {
                'slave_id': bytes.decode(_recv_exact_n_bytes(sock, body_bytes), 'utf-8')
            }
        else:
            fingerprint = hex(int.from_bytes(sock.recv(32), 'big'))[2:]
            body = dict()

            if msg_type == _MsgType.MapTask:
                body_bytes = int.from_bytes(_recv_exact_n_bytes(sock, 8), 'big')
                body['MapFunc'] = bytes.decode(_recv_exact_n_bytes(sock, body_bytes), 'utf-8')

                body_bytes = int.from_bytes(_recv_exact_n_bytes(sock, 8), 'big')
                body['FileFunc'] = bytes.decode(_recv_exact_n_bytes(sock, body_bytes), 'utf-8')

        return msg_type, fingerprint, body

    except Exception as e:
        print(e)
        return None, None, None


def _connect(ip: str, port: int, req: bytes = bytes()):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    if len(req):
        sock.sendall(req)

    return sock


def _make_req(msg_type: _MsgType, body: list, fingerprint: str = str()):
    req = bytes()

    # MsgType
    req += msg_type.value.to_bytes(1, 'big')

    # fingerprint
    if len(fingerprint):
        req += int(fingerprint, 16).to_bytes(32, 'big')

    # body
    for i in body:
        body_bytes = str.encode(i, 'utf-8')
        req += len(body_bytes).to_bytes(8, 'big')
        req += body_bytes

    return req


class _ThreadedMasterTCPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        msg_type, fingerprint, body = _receive_and_decode_Msg(self.request)

        if msg_type == _MsgType.MapTask:
            prepared_slaves = []

            self.server.task_lock.acquire()
            self.server.task_dict[fingerprint] = body
            self.server.task_lock.release()

            for s_id, s in self.server.slave_dict.items():
                map_req = _make_req(_MsgType.MapTask, [body['MapFunc'], body['FileFunc']], fingerprint)
                sock = _connect(s['ip'], s['port'], map_req)
                _msg_type, _fingerprint, _body = _receive_and_decode_Msg(sock)
                if _msg_type == _MsgType.MapTask_Res and _fingerprint == fingerprint:
                    prepared_slaves.append(s_id)
                    print(s_id, 'is prepared for Map task')
                else:
                    print('Cannot connect to', s['slave_id'], 'when assigning Map task')

            self.request.sendall(_make_req(_MsgType.MapTask_Res, prepared_slaves, fingerprint))


class _ThreadedSlaveTCPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        msg_type, fingerprint, body = _receive_and_decode_Msg(self.request)

        if msg_type == _MsgType.Heartbeat:
            self.request.sendall(_make_req(_MsgType.Heartbeat_Res, body.values()))

        elif msg_type == _MsgType.MapTask:
            self.server.task_lock.acquire()
            self.server.task_dict[fingerprint] = body
            self.server.task_lock.release()

            self.request.sendall(_make_req(_MsgType.MapTask_Res, [], fingerprint))
            # !!!!!!!!
            MapFunc = _parse_code_to_func(body['MapFunc'])
            FileFunc = _parse_code_to_func(body['FileFunc'])
            MapFunc()
            FileFunc()


class _ThreadedMasterTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    task_lock = Lock()
    task_dict = dict()

    slave_lock = Lock()
    slave_dict = dict()


class _ThreadedSlaveTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    task_lock = Lock()
    task_dict = dict()


class _Server:

    def __init__(self, server):
        self._server = server
        self.__server_running = False

    def run(self):
        if self.__server_running:
            print('Master server has been started.')
        else:
            server_thread = threading.Thread(target=self._server.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            self.__server_running = True
            # print("Server loop running in thread:", server_thread.name)

    def close(self):
        if not self.__server_running:
            print('Master server has not been started.')
        else:
            self._server.shutdown()
            self._server.server_close()
            self.__server_running = False

    def server_address(self):
        return self._server.server_address


class Master(_Server):

    def __init__(self):
        _Server.__init__(self, _ThreadedMasterTCPServer(('', 0), _ThreadedMasterTCPRequestHandler))

    def register_slave(self, slave_id: str, slave_ip: str, slave_port: int):
        if slave_id in self._server.slave_dict:
            print('Slave', '"' + slave_id + '"', 'already exists')
        else:
            req = _make_req(_MsgType.Heartbeat, [slave_id])
            conn = _connect(slave_ip, slave_port, req)
            msg_type, fingerprint, body = _receive_and_decode_Msg(conn)
            conn.close()

            if msg_type == _MsgType.Heartbeat_Res and body['slave_id'] == slave_id:
                print('Successfully register slave', '"' + slave_id + '"')
                self._server.slave_dict[slave_id] = {
                    'ip': slave_ip,
                    'port': slave_port
                }


class Slave(_Server):

    def __init__(self):
        _Server.__init__(self, _ThreadedSlaveTCPServer(('', 0), _ThreadedSlaveTCPRequestHandler))

# master = Master()
# slave = Slave()
# master.run()
# slave.run()
# ip, port = slave.server_address()
# master.register_slave('a', ip, port)
# master.register_slave('a', ip, port)
# ip, port = master.server_address()
# print(type(ip), type(port))
# _client(ip, port, '123')
# client(ip, port, '123')
# client(ip, port, '123')
# master.close()

master = Master()
slave1 = Slave()
slave2 = Slave()
master.run()
slave1.run()
slave2.run()
ip, port = slave1.server_address()
master.register_slave('slave1', ip, port)
ip, port = slave2.server_address()
master.register_slave('slave2', ip, port)


def MapFunc():
    print('Map Func')


def FileFunc():
    print('File Func')


fingerprint = hashlib.sha256((ip + str(port) + inspect.getsource(MapFunc) + inspect.getsource(FileFunc)).encode('utf-8')).hexdigest()
map_req = _make_req(_MsgType.MapTask, [inspect.getsource(MapFunc), inspect.getsource(FileFunc)], fingerprint)
ip, port = master.server_address()
print(ip, port)
_connect(ip, port, map_req)
while True:
    pass
