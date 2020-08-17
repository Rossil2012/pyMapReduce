import socket
import threading
import socketserver
import hashlib
import inspect
import sys
import math
import time
import json
from queue import Queue
from enum import Enum
from types import FunctionType
from multiprocessing import Lock, Process, Manager


class _MsgType(Enum):

    HeartBeat = 0
    HeartBeat_Res = 1

    AllTask = 2
    AllTask_Res = 3

    MapTask = 4
    MapTask_Res = 5

    ReduceTask = 6
    ReduceTask_Res = 7


def _parse_code_to_func(source: str) -> FunctionType:
    parsed_code = str()
    lines = source.splitlines(False)

    def_line = lines[0].replace('(self', '(', 1)
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


def _connect(ip: str, port: int):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.connect((ip, port))

    return sock


def _recv_exact_n_bytes(sock: socket.socket, n: int):
    fetch_max = 1024
    cur_bytes = 0
    ret = bytes()
    while cur_bytes < n:
        to_fetch = fetch_max if n - cur_bytes > fetch_max else n - cur_bytes
        fetch = sock.recv(to_fetch)
        ret += fetch
        cur_bytes += len(fetch)

    return ret


def _receive_and_decode_Msg(sock: socket.socket):
    def _get_body_content_one():
        body_bytes = int.from_bytes(_recv_exact_n_bytes(sock, 8), 'big')
        return bytes.decode(_recv_exact_n_bytes(sock, body_bytes), 'utf-8')

    msg_type = _MsgType(int.from_bytes(_recv_exact_n_bytes(sock, 1), 'big'))
    blocking_state = sock.getblocking()
    sock.setblocking(True)

    fingerprint = None

    if msg_type == _MsgType.HeartBeat or msg_type == _MsgType.HeartBeat_Res:
        body = {
            'slave_id': _get_body_content_one()
        }
    else:
        fingerprint = hex(int.from_bytes(sock.recv(32), 'big'))[2:]
        body = dict()

        if msg_type == _MsgType.AllTask:
            body['mapFunc'] = _get_body_content_one()
            body['reduceFunc'] = _get_body_content_one()
            body['file'] = json.loads(_get_body_content_one())

        elif msg_type == _MsgType.MapTask:
            body['mapFunc'] = _get_body_content_one()
            body['file'] = json.loads(_get_body_content_one())

        elif msg_type == _MsgType.ReduceTask:
            body['reduceFunc'] = _get_body_content_one()
            body['file'] = json.loads(_get_body_content_one())

        elif msg_type == _MsgType.MapTask_Res or msg_type == _MsgType.ReduceTask_Res:
            body['result'] = json.loads(_get_body_content_one())

    sock.setblocking(blocking_state)

    return msg_type, fingerprint, body


def _get_all_Msgs(sock: socket.socket):
    all_msgs = []
    blocking_state = sock.getblocking()
    sock.setblocking(False)

    while True:
        try:
            msg_type, fingerprint, body = _receive_and_decode_Msg(sock)
            all_msgs.append({
                'msg_type': msg_type,
                'fingerprint': fingerprint,
                'body': body
            })
        except BlockingIOError:
            sock.setblocking(blocking_state)
            return all_msgs


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

        if msg_type == _MsgType.AllTask:
            if fingerprint in self.server._tasks:
                # !!!!!!
                print('Task', fingerprint, 'is running...')
                return

            distributed_tasks = self.server._distribute_tasks(body['file'])
            self.server._tasks_lock.acquire()
            self.server._tasks[fingerprint] = {
                'map': {
                    'func': body['mapFunc'],
                    'file': json.dumps(body['file']),
                    'running_sub_tasks': distributed_tasks,
                    'result': []
                },
                'reduce': {
                    'func': body['reduceFunc'],
                    'running_sub_tasks': dict(),
                    'result': []
                }
            }
            self.server._tasks_lock.release()

            for s_id, file in distributed_tasks.items():
                map_req = _make_req(_MsgType.MapTask, [body['mapFunc'], json.dumps(file)], fingerprint)
                self.server._safe_sendall(self.server._slaves[s_id]['conn'], map_req)


class _ThreadedMasterTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


class Master(_ThreadedMasterTCPServer):

    def __init__(self):
        _ThreadedMasterTCPServer.__init__(self, ('', 0), _ThreadedMasterTCPRequestHandler)
        self._socket_queue = Queue()
        self._server_running = False
        self._slaves = dict()
        self._tasks = dict()
        self._tasks_lock = Lock()

    def _safe_sendall(self, sock: str, req: bytes):
        self._socket_queue.put({
            sock: req
        })

    def _distribute_tasks(self, file: list):
        print(file)
        ret = dict()
        alive_slaves = []
        for s_id, s_item in self._slaves.items():
            if s_item['alive']:
                alive_slaves.append(s_id)

        len_each = math.floor(len(file) / len(alive_slaves))
        for i in range(0, len(alive_slaves) - 1):
            ret[alive_slaves[i]] = file[i*len_each:(i+1)*len_each]

        ret[alive_slaves[len(alive_slaves)-1]] = file[(len(alive_slaves)-1)*len_each:]
        print(ret)

        return ret

    def _threaded_sendall(self):
        while True:
            task = self._socket_queue.get()
            for sock, req in task.items():
                sock.sendall(req)

    def _handle_Msgs(self, slave_id, msgs):
        print(msgs)
        self._slaves[slave_id]['timeout_cnt'] += 1
        self._safe_sendall(self._slaves[slave_id]['conn'], _make_req(_MsgType.HeartBeat, [slave_id]))

        for msg in msgs:
            if msg['msg_type'] == _MsgType.HeartBeat_Res:
                self._slaves[slave_id]['timeout_cnt'] = 0
                self._slaves[slave_id]['alive'] = True

            elif msg['msg_type'] == _MsgType.MapTask_Res:
                map_task_rec = self._tasks[msg['fingerprint']]['map']
                map_task_rec['running_sub_tasks'].pop(slave_id)
                map_task_rec['result'].extend(msg['body']['result'])

                if len(map_task_rec['running_sub_tasks']) == 0:
                    print('all', self._tasks[msg['fingerprint']])
                    print('maptaskrec', map_task_rec)
                    reduce_task_rec = self._tasks[msg['fingerprint']]['reduce']
                    reduce_task_rec['running_sub_tasks'] = self._distribute_tasks(map_task_rec['result'])

                    for s_id, file in reduce_task_rec['running_sub_tasks'].items():
                        reduce_req = _make_req(_MsgType.ReduceTask, [reduce_task_rec['func'], json.dumps(file)], msg['fingerprint'])
                        self._safe_sendall(self._slaves[s_id]['conn'], reduce_req)

            elif msg['msg_type'] == _MsgType.ReduceTask_Res:
                reduce_task_rec = self._tasks[msg['fingerprint']]['reduce']
                reduce_task_rec['running_sub_tasks'].pop(slave_id)
                reduce_task_rec['result'].extend(msg['body']['result'])
                print(msg['body']['result'])

                if len(reduce_task_rec['running_sub_tasks']) == 0:
                    self._tasks.pop(msg['fingerprint'])
                    print('Final Result:', reduce_task_rec['result'])

        if self._slaves[slave_id]['timeout_cnt'] > 2:
            self._slaves[slave_id]['alive'] = False
            print(slave_id, 'is down.')

    def register_slave(self, slave_id: str, slave_ip: str, slave_port: int):
        if slave_id in self._slaves:
            print('Slave', '"' + slave_id + '"', 'already exists')
        else:
            conn = _connect(slave_ip, slave_port)
            self._slaves[slave_id] = {
                'conn': conn,
                'alive': True,
                'timeout_cnt': 0
            }
            self._safe_sendall(self._slaves[slave_id]['conn'], _make_req(_MsgType.HeartBeat, [slave_id]))

            print('Successfully register slave', '"' + slave_id + '"')

    def run(self):
        if self._server_running:
            print('Master server has been started.')
        else:
            server_thread = threading.Thread(target=self.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            self._server_running = True

            socket_send_thread = threading.Thread(target=self._threaded_sendall)
            socket_send_thread.daemon = True
            socket_send_thread.start()

            print('Listen on', self.server_address)

            while True:
                for slave_id, s in self._slaves.items():
                    all_msgs = _get_all_Msgs(s['conn'])
                    self._handle_Msgs(slave_id, all_msgs)

                time.sleep(5)

    def close(self):
        if not self._server_running:
            print('Master server has not been started.')
        else:
            self.shutdown()
            self.server_close()
            self._server_running = False


class Slave:

    def __init__(self):
        self._tasks = dict()
        self._jobs = dict()
        self._manager = Manager()
        self._job_results = self._manager.dict()
        self._conn = socket.socket()

    def _handle_Msg(self, msg_type, fingerprint, body):
        def handle_MapReduceTask():
            func = _parse_code_to_func(body['mapFunc']) if msg_type == _MsgType.MapTask else _parse_code_to_func(body['reduceFunc'])

            res = []
            for pair in body['file']:
                for k, v in pair.items():
                    res.extend(func(k, v))

            self._job_results[fingerprint] = {
                'result': res,
                'msg_type': msg_type
            }

        if msg_type == _MsgType.HeartBeat:
            self._conn.sendall(_make_req(_MsgType.HeartBeat_Res, [body['slave_id']]))

        elif msg_type == _MsgType.MapTask or msg_type == _MsgType.ReduceTask:
            map_process = Process(target=handle_MapReduceTask)
            map_process.start()
            self._jobs[fingerprint] = map_process

    def _handle(self):
        while True:
            try:
                msg_type, fingerprint, body = _receive_and_decode_Msg(self._conn)
            except BlockingIOError:
                time.sleep(3)
                continue
            print(msg_type, fingerprint, body)
            self._handle_Msg(msg_type, fingerprint, body)

            finished_tasks = []
            for _fingerprint, job in self._jobs.items():
                if not job.is_alive():
                    finished_tasks.append(_fingerprint)
                    print(self._job_results[_fingerprint])
                    res = self._job_results[_fingerprint]
                    _msg_type = _MsgType.MapTask_Res if res['msg_type'] == _MsgType.MapTask else _MsgType.ReduceTask_Res
                    self._conn.sendall(_make_req(_msg_type, [json.dumps(res['result'])], _fingerprint))

            for task in finished_tasks:
                self._jobs.pop(task)
                self._job_results.pop(task)

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('', 0))
        server.listen(1)
        print('Listen on', server.getsockname())
        conn, address = server.accept()
        conn.setblocking(False)

        self._conn = conn
        self._handle()

