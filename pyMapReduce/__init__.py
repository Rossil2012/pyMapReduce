import math
import time
import json
import socket
import hashlib
import inspect
import threading
import socketserver
from enum import Enum
from queue import Queue
from types import FunctionType
from multiprocessing import Lock, Process, Manager


class _MsgType(Enum):
    """ Type of messages transferred among Master, Slaves and Clients """

    # Master - Slave
    HeartBeat = 0
    HeartBeat_Res = 1

    # Client - Master
    AllTask = 2
    AllTask_Res = 3

    # Master - Slave
    MapTask = 4
    MapTask_Res = 5

    # Master - Slave
    ReduceTask = 6
    ReduceTask_Res = 7


def _parse_code_to_func(source: str) -> FunctionType:
    """ Format code string(trim beginning spaces and get rid of 'self') and compile to executable FunctionType """

    parsed_code = str()
    lines = source.splitlines(False)

    def_line = lines[0].replace('self', '', 1).replace(',', '', 1)
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
    """ Connect to (ip, port) and return socket """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))

    return sock


def _recv_exact_n_bytes(sock: socket.socket, n: int):
    """ Get exact n bytes from socket """

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
    """ Receive bytes and parse to a message(message type, fingerprint(SHA256 of the task), body) """

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

        elif msg_type == _MsgType.MapTask_Res or msg_type == _MsgType.ReduceTask_Res or msg_type == _MsgType.AllTask_Res:
            body['result'] = json.loads(_get_body_content_one())

    sock.setblocking(blocking_state)

    return msg_type, fingerprint, body


def _get_all_Msgs(sock: socket.socket):
    """ Get all the prepared messages from socket """

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
    """ Pack a message """

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
    """ Request handler of socketserver """

    def handle(self):
        self.server._handle_request(self.request)
        return


class _ThreadedMasterServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """
    A multi-threaded master server in a cluster, keeping multiple slave servers, based on socketserver package.
    The master use one thread to listen, one thread to send messages, more threads to process requests from clients.
    It will periodically trigger _periodical_event(self), process clients's requests by
        _handle_request(self, sock: socket.socket). Those two class methods must be override.
    """

    def __init__(self, address=('', 0)):
        socketserver.ThreadingMixIn.__init__(self)
        socketserver.TCPServer.__init__(self, address, _ThreadedMasterTCPRequestHandler)

        self._slaves = dict()           # Slave servers
        self._socket_queue = Queue()    # Messages to be sent by socket_send_thread
        self._server_running = False    # Is socketserver running?
        self._event_period = 5          # Period of events (sec)

    def shutdown_request(self, request):
        """ Override socketserver.TCPServer to avoid connection with clients from killed """

        pass

    def _safe_sendall(self, server_id: str, sock: str, req: bytes, to_close=False):
        """ Enqueue the requests to avoid races of sockets """

        self._socket_queue.put({
            'server_id': server_id,
            'sock': sock,
            'req': req,
            'to_close': to_close
        })

    def _threaded_sendall(self):
        """ Thread to send requests (!! may be slow when there are a lot of slaves !!) """

        while True:
            task = self._socket_queue.get()
            try:
                task['sock'].sendall(task['req'])
                if task['to_close']:
                    task['sock'].close()
            except socket.error as e:
                print('Send to', task['server_id'], 'error:', e)
                continue

    def _periodical_event(self):
        """ The event to be executed periodically (override) """

        raise NotImplementedError('_periodical_event is not implemented')

    def _handle_request(self, sock: socket.socket):
        """ The method to process the requests of clients (override) """

        raise NotImplementedError('_handle_request is not implemented')

    def run(self):
        """ Start the server """

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

            while self._server_running:
                self._periodical_event()
                time.sleep(self._event_period)

    def close(self):
        """ Close the server """

        if not self._server_running:
            print('Master server has not been started.')
        else:
            self.shutdown()
            self.server_close()
            self._server_running = False


class Master(_ThreadedMasterServer):

    def __init__(self, port=0):
        _ThreadedMasterServer.__init__(self, ('', port))
        self._tasks = dict()            # All running tasks
        self._tasks_lock = Lock()       # Lock for tasks

    def _handle_request(self, sock: socket.socket):
        """
        Override _ThreadedMasterServer._handle_request
        This method may be called by many processes spontaneously
        """

        msg_type, fingerprint, body = _receive_and_decode_Msg(sock)

        if msg_type == _MsgType.AllTask:
            if fingerprint in self._tasks:
                print('Task', fingerprint, 'is running...')
                return

            # Record the task
            distributed_tasks = self._distribute_tasks(body['file'])
            self._tasks_lock.acquire()
            self._tasks[fingerprint] = {
                'client': sock,
                'map': {
                    'func': body['mapFunc'],
                    'file': json.dumps(body['file']),
                    'running_sub_tasks': distributed_tasks,
                    'result': []
                },
                'reduce': {
                    'func': body['reduceFunc'],
                    'running_sub_tasks': dict(),
                    'result': {}
                }
            }
            self._tasks_lock.release()

            # Assign Map Task to Slaves
            for s_id, file in distributed_tasks.items():
                map_req = _make_req(_MsgType.MapTask, [body['mapFunc'], json.dumps(file)], fingerprint)
                self._safe_sendall(s_id, self._slaves[s_id]['conn'], map_req)

    def _periodical_event(self):
        """ Override _ThreadedMasterServer._periodical_event """

        # Get messages from all connections with Slaves and process them
        for slave_id, s in self._slaves.items():
            all_msgs = _get_all_Msgs(s['conn'])
            print('Master receive messages from slave', slave_id, ':', all_msgs)
            self._handle_Msgs(slave_id, all_msgs)

    def _distribute_tasks(self, file: list):
        """ Distribute tasks for Slaves """

        ret = dict()
        alive_slaves = []
        for s_id, s_item in self._slaves.items():
            if s_item['alive']:
                alive_slaves.append(s_id)

        len_each = math.floor(len(file) / len(alive_slaves))
        for i in range(0, len(alive_slaves) - 1):
            ret[alive_slaves[i]] = file[i*len_each:(i+1)*len_each]

        i = len(alive_slaves) - 1
        ret[alive_slaves[i]] = file[i*len_each:]

        print('ret', ret)

        return ret

    def _handle_Msgs(self, slave_id, msgs):
        """ Process all messages of one Slave """

        def _gather_results(results: list):
            aft = dict()
            for pair in results:
                for k, v in pair.items():
                    if k not in aft:
                        aft[k] = [v]
                    else:
                        aft[k].append(v)

            ret = []
            for k, v in aft.items():
                ret.append({k: v})
            return ret

        self._slaves[slave_id]['timeout_cnt'] += 1
        self._safe_sendall(slave_id, self._slaves[slave_id]['conn'], _make_req(_MsgType.HeartBeat, [slave_id]))

        for msg in msgs:
            _fingerprint = msg['fingerprint']

            # HeartBeat_Res: Reset timeout_cnt and alive
            if msg['msg_type'] == _MsgType.HeartBeat_Res:
                self._slaves[slave_id]['timeout_cnt'] = 0
                self._slaves[slave_id]['alive'] = True

            # MapTask_Res: One Map sub_task is finished
            elif msg['msg_type'] == _MsgType.MapTask_Res:
                map_task_rec = self._tasks[_fingerprint]['map']
                map_task_rec['running_sub_tasks'].pop(slave_id)
                map_task_rec['result'].extend(msg['body']['result'])

                # All Map sub_tasks are finished, then assign reduce tasks
                if len(map_task_rec['running_sub_tasks']) == 0:
                    reduce_task_rec = self._tasks[_fingerprint]['reduce']
                    aft_gathering = _gather_results(map_task_rec['result'])
                    reduce_task_rec['running_sub_tasks'] = self._distribute_tasks(aft_gathering)

                    for s_id, file in reduce_task_rec['running_sub_tasks'].items():
                        reduce_req = _make_req(_MsgType.ReduceTask, [reduce_task_rec['func'], json.dumps(file)], _fingerprint)
                        self._safe_sendall(s_id, self._slaves[s_id]['conn'], reduce_req)

            # ReduceTask_Res: One Reduce sub_task is finished
            elif msg['msg_type'] == _MsgType.ReduceTask_Res:
                reduce_task_rec = self._tasks[_fingerprint]['reduce']
                reduce_task_rec['running_sub_tasks'].pop(slave_id)
                reduce_task_rec['result'].update(msg['body']['result'])

                # All sub_tasks are finished, print and send result to client
                if len(reduce_task_rec['running_sub_tasks']) == 0:
                    self._safe_sendall('client', self._tasks[_fingerprint]['client'], _make_req(_MsgType.AllTask_Res, [json.dumps(reduce_task_rec['result'])], _fingerprint), True)
                    self._tasks.pop(_fingerprint)

        # Check timeout
        if self._slaves[slave_id]['timeout_cnt'] > 3:
            self._slaves[slave_id]['alive'] = False
            print(slave_id, 'is down.')

    def run(self):
        """ Start the Master """

        print('Master Listen on', self.server_address)
        _ThreadedMasterServer.run(self)

    def register_slave(self, slave_id: str, slave_ip: str, slave_port: int):
        """ Register a slave """

        if slave_id in self._slaves:
            print('Slave', '"' + slave_id + '"', 'already exists')
        else:
            conn = _connect(slave_ip, slave_port)
            self._slaves[slave_id] = {
                'conn': conn,
                'alive': True,
                'timeout_cnt': 0
            }
            self._safe_sendall(slave_id, self._slaves[slave_id]['conn'], _make_req(_MsgType.HeartBeat, [slave_id]))

            print('Successfully register slave', '"' + slave_id + '"')


class Slave:

    def __init__(self, port=0):
        self._server_running = False                    # Whether the slave is running
        self._port = port                               # Default port, 0 means distributed by system
        self._tasks = dict()                            # All running tasks
        self._jobs = dict()                             # All running worker processes
        self._manager = Manager()
        self._job_results = self._manager.dict()        # Shared dict for workers
        self._conn = socket.socket()                    # Connection with Master
        self._slave_id = 'Slave ?'                      # Slave_id passed from HeartBeat, default 'Slave ?'

    def _handle_Msg(self, msg_type, fingerprint, body):
        """ Process a message """

        def handle_MapTask():
            func = _parse_code_to_func(body['mapFunc'])
            res = []
            if inspect.isgeneratorfunction(func):
                for pair in body['file']:
                    for k, v in pair.items():
                        for yield_k, yield_v in func(k, v):
                            res.append({yield_k: yield_v})
            else:
                for pair in body['file']:
                    for k, v in pair.items():
                        res.extend(func(k, v))

            self._job_results[fingerprint] = {
                'result': res,
                'msg_type': msg_type
            }

        def handle_ReduceTask():
            func = _parse_code_to_func(body['reduceFunc'])
            res = dict()

            for pair in body['file']:
                for k, v in pair.items():
                    res[k] = func(k, v)

            self._job_results[fingerprint] = {
                'result': res,
                'msg_type': msg_type
            }

        # HeartBeat: Send HeartBeat_Res to Master
        if msg_type == _MsgType.HeartBeat:
            self._slave_id = body['slave_id']
            self._conn.sendall(_make_req(_MsgType.HeartBeat_Res, [body['slave_id']]))

        # MapTask: Start a worker process to execute Map Task
        elif msg_type == _MsgType.MapTask:
            map_process = Process(target=handle_MapTask)
            map_process.start()
            self._jobs[fingerprint] = map_process

        # ReduceTask: Start a worker process to execute Reduce Task
        elif msg_type == _MsgType.ReduceTask:
            map_process = Process(target=handle_ReduceTask)
            map_process.start()
            self._jobs[fingerprint] = map_process

    def _task_check(self):
        """ Check whether there are finished tasks """

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

    def _handle(self):
        """ Periodical event """

        while self._server_running:
            try:
                self._task_check()
                msg_type, fingerprint, body = _receive_and_decode_Msg(self._conn)
            except BlockingIOError:
                time.sleep(5)
                continue
            self._handle_Msg(msg_type, fingerprint, body)
            print(self._slave_id, 'receive message:', msg_type, fingerprint, body)

    def run(self):
        """ Start the slave """

        if self._server_running:
            print('Slave has been started.')
        else:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind(('', self._port))
            server.listen(1)
            print('Slave Listen on', server.getsockname())
            conn, address = server.accept()
            conn.setblocking(False)

            self._conn = conn
            self._server_running = True
            self._handle()

    def close(self):
        """ Close the Slave """

        if not self._server_running:
            print('Slave has not been started.')
        else:
            self._server_running = False


class Job:

    def __init__(self, url: str, port: int):
        self.File = None            # Should be override
        self.master_url = url
        self.master_port = port

    def map(self, key, value):
        """ Map function (override) """

        raise NotImplementedError('Map function is not implemented.')

    def reduce(self, key, values):
        """ Reduce function (override) """

        raise NotImplementedError('Reduce function is not implemented.')

    def run(self):
        """ Execute the task """

        if self.File is None:
            raise NotImplementedError('File is missing.')

        map_code = inspect.getsource(self.map)
        reduce_code = inspect.getsource(self.reduce)

        fingerprint = hashlib.sha256((map_code + reduce_code + json.dumps(self.File)).encode('utf-8')).hexdigest()

        req = _make_req(_MsgType.AllTask, [map_code, reduce_code, json.dumps(self.File)], fingerprint)

        conn = _connect(self.master_url, self.master_port)
        conn.sendall(req)
        msg_type, fingerprint, body = _receive_and_decode_Msg(conn)

        print(fingerprint, 'Final Result:', body['result'])
