import argparse
import socket
import time
from signal import SIGINT, SIGTERM
from signal import signal, getsignal
from sys import exit
from threading import Thread
from threading import enumerate as thread_enum

class TaskQueue:
    def __init__(self):        
        self._task_id = 1
        self._queue_list = dict()
        #  ['1'] = [<length>, <data>, <state_for_timer>, <task_list>]

    def get_new_task_id(self):
        id = self._task_id + 1
        self._task_id = id
        return self._task_id

    def add_to_queue(self, qname, qlen, qdata):
        t = dict()        
        if qname in self._queue_list:
            t = self._queue_list[qname]        
        d = dict()
        d['length'] = qlen
        d['data'] = qdata            
        d['state'] = 'waiting'            
        id = self.get_new_task_id()
        t[id] = d
        self._queue_list[qname] = t
        return id


    def get_queue(self, qname):
        q = self._queue_list[qname]
        t = None
        if len(q) > 0:
            for k, v in q.items():
                if v['state'] == "waiting":
                    q[k]['state'] = 'processing'
                    t = q[k]
                    return str(k), str(t['length']), str(t['data'])
        return None, None, None

    def ack_task(self, qname, taskid):
        if qname in self._queue_list:
            q = self._queue_list[qname] 
            if taskid in q:
                del q[taskid]
                return "YES"
        return "NO"

    def check_task(self, qname, taskid):
        if qname in self._queue_list:
            q = self._queue_list[qname] 
            if taskid in q:
                return "YES"
        return "NO"

    def save_state():
        pass

    def print(self):
        print(self._queue_list)


class TaskQueueServer:

    def __init__(self, ip, port, path, timeout):
        #configuration
        self._connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._ip = ip
        self._port = port
        self._path = path
        self._timeout = timeout
        self._connection_count = 2

        self._task_queue = TaskQueue()

    def client_thread(self, conn, addr, tq):        
        while True:
            data = conn.recv(2048)
            if data == b'quit\n' or data == b'stop\n':                    
                print(data)
                conn.shutdown(1)
                conn.close()
                break
            
            if data[:3] == b'ADD':
                cmd, qname, qlen, qdata = data.split()
                print(f'RCV: {cmd.decode()} {qname.decode()} {qlen.decode()} {qdata.decode()}')
                id = str(tq.add_to_queue(qname.decode(), qlen.decode(), qdata.decode()))
                print(f'SND: {id}')
                conn.sendall(id.encode())

            elif data[:3] == b'GET':
                cmd, qname = data.split()
                print(f'RCV: {cmd.decode()} {qname.decode()}')
                id, qlen, qdata = tq.get_queue(qname.decode())
                s = "NONE"
                if id is not None:
                    s = "{} {} {}".format(id, qlen, qdata)
                print(f'SND: {s}')
                conn.sendall(s.encode())

            elif data[:2] == b'IN':
                cmd, qname, taskid = data.split()
                print(f'RCV: {cmd.decode()} {qname.decode()} {taskid.decode()}')
                s = tq.check_task(qname.decode(), int(taskid.decode()))
                print(f'SND: {s}')
                conn.sendall(s.encode())

            elif data[:3] == b'ACK':
                cmd, qname, taskid = data.split()
                print(f'RCV: {cmd.decode()} {qname.decode()} {taskid.decode()}')                
                s = tq.ack_task(qname.decode(), int(taskid.decode()))
                print(f'SND: {s}')
                conn.sendall(s.encode())

            elif data[:4] == b'SAVE':
                print()
        tq.print()
        print("stop thread")



    def run(self):
        print("Server start")
        self._connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._connection.bind((self._ip, self._port))
        self._connection.listen(self._connection_count)    
        self._connection.settimeout(1.0)            
        new_connections = []
        thread_list = []

        print("run loop")
        current_connection = None
        while True:
            try:                
                current_connection, address = self._connection.accept()
            except socket.timeout:
                pass
            #except (KeyboardInterrupt, InterruptedError):
            #    print("exception")
            #    self._connection.close()
            #    break
            if current_connection or current_connection is not None:
                try:
                    if current_connection not in new_connections:
                        new_connections.append(current_connection)                        
                        print("accept: ", address)
                        t = Thread(target=self.client_thread, args=(current_connection, address, self._task_queue))
                        print(t.getName())
                        if t not in thread_list:
                            thread_list.append(t)
                            #print(thread_enum())
                            t.start()
                            for k in thread_list:
                                if not k.is_alive():
                                    thread_list.remove(k)
                except:
                    self._connection.close()
                    break


def signal_handler(signal_received, frame):    
    print('SIGINT or SIGTERM or CTRL-C detected. Exiting gracefully')    
    exit(0)



def parse_args():
    parser = argparse.ArgumentParser(description='This is a simple task queue server with custom protocol')
    parser.add_argument(
        '-p',
        action="store",
        dest="port",
        type=int,
        default=5555,
        help='Server port')
    parser.add_argument(
        '-i',
        action="store",
        dest="ip",
        type=str,
        default='127.0.0.1',
        help='Server ip adress')
    parser.add_argument(
        '-c',
        action="store",
        dest="path",
        type=str,
        default='./',
        help='Server checkpoints dir')
    parser.add_argument(
        '-t',
        action="store",
        dest="timeout",
        type=int,
        default=300,
        help='Task maximum GET timeout in seconds')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    signal(SIGINT, signal_handler)
    signal(SIGTERM, signal_handler)
    server = TaskQueueServer(**args.__dict__)
    server.run()
    print("server stopped")
