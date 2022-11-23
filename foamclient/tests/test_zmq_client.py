import pickle
from queue import Empty, Queue
from threading import Thread
import time

import zmq
from foamclient import ZmqClient, DeserializerType


_PORT = 12345


class Server:
    def __init__(self, sock: str):
        self._ctx = zmq.Context()

        if sock == 'PUSH':
            self._sock_type = zmq.PUSH
        elif sock == 'REP':
            self._sock_type = zmq.REP
        elif sock == 'PUB':
            self._sock_type = zmq.PUB
        else:
            raise ValueError('Unsupported ZMQ socket type: %s' % str(sock))

        self._thread = Thread(target=self._run)
        self._buffer = Queue(maxsize=5)
        self._running = False
        self._counter = 0

    def start(self):
        self._thread.start()

    def _init_socket(self):
        socket = self._ctx.socket(self._sock_type)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.RCVTIMEO, 100)
        socket.bind(f"tcp://*:{_PORT}")
        return socket

    def _run(self) -> None:
        socket = self._init_socket()
        self._running = True
        rep_ready = False
        while self._running:
            if self._sock_type == zmq.REP and not rep_ready:
                try:
                    request = socket.recv()
                    assert request == b'READY'
                    rep_ready = True
                except zmq.error.Again:
                    continue

            try:
                data = self._buffer.get(timeout=0.1)
                socket.send(data)
                if self._sock_type == zmq.REP:
                    rep_ready = False
            except Empty:
                continue

    def feed(self):
        self._buffer.put(pickle.dumps(f"data{self._counter}"))
        self._counter += 1

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self._running = False
        self._thread.join()
        self._ctx.destroy()


def test_zmq_client_push_pull():
    with Server("PUSH") as server:
        with ZmqClient(f"tcp://localhost:{_PORT}",
                       sock="PULL",
                       timeout=1.0,
                       deserializer=pickle.loads) as client:
            for i in range(3):
                server.feed()
                assert client.next() == f"data{i}"


def test_zmq_client_rep_req():
    with Server("REP") as server:
        with ZmqClient(f"tcp://localhost:{_PORT}",
                       sock="REQ",
                       timeout=1.0,
                       deserializer=pickle.loads) as client:
            for i in range(3):
                server.feed()
                assert client.next() == f"data{i}"


def test_zmq_client_pub_sub():
    with Server("PUB") as server:
        with ZmqClient(f"tcp://localhost:{_PORT}",
                       sock="SUB",
                       timeout=1.0,
                       deserializer=pickle.loads) as client:
            # It takes a little time (a few milliseconds) for the connection to be set up,
            # and in that time lots of messages can be lost. The publisher needs to sleep
            # a little before starting to publish.
            time.sleep(0.1)
            for i in range(3):
                server.feed()
                assert client.next() == f"data{i}"
