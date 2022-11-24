import pickle
from queue import Empty, Queue
from threading import Thread
import time

import pytest

import zmq
from foamclient import ZmqClient, DeserializerType


_PORT = 12345


class ZmqServer:
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

    def feed(self, *, serializer=pickle.dumps):
        if serializer is None:
            data = f"data{self._counter}".encode('utf-8')
        else:
            data = pickle.dumps(f"data{self._counter}")
        self._buffer.put(data)
        self._counter += 1

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self._running = False
        self._thread.join()
        self._ctx.destroy()


@pytest.mark.parametrize("server_sock,client_sock", [("PUSH", "PULL"), ("PUB", "SUB"), ("REP", "REQ")])
def test_zmq_client_push_pull(server_sock,client_sock):
    with ZmqServer("PUSH") as server:
        with ZmqClient(f"tcp://localhost:{_PORT}",
                       deserializer=pickle.loads,
                       sock="PULL",
                       timeout=1.0) as client:
            # It takes a little time (a few milliseconds) for the pub-sub connection to be set up,
            # and in that time lots of messages can be lost. The publisher needs to sleep
            # a little before starting to publish.
            if server_sock == "PUB":
                time.sleep(0.1)
            for i in range(3):
                server.feed()
                assert client.next() == f"data{i}"


def test_zmq_client_none_deserializer():
    with ZmqServer("PUSH") as server:
        with ZmqClient(f"tcp://localhost:{_PORT}",
                       sock="PULL",
                       timeout=1.0) as client:
            server.feed(serializer=None)
            assert bytes(client.next()) == b"data0"


def test_zmq_client_predefined_deserializer():
    with ZmqServer("PUSH") as server:
        with ZmqClient(f"tcp://localhost:{_PORT}",
                       deserializer=DeserializerType.SLS,
                       sock="PULL",
                       timeout=1.0) as client:
            server.feed()
            assert client.next() == "data0"
