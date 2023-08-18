from queue import Empty, Queue
from threading import Thread
import time
from typing import Callable, Optional, Union

import pytest
from .conftest import (
    assert_result_equal, AvroDataGenerator, PickleDataGenerator,
    StringDataGenerator
)

import fastavro
import zmq
from foamclient import create_serializer, ZmqConsumer

_PORT = 12345


class ZmqProducer:
    def __init__(self, sock: str, *,
                 serializer: Union[str, Callable],
                 schema: Optional[object] = None,
                 multipart: bool = False):
        self._ctx = zmq.Context()

        if sock == 'PUSH':
            self._sock_type = zmq.PUSH
        elif sock == 'REP':
            self._sock_type = zmq.REP
        elif sock == 'PUB':
            self._sock_type = zmq.PUB
        else:
            raise ValueError('Unsupported ZMQ socket type: %s' % str(sock))

        self._multipart = multipart

        if callable(serializer):
            self._pack = serializer
        else:
            self._pack = create_serializer(
                serializer, schema, multipart=multipart)

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
                payload = self._pack(self._buffer.get(timeout=0.1))
                if self._multipart:
                    for i, item in enumerate(payload):
                        if i == len(payload) - 1:
                            socket.send(item)
                        else:
                            socket.send(item, zmq.SNDMORE)
                else:
                    socket.send(payload)

                if self._sock_type == zmq.REP:
                    rep_ready = False
            except Empty:
                continue

    def produce(self, data: object):
        self._buffer.put(data)
        self._counter += 1

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self._running = False
        self._thread.join()
        self._ctx.destroy()


@pytest.mark.parametrize(
    "server_sock,client_sock", [("REP", "REQ")])
@pytest.mark.parametrize(
    "serializer, deserializer", [("avro", "avro"),
                                 ("pickle", "pickle"),
                                 (lambda x: x.encode(), lambda x: x.bytes.decode())])
def test_zmq_clients(serializer, deserializer, server_sock, client_sock):
    if serializer == "avro":
        gen = AvroDataGenerator()
        schema = gen.schema
    elif serializer == "pickle":
        gen = PickleDataGenerator()
        schema = None
    else:
        gen = StringDataGenerator()
        schema = None

    with ZmqProducer(server_sock, serializer=serializer, schema=schema) as producer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         deserializer=deserializer,
                         schema=schema,
                         sock=client_sock,
                         timeout=1.0) as consumer:
            # It takes a little time (a few milliseconds) for the pub-sub connection to be set up,
            # and in that time lots of messages can be lost. The publisher needs to sleep
            # a little before starting to publish.
            if server_sock == "PUB":
                time.sleep(0.1)

            for _ in range(3):
                data_gt = gen.next()
                producer.produce(data_gt)
                data = consumer.next()
                assert_result_equal(data_gt, data)


def test_default_deserializer():
    gen = AvroDataGenerator()
    with ZmqProducer("PUSH",
                     serializer="avro",
                     schema=gen.schema) as producer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         sock="PULL",
                         timeout=1.0) as consumer:
            data_gt = gen.next()
            producer.produce(data_gt)
            assert_result_equal(consumer.next(schema=gen.schema), data_gt)


def test_schema_overiding():
    false_schema = fastavro.parse_schema({
        "type": "record",
        "namespace": "unittest",
        "name": "data",
        "fields": []
    })
    gen = AvroDataGenerator()
    with ZmqProducer("PUSH",
                     serializer="avro",
                     schema=gen.schema) as producer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         deserializer="avro",
                         schema=false_schema,
                         sock="PULL",
                         timeout=1.0) as consumer:
            data_gt = gen.next()
            producer.produce(data_gt)
            # schema is overridden here
            assert_result_equal(consumer.next(schema=gen.schema), data_gt)


def test_callable_deserializer():
    with ZmqProducer("PUSH", serializer=lambda x: x.encode()) as producer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         sock="PULL",
                         deserializer=lambda x: x,
                         timeout=1.0) as consumer:
            producer.produce("data0")
            assert bytes(consumer.next()) == b"data0"


def test_multipart_data():
    with pytest.raises(ValueError, match="does not support multipart message"):
        ZmqProducer("REP", serializer="avro", multipart=True)

    data_gt = [
        {"a": 123},
        {"b": "Hello world"}
    ]

    with ZmqProducer("PUSH",
                     serializer="pickle",
                     multipart=True) as producer:
        with ZmqConsumer(f"tcp://localhost:{_PORT}",
                         deserializer="pickle",
                         sock="PULL",
                         multipart=True,
                         timeout=1.0) as consumer:
            producer.produce(data_gt)
            assert consumer.next() == [{'a': 123}, {'b': 'Hello world'}]
