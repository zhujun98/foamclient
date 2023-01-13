"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
from typing import Any, Callable, Optional, Union

import zmq

from .serializer import SerializerType, create_deserializer


class ZmqConsumer:
    """Provide API for consuming zmq data stream."""
    def __init__(self, endpoint: str, *,
                 deserializer: Union[SerializerType, Callable] = SerializerType.AVRO,
                 schema: Optional[object] = None,
                 timeout: Optional[float] = None,
                 sock: str = "PULL",
                 context: Optional[zmq.Context] = None,
                 request: bytes = b"READY"):
        """Initialization.

        :param endpoint: endpoint of the ZMQ connection.
        :param deserializer: deserializer type or a callable object which
            deserializes the data.
        :param schema: optional data (Reader's) schema for the serializer.
        :param timeout: socket timeout in seconds.
        :param sock: socket type.
        :param context: ZMQ context.
        :param request: acknowledgement sent to the REP server when the socket
            type is REQ.
        """
        self._ctx = context or zmq.Context()
        self._socket = None
        self._request = request

        self._req_ready = False
        sock = sock.upper()
        if sock == 'PULL':
            self._socket = self._ctx.socket(zmq.PULL)
        elif sock == 'REQ':
            self._socket = self._ctx.socket(zmq.REQ)
        elif sock == 'SUB':
            self._socket = self._ctx.socket(zmq.SUB)
            self._socket.setsockopt(zmq.SUBSCRIBE, b'')
        else:
            raise ValueError('Unsupported ZMQ socket type: %s' % str(sock))

        self._sock_type = self._socket.type
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.set_hwm(1)
        self._socket.connect(endpoint)

        if timeout is not None:
            self._socket.setsockopt(zmq.RCVTIMEO, int(timeout * 1000))

        if callable(deserializer):
            self._unpack = deserializer
        else:
            self._unpack = create_deserializer(deserializer, schema)

    def next(self, schema: Optional[object] = None) -> Any:
        """Return the next data item.

        :param schema: optional data schema for the serializer. If given,
            it overrides the default schema of the consumer.
        """
        if self._sock_type == zmq.REQ and not self._req_ready:
            self._socket.send(self._request)
            self._req_ready = True

        try:
            msg = self._socket.recv(copy=False)
        except zmq.ZMQError:
            raise TimeoutError
        self._req_ready = False

        if schema is not None:
            return self._unpack(msg, schema=schema)
        return self._unpack(msg)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._ctx.destroy()
