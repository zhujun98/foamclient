"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
from typing import Callable, List, Optional, Union

import zmq

from .serializer import create_deserializer


class ZmqConsumer:
    """Provide API for consuming zmq data stream."""
    def __init__(self, endpoint: str, *,
                 deserializer: Union[str, Callable] = "avro",
                 schema: Optional[object] = None,
                 context: Optional[zmq.Context] = None,
                 sock: str = "PULL",
                 hwm: Optional[int] = None,
                 timeout: Optional[float] = None,
                 multipart: bool = False,
                 request: bytes = b"READY"):
        """Initialization.

        :param endpoint: endpoint of the ZMQ connection.
        :param deserializer: deserializer type or a callable object which
            deserializes the data.
        :param schema: reader's schema for the deserializer (optional).
        :param context: ZMQ context.
        :param sock: socket type.
        :param hwm: high water mark for ZMQ socket.
        :param timeout: socket timeout in seconds.
        :param multipart: whether the data will be sent as a multipart message.
        :param request: acknowledgement sent to the REP server when the socket
            type is REQ.
        """
        self._ctx = context or zmq.Context()
        self._socket = None
        self._multipart = multipart
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
        hwm = 1 if hwm is None else hwm
        self._socket.set_hwm(hwm)
        self._socket.connect(endpoint)

        if timeout is not None:
            self._socket.setsockopt(zmq.RCVTIMEO, int(timeout * 1000))

        if callable(deserializer):
            self._unpack = deserializer
        else:
            self._unpack = create_deserializer(
                deserializer, schema=schema, multipart=self._multipart)

    def next(self) -> Union[List[object], object]:
        """Return the next data item.

        :param schema: optional data schema for the serializer. If given,
            it overrides the default schema of the consumer.

        :raise TimeoutError
        """
        if self._sock_type == zmq.REQ and not self._req_ready:
            self._socket.send(self._request)
            self._req_ready = True

        try:
            if self._multipart:
                msg = self._socket.recv_multipart(copy=False)
            else:
                msg = self._socket.recv(copy=False)
        except zmq.ZMQError:
            raise TimeoutError
        self._req_ready = False

        return self._unpack(msg)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._ctx.destroy()
