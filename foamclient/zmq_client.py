"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
from typing import Any, Optional

import zmq

from .deserializer import DeserializerType, create_deserializer


class ZmqClient:
    """Provide API for receiving data from the data switch."""
    def __init__(self, endpoint: str, deserializer: DeserializerType, *,
                 timeout: Optional[int] = None,
                 sock: str = "PULL",
                 context: Optional[zmq.Context] = None,
                 req: bytes = b"READY"):
        """Initialization.

        :param endpoint: endpoint of the ZMQ connection.
        :param deserializer: deserializer type.
        :param timeout: socket timeout in seconds.
        :param sock: socket type.
        :param context: ZMQ context.
        :param req: acknowledgement sent to the REP server when the socket
            type is REQ.
        """
        self._ctx = context or zmq.Context()
        self._socket = None
        self._req = req

        self._recv_ready = True
        sock = sock.upper()
        if sock == 'PULL':
            self._socket = self._ctx.socket(zmq.PULL)
        elif sock == 'REQ':
            self._socket = self._ctx.socket(zmq.REQ)
            self._recv_ready = False
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

        self._unpack = create_deserializer(deserializer)

    def next(self) -> Any:
        if self._sock_type == zmq.REQ and not self._recv_ready:
            self._socket.send(self._req)
            self._recv_ready = True

        try:
            msg = self._socket.recv_multipart(copy=False)
        except zmq.ZMQError:
            raise TimeoutError
        self._recv_ready = False

        return self._unpack(msg[0])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._ctx.destroy(linger=0)
