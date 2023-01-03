"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
from typing import Any, Callable, Optional, Union

import redis

from .deserializer import DeserializerType, create_deserializer


class RedisConsumer:
    """Provide API for consuming data stored in Redis."""

    def __init__(self, host: str, port: int,
                 deserializer: Union[DeserializerType, Callable], *,
                 timeout: Optional[int] = None):
        """Initialization.

        :param host: hostname of the Redis server.
        :param port: port of the Redis server.
        :param deserializer: deserializer or deserializer type.
        :param timeout: subscribe timeout in seconds.
        """
        self._client = redis.Redis(host=host, port=port, password="sls2.0")
        self._streams = {}

        self._timeout = int(timeout * 1000)  # to milliseconds

        if callable(deserializer):
            self._unpack = deserializer
        else:
            self._unpack = create_deserializer(deserializer)

    def subscribe(self, stream: str) -> None:
        """Subscribe to a given stream.

        :param stream: stream name.
        """
        self._streams[stream] = '$'

    def consume(self, count: int = 1) -> [dict]:
        """Consume a list of data items.

        :param count: the maximum number of data items too return.
        """
        data = self._client.xread(self._streams, count, block=self._timeout)
        if not data:
            raise TimeoutError

        schema = {
            "samples": None,
            "encoder": None,
            "index": None
        }
        ret = []
        for _, item in data[0][1]:
            ret.append({key: self._unpack(item[key.encode()])
                        for key in schema})
        return ret

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._client.close()
