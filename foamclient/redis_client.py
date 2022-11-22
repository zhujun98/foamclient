"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
from typing import Any, Optional

import redis

from .deserializer import DeserializerType, create_deserializer


class RedisClient:
    """Provide API for receiving data from the data switch."""

    def __init__(self, host: str, port: int, deserializer: DeserializerType, *,
                 timeout: Optional[int] = None):
        """Initialization.

        :param host: hostname of the Redis server.
        :param port: port of the Redis server.
        :param deserializer: deserializer type.
        :param timeout: subscribe timeout in seconds.
        """
        self._client = redis.Redis(host=host, port=port, password="sls2.0")

        self._subscriber = None

        self._timeout = timeout

        self._unpack = create_deserializer(deserializer)

    def subscribe(self, channel: str) -> None:
        """Subscribe to the given channel.

        :param channel: Redis pub channel to subscribe.
        """
        self._subscriber = self._client.pubsub()
        self._subscriber.subscribe(channel)

    def next(self) -> Any:
        msg = self._subscriber.get_message(
            ignore_subscribe_messages=True, timeout=self._timeout)
        if msg is None:
            raise TimeoutError
        key = int(msg['data'])
        data = self._client.hgetall(key)
        return {key.decode(): self._unpack(value)
                for key, value in data.items()}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._subscriber.close()
        self._client.close()
