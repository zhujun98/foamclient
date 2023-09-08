"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
from abc import ABC
from typing import Any, Callable, List, Optional, Union

import redis

from .serializer import create_serializer, create_deserializer


class BaseRedisClient(ABC):
    def __init__(self, host: str, port: int, password: Optional[str] = None):
        """Initialization.

        :param host: hostname of the Redis server.
        :param port: port of the Redis server.
        :param password: Redis password.
        """
        self._db = redis.Redis(host=host, port=port, password=password)

    @property
    def client(self):
        return self._db

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._db.close()


class RedisConsumer(BaseRedisClient):
    """Provide API for consuming data stored in Redis."""

    def __init__(self,
                 *args,
                 deserializer: Union[str, Callable] = "avro",
                 schema: Optional[object] = None,
                 block: int = 100):
        """Initialization.

        :param deserializer: deserializer type or a callable object which
            deserializes the data.
        :param schema: reader's schema for the deserializer (optional).
        :param block: BLOCK parameter in XREAD.
        """
        super().__init__(*args)

        if callable(deserializer):
            self._unpack = deserializer
        else:
            self._unpack = create_deserializer(deserializer, schema=schema)

        self._block = block

    def consume(self, stream: str, stream_id: Optional[str] = None) -> (str, object):
        """Consume given number of records.

        :param stream: the maximum number of data items too return.
        :param stream_id: the last received ID. Only entries with an ID greater
            than this value will be returned. If None, the latest entry will
            be returned.

        :returns: a tuple of stream ID and the decoded data.

        :raises: TimeoutError, RuntimeError
        """
        if stream_id is None:
            stream_id = "$"
        responses = self._db.xread({stream: stream_id}, 1, block=self._block)
        if not responses:
            raise TimeoutError

        stream_id, record = responses[0][1][0]
        decoded = self._unpack(record[b"data"])

        return stream_id, decoded


class RedisProducer(BaseRedisClient):
    """Provide API for writing data into Redis."""

    def __init__(self,
                 *args,
                 serializer: Union[str, Callable] = "avro",
                 schema: Optional[object] = None,
                 maxlen: int = 10):
        """Initialization.

        :param serializer: serializer type or a callable object which
            serializes the data.
        :param schema: reader's schema for the deserializer (optional).
        :param maxlen: maximum size of the Redis stream.
        """
        super().__init__(*args)

        if callable(serializer):
            self._pack = serializer
        else:
            self._pack = create_serializer(serializer, schema=schema)

        self._maxlen = maxlen

    def produce(self, stream: str, item: Any) -> str:
        """Produce data item to stream.

        :param stream: stream to produce data item to.
        :param item: data item.

        :returns: stream ID.

        :raises: RuntimeError
        """
        encoded = self._pack(item)
        stream_id = self._db.xadd(stream, {"data": encoded}, maxlen=self._maxlen)
        return stream_id.decode()


class RedisClient:
    def __init__(self):
        self._db = None

    def __get__(self, instance, instance_type):
        if self._db is None:
            self._db = redis.Redis(**instance.config)
        return self._db


class RedisSubscriber:
    def __init__(self):
        self._sub = None

    def __get__(self, instance, instance_type):
        if self._sub is None:
            self._sub = redis.Redis(**instance.config).pubsub()
        return self._sub
