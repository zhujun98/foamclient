"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
from abc import ABC
from typing import Any, Callable, Optional, Union

import redis

from .deserializer import DeserializerType, create_deserializer
from .serializer import SerializerType, create_serializer
from .schema_registry import CachedSchemaRegistry


class BaseRedisClient(ABC):
    def __init__(self, host: str, port: int, password: Optional[str] = None, *,
                 timeout: Optional[int] = None):
        """Initialization.

        :param host: hostname of the Redis server.
        :param port: port of the Redis server.
        :param password: Redis password.
        :param timeout: subscribe timeout in seconds.
        """
        self._client = redis.Redis(host=host, port=port, password=password)

        if timeout is None:
            self._timeout = timeout
        else:
            self._timeout = int(timeout * 1000)  # to milliseconds

    @property
    def client(self):
        return self._client

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._client.close()


RedisClient = redis.Redis  # forwarding


class RedisConsumer(BaseRedisClient):
    """Provide API for consuming data stored in Redis."""

    def __init__(self,
                 *args,
                 deserializer: Union[DeserializerType, Callable] = None,
                 **kwargs):
        """Initialization.

        :param deserializer: deserializer or deserializer type.
        """
        super().__init__(*args, **kwargs)
        self._stream = {}

        if deserializer is None:
            self._unpack = lambda x: x
        elif callable(deserializer):
            self._unpack = deserializer
        else:
            self._unpack = create_deserializer(deserializer)

        self._schema_registry = CachedSchemaRegistry(self._client)

    def subscribe(self, stream: str) -> None:
        """Subscribe to a given stream.

        :param stream: stream name.
        """
        self._stream[stream] = '$'

    def consume(self, count: int = 1) -> ([dict], dict):
        """Consume a list of data items.

        :param count: the maximum number of data items too return.

        :raises: TimeoutError, RuntimeError
        """
        # the returned data is a list with at most 'count' items
        data = self._client.xread(
            self._stream, count, block=self._timeout)
        if not data:
            raise TimeoutError

        schema = self._schema_registry.get(data[0][0].decode())
        if schema is None:
            raise RuntimeError(
                f"Unable to retrieve schema for '{self._stream}'")

        ret = []
        for _, item in data[0][1]:
            ret.append({
                field["name"]: self._unpack(item[field["name"].encode()])
                for field in schema["fields"]
            })
        return ret, schema


class RedisProducer(BaseRedisClient):
    """Provide API for writing data into Redis."""

    def __init__(self,
                 *args,
                 serializer: Union[SerializerType, Callable] = None,
                 maxlen: int = 10,
                 **kwargs):
        """Initialization.

        :param serializer: serializer or serializer type.
        :param maxlen: maximum size of the Redis stream.
        """
        super().__init__(*args, **kwargs)

        self._maxlen = maxlen

        if serializer is None:
            self._unpack = lambda x: x
        elif callable(serializer):
            self._pack = serializer
        else:
            self._pack = create_serializer(serializer)

        self._schema_registry = CachedSchemaRegistry(self._client)

    def _encode_with_schema(self, item, schema):
        return {field["name"]: self._pack(item[field["name"]])
                for field in schema["fields"]}

    def produce(self, stream: str, item: Any, schema: dict) -> str:
        """Produce data item to stream.

        :param stream: stream to produce data item to.
        :param item: data item.
        :param schema: data item schema.

        :raises: RuntimeError
        """
        stream_id = self._client.xadd(
            stream, self._encode_with_schema(item, schema),
            maxlen=self._maxlen
        )
        self._schema_registry.set(stream, schema)
        return stream_id.decode()
