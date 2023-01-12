"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
from typing import Any, Callable, Optional, Union

import redis

from .schema_registry import CachedSchemaRegistry
from .serializer import SerializerType, create_serializer


class RedisProducer:
    """Provide API for writing data into Redis."""

    def __init__(self, host: str, port: int,
                 serializer: Union[SerializerType, Callable], *,
                 password: Optional[str] = None,
                 timeout: Optional[int] = None,
                 maxlen: int = 10):
        """Initialization.

        :param host: hostname of the Redis server.
        :param port: port of the Redis server.
        :param serializer: serializer or serializer type.
        :param password: Redis password.
        :param timeout: subscribe timeout in seconds.
        :param maxlen: maximum size of the Redis stream.
        """
        self._client = redis.Redis(host=host, port=port, password=password)

        self._timeout = timeout  # FIXME: not used for now
        self._maxlen = maxlen

        if callable(serializer):
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

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._client.close()
