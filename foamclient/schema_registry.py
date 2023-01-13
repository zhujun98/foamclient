"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
import pickle
from typing import Optional

import redis


class CachedSchemaRegistry:
    """Provide API for schema read and write."""

    def __init__(self, client: redis.Redis):
        """Initialization.

        :param client: Redis client.
        """
        self._db = client

        self._schemas = {}

    def get(self, stream: str) -> Optional[dict]:
        """Get schema for a given data stream.

        :param stream: name of the data stream.
        """
        if stream in self._schemas:
            return self._schemas[stream]

        try:
            schema = self._db.execute_command(
                'HGET', f"{stream}:schema", "schema")
            if schema is not None:
                schema = pickle.loads(schema)
                self._schemas[stream] = schema
                return schema
        except redis.exceptions.ConnectionError:
            ...

    def set(self, stream: str, schema) -> None:
        """Set schema for a given data stream.

        :param stream: name of the data stream.
        :param schema: data schema.
        """
        if stream in self._schemas:
            return

        # TODO: add an option to check whether the schema has changed

        try:
            self._db.execute_command(
                'HSET', f"{stream}:schema", "schema", pickle.dumps(schema))
            self._schemas[stream] = schema
        except redis.exceptions.ConnectionError:
            return
