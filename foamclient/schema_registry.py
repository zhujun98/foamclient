"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
import json
from typing import Optional

import fastavro
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
                'HGET', f"{stream}:_schema", "0")
            if schema is not None:
                parsed_schema = fastavro.parse_schema(json.loads(schema))
                self._schemas[stream] = parsed_schema
                return parsed_schema
        except redis.exceptions.ConnectionError:
            ...

    def set(self, stream: str, schema: dict) -> Optional[dict]:
        """Set schema for a given data stream.

        :param stream: name of the data stream.
        :param schema: data schema.
        """
        if stream in self._schemas:
            # return the parsed schema
            return self._schemas[stream]

        # TODO: add an option to check whether the schema has changed, e.g. check version

        try:
            # save the raw schema
            self._db.execute_command(
                'HSET', f"{stream}:_schema", "0", json.dumps(schema))
            # cache the parsed schema
            parsed_schema = fastavro.parse_schema(schema)
            self._schemas[stream] = parsed_schema
            return parsed_schema
        except redis.exceptions.ConnectionError:
            ...
