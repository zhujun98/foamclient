"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import abc
from enum import Enum
import io
import pickle
from typing import Optional

import avro.schema
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter


class SerializerType(Enum):
    AVRO = 0
    PICKLE = 1


class AbstractSerializer(abc.ABC):
    def __init__(self, schema: object = None):
        self._schema = schema

    @abc.abstractmethod
    def __call__(self, datum: object,
                 schema: Optional[object] = None,
                 **kwargs) -> object:
        ...


class Serializer(AbstractSerializer):
    def __init__(self, schema: Optional[avro.schema.Schema] = None):
        super().__init__(schema)
        self._writer = DatumWriter(schema)

    def __call__(self, datum: object,
                 schema: Optional[avro.schema.Schema] = None,
                 **kwargs) -> bytes:
        """Override."""
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        if schema is not None:
            self._writer.writers_schema = schema
        self._writer.write(datum, encoder)
        return bytes_writer.getvalue()


class PySerializer(AbstractSerializer):
    def __call__(self, datum: object, **kwargs) -> object:
        """Override."""
        return pickle.dumps(datum)


class AbstractDeserializer(abc.ABC):
    def __init__(self, schema: object = None):
        self._schema = schema

    @abc.abstractmethod
    def __call__(self, buf: bytes,
                 schema: Optional[object] = None,
                 **kwargs) -> object:
        ...


class Deserializer(AbstractSerializer):
    def __init__(self, schema: Optional[avro.schema.Schema] = None):
        super().__init__(schema)
        self._reader = DatumReader(writers_schema=schema)

    def __call__(self, buf: bytes,
                 schema: Optional[avro.schema.Schema] = None,
                 **kwargs) -> object:
        """Override."""
        decoder = BinaryDecoder(io.BytesIO(buf))
        if schema is not None:
            self._reader.writers_schema = schema
        return self._reader.read(decoder)


class PyDeserializer(AbstractSerializer):
    def __call__(self, buf: bytes, **kwargs) -> object:
        """Override."""
        return pickle.loads(buf)


def create_serializer(tp: SerializerType, schema: Optional[object] = None)\
        -> AbstractSerializer:
    if tp == SerializerType.AVRO:
        return Serializer(schema)
    if tp == SerializerType.PICKLE:
        return PySerializer()
    raise ValueError


def create_deserializer(tp: SerializerType, schema: Optional[object] = None)\
        -> AbstractSerializer:
    if tp == SerializerType.AVRO:
        return Deserializer(schema)
    if tp == SerializerType.PICKLE:
        return PyDeserializer()
    raise ValueError
