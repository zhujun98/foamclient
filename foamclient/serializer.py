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

import fastavro
import numpy as np


class AvroSchema:

    ndarray = {
        "type": "record",
        "logicalType": "ndarray",
        "name": "NDArray",
        "fields": [
            {"name": "shape", "type": {"items": "int", "type": "array"}},
            {"name": "dtype", "type": "string"},
            {"name": "data", "type": "bytes"}
        ]
    }


class SerializerType(Enum):
    AVRO = 0
    PICKLE = 1


class AbstractSerializer(abc.ABC):
    def __init__(self, schema: Optional[object] = None):
        self._schema = schema

    @abc.abstractmethod
    def __call__(self, datum: object,
                 schema: Optional[object] = None,
                 **kwargs) -> object:
        ...


def encode_ndarray(arr: np.ndarray, *args):
    return {
        "shape": list(arr.shape),
        "dtype": arr.dtype.str,
        "data": arr.tobytes()
    }


class Serializer(AbstractSerializer):

    fastavro.write.LOGICAL_WRITERS['record-ndarray'] = encode_ndarray

    def __init__(self, schema: Optional[object] = None):
        super().__init__(schema)

    def __call__(self, datum: dict,
                 schema: Optional[object] = None,
                 **kwargs) -> bytes:
        """Override."""
        bytes_writer = io.BytesIO()
        if schema is None:
            schema = self._schema
        fastavro.schemaless_writer(bytes_writer, schema, datum)
        return bytes_writer.getvalue()


class PySerializer(AbstractSerializer):
    def __call__(self, datum: object, **kwargs) -> object:
        """Override."""
        return pickle.dumps(datum)


class AbstractDeserializer(abc.ABC):
    def __init__(self, schema: Optional[object] = None):
        self._schema = schema

    @abc.abstractmethod
    def __call__(self, buf: bytes,
                 schema: Optional[object] = None,
                 **kwargs) -> object:
        ...


def decode_ndarray(item, *args):
    return np.frombuffer(
        item['data'],
        dtype=np.dtype(item['dtype'])).reshape(item['shape'])


class Deserializer(AbstractSerializer):

    fastavro.read.LOGICAL_READERS['record-ndarray'] = decode_ndarray

    def __init__(self, schema: Optional[object] = None):
        super().__init__(schema)

    def __call__(self, buf: bytes,
                 schema: Optional[object] = None,
                 **kwargs) -> object:
        """Override."""
        bytes_reader = io.BytesIO(buf)
        if schema is None:
            schema = self._schema
        return fastavro.schemaless_reader(bytes_reader, schema)


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
