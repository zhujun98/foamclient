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
import numpy as np


class SerializerType(Enum):
    AVRO = 0
    PICKLE = 1


class AbstractSerializer(abc.ABC):
    @abc.abstractmethod
    def __call__(self, datum: object,
                 schema: Optional[object] = None,
                 **kwargs) -> object:
        ...


class Serializer(AbstractSerializer):
    def __init__(self, schema: Optional[avro.schema.Schema] = None):
        super().__init__()
        self._writer = DatumWriter(schema)

    def _preprocess(self, datum):
        ret = {}
        for k, v in datum.items():
            if isinstance(v, np.ndarray):
                ret[k] = {
                    "shape": list(v.shape),
                    "dtype": v.dtype.str,
                    "data": v.tobytes()
                }
            else:
                ret[k] = v
        return ret

    def __call__(self, datum: dict,
                 schema: Optional[avro.schema.Schema] = None,
                 **kwargs) -> bytes:
        """Override."""
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        if schema is not None:
            self._writer.writers_schema = schema

        self._writer.write(self._preprocess(datum), encoder)
        return bytes_writer.getvalue()


class PySerializer(AbstractSerializer):
    def __call__(self, datum: object, **kwargs) -> object:
        """Override."""
        return pickle.dumps(datum)


class AbstractDeserializer(abc.ABC):
    @abc.abstractmethod
    def __call__(self, buf: bytes,
                 schema: Optional[object] = None,
                 **kwargs) -> object:
        ...


class Deserializer(AbstractSerializer):
    def __init__(self, schema: Optional[avro.schema.Schema] = None):
        super().__init__()
        self._reader = DatumReader(writers_schema=schema)

    def _postprocess(self, datum, schema):
        for field in schema.fields:
            name = field.name
            item = datum[name]
            props = field.type.props
            if props.get('logicalType', None) == 'ndarray':
                datum[name] = np.frombuffer(
                    item['data'],
                    dtype=np.dtype(item['dtype'])).reshape(item['shape'])
        return datum

    def __call__(self, buf: bytes,
                 schema: Optional[avro.schema.Schema] = None,
                 **kwargs) -> object:
        """Override."""
        decoder = BinaryDecoder(io.BytesIO(buf))
        if schema is not None:
            self._reader.writers_schema = schema
        data = self._reader.read(decoder)
        return self._postprocess(data, self._reader.writers_schema)


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
