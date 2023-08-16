"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import abc
from collections.abc import Iterable
from enum import Enum
import io
import pickle
from typing import Optional, Union

import fastavro
import numpy as np


class AvroSchemaExt:

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
    def __init__(self, schema: Optional[object] = None, *,
                 multipart: bool = False):
        self._schema = schema
        self._multipart = multipart

    @abc.abstractmethod
    def __call__(self, data: object,
                 schema: Optional[object] = None,
                 **kwargs) -> Union[list, object]:
        ...


def encode_ndarray(arr: np.ndarray, *args):
    return {
        "shape": list(arr.shape),
        "dtype": arr.dtype.str,
        "data": arr.tobytes()
    }


class AvroSerializer(AbstractSerializer):

    fastavro.write.LOGICAL_WRITERS['record-ndarray'] = encode_ndarray

    def __init__(self, schema: Optional[object] = None, **kwargs):
        if kwargs.get('multipart', False):
            raise ValueError(
                "Avro deserializer does not support multipart message")
        super().__init__(schema, **kwargs)

    def __call__(self, data: dict,
                 schema: Optional[object] = None,
                 **kwargs) -> bytes:
        """Override."""
        bytes_writer = io.BytesIO()
        if schema is None:
            schema = self._schema
        fastavro.writer(bytes_writer, schema, [data])
        return bytes_writer.getvalue()


class PickleSerializer(AbstractSerializer):
    def __init__(self, schema: None, **kwargs):
        if schema is not None:
            raise ValueError("Pickle serializer does not support schema")
        super().__init__(schema, **kwargs)

    def __call__(self, data: Union[object, Iterable[object]], **kwargs)\
            -> Union[list, object]:
        """Override."""
        if self._multipart:
            return [pickle.dumps(item) for item in data]
        return pickle.dumps(data)


class AbstractDeserializer(abc.ABC):
    def __init__(self, schema: Optional[object] = None, *,
                 multipart: bool = False):
        self._schema = schema
        self._multipart = multipart

    @abc.abstractmethod
    def __call__(self, buf: bytes,
                 schema: Optional[object] = None,
                 **kwargs) -> Union[list, object]:
        ...


def decode_ndarray(item, *args):
    return np.frombuffer(
        item['data'],
        dtype=np.dtype(item['dtype'])).reshape(item['shape'])


class AvroDeserializer(AbstractSerializer):

    fastavro.read.LOGICAL_READERS['record-ndarray'] = decode_ndarray

    def __init__(self, schema: Optional[object] = None, **kwargs):
        if kwargs.get('multipart', False):
            raise ValueError(
                "Avro deserializer does not support multipart message")
        super().__init__(schema, **kwargs)

    def __call__(self, buf: bytes,
                 schema: Optional[object] = None,
                 **kwargs) -> object:
        """Override."""
        bytes_reader = io.BytesIO(buf)
        if schema is None:
            schema = self._schema
        return next(fastavro.reader(bytes_reader, schema))


class PickleDeserializer(AbstractSerializer):
    def __init__(self, schema: None, **kwargs):
        if schema is not None:
            raise ValueError("Pickle serializer does not support schema")
        super().__init__(None, **kwargs)

    def __call__(self, buf: Union[bytes, Iterable[bytes]], **kwargs)\
            -> Union[list, object]:
        """Override."""
        if self._multipart:
            return [pickle.loads(item) for item in buf]
        return pickle.loads(buf)


def create_serializer(tp: SerializerType, *args, **kwargs)\
        -> AbstractSerializer:
    if tp == SerializerType.AVRO:
        return AvroSerializer(*args, **kwargs)
    if tp == SerializerType.PICKLE:
        return PickleSerializer(*args, **kwargs)
    raise ValueError


def create_deserializer(tp: SerializerType, *args, **kwargs)\
        -> AbstractSerializer:
    if tp == SerializerType.AVRO:
        return AvroDeserializer(*args, **kwargs)
    if tp == SerializerType.PICKLE:
        return PickleDeserializer(*args, **kwargs)
    raise ValueError
