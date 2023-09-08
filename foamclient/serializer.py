"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import abc
from collections.abc import Iterable
import io
import pickle
from typing import List, Optional, Union

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


class AbstractSerializer(abc.ABC):
    def __init__(self, *, multipart: bool = False):
        self._multipart = multipart

    @abc.abstractmethod
    def __call__(self, data: Union[object, Iterable[object]])\
            -> Union[bytes, List[bytes]]:
        # Returns a list for a multipart message.
        ...


def encode_ndarray(arr: np.ndarray, *args):
    return {
        "shape": list(arr.shape),
        "dtype": arr.dtype.str,
        "data": arr.tobytes()
    }


class AvroSerializer(AbstractSerializer):

    fastavro.write.LOGICAL_WRITERS['record-ndarray'] = encode_ndarray

    def __init__(self, schema: dict, **kwargs):
        super().__init__(**kwargs)

        if kwargs.get('multipart', False):
            raise ValueError(
                "Avro deserializer does not support multipart message")

        try:
            self._schema = fastavro.parse_schema(schema)
        except TypeError:
            raise TypeError("Parsing Avro schema failed. Invalid type: ", type(schema))

    def __call__(self, data: object) -> bytes:
        """Override."""
        bytes_writer = io.BytesIO()
        fastavro.writer(bytes_writer, self._schema, [data])
        return bytes_writer.getvalue()


class PickleSerializer(AbstractSerializer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __call__(self, data: Union[object, Iterable[object]])\
            -> Union[bytes, List[bytes]]:
        """Override."""
        if self._multipart:
            return [pickle.dumps(item) for item in data]
        return pickle.dumps(data)


class AbstractDeserializer(abc.ABC):
    def __init__(self, *, multipart: bool = False):
        self._multipart = multipart

    @abc.abstractmethod
    def __call__(self, buf: Union[bytes, Iterable[bytes]])\
            -> Union[object, List[object]]:
        # Returns a list for a multipart message.
        ...


def decode_ndarray(item, *args):
    return np.frombuffer(
        item['data'],
        dtype=np.dtype(item['dtype'])).reshape(item['shape'])


class AvroDeserializer(AbstractSerializer):

    fastavro.read.LOGICAL_READERS['record-ndarray'] = decode_ndarray

    def __init__(self, schema: dict, **kwargs):
        super().__init__(**kwargs)

        if kwargs.get('multipart', False):
            raise ValueError(
                "Avro deserializer does not support multipart message")

        try:
            self._schema = fastavro.parse_schema(schema)
        except TypeError:
            raise TypeError("Parsing Avro schema failed. Invalid type: ", type(schema))

    def __call__(self, buf: bytes) -> object:
        """Override."""
        bytes_reader = io.BytesIO(buf)
        return next(fastavro.reader(bytes_reader, self._schema))


class PickleDeserializer(AbstractSerializer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __call__(self, buf: Union[bytes, Iterable[bytes]])\
            -> Union[object, List[object]]:
        """Override."""
        if self._multipart:
            return [pickle.loads(item) for item in buf]
        return pickle.loads(buf)


def create_serializer(name: str, *, schema: Optional[dict] = None, multipart: bool = False)\
        -> AbstractSerializer:
    if name.lower() == "avro":
        return AvroSerializer(schema, multipart=multipart)
    if name.lower() == "pickle":
        if schema is not None:
            raise ValueError("'pickle' serializer does not support schema")
        return PickleSerializer(multipart=multipart)
    raise ValueError


def create_deserializer(name: str, *, schema: Optional[dict] = None, multipart: bool = False)\
        -> AbstractSerializer:
    if name.lower() == "avro":
        return AvroDeserializer(schema, multipart=multipart)
    if name.lower() == "pickle":
        if schema is not None:
            raise ValueError("'pickle' serializer does not support schema")
        return PickleDeserializer(multipart=multipart)
    raise ValueError
