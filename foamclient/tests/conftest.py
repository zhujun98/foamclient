from abc import ABC, abstractmethod

import fastavro
import numpy as np

from foamclient import AvroSchemaExt


class AbstractDataGenerator(ABC):

    schema = None

    def __init__(self):
        self._counter = 0

    @abstractmethod
    def next(self):
        ...


class StringDataGenerator(AbstractDataGenerator):
    def next(self):
        self._counter += 1
        return f"data{self._counter}"


class AvroDataGenerator(AbstractDataGenerator):
    schema = {
        "namespace": "unittest",
        "type": "record",
        "name": "Testdata",
        "fields": [
            {
                "name": "integer",
                "type": "long"
            },
            {
                "name": "string",
                "type": "string"
            },
            {
                "name": "array1d",
                "type": AvroSchemaExt.ndarray
            },
        ]
    }

    def next(self):
        data = {
            "integer": self._counter,
            "string": f"data{self._counter}",
            "array1d": np.ones(10, dtype=np.int32) * self._counter,
        }
        self._counter += 1
        return data

    def dataset1(self):
        schema = {
            "namespace": "unittest",
            "name": "Testdata1",
            "type": "record",
            "fields": [
                {
                    "name": "integer",
                    "type": "long"
                },
                {
                    "name": "string",
                    "type": "string"
                },
                {
                    "name": "array1d",
                    "type": AvroSchemaExt.ndarray
                },
            ]
        }
        return {
            "integer": 123,
            "string": f"data123",
            "array1d": np.array([1, 2, 3], dtype=np.int32)
        }, schema

    def dataset2(self):
        schema = {
            "namespace": "unittest",
            "name": "Testdata2",
            "type": "record",
            "fields": [
                {
                    "name": "boolean",
                    "type": "boolean"
                },
                {
                    "name": "float",
                    "type": "float"
                },
                {
                    "name": "record",
                    "type": {
                        "type": "record",
                        "name": "NestedRecord",
                        "fields": [
                            {
                                "name": "double",
                                "type": "double"
                            },
                            {
                                "name": "array2d",
                                "type": AvroSchemaExt.ndarray
                            }
                        ]
                    }
                },
            ]
        }
        return {
            "boolean": True,
            "float": 1.0,
            "record": {
                "double": 2.0,
                "array2d": np.array([(1., 2.), (3., 4.)], dtype=np.float32)
            }
        }, schema


class PickleDataGenerator(AbstractDataGenerator):
    def next(self):
        data = {
            "integer": self._counter,
            "string": f"data{self._counter}"
        }
        self._counter += 1
        return data


def assert_result_equal(left, right):
    if isinstance(left, dict):
        for k, v in left.items():
            assert k in right
            assert isinstance(right[k], type(v))
            if isinstance(v, np.ndarray):
                np.testing.assert_array_equal(v, right[k])
            else:
                assert_result_equal(v, right[k])
    else:
        assert left == right
