from abc import ABC, abstractmethod
import json

import avro.schema
import numpy as np


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
    schema = avro.schema.parse(json.dumps({
        "namespace": "unittest",
        "type": "record",
        "name": "test_data",
        "fields": [
            {
                "name": "index",
                "type": "long"
            },
            {
                "name": "name",
                "type": "string"
            },
        ]
    }))

    def next(self):
        data = {
            "index": self._counter,  # integer
            "name": f"data{self._counter}",  # string
        }
        self._counter += 1
        return data


class PickleDataGenerator(AbstractDataGenerator):
    def next(self):
        data = {
            "index": self._counter,
            "name": f"data{self._counter}"
        }
        self._counter += 1
        return data
