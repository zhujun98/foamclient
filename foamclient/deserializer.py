"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import abc
from enum import Enum
import pickle


class DeserializerType(Enum):
    SLS = 1


class Deserializer(abc.ABC):
    @abc.abstractmethod
    def __call__(self, msg, **kwargs) -> None:
        ...


class SlsDeserializer(Deserializer):
    def __call__(self, msg, **kwargs) -> None:
        """Override."""
        return pickle.loads(msg)


def create_deserializer(tp: DeserializerType) -> Deserializer:
    if tp == DeserializerType.SLS:
        return SlsDeserializer()
    raise ValueError
