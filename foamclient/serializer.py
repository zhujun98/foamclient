"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu <jun.zhu@psi.ch>
"""
import abc
from enum import Enum
import pickle


class SerializerType(Enum):
    SLS = 1


class Serializer(abc.ABC):
    @abc.abstractmethod
    def __call__(self, msg, **kwargs) -> None:
        ...


class SlsSerializer(Serializer):
    def __call__(self, msg, **kwargs) -> None:
        """Override."""
        return pickle.dumps(msg)


def create_serializer(tp: SerializerType) -> Serializer:
    if tp == SerializerType.SLS:
        return SlsSerializer()
    raise ValueError
