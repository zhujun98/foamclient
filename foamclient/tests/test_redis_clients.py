from foamclient import (
    DeserializerType, RedisConsumer, RedisProducer, SerializerType
)


def test_redis_consumer():
    with RedisConsumer("localhost", 12345, DeserializerType.SLS) as consumer:
        ...


def test_redis_producer():
    with RedisProducer("localhost", 12345, SerializerType.SLS) as producer:
        ...
