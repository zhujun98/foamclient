from foamclient import (
    DeserializerType, RedisConsumer, RedisProducer, SerializerType
)


def test_redis_consumer():
    with RedisConsumer(DeserializerType.SLS, "localhost", 12345) as consumer:
        ...


def test_redis_producer():
    with RedisProducer(SerializerType.SLS, "localhost", 12345) as producer:
        ...
