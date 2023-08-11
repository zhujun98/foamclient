from unittest.mock import patch
from foamclient import RedisConsumer, RedisProducer, SerializerType


def test_redis_consumer():
    with patch("redis.Redis") as mocked:
        with RedisConsumer("localhost", 12345,
                           deserializer=SerializerType.AVRO) as consumer:
            args, kwargs = mocked.call_args_list[0]
            assert args == tuple()
            assert kwargs == dict(host="localhost", port=12345, password=None)


def test_redis_producer():
    with patch("redis.Redis") as mocked:
        with RedisProducer("localhost", 12345,
                           serializer=SerializerType.AVRO) as producer:
            args, kwargs = mocked.call_args_list[0]
            assert args == tuple()
            assert kwargs == dict(host="localhost", port=12345, password=None)
