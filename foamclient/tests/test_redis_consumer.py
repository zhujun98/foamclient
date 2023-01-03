from foamclient import RedisConsumer, DeserializerType


def test_redis_consumer():
    with RedisConsumer("localhost", 12345, DeserializerType.SLS) as consumer:
        ...
