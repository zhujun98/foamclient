from foamclient import RedisClient, DeserializerType


def test_zmq_client():
    with RedisClient("localhost", 12345, DeserializerType.SLS) as client:
        ...
