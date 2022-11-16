from foamclient import ZmqClient, DeserializerType


def test_zmq_client():
    with ZmqClient("tcp://localhost:12345", DeserializerType.SLS):
        ...
