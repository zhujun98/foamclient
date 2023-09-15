import pytest
from collections import deque
from threading import Thread
import time

from redis import ConnectionError

from foamclient import RedisConsumer, RedisProducer
from foamclient.tests.conftest import assert_result_equal, AvroDataGenerator


NUM_RECORDS = 10


def consume(consumer, stream, output: deque):
    try:
        stream_id = None
        for _ in range(NUM_RECORDS):
            stream_id, record = consumer.consume(stream, stream_id)
            output.append(record)
    except ConnectionError:
        ...


def test_redis_clients():
    gen = AvroDataGenerator()
    schema = gen.schema
    stream = f"{schema['namespace']}_{schema['name']}"
    with (RedisProducer("localhost", 6379, serializer="avro", schema=schema) as producer):
        with RedisConsumer("localhost", 6379,
                           deserializer="avro",
                           schema=schema,
                           block=1000) as consumer:

            outputs = deque()
            t = Thread(target=consume, args=(consumer, stream, outputs))
            t.start()

            # Launching a thread may take some time.
            time.sleep(0.1)

            inputs = deque()
            for _ in range(NUM_RECORDS):
                raw = gen.next()
                try:
                    producer.produce(stream, raw)
                except ConnectionError:
                    print("Test skipped: no Redis connection")
                    return  # skip if there is no Redis connection
                inputs.append(raw)

            t.join()

            for i, o in zip(inputs, outputs):
                assert_result_equal(i, o)
