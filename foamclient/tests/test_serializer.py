import pytest

from foamclient import (
    create_serializer, create_deserializer
)

from foamclient.tests.conftest import (
    assert_result_equal, AvroDataGenerator
)


@pytest.mark.parametrize("dataset", ["dataset1", "dataset2"])
def test_avro_serializer(dataset):
    gen = AvroDataGenerator()
    data, schema = getattr(gen, dataset)()

    pack = create_serializer("avro", schema=schema)
    unpack = create_deserializer("avro", schema=schema)

    encoded = pack(data)
    decoded = unpack(encoded)

    assert_result_equal(decoded, data)
