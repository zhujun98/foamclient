import pytest

from foamclient import (
    create_serializer, create_deserializer
)

from .conftest import assert_result_equal, AvroDataGenerator


@pytest.mark.parametrize("dataset", ["dataset1", "dataset2"])
def test_avro_serializer(dataset):
    gen = AvroDataGenerator()
    encoder = create_serializer("avro")
    decoder = create_deserializer("avro")
    data_gt, schema = getattr(gen, dataset)()
    data = decoder(encoder(data_gt, schema), schema)
    assert_result_equal(data, data_gt)
