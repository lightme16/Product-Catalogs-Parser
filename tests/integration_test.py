import json

import pytest

from src.parser import MongoStorage, Parser


@pytest.mark.parametrize("data_format,sample_path,expected_path", [
    ("csv", 'tests/samples/products.txt', 'tests/samples/products.txt.result'),
    ("xml", 'tests/samples/products.xml', 'tests/samples/products.xml.result'),
])
def test_full_cycle(data_format, sample_path, expected_path):
    mongo_storage = MongoStorage()
    # cleanup after previous run
    mongo_storage.collection.drop()

    parsed_data = Parser(data_format).parse(sample_path)
    mongo_storage.store(parsed_data)

    data_in_collection = [
        {k: v for k, v in d.items() if k != '_id'}
        for d in mongo_storage.collection.find()
    ]
    with open(expected_path) as f:
        assert data_in_collection == json.load(f)
