import json

import pytest

from src.parser import Parser


@pytest.mark.parametrize("data_format,sample_path,expected_path", [
    ("csv", 'tests/samples/products.txt', 'tests/samples/products.txt.result'),
    ("xml", 'tests/samples/products.xml', 'tests/samples/products.xml.result'),
])
def test_parsers(data_format, sample_path, expected_path):
    parsed_data = list(Parser(data_format).parse(sample_path))
    with open(expected_path) as f:
        assert parsed_data == json.load(f)
