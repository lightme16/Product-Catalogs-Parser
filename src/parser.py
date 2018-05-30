import functools
import os
import sys
import time
from abc import ABCMeta, abstractmethod
from typing import Generator

import pandas as pd
import pymongo
from lxml import etree
from pymongo import MongoClient

CSV, XML = 'csv', 'xml'

# Mapping of different data format keys to generic model keys
MODEL_TO_CSV_DICT = {
    'id': 'product_id',
    'title': 'product_name',
    'sku_number': 'sku_number',
    'url': 'product_url',
    'image_url': 'product_image_url',
    'buy_url': 'buy_url',
    'description': 'long_product_description',
    'discount': 'discount',
    'discount_type': 'discount_type',
    'currency': 'currency',
    'retail_price': 'retail_price',
    'sale_price': 'sale_price',
    'brand': 'brand',
    'manufacture': 'manufacture_name',
    'shipping': 'shipping',
    'availability': 'availability',
}
MODEL_TO_XML_DICT = {
    'id': 'item_unique_id',
    'title': 'item_title',
    'sku_number': 'item_sku',
    'url': 'item_page_url',
    'image_url': 'item_image_url',
    'buy_url': 'offer_page_url',
    'description': 'book_size_description',
    'discount': None,
    'discount_type': None,
    'currency': None,
    'retail_price': 'item_price',
    'sale_price': 'list_price',
    'brand': 'item_seller',
    'manufacture': 'item_platform',
    'shipping': "item_shipping_charge",
    'availability': 'amzn_sales_restriction',
}


def measure_time(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        start = time.time()
        rv = f(*args, **kwargs)
        took = time.time() - start
        print('Operation %s took %s' % (f.__name__, took))
        return rv

    return wrapper


class Storage(metaclass=ABCMeta):
    """
    Base class that responsible for saving data into persistent storage.
    In order to add new realization of storage create a subclass that will
    implement `store` function.
    """

    @abstractmethod
    def store(self, data: Generator[dict]):
        pass


class MongoStorage(Storage):

    def __init__(self):
        db_host = os.environ.get('DB_HOST', 'localhost')
        self.client = MongoClient(db_host, 27017)
        self.collection = self.client.db.parsed
        index_name = 'id'
        if index_name not in self.collection.index_information():
            self.collection.create_index(index_name, unique=True)

    @measure_time
    def store(self, data: Generator[dict]):
        bulk = pymongo.bulk.BulkOperationBuilder(self.collection,
                                                 ordered=False)
        for doc in data:
            # if doc exists, it will be updated
            bulk.find({"id": doc["id"]}).upsert().replace_one(doc)
        bulk.execute()


class ParserBase(metaclass=ABCMeta):
    """
    Base class that responsible for parsing data from file. You can easiliy
    add parsers for new format by creating a subclasses.
    """

    @classmethod
    @abstractmethod
    def parse(cls, path) -> Generator[Generator]:
        """
        Method that actualy parse data. Should return iterator in order to not
        load whole file into memory.
        """
        pass


class CsvParser(ParserBase):
    HEADERS_CSV = [
        'product_id',
        'product_name',
        'sku_number',
        'primary_category',
        'secondary_category',
        'product_url',
        'product_image_url',
        'buy_url',
        'short_product_description',
        'long_product_description',
        'discount', 'discount_type',
        'sale_price',
        'retail_price',
        'begin_date',
        'end_date',
        'brand',
        'shipping',
        'keywords',
        'manufacture_part_number',
        'manufacture_name',
        'shipping_information',
        'availability',
        'universal_product_code',
        'class_id',
        'currency',
        'm1',
        'pixel',
        'miscellaneous_attribute',
        'attribute_2', 'attribute_3',
        'attribute_4',
        'attribute_5',
        'attribute_6',
        'attribute_7',
        'attribute_8',
        'attribute_9',
        'attribute_10'
    ]

    @classmethod
    def parse(cls, path) -> Generator[dict]:
        for chunk in pd.read_csv(path, sep='|', header=None,
                                 names=cls.HEADERS_CSV, iterator=True):
            yield from chunk.fillna('').to_dict('records')


class XmlParser(ParserBase):
    @classmethod
    def parse(cls, path) -> Generator[dict]:
        for event, elem in etree.iterparse(path, events=('end',),
                                           tag='item_basic_data'):
            yield {child.tag: child.text for child in elem}
            # cleanup in order to save RAM
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]


class Parser:
    """
    Class that responsible for parsing different file types.
    Implementing facade pattern. Aggregates concrete parsers implementation
    that used for parsing various data formats.
    In order to add support of the new data formats you need to add information
    about it into PARSERS_DATA dict. You need to provide realization of the
    parsers class, also add mapping of the new format naming to generic
    model, that used in the DB.
    """
    PARSERS_DATA = {
        CSV: {'parser': CsvParser, 'model_mapping': MODEL_TO_CSV_DICT},
        XML: {'parser': XmlParser, 'model_mapping': MODEL_TO_XML_DICT}
    }

    def __init__(self, data_format):
        try:
            self._parser = self.PARSERS_DATA[data_format]['parser']
        except KeyError:
            raise ValueError('No parser for %s format' % data_format)
        else:
            self.format = data_format

    def coerce_model(self, parsed_data: Generator[dict]) -> Generator[dict]:
        """Sets new keys for the parsed data according to current model"""
        mapping = self.PARSERS_DATA[self.format]['model_mapping']
        for item in parsed_data:
            yield {
                model_name: item[concrete_format_name]
                for model_name, concrete_format_name in mapping.items()
                if concrete_format_name and item.get(concrete_format_name)
            }

    def parse(self, path) -> Generator[dict]:
        return self.coerce_model(self._parser.parse(path))


if __name__ == '__main__':
    try:
        mongo_storage = MongoStorage()
        for data_format, path in [
            ('csv', '../tests/samples/products_full.txt'),
            ('xml', '../tests/samples/products_full.xml')
        ]:
            data = Parser(data_format).parse(path)
            mongo_storage.store(data)
    except Exception as e:
        sys.exit(e)
    else:
        print('All done!')
