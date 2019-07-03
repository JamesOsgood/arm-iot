#!/usr/bin/env python3
import random
import sys
import getopt
import datetime
from pymongo import MongoClient
from shared.schema_demo_base import SchemaDemoBase

class SchemaDemo(SchemaDemoBase):

    def clear_all(self):
        client = MongoClient(self.get_connection_string())
        db = client.b2b
        collections = ['alerts', 'schema_demo1', 'schema_demo2', 'schema_demo3', 'schema_demo4', 'schema_demo5' ]
        for collection in collections:
            print(f'Dropping {collection}')
            db[collection].drop({})

if __name__ == "__main__":
    try:
        test = SchemaDemo()
        test.clear_all()

    except KeyboardInterrupt:
        print('Exiting')
