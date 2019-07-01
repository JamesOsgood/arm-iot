#!/usr/bin/env python3
import random
import sys
import getopt
import datetime
from pymongo import InsertOne
from shared.schema_demo_base import SchemaDemoBase

class SchemaDemo(SchemaDemoBase):

    def create_doc(self):
        doc = {
            'sensor_id': 12345,
            'measurements' : []
        }
        return doc

    def create_sub_doc(self, time, delta):
        doc = {
            'timestamp': time + datetime.timedelta(minutes=delta),
            'temperature': random.randint(62, 66),
            'moisture': random.randint(500, 600)
        }
        return doc

    def insert_docs(self, collection):
        n = self.options['n']
        b = self.options['b']
        start = self.options['start-date']
        for i in range(0, n, b):
            doc = self.create_doc()
            for j in range(b):
                sub_doc = self.create_sub_doc(start, i+j)
                doc['measurements'].append(sub_doc)
                self.inc_doc_count()

            if self.batch:
                operation = InsertOne(doc)
                self.add_to_batch(collection, operation)
            else:
                collection.insert_one(doc)

        self.test_done(collection)

if __name__ == "__main__":
    try:
        test = SchemaDemo()
        collection = test.init(sys.argv, 'schema_demo2')
        if collection:
            test.insert_docs(collection)
    except KeyboardInterrupt:
        print('Exiting')
