#!/usr/bin/env python3
import random
import sys
import getopt
import datetime
from pymongo import MongoClient, InsertOne
from shared.schema_demo_base import SchemaDemoBase

class SchemaDemo(SchemaDemoBase):

    def create_doc(self, time, delta):
        doc = {
            'sensor_id': 12345,
            'timestamp': time + datetime.timedelta(minutes=delta),
            'temperature': random.randint(62, 66),
            'moisture': random.randint(500, 600)
        }
        return doc

    def insert_docs(self, collection):
        n = self.options['n']
        start = self.options['start-date']
        for i in range(n):
            doc = self.create_doc(start, i)
            
            if self.batch:
                operation = InsertOne(doc)
                self.add_to_batch(collection, operation )
                self.inc_doc_count()
            else:
                collection.insert_one(doc)
                self.inc_doc_count()
        
        self.test_done(collection)

if __name__ == "__main__":
    try:
        test = SchemaDemo()
        collection = test.init(sys.argv, 'schema_demo1', drop_collection=True)
        if collection:
            test.insert_docs(collection)

    except KeyboardInterrupt:
        print('Exiting')
