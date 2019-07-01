#!/usr/bin/env python3
import random
import sys
import getopt
import datetime
from pymongo import UpdateOne
from shared.schema_demo_base import SchemaDemoBase

class SchemaDemo(SchemaDemoBase):
    
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
        end = start + datetime.timedelta(minutes=60)
        for i in range(0, n, b):
            for j in range(b):
                doc = self.create_sub_doc(start, j)
                
                if self.batch:
                    operation = UpdateOne( {'sensor_id': 12345, 'date1': start, 'date2': end},
                                           {'$addToSet': {'measurements': doc}},
                                           upsert=True
                                          )
                    self.add_to_batch(collection, operation)
                else:
                    collection.update_one(
                        {'sensor_id': 12345, 'date1': start, 'date2': end},
                        {'$addToSet': {'measurements': doc}},
                        upsert = True)
                self.inc_doc_count()
            start = end
            end = start + datetime.timedelta(minutes=60)
        self.test_done(collection)

if __name__ == "__main__":
    try:
        test = SchemaDemo()
        collection = test.init(sys.argv, 'schema_demo3')
        if collection:
            test.insert_docs(collection)
    except KeyboardInterrupt:
        print('Exiting')
