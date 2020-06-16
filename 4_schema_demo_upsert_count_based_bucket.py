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
        bucket_size = b / 2
        start = self.options['start-date']
        for i in range(n):
            doc = self.create_sub_doc(start, i)
            
            if self.batch:
                operation = UpdateOne(                    
                    {'sensor_id': 12345, 'count': {'$lt': bucket_size}},
                    {
                        '$min': {'date1': doc['timestamp']},
                        '$max': {'date2': doc['timestamp']},
                        '$push': {'measurements': doc},
                        '$inc': {'count': 1, 'sum_temp': doc['temperature'], 'sum_moisture': doc['moisture']}
                    },
                    upsert = True)
                self.add_to_batch(collection, operation)
            else:
                collection.update_one(
                    {'sensor_id': 12345, 'count': {'$lt': bucket_size}},
                    {
                        '$min': {'date1': doc['timestamp']},
                        '$max': {'date2': doc['timestamp']},
                        '$push': {'measurements': doc},
                        '$inc': {'count': 1, 'sum_temp': doc['temperature'], 'sum_moisture': doc['moisture']}
                    },
                    upsert = True)
            
            self.inc_doc_count()

if __name__ == "__main__":
    try:
        test = SchemaDemo()
        collection = test.init(sys.argv, 'schema_demo4', drop_collection=True)
        if collection:
            test.insert_docs(collection)
    except KeyboardInterrupt:
        print('Exiting')
