#!/usr/bin/env python3
import datetime
import getopt
import json
import random
import sys

from bson import json_util
from pymongo import UpdateOne

from shared.schema_demo_base import SchemaDemoBase

class SchemaDemo(SchemaDemoBase):

    def get_max_timestamp(self, collection):
        pipeline = [
            {
                '$group': {
                    '_id': 'max', 
                    'maxTime': {
                        '$max': '$date2'
                    }
                }
            }
        ]
        docs = list(collection.aggregate(pipeline))
        return docs[0]['maxTime']

    def create_sub_doc_version2(self, ts, value):

        doc = {
            'timestamp': ts,
            'temperature': random.randint(62, 66),
            'moisture': random.randint(500, 600),
            'pressure': value
        }
        
        return doc

    def fire_trigger(self, collection, value):

        b = self.options['b']
        ts = self.get_max_timestamp(collection)
        doc = self.create_sub_doc_version2(ts, value)
        collection.update_one(                    
                {'sensor_id': 12345, 'count': {'$lt': b/2}},
                {
                    '$min': {'date1': doc['timestamp']},
                    '$max': {'date2': doc['timestamp']},
                    '$push': {'measurements': doc},
                    '$inc': {'count': 1, 'sum_temp': doc['temperature'], 'sum_moisture': doc['moisture']}
                },
                upsert = True)


if __name__ == "__main__":
    try:
        test = SchemaDemo()
        collection = test.init(sys.argv, 'schema_demo5', drop_collection=False)
        if collection:
            test.fire_trigger(collection, 1050)
    except KeyboardInterrupt:
        print('Exiting')
