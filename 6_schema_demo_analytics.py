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

    def find_pressure_average(self, collection):

        pipeline = [
            {
                '$unwind': {
                    'path': '$measurements'
                }
            }, {
                '$group': {
                    '_id': '$sensor_id', 
                    'avg_pressure': {
                        '$avg': '$measurements.pressure'
                    }
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        return results[0]['avg_pressure']

    def find_pressure_spike(self, collection, value):

        pipeline = [
            {
                '$match': {
                    'measurements.pressure': {
                        '$gte': value
                    }
                }
            }, {
                '$project': {
                    'measurement': {
                        '$filter': {
                            'input': '$measurements', 
                            'as': 'item', 
                            'cond': {
                                '$gte': [
                                    '$$item.pressure', 1030
                                ]
                            }
                        }
                    }, 
                    'sensor_id': 1, 
                    'count': 1
                }
            }, {
                '$unwind': {
                    'path': '$measurement'
                }
            }, {
                '$sort': {
                    'measurement.pressure': -1
                }
            }, {
                '$limit': 3
            }
        ]
        
        results = collection.aggregate(pipeline)
        for doc in results:
            print(json.dumps(doc, default=json_util.default, indent=4))

if __name__ == "__main__":
    try:
        test = SchemaDemo()
        collection = test.init(sys.argv, 'schema_demo5', drop_collection=False)
        if collection:
            avg_pressure = test.find_pressure_average(collection)
            test.find_pressure_spike(collection, avg_pressure )
    except KeyboardInterrupt:
        print('Exiting')
