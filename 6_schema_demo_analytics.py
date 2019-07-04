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

if __name__ == "__main__":
    try:
        test = SchemaDemo()
        collection = test.init(sys.argv, 'schema_demo5', drop_collection=False)
        if collection:
            avg_pressure = test.find_pressure_average(collection)
            print(f'Average is {avg_pressure}')
    except KeyboardInterrupt:
        print('Exiting')
