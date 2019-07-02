#!/usr/bin/env python3
import random
import sys
import getopt
import datetime
from pymongo import MongoClient

class SchemaDemoBase(object):

    def __init__(self):
        # Set up batch
        self.batch = True
        self.current_batch = []
        self.BATCH_SIZE = 500
        self.count = 0

    def get_connection_string(self):
        with open('connection_string.txt') as f:
            return f.readline()

    def parse_args(self, argv):
        options = {
            'help': False,
            'quiet': False,
            'n': 10080,
            'b' : 60,
            'start-date': datetime.datetime(2019, 1, 1),
            'drop' : False
        }
        opts, args = getopt.getopt(argv[1:], "hqdn:")
        for opt, arg in opts:
            if opt == '-h':
                options['help'] = True
            elif opt == '-q':
                options['quiet'] = True
            elif opt == '-d':
                options['drop'] = True
            elif opt == '-b':
                options['b'] = int(arg)
            elif opt == '-n':
                options['n'] = int(arg)

        return options


    def add_to_batch(self, collection, operation):
        # Add operation to batch
        self.current_batch.append(operation)

        # Send batch to server
        if len(self.current_batch) == self.BATCH_SIZE:
            collection.bulk_write(self.current_batch)
            self.current_batch = []

    def test_done(self, collection):
        # Send batch to server
        if self.batch:
            if len(self.current_batch) > 0:
                collection.bulk_write(self.current_batch)
                self.current_batch = []
        print(f'{self.count} documents inserted')

    def inc_doc_count(self):
        self.count = self.count + 1
        if self.count % self.BATCH_SIZE == 0:
            print(f'{self.count} documents inserted')

    def init(self, argv, collection_name, drop_collection=True):
        self.options = self.parse_args(argv)
        if self.options['help']:
            print("usage: %s [-h] [-q] [-d] [-n <count>]")
            return None
        else:
            conn_str = self.get_connection_string()
            print( f'Connecting to {conn_str}' )
            connection = MongoClient(conn_str)
            db = connection['b2b']
            collection = db[collection_name]

            if drop_collection or self.options['drop']:
                print("Dropping collection")
                collection.drop()

            return collection

