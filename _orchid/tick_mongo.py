# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

#import pytz
from datetime import datetime, timedelta, time
from optparse import OptionParser
from msg.mcast import multicast
from pymongo import MongoClient, ASCENDING  # , DESCENDING
from data.db import mongo_truncate
# from pandas import pd
#import timed


class tick_mongo(multicast):

    def __init__(self, drop=False, truncate=None, verbose=False, **kwargs):

        multicast.__init__(self, **kwargs)
        self.type_ = 'mongo'
        self.verbose = verbose
        self.drop = drop
        self.truncate = truncate
        self.tick = self.db_init('flow', 'tick')
        self.bbo = self.db_init('flow', 'bbo')

    ##############
    def db_init(self, db_name, collection_name):
        # initializing database
        client = MongoClient(max_pool_size=100)
        db = client[db_name]

        if self.drop:
            db.drop_collection(collection_name)

        collection = db[collection_name]
        collection.ensure_index(
            [('ticker', ASCENDING),
             ('source', ASCENDING),
             ('timestamp', ASCENDING)])

        if self.truncate is not None:
            mongo_truncate(
                collection,
                datetime.utcnow() + timedelta(hours=-self.truncate))

        return collection

    def process(self, msg, port):
        if port not in ['TICK', 'TOB']:
            return

        ts = datetime.now()
        if ts.time() > time(19, 0):
            print 'stop after 7pm daily...'
            self.signal = 'stop'
            return

        msg['timestamp'] = datetime.utcfromtimestamp(msg['timestamp'])
        msg['quote_time'] = datetime.utcfromtimestamp(msg['quote_time'])

        if port == 'TICK':
            self.tick.insert(msg, w=0)  # unacknowledged writes
        if port == 'TOB':
            msg['source'] = ''
            msg['mid'] = (msg['ask'] + msg['bid']) / 2
            del msg['bid.src']
            del msg['ask.src']
            self.bbo.insert(msg, w=0)

if __name__ == "__main__":

    usage = "usage: %prog [options] <up_port=MASTER|BEAT> <down_port=REPORT>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      help="showing detailed info")
    parser.add_option("-p", "--purge", dest="purge", action="store_true",
                      help="purge the tick collection(table)")
    parser.add_option("-t", dest="truncate", type="int",
                      help="truncating the db to keep last # hours (default: %default)",
                      metavar="truncate", default=None)

    (options, args) = parser.parse_args()
    up_port = ['TICK', 'TOB']
    down_port = []

    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
        parser.error("incorrect number of arguments")

    db = tick_mongo(up_port=up_port, down_port=down_port, drop=options.purge,
                    truncate=options.truncate, verbose=options.verbose)

    print 'start tick ingestion...'
    db.prepare()
    db.listening()
