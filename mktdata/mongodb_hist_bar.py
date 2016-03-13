# -*- coding: utf-8 -*-
"""
Created on Thu Oct 30 16:00:19 2014

@author: han.yan
"""

from datetime import datetime
import pandas as pd
from multiprocessing.pool import ThreadPool

from pymongo import MongoClient, ASCENDING
from api import bbg
from cache import environ
from data.db import mongo_write_bar

pd.set_option('display.notebook_repr_html',False)
pd.set_option('display.width',100)
pd.set_option('display.max_columns',6)
pd.set_option('display.max_rows',20)
pd.set_option('display.float_format', '{:,g}'.format)

def main():
    print 'initialization....'

    test = environ.get_env('ETF_1Min')
    universe = test.read_univ()

    ### initalization
    client = MongoClient(max_pool_size = 200)
    collection = client.flow.bar
    collection.ensure_index([('ticker', ASCENDING), ('source', ASCENDING), ('timestamp', ASCENDING)])

    session = bbg.create_session(start = True)

    events = ['BID','ASK','TRADE']
    pool = ThreadPool(processes = 200)
    #pool = Pool(processes = 20)

    pricing_source = 'US'
    asset_type = 'Equity'
    start_date = '2014-04-01'
    cutoff_time = datetime.utcnow()
    #db.bar.findOne({query:{ticker:'SPY'}, orderby:{timestamp:-1}})
    res=[]

    for event in events:
        for ticker in universe:
            args = (collection, session, ticker, asset_type, event,
                    pricing_source, start_date, cutoff_time, False)
            res.append(pool.apply_async(mongo_write_bar, args))

    pool.close()
    pool.join()
    #tmp = [x.get(timeout = 10000) for x in res]
    #return tmp
    client.close()
    session.stop()

if __name__ == '__main__':

    try:
        main()
    except KeyboardInterrupt:
        print 'Ctrl-C detected, exiting...'
