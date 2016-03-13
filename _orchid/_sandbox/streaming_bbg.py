# -*- coding: utf-8 -*-
"""
Created on Mon Jun 23 11:16:21 2014

@author: han.yan
"""
#import os
#import pandas as pd
# from dateutil import parser

#import pytz
from datetime import datetime
from optparse import OptionParser
import time
#import numpy as np

from api import bbg
from _sandbox import streaming_oanda as oad ### only for retriving universe
from msg.msgpk import stream

latest = {}
timestamp = lambda x: (x - datetime(1970, 1, 1)).total_seconds()

def action(msg, cnt, cursor = None, publisher = None, silence = False, outbound = None, packer = None):
    #print msg
    row = bbg.parse_BBG_stream(msg)
    if row is None:
        return
    ticker, quote_time, type_, subtype, price, size, db_time = row
    
    if not latest.has_key(ticker):
        latest[ticker] = {'bid':float('nan'), 'ask':float('nan'), 
                    'trade':float('nan')}
    else:
        if type_ == 'TRADE':
            latest[ticker]['trade'] = price
        elif subtype == 'ASK':
            latest[ticker]['ask'] = price
        elif subtype == 'BID':
            latest[ticker]['bid'] = price
        latest[ticker]['time'] = quote_time
 
    instrument, dealer = ticker.split(' ')[:2]
    dt = {}
    dt['instrument'] = instrument
    dt['dealer'] = dealer
    dt['bid'] = latest[ticker]['bid']
    dt['ask'] = latest[ticker]['ask']
    
#    if np.isnan(dt['bid']) and instrument == 'GBPJPY':
#        print msg
    
    quote_time = quote_time#.replace(tzinfo = None)
    rec_time = time.time()
    
    dt['time'] = timestamp(quote_time)#.isoformat()
    dt['stamp'] = rec_time #timestamp(rec_time)#rec_time.isoformat()

    outbound.sending(dt)  ## send
    
    if not silence:
       lag = (rec_time - quote_time).total_seconds()
       print "delay:%.3f cnt:%g\t%s %s bid:%f  ask:%f  trade:%f  time:%s" % \
       (lag, cnt, instrument, dealer, dt['bid'], dt['ask'], dt['trade'], dt['stamp'])

if __name__ == "__main__":

    usage = "usage: %prog [options] [port1=8001] [port2=....] [port3=....] ..."
    parser = OptionParser(usage)
    parser.add_option("-s", "--silence", dest = "silence", action = "store_true", 
                        help = "hide streaming data")

    ports = [8001]
    silence = False
    
    (options, args) = parser.parse_args()
    if len(args) >= 1:
        ports = [int(x) for x in args]
    if options.silence:
        silence = True

    universe = oad.get_all_instruments()
    instruments = [x.replace('_', '').encode('ascii','ignore') for x in universe.instrument]
    sources = ['BGN','CMPN','BTFX']
    postfix = 'Curncy'
    table_name = 'flow'
    security_list = [(x + ' ' + y + ' ' + postfix) for x in instruments for y in sources]
    
    ## incoming streams from BBG Desktop API
    session = bbg.create_session(start = True)
                
    #### calling custom stream class for sending outbound msgpacks
    outbound = stream(down_port = ports, sleep_time = 0)
    outbound.prepare()  ### establishing connection(s)

    try:
        bbg.BBG_stream(session, security_list, action, fields = 'LAST_TRADE,BEST_BID,BEST_ASK',
                       silence = silence, outbound = outbound)
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."
    finally:
        # Stop the session  
        session.stop()
