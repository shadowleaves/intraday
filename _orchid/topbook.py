# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 14:56:22 2014

@author: han.yan
"""

from msg.mcast import multicast
from utils.utils import miu, isnan, timeflag


def max_dict(dt):
    """ a) create a list of the dict's keys and values;
        b) return the key with the max value"""
    v = list(dt.values())
    key = list(dt.keys())[v.index(max(v))]
    return key, dt[key]


def min_dict(dt):
    """ a) create a list of the dict's keys and values;
        b) return the key with the max value"""
    v = list(dt.values())
    key = list(dt.keys())[v.index(min(v))]
    return key, dt[key]
#max_dict = lambda x:max(x, key = x.get)
#min_dict = lambda x:min(x, key = x.get)


class topbook(multicast):

    def __init__(self, book_depth=10, verbose=False, **kwargs):
        multicast.__init__(self, **kwargs)
        self.type_ = 'topbook'
        self.book_depth = book_depth
        self.book = {}
        self.top = {}
        self.verbose = verbose

    def process(self, msg, port):
        t0 = timeflag()
        # or msg['type_'] == 'QUOTE' and msg['source'] != 'BTFX':
        if port != 'TICK':
            return

        ticker = msg['ticker']
        dealer = msg['source']
        if not self.book.has_key(ticker):
            self.book[ticker] = {
                'bid': {'n/a': float('nan')}, 'ask': {'n/a': float('nan')}, 'trade': float('nan')}
            self.top[ticker] = {
                'bid': {'n/a': float('nan')}, 'ask': {'n/a': float('nan')}, 'trade': float('nan')}

        if msg['type_'] == 'TRADE':
            self.book[ticker]['trade'] = msg['price']

        elif msg['subtype'] == 'ASK':
            ask = msg['price']
            # removing cross bids
            self.book[ticker]['bid'] = {x: self.book[ticker]['bid'][x]
                                        for x in self.book[ticker]['bid'] if self.book[ticker]['bid'][x] < ask}
            self.book[ticker]['ask'][dealer] = ask

        elif msg['subtype'] == 'BID':
            bid = msg['price']
            # removing cross asks
            self.book[ticker]['ask'] = {x: self.book[ticker]['ask'][x]
                                        for x in self.book[ticker]['ask'] if self.book[ticker]['ask'][x] > bid}
            self.book[ticker]['bid'][dealer] = bid

        dt = {}
        dt['ticker'] = ticker
        dt['bid'] = dt['ask'] = float('nan')

#        if ticker == 'CDXIG522':
#            print self.book[ticker]
#            print '--------------------'

        if len(self.book[ticker]['bid']) > 0:
            dt['bid.src'], dt['bid'] = max_dict(self.book[ticker]['bid'])

        if len(self.book[ticker]['ask']) > 0:
            dt['ask.src'], dt['ask'] = min_dict(self.book[ticker]['ask'])

        dt['trade'] = self.book[ticker]['trade']

        if dt['bid'] == self.top[ticker]['bid'] and \
           dt['ask'] == self.top[ticker]['ask'] and \
           dt['trade'] == self.top[ticker]['trade']:
            return

        if isnan(dt['bid']) or isnan(dt['ask']) or self.signal == 'pause':
            return

        self.top[ticker]['bid'] = dt['bid']
        self.top[ticker]['ask'] = dt['ask']
        self.top[ticker]['trade'] = dt['trade']

        dt['timestamp'] = msg['timestamp']
        dt['quote_time'] = msg['quote_time']

        if self.verbose and dt['bid'] > dt['ask']:
            print '********* %s %f %f' % (ticker, dt['bid'], dt['ask'])

        delta = (timeflag() - t0) * 1e6
        if self.verbose:
            print 'TOB latency: %f %ss' % (delta, miu)
        self.sending(dt, 'TOB')

if __name__ == "__main__":
    from optparse import OptionParser

    usage = "usage: %prog [options] <up_port=TICK> <down_port=TOB>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      help="show streaming data")

    (options, args) = parser.parse_args()

    up_port = ['TICK']
    down_port = ['TOB']

    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
        parser.error("incorrect number of arguments")

    topbook = topbook(
        up_port=up_port, down_port=down_port, verbose=options.verbose)
    topbook.prepare()
    topbook.listening()
    # thread.start_new_thread(topbook.listening,())  ### separate thread for
    # listening
