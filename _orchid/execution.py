# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

from msg.mcast import multicast
from datetime import datetime
import pandas as pd
from operator import itemgetter
import numpy as np
import random

pd.set_option('display.notebook_repr_html', False)
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 7)
pd.set_option('display.width', 120)


def get_pip():
    from _orchid.streaming_sandbox import oad_stream
    print 'getting pip unit info from oanda...'
    oad = oad_stream()
    info = oad.read_univ_details()
    pip = info.pip
    pip.index = [x.replace('_', '') for x in info.instrument]
    # pip.to_csv('g:/universe.csv')
    return pip

#pip_list = get_pip()
sign = lambda x: x and (1, -1)[x < 0]


def top(x, n=5):
    return sorted(x.iteritems(), key=itemgetter(1))[:n]


def bottom(x, n=5):
    return sorted(x.iteritems(), key=itemgetter(1), reverse=True)[:n]


class execution(multicast):

    def __init__(self, **kwargs):
        multicast.__init__(self, **kwargs)
        self.latest = {}
        self.orders = {}
        self.alpha = {}
        self.timestamp = None
        self.initial_order_val = 0
        self.filled_order_val = 0
        self.ep = 1e-15
        self.type_ = 'execution'
        random.seed()

    def process(self, msg, port):
        if port == 'TICK':  # streaming
            inst = msg['ticker']
            if not self.latest.has_key(inst):
                self.latest[inst] = {}

            type_ = msg['type_']
            subtype = msg['subtype']
            price = msg['price']
            asset = msg['asset']
            direction = msg['dir']
            #time = msg['quote_time']
            mapping = {
                'TRADE': {'NEW': 'trade'}, 'QUOTE': {'BID': 'bid', 'ASK': 'ask'}}

            subtype = mapping[type_][subtype]
            self.latest[inst][subtype] = price
            self.asset = asset
            self.msg = msg

            self.execution(inst, price, subtype, direction)

        elif port == 'ORDER':  # orders
            self.timestamp = datetime.utcnow()
            print 'orders: %d %s' % (len(msg), self.timestamp)
            # proposed tradebook
            self.new_limit_order(msg)  # cancel pendings and placing new orders

    def new_limit_order(self, tradebook):
        self.orders = {}  # cancel all pending orders
        self.initial_order_val = 0
        self.filled_order_val = 0
        for ticker in tradebook:
            if self.latest.has_key(ticker) and \
                    self.latest[ticker].has_key('ask') and \
                    self.latest[ticker].has_key('bid'):

                ask = self.latest[ticker]['ask']  # TODO: should be best_ask
                bid = self.latest[ticker]['bid']

                urgency = 0  # if self.asset == 'Curncy' else 0
                # 0.5 means mid-point liqiduity provider order
                spd = (ask - bid) * urgency
                limit_px = (
                    bid + spd) if tradebook[ticker] > 0 else (ask - spd)
                amt = abs(tradebook[ticker])
                side = 'buy' if tradebook[ticker] > 0 else 'sell'

                if np.isnan(limit_px) or np.isnan(amt):
                    continue

                self.orders[ticker] = {}
                self.orders[ticker]['amt'] = amt
                self.orders[ticker]['ori_amt'] = amt
                self.orders[ticker]['side'] = side
                self.orders[ticker]['limit'] = limit_px
                # print '%s limit %s order size %.2f at %.2f (%g spd)' %
                # (ticker, side, amt, limit_px, spd)
                self.initial_order_val = self.initial_order_val + \
                    amt * limit_px

    def execution(self, ticker, last_px, subtype, direction):

        fills = {}
        if self.orders.has_key(ticker):
            side = self.orders[ticker]['side']
            limit = self.orders[ticker]['limit']

            lifted = False
            # only equity trade prices are real, curncy trade px are actually mid
            # when trade crosses (not touches) limit orders, that means even the tail of
            # the order queue is lifted. thus it's safe to assume orders lifted
            if subtype == 'trade':
                if side == 'buy' and last_px < limit or side == 'sell' and last_px > limit:
                    lifted = True  # better trade
                elif last_px == limit and 1 - random.random() > 0.5:
                    lifted = True  # same trade

            # top of the book is better than best_bid or best_ask
            # means that the order would've been lifted
            elif subtype in ('bid', 'ask') and \
                    (side == 'buy' and last_px < limit or side == 'sell' and last_px > limit):
                lifted = True  # better quote

            if lifted:
                fills[ticker] = {}
                fills[ticker]['avg_px'] = limit
                fills[ticker]['side'] = self.orders[ticker]['side']
                fills[ticker]['amt'] = min(
                    self.orders[ticker]['amt'], self.orders[ticker]['ori_amt'] / 10)

        # sending back fills
        if fills != {}:
            self.sending(fills, 'FILL')
            self.update_orders(fills)

        # updating left-over orders
        if self.orders.has_key(ticker):
            side = self.orders[ticker]['side']
            limit_px = self.orders[ticker]['limit']
            if subtype == 'bid' and side == 'buy' or subtype == 'ask' and side == 'sell':
                if limit_px != last_px:
                    print 'updating %s %s order from %g to %g' % (ticker, side, limit_px, last_px)
                    self.orders[ticker]['limit'] = last_px

    def update_orders(self, fills):
        # adjusting order sizes after fills
        if fills == {}:
            return

        for ticker in fills:
            amt = fills[ticker]['amt']
            avg_px = fills[ticker]['avg_px']

            self.orders[ticker]['amt'] = self.orders[ticker]['amt'] - amt
            self.filled_order_val = self.filled_order_val + amt * avg_px

            if abs(self.orders[ticker]['amt']) < self.ep:
                del self.orders[ticker]  # order finished

        pct = self.filled_order_val / self.initial_order_val * 100

        rec_time = datetime.utcfromtimestamp(self.msg['timestamp'])
        now = datetime.utcnow()
        lag = (now - rec_time).total_seconds()
        # print lag
        print 'order filled %.2f%%, time at %s, lag at %f sec' % (pct, now, lag)


if __name__ == "__main__":

    from optparse import OptionParser

    usage = "usage: %prog [options] <up_port=ORDER|TICK> <down_port=FILL>"
    parser = OptionParser(usage)
#    parser.add_option("-v", "--verbose", dest = "verbose", action = "store_true",
#                        default = True, help = "showing detailed info")

    (options, args) = parser.parse_args()
    up_port = 'ORDER|SBL|TICK'.split('|')
    down_port = 'FILL'.split('|')

    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
        parser.error("incorrect number of arguments")

    q = execution(up_port=up_port, down_port=down_port)
    q.prepare()
    q.listening()
