# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

from msg.msgpk import stream
from datetime import datetime
import pandas as pd
from operator import itemgetter
import numpy as np
from _sandbox import streaming_oanda as stm


pd.set_option('display.notebook_repr_html',False)
pd.set_option('display.max_rows',20)
pd.set_option('display.max_columns',7)
pd.set_option('display.width', 120)

info = stm.get_all_instruments()
pip = info.pip
pip.index = info.instrument

sign = lambda x: x and (1, -1)[x<0]

def top(x, n = 5):
    return sorted(x.iteritems(), key=itemgetter(1))[:n]
def bottom(x, n = 5):
    return sorted(x.iteritems(), key=itemgetter(1), reverse = True)[:n]

class execution(stream):
    def __init__(self, silence = False, **kwargs):        
        self.latest = {}
        self.orders = {}
        self.alpha = {}
        self.timestamp = None
        self.initial_order_val = 0
        self.filled_order_val = 0
        self.ep = 1e-15
        
        self.silence = silence
        stream.__init__(self, **kwargs)
        self.prepare()
        
    def action(self, msg):
        self.timestamp = datetime.utcnow()
        
        if msg.has_key('instrument'): ### streaming
            try:
                inst = msg['instrument']
                if not self.latest.has_key(inst):
                     self.latest[inst] = {}
                self.latest[inst]['ask'] = msg['ask']
                self.latest[inst]['bid'] = msg['bid']
                self.latest[inst]['mid'] = (msg['ask']+msg['bid'])/2
                self.msg = msg
            except:
                print '############'
            try:
                fills = self.execution()
                self.update_orders(fills)
            except:
                print '!!!!!!!!!!'
        else:  ### orders
            try:
                print 'orders: %d %s' % (len(msg), self.timestamp)
                ### proposed tradebook
                self.new_limit_order(msg)  ### cancel pendings and placing new orders
            except:
                print '~~~~~~~~~~~~'
    
    def new_limit_order(self, tradebook):
        self.orders = {}  ### cancel all pending orders
        self.initial_order_val = 0
        self.filled_order_val = 0
        for ticker in tradebook:
            if self.latest.has_key(ticker):
                ask = self.latest[ticker]['ask']
                bid = self.latest[ticker]['bid']
                
                spd = (ask - bid) * 0 #pip[ticker] * 1  ### agressiveness, 0 means passive
                limit_px = (bid + spd) if tradebook[ticker]>0 else (ask - spd) 
                amt = abs(tradebook[ticker])                    
                side = 'buy' if tradebook[ticker]>0 else 'sell'
                
                if np.isnan(limit_px) or np.isnan(amt):
                    continue
                
                self.orders[ticker] = {}                
                self.orders[ticker]['amt'] = amt
                self.orders[ticker]['side'] = side
                self.orders[ticker]['limit'] = limit_px
                #print '%s limit %s order size %.2f at %.2f (%g spd)' % (ticker, side, amt, limit_px, spd)
                self.initial_order_val = self.initial_order_val + amt * limit_px

    def execution(self):
        ticker = self.msg['instrument']
        ask = self.msg['ask']
        bid = self.msg['bid']
        #mid = (ask+bid)/2
        fills = {}
        if self.orders.has_key(ticker):
            
            side = self.orders[ticker]['side']
            limit = self.orders[ticker]['limit']
            if (side == 'buy' and limit >= ask) or (side == 'sell' and limit <= bid): ## cross
                fills[ticker] = {}
                fills[ticker]['avg_px'] = ask if side == 'buy' else bid
                fills[ticker]['side'] = self.orders[ticker]['side']
                fills[ticker]['amt'] = self.orders[ticker]['amt'] ### or a fraction of
                
        #### sending back fills
        if fills != {}:
            for link in self.revlink:
                link.send(self.packer.pack({'fills':fills}))
        return fills
    
    def update_orders(self, fills):
        ### adjusting order sizes after fills
        if fills == {}:
            return
            
        for ticker in fills:
            amt = fills[ticker]['amt']
            avg_px = fills[ticker]['avg_px']
            
            self.orders[ticker]['amt'] = self.orders[ticker]['amt'] - amt
            self.filled_order_val = self.filled_order_val + amt * avg_px
            
            if abs(self.orders[ticker]['amt']) < self.ep:
                del self.orders[ticker]   ### order finished
        
        pct = self.filled_order_val / self.initial_order_val * 100

        rec_time = datetime.utcfromtimestamp(self.msg['stamp'])
        now = datetime.utcnow()
        lag = (now - rec_time).total_seconds()
        #print lag
        print 'order filled %.2f%%, time at %s, lag at %f sec' % (pct, now, lag)


if __name__ == "__main__":

    from optparse import OptionParser
    
    usage = "usage: %prog [options] <up_port=8004|8005> <down_port=8006>"
    parser = OptionParser(usage)
    parser.add_option("-s", "--silence", dest = "silence", action = "store_true", 
                        help = "hide streaming data")

    up_port = [8005, 8007]
    down_port = []
    silence = False
    
    (options, args) = parser.parse_args()
    if len(args) > 2:
        parser.error("incorrect number of arguments")
    if len(args) == 2:
        up_port = [int(args[0])]
        down_port = [int(args[1])]
#    else:
#        raise Exception('must specify both uplink and downlink ports')
    if options.silence:
        silence = True
    
    q = execution(up_port = up_port, down_port = down_port, rev_port = [8005], silence = silence, sleep_time = 1e-6)
    q.listening()
