# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

from msg.mcast import multicast
from datetime import datetime
import pandas as pd
from operator import itemgetter
# from dateutil.parser import parse
# from _sandbox import streaming_oanda as stm
import numpy as np
from utils.utils import isnan
import os

pd.set_option('display.notebook_repr_html',False)
pd.set_option('display.max_rows',20)
pd.set_option('display.max_columns',7)
pd.set_option('display.width', 120)

sign = lambda x: x and (1, -1)[x<0]

def top(x, n = 5):
    return sorted(x.iteritems(), key=itemgetter(1))[:n]
def bottom(x, n = 5):
    return sorted(x.iteritems(), key=itemgetter(1), reverse = True)[:n]

class portfolio(multicast):
    def __init__(self, verbose = False, **kwargs):
        multicast.__init__(self, **kwargs)
        self.positions = {}
        self.alpha = {}
        self.timestamp = None
        self.ep = 1e-15
        self.capital = 10000
        self.leverage = 10
        self.entry_signal = 1
        self.exit_signal = -1
        self.realized = {}
        self.unrealized = {}
        #self.prev_pnl = 0
        self.mid = {}
        self.verbose = verbose
        self.type_ = 'portfolio'
        self.pnl_report_file = 'h:/pnl_report_file_%s.csv' % datetime.today().date()
        #self.verbose = verbosE
        
    def process(self, msg, port):
        self.timestamp = datetime.utcnow()
        if port == 'ALPHA':
            print 'alpha recv ts: %s' % self.timestamp
            self.alpha = {x:msg['alpha'][x] for x in msg['alpha']}
            self.sd = {x:msg['sd'][x] for x in msg['sd']}
            ### proposed tradebook               
            tradebook = self.arma(self.entry_signal, self.exit_signal)
            if len(tradebook):
                self.send_order(tradebook)
                
        elif port == 'FASTBEAT':   ### marking portfolio
            self.mid = {x:(msg[x]['ask'] + msg[x]['bid'])/2 for x in msg}
            self.mtm()            
            
        elif port == 'FILL':
            self.update_positions(msg)
        
    def send_order(self, tradebook): ### adding EMSX api here
        self.sending(tradebook, 'ORDER')
    
    def update_positions(self, fills):
        order_type = 'n/a'; ticker = 'n/a'   
        
        for ticker in fills:
            amt = fills[ticker]['amt']
            avg_px = fills[ticker]['avg_px']
            sgn = 1 if fills[ticker]['side'] == 'buy' else -1
                        
            if ticker not in self.positions:
                self.positions[ticker] = {'amt':0, 'avgCost':0}
                self.realized[ticker] = 0
            ### update positions and avgCost
            old_amt = self.positions[ticker]['amt']
            avg_cost = self.positions[ticker]['avgCost']
            old_val = old_amt * avg_cost
            
            if sgn * sign(old_val) >= 0: ### new trade, updating avgCost
                self.positions[ticker]['avgCost'] = (abs(old_val) + (amt * avg_px)) / (abs(old_amt) + amt)
                order_type = 'new'
            else: ## unwinding trades. avgCost doesn't change. calculate realized PnL
                self.realized[ticker] = self.realized[ticker] + (avg_px - avg_cost) * amt * sign(old_val)
                order_type = 'unwind'
            
            self.positions[ticker]['amt'] = self.positions[ticker]['amt'] + sgn * amt
            if self.verbose:
                print '%s:%s at %f, now %s' % \
                (order_type, ticker, fills[ticker]['avg_px'], datetime.utcnow())
                
        
    def mtm(self):
        ### marking unrealized pnl
        self.unrealized = {}
        
        gross = net = 0
        for ticker in self.positions:
            self.unrealized[ticker] = 0
            pos = self.positions[ticker]['amt']
            avg_cost = self.positions[ticker]['avgCost']
            
            #### mark to mid price
            mtm_px = self.mid[ticker] if ticker in self.mid.keys() else float('nan')
            val = (mtm_px - avg_cost) * pos
            self.unrealized[ticker] = val if np.isfinite(val) else 0
            
            mtm_px = 0 if isnan(mtm_px) else mtm_px
            gross += abs(mtm_px * pos)
            net += mtm_px * pos
        
        #print self.realized
        plr = sum(self.realized.values()) / self.capital * 1e4
        plu = sum(self.unrealized.values()) / self.capital * 1e4
        gross = gross / self.capital
        net = net / self.capital
        
        row = (plr+plu, plr, plu, gross, net)
        print 'total bps %.2f = PLR %.2f + PLU %.2f, gross %.2f, net %.2f' % row
        df = pd.DataFrame([row], index = [datetime.now()],
                          columns = ('PLR+PLU', 'PLR', 'PLU', 'Gross', 'Net'))
        if os.path.isfile(self.pnl_report_file):
            with open(self.pnl_report_file, 'a') as f:
                df.to_csv(f, header=False)
        else:
            with open(self.pnl_report_file, 'w') as f:
                df.to_csv(f)
                
    def arma(self, entry_signal, exit_signal, alpha_weighted = False):
        trades = {}
        unit = self.capital / len(self.alpha) * self.leverage
        ep =self.ep
        for key in self.alpha:
            if key not in self.positions:
                self.positions[key] = {'amt':0, 'avgCost':0}
                self.realized[key] = 0
        
        #### entry/exit names
        long_exit   = [key for key in self.alpha if key in self.positions and self.positions[key]['amt'] > 0 & (self.alpha[key] <  exit_signal ) ]
        short_exit  = [key for key in self.alpha if key in self.positions and self.positions[key]['amt'] < 0 & (self.alpha[key] > -exit_signal ) ]
        long_entry  = [key for key in self.alpha if (abs(self.positions[key]['amt']) < ep) & (self.alpha[key] >  entry_signal) ]
        short_entry = [key for key in self.alpha if (abs(self.positions[key]['amt']) < ep) & (self.alpha[key] < -entry_signal) ]
        
        #### trade sizing
        trades.update({ x: -self.positions[x]['amt'] for x in long_exit })
        trades.update({ x: -self.positions[x]['amt'] for x in short_exit })
        
        if alpha_weighted:
            trades.update({ x:  unit / self.mid[x] * self.alpha[x] for x in long_entry  if x in self.mid and x in self.alpha})
            trades.update({ x:  unit / self.mid[x] * self.alpha[x] for x in short_entry if x in self.mid and x in self.alpha})
        else:
            trades.update({ x:  unit / self.mid[x] for x in long_entry  if x in self.mid })
            trades.update({ x: -unit / self.mid[x] for x in short_entry if x in self.mid })        
        
        return trades

if __name__ == "__main__":

    from optparse import OptionParser
    
    usage = "usage: %prog [options] <up_port=ALPHA|FASTBEAT|SBL|FILL> <down_port=ORDER|POS>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest = "verbose", action = "store_true", 
                        default = False, help = "showing detailed info")
#    parser.add_option("-i",
#              dest="interval", type="int", help="algo bar interval (default: %default)",
#              metavar="interval", default=5)
    
    (options, args) = parser.parse_args()
    up_port = 'ALPHA|FASTBEAT|SBL|FILL'.split('|')
    down_port = 'ORDER|POS'.split('|')
    
    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
         parser.error("incorrect number of arguments")
    
    portfolio = portfolio(up_port = up_port, down_port = down_port, verbose = options.verbose)
    portfolio.prepare()    
    portfolio.listening()
