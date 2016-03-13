# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

from msg.msgpk import stream
from datetime import datetime
import pandas as pd
from operator import itemgetter
# from dateutil.parser import parse
from _sandbox import streaming_oanda as stm
import numpy as np

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

class portfolio(stream):
    def __init__(self, silence = False, **kwargs):
        self.positions = {}
        self.alpha = {}
        self.timestamp = None
        self.ep = 1e-15
        self.capital = 10000
        self.leverage = 100
        self.realized = {}
        self.unrealized = {}
        self.prev_pnl = 0
        
        self.silence = silence
        stream.__init__(self, **kwargs)
        self.prepare()
        
    def action(self, msg):
        self.timestamp = datetime.utcnow()
        if msg.has_key('alpha'):
            print 'alpha: before pca: %s trade: %s' % (str(msg['alpha']['stamp']), self.timestamp)
            try:
                del msg['alpha']['stamp']
                self.alpha = msg['alpha'].copy()
                self.mid = msg['mid'].copy()            
                
            ### proposed tradebook               
                tradebook = self.arma()
                self.send_order(tradebook)
            except:
                print '@@@@@@@@@@@@@@@@'
                
        elif msg.has_key('fills'):
            try:
                self.update_positions(msg['fills'])
            except:
                print '!!!!!!!!!!!!!!!!!'
        
    def send_order(self, tradebook):
        for link in self.downlink:
                link.send(self.packer.pack(tradebook))
    
    def update_positions(self, fills):
        order_type = 'n/a'; ticker = 'n/a'   
        
        for ticker in fills:
            amt = fills[ticker]['amt']
            avg_px = fills[ticker]['avg_px']
            sgn = 1 if fills[ticker]['side'] == 'buy' else -1
                        
            ### update positions and avgCost
            old_amt = self.positions[ticker]['amt']
            avg_cost = self.positions[ticker]['avgCost']            
            old_val = old_amt * avg_cost
            
            if sgn * sign(old_val) >= 0: ### new trade
                self.positions[ticker]['avgCost'] = (abs(old_val) + (amt * avg_px)) / (abs(old_amt) + amt)
                order_type = 'new'
            else: ## unwinding trades. avgCost doesn't change. calculate realized PnL
                self.realized[ticker] = self.realized[ticker] + (avg_px - avg_cost) * amt * sign(old_val)            
                order_type = 'unwind'
            
            self.positions[ticker]['amt'] = self.positions[ticker]['amt'] + sgn * amt
        
        ### marking unrealized pnl
        self.unrealized = {}
        last_filled = ticker
        
        for ticker in self.positions:
            self.unrealized[ticker] = 0
            pos = self.positions[ticker]['amt']
            avg_cost = self.positions[ticker]['avgCost']
            mtm_px = self.mid[ticker] if ticker in self.mid.keys() else float('nan')
            val = (mtm_px - avg_cost) * pos            
            self.unrealized[ticker] = val if np.isfinite(val) else 0
                
        #print self.realized
        plr = sum(self.realized.values())
        plu = sum(self.unrealized.values())
        
        #print lag
       # if plr + plu != self.prev_pnl:
        print 'total PnL $%-6.2f = PLR $%-6.2f + PLU $%-6.2f, %s:%s at %f, now %s' % \
                (plr+plu, plr, plu, order_type, last_filled, fills[last_filled]['avg_px'], datetime.utcnow())
                
        self.prev_pnl = plr + plu
        #%s order of %s at px %f for %f' % \
                    #(pct, pnl, order_type, ticker, avg_px, amt)
        return
        
        
    def arma(self, entry_signal = 1, exit_signal = 0):
        trades = {}
        unit = self.capital / len(self.alpha) * self.leverage
        ep =self.ep
        if self.positions == {}:
            self.positions = {key:{'amt':0, 'avgCost':0} for key in self.alpha}
            self.realized = {key:0 for key in self.alpha}
        
        #### inverse XAU/XAG signal:
        #if 'XAU_JPY' in self.alpha.keys():
        #    self.alpha['XAU_JPY'] = 0
        leading = [key for key in self.alpha if 'XAU' in key or 'XAG' in key]
        for key in leading:
            self.alpha[key] = 0 #-self.alpha[key]
            
        #### entry/exit names
        long_exit   = [key for key in self.alpha if key in self.positions and (abs(self.positions[key]['amt']) > ep) & (self.alpha[key] <  exit_signal ) ]
        short_exit  = [key for key in self.alpha if key in self.positions and (abs(self.positions[key]['amt']) > ep) & (self.alpha[key] > -exit_signal ) ]
        long_entry  = [key for key in self.alpha if (abs(self.positions[key]['amt']) < ep) & (self.alpha[key] >  entry_signal) ]
        short_entry = [key for key in self.alpha if (abs(self.positions[key]['amt']) < ep) & (self.alpha[key] < -entry_signal) ]
        
        #### trade sizing
        trades.update({ x: -self.positions[x]['amt'] for x in long_exit })
        trades.update({ x: -self.positions[x]['amt'] for x in short_exit })
        trades.update({ x:  unit / self.mid[x] for x in long_entry  if x in self.mid })
        trades.update({ x: -unit / self.mid[x] for x in short_entry if x in self.mid })
        
        return trades

if __name__ == "__main__":

    from optparse import OptionParser
    
    usage = "usage: %prog [options] <up_port=8004|8005> <down_port=8006>"
    parser = OptionParser(usage)
    parser.add_option("-s", "--silence", dest = "silence", action = "store_true", 
                        help = "hide streaming data")

    up_port = 8004
    down_port = 8005
    silence = False
    
    (options, args) = parser.parse_args()
    if len(args) > 2:
        parser.error("incorrect number of arguments")
    if len(args) == 2:
        up_port = [int(x) for x in args[0].split('|')]
        down_port = [int(x) for x in args[1].split('|')]
#    else:
#        raise Exception('must specify both uplink and downlink ports')
    if options.silence:
        silence = True
    
    q = portfolio(up_port = up_port, down_port = down_port, silence = silence, sleep_time = 1e-6)
    q.listening()
