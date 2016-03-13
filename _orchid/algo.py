# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

import pandas as pd
from model import pca
import time, os
from datetime import datetime##, timedelta
from cache import environ
from cache.market import MarketData
# from cache.alpha import Alpha
from msg.mcast import multicast
from utils.utils import timeflag

pd.set_option('display.notebook_repr_html',False)
pd.set_option('display.max_rows',20)
pd.set_option('display.max_columns',5)
pd.set_option('display.width', 80)

class algobeat(multicast):
    def __init__(self, bars = 60, env_name = None, sd_window = 60,
                 nfactors = 10, verbose = False, init = False, **kwargs):
        
        multicast.__init__(self, **kwargs)
        self.type_ = 'algo'
        self.bars = bars
        self.sd_window = sd_window
        self.nfactors = nfactors
        self.last_rows = 2
        self.env = environ.get_env(env_name)
        self.universe = self.env.read_univ()
        self.verbose = verbose
        
        ### historical bars
        t0 = timeflag()        
        print 'reading ohlc bars from hdf5...'
        
        self.market_data = MarketData(env = self.env, update = False, use_db = True)
        self.market_data.load_prices(last_periods = bars + sd_window + 10)
        
        delta = (timeflag() - t0)
        print 'historical bars load time: %f sec' % (delta)
        
        self.df = self.market_data.mid['open']
        self.df.columns = [x.encode('ascii','ignore') for x in self.df.columns]
        
        required = self.bars + self.sd_window + 1
        if self.df.shape[0] < required:
            print('insufficient kickstarting data, %d rows more needed' % (required - self.df.shape[0]))
            raise SystemError
        
        self.df.ffill(inplace = True)
        self.model = pca.PCAModel(self.df, self.nfactors, self.bars, method='nipals')
        self.model.fit_model()
        
        self.alpha = self.model.signal
        self.alpha_mat = pd.DataFrame(columns = self.df.columns)
        self.alpha_mat[self.alpha.columns] = self.alpha
        
        ### sharpe of trailing alpha signal
        sd_mat = pd.rolling_std(self.alpha_mat, self.sd_window, min_periods = 3).replace(0, float('nan'))
        self.sharpe_mat = self.alpha_mat / sd_mat
        self.sharpe_mat = self.sharpe_mat.iloc[-self.bars:]
        self.sharpe_mat = self.sharpe_mat.replace(float('nan'), 0)
        
        pd.set_option('display.max_columns',5)        
        print 'sharpe_mat:'
        print self.sharpe_mat.tail(10)
        
    def process(self, msg, port):        
        if 'BEAT' not in port:
            return
        
        t0 = time.time()    
        #### incoming msg
        mid = {x:(msg[x]['ask'] + msg[x]['bid'])/2 for x in msg}
        #print pd.DataFrame(test)
        if len(mid)<1:
            return
        
        hit = sorted(set(mid) & set(self.df.columns))
        ts = datetime.utcnow()
        
        ### adding a slice
        self.df.loc[ts, hit] = [mid[x] for x in hit]
        
        #### rolling shape
        #self.df.ffill(inplace=True)
        extra = self.df.shape[0] - ( self.bars + self.sd_window + self.last_rows)
        if extra > 0:
            self.df.drop(self.df.index[:extra], inplace = True)
        elif extra < 0:
            print 'waiting for df size: %d' % self.df.shape[0]
            return
        
        ### fitting PCA model
        model = pca.PCAModel(self.df, self.nfactors, self.bars, method='nipals', last_rows = self.last_rows)
        model.fit_model(verbose = self.verbose)
        alpha = model.signal
        #alpha = pd.DataFrame(columns = self.df.columns)
        #alpha[signal.columns] = signal
        
        ### backup slice
        backup_alpha = self.alpha_mat[-self.last_rows:].copy()
        backup_sharpe = self.sharpe_mat[-self.last_rows:].copy()
        
        ### one extra line of alpha and sharpe
        shift = self.alpha_mat.shape[0] - self.last_rows + 1
        
        for ts in alpha.index[-self.last_rows:]:
            self.alpha_mat.loc[ts] = alpha.loc[ts]
            sd = self.alpha_mat[-self.sd_window:].std(axis = 0).replace(0, float('nan'))
            self.sharpe_mat.loc[ts] = (alpha.loc[ts] / sd).replace(float('nan'), 0)
            shift += 1
            
        ### send alpha slice via msgpack
        alpha_out = self.sharpe_mat[-1:]
        #timestamp = alpha_out.index.format()           
        alpha_out = alpha_out.to_dict('records')[0]
        #alpha_out['timestamp'] = timestamp  ### add a timestamp
        sd = self.df.std(axis = 0).replace(0, float('nan')).to_dict()
        self.sending({'alpha': alpha_out, 'sd': sd}, 'ALPHA')
        
        check_alpha = self.alpha_mat[-(self.last_rows+1):][:-1]
        check_sharpe = self.sharpe_mat[-(self.last_rows+1):][:-1]
        unity_alpha = (check_alpha.values - backup_alpha.values).sum()
        unity_sharpe = (check_sharpe.values - backup_sharpe.values).sum()
        print 'alpha unity check = %g' % unity_alpha
        print 'sharpe unity check = %g' % unity_sharpe
        
        ### timing
        t1 = time.time()
        if self.verbose:
            print self.sharpe_mat.tail(10)
            print 'total time used for db & alpha generation: %.2f ms' % ((t1-t0)*1e3)
            print 'current time: %s' % datetime.utcfromtimestamp(t1)
            
        #### trimming alpha and sharpe mat; dropping old values from beginning
        extra = self.alpha_mat.shape[0] - ( self.bars + self.sd_window + self.last_rows )
        if extra > 0:
            self.alpha_mat.drop(self.alpha_mat.index[:extra], inplace = True)
        
        extra = self.sharpe_mat.shape[0] - ( self.bars + self.last_rows )
        if extra > 0:
            self.sharpe_mat.drop(self.sharpe_mat.index[:extra], inplace = True)
            

if __name__ == "__main__":

    from optparse import OptionParser
    
    usage = "usage: %prog [options] <up_port=BEAT> <down_port=ALPHA>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest = "verbose", action = "store_true", 
                        default = True, help = "showing detailed info")
    parser.add_option("-e",
              dest="env", default = 'FX_1Min', help="environment version (default: %default)",
              metavar="enVer")
#    parser.add_option("-i",
#              dest="interval", type="int", help="algo bar size in Mins (default: %default)",
#              metavar="interval", default=1)
    
    (options, args) = parser.parse_args()
    up_port = ['BEAT']
    down_port = ['ALPHA']
    
    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
         parser.error("incorrect number of arguments")

    algo = algobeat(up_port = up_port, down_port = down_port, 
                    env_name = options.env, verbose = options.verbose)
    
    if 'SPYDER_SHELL_ID' not in os.environ:
        algo.prepare()
        algo.listening()
