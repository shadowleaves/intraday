# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 15:06:04 2015

@author: han.yan
"""


#import os
#import gzip
#import re
#import pytz
import pandas as pd
import numpy as np
# from datetime import datetime
# from ciso8601 import parse_datetime
# from utils.utils import timeflag, chunking
from utils.utils import min_vol_df
# from multiprocessing import Pool

pd.set_option('display.notebook_repr_html', False)
pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 20)
        
     
if __name__ == '__main__':
 
    h5file = 'h:/cache/tick/CL1_trade.h5'
    tick = pd.read_hdf(h5file, 'data')
    threshold = 0.05
    
    # making minimal volatility mid-px
    mid = min_vol_df(bid, ask)
    mid = min_vol_df(mid, trade)
    
    # filter using mid
    hit = (bid / mid - 1).abs() > threshold
    bid[hit] = np.nan
    hit = (ask / mid - 1).abs() > threshold
    ask[hit] = np.nan
    
    hit = (trade / mid - 1).abs() > threshold
    trade[hit] = np.nan
    
    #settle = px.settle
    new = pd.concat((bid, ask, px.settle, trade, mid), axis=1)
    new.columns= list(px.columns) + ['mid']
    new = new.ffill()
    #new[np.isnan(px)] = np.nan
    cross = new.ask < new.bid
#    new.ask[cross] = np.nan
#    new.bid[cross] = np.nan

    hpx = new.resample('1H', 'last')
    hvol = vol.resample('1H', 'sum')
    
    panel = pd.Panel({'px': hpx, 'vol': hvol})
    panel.to_hdf('h:/cache/tick/CL1_1H.h5', 'data')
    
    #mid = min_vol_df(pd.DataFrame(new.bid), pd.DataFrame(new.ask))
    #vol.ask[cross] = np.nan
    #vol.bid[cross] = np.nan    
    #valid = np.isfinite(px).sum(1)>0
    #px = px[valid]
    #vol = vol[valid]
