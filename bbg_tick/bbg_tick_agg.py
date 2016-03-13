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


from numba import jit


@jit(nopython=True)
def clean(arr, threshold=0.01):

    n = arr.shape[0]
    prev = arr[0]
    res = arr.copy()
    for i in xrange(n):
        if abs(res[i] / prev - 1) > threshold:
            res[i] = prev
        prev = res[i]
    return res

if __name__ == '__main__':

    h5file = 'h:/cache/tick/CL1_tick.h5'
    px = pd.read_hdf(h5file, 'tick')
    vol = pd.read_hdf(h5file, 'volume')
    threshold = 0.05

    bid = px.bid.copy()
    ask = px.ask.copy()
    trade = px.trade.copy()

    # filter using trade px
    fill_trade = trade.ffill()
    hit = (bid / fill_trade - 1).abs() > threshold
    bid[hit] = np.nan
    hit = (ask / fill_trade - 1).abs() > threshold
    ask[hit] = np.nan

    bid = pd.DataFrame(bid.ffill().values.reshape(-1, 1), index=px.index)
    ask = pd.DataFrame(ask.ffill().values.reshape(-1, 1), index=px.index)
    trade = pd.DataFrame(trade.ffill().values.reshape(-1, 1), index=px.index)

    # making minimal volatility mid-px
    mid = min_vol_df(bid, ask)
    mid = min_vol_df(mid, trade)

    # filter using mid
    hit = (bid / mid - 1).abs() > threshold
    bid[hit] = np.nan
    hit = (ask / mid - 1).abs() > threshold
    ask[hit] = np.nan

#    hit = (trade / mid - 1).abs() > threshold
#    trade[hit] = np.nan

    #settle = px.settle

    new = pd.concat((bid, ask, px.settle, trade, mid), axis=1)
    new.columns = list(px.columns) + ['mid']

    #new[np.isnan(px)] = np.nan
    cross = new.ask < new.bid
#    new.ask[cross] = np.nan
#    new.bid[cross] = np.nan

    hpx = new.resample('1H', 'max', fill_method='ffill').ffill()
    hvol = vol.resample('1H', 'sum')

    hpx.trade = clean(hpx.trade, 0.10)

    panel = pd.Panel({'px': hpx, 'vol': hvol})
    panel.to_hdf('h:/cache/tick/CL1_1H.h5', 'data')

    #mid = min_vol_df(pd.DataFrame(new.bid), pd.DataFrame(new.ask))
    #vol.ask[cross] = np.nan
    #vol.bid[cross] = np.nan
    #valid = np.isfinite(px).sum(1)>0
    #px = px[valid]
    #vol = vol[valid]
