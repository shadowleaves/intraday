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
# from utils.utils import min_vol_df
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
    #vol = pd.read_hdf(h5file, 'volume')
    threshold = 0.05

#    bid = px.bid.copy()
#    ask = px.ask.copy()
    trade = px.trade.copy()
    trade = trade[np.isfinite(trade)]
    #res = clean(trade.values, 0.01)
    #
    small = trade.resample('5Min', 'last')
    res = clean(small, 0.01)
    df = pd.DataFrame(res, index=small.index).ffill()

    #mid = min_vol_df(pd.DataFrame(new.bid), pd.DataFrame(new.ask))
    #vol.ask[cross] = np.nan
    #vol.bid[cross] = np.nan
    #valid = np.isfinite(px).sum(1)>0
    #px = px[valid]
    #vol = vol[valid]
