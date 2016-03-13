# -*- codingding: utf-8 -*-
"""
Created on Wed Jan 21 15:06:04 2015

@author: han.yan
"""

import pandas as pd
import numpy as np
# from utils.utils import min_vol_df

pd.set_option('display.notebook_repr_html', False)
pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 20)
        
     
if __name__ == '__main__':
     
    h5file = 'h:/cache/tick/CL1_1H.h5'
    panel = pd.read_hdf(h5file, 'data')
    
    px = panel.px
    vol = panel.vol.trade
    
    log_px = np.log(px)
    log_ret = log_px.diff(1)
    
    RI = pd.DataFrame.from_csv('h:/cache/tick/CL1_caliber.csv', header=False)
    RI.columns = ['CL1']
    log_returns = np.log(RI.ffill()).diff(1)
    
    hit = (px.index>=RI.index.min()) & (px.index<=RI.index.max())
    px = px[hit].ffill()
    vol = vol[hit]
    
    store = pd.HDFStore('h:/cache/tick/CL1_RI.h5', 'w')
    cols = ['ask','bid','settle','trade','mid','vol']
    series = pd.DataFrame(columns=cols)
    
    prev_settle = np.nan
    shift = RI.shift(1)
    for row in px.iterrows():
        timestamp, values = row
        day = timestamp.date()
        
        if timestamp.time().hour == 18 and series.shape[0]>0:            
            if day in RI.index:
                fold = 1 / series.settle[-1] * RI.loc[day][0]
                series *= fold
                print timestamp, fold
            else:
                series[:] = np.nan                
            store.append('data', series.convert_objects())
            series = pd.DataFrame(columns = cols)
        
        values['vol'] = vol[timestamp]
        series.loc[timestamp] = values
        prev_settle = values.settle
    
    store.close()
    
    test = pd.read_hdf('h:/cache/tick/CL1_RI.h5', 'data')
    daily = test.resample('1D', 'last')
    hit = daily.index & RI.index
    comp = pd.concat((daily.loc[hit], RI.loc[hit]), axis=1)
    comp[['settle','CL1']].tail(50).plot()
    
    
    #mom = pd.rolling_sum(log_ret, 12, min_periods=1)
    #vol = pd.read_hdf(h5file, 'vol')
