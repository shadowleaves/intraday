# -*- coding: utf-8 -*-
"""
Created on Thu Oct 30 16:00:19 2014

@author: han.yan
"""

import pandas as pd
from cache import environ
from db import mongo_ohlc, monary_bar
from monary import Monary

# from pymongo import MongoClient
# from datetime import time, timedelta

pd.set_option('display.notebook_repr_html', False)
pd.set_option('display.width', 100)
pd.set_option('display.max_columns', 6)
pd.set_option('display.max_rows', 20)
pd.set_option('display.float_format', '{:,g}'.format)

if __name__ == '__main__':
    # 20-30sec for ETF, 5-10sec for FX (BTFX)

    if False:
        client = Monary()
        # collection = client.flow['bar']# if mode == 'bbo' else client.flow
        dt = {}
        monary_bar(dt, client, 'SPY', 'TRADE', step_min=1)
        client.close()

    else:
        env = environ.get_env('FX_1Min')
        res, dt = mongo_ohlc(env, last_periods=100, mode='bbo', processes=20,
                             type_list=['bid'])
#
#        res, dt = mongo_ohlc(env, last_periods = 100, mode = 'bar', processes = 20,
# type_list = ['TRADE'], type_field = 'event')
