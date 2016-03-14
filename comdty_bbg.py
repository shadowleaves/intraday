#!/usr/bin/env python
from environ.bbg import BBG
from datetime import time, datetime, timedelta
from utils.filesys import cpath
import pytz
import os
from string import capwords
import pandas as pd


def get_env(name, **kwargs):
    '''retrieve environment class by name
        Required:
            name - string of the env class. e.g.: 'R1K-1D'
    '''
    if '-' in name:
        x, freq = name.split('-')
    else:
        x = name
        freq = '1D'

    assert x in globals(), 'unrecognized environment %s...' % x
    return globals()[x](freq=freq, **kwargs)


class CNCmdty(BBG):

    def __init__(self, **kwargs):

        super(CNCmdty, self).__init__(composite_ex='',
                                      asset_class='Comdty', **kwargs)

        self.keep_ex = False
        self.curncy = 'CNY'

        self.tz = pytz.timezone('Asia/Shanghai')
        self.intraday = False
        self.mkt_start_time = time(9, 30)
        self.mkt_end_time = time(16, 0)
        today = datetime.today()
        cut_off = datetime.combine(today.date(), self.mkt_end_time)

        if today < cut_off:
            today += timedelta(days=-1)

        self.mkt_start_date = datetime(2015, 1, 1)  # 1999
        self.mkt_end_date = today.replace(hour=0, minute=0, second=0,
                                          microsecond=0)

        self.static_universe = True
        self.intraday = False
        self.weekday_only = True
        self.tz_convert = True
        self.mkt_dbname = 'intraday'

        self.pivots['bm'] = 'SPY US Equity'

        # def files
        self.def_path = cpath('$HOME/codebase/intraday/def')
        self.univ_file = os.path.join(self.def_path, 'ccom.csv')

    def load_univ(self):
        df = pd.read_csv(self.univ_file)
        df.Source = [' '.join(capwords(x.lower()).split(' ')[:-1])
                     for x in df.Source]
        sec_univ = df.Ticker
        pass

    def read_univ():
        pass


if __name__ == '__main__':

    env = get_env('CNCmdty-1D')
    univ = env.load_univ()
    print univ
