# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 10:02:26 2015

@author: han.yan
"""

import numpy as np
import pandas as pd
import os
import pytz

from mktdata.market import MarketData
from mktdata import load5_file


class MarketDataIntraday(MarketData):

    def __init__(self, use_db=False, **kwargs):
        super(MarketDataIntraday, self).__init__(**kwargs)

    def load_db(self, last_periods=100, overhead=5):
        """ loading bar data from memory-db.

        optional:
        last_periods : number of most recent periods to retrieve
        overhead : number of extra row as overhead
        """
        from data.db import mongo_ohlc

        if 'Min' in self.env.freq:
            interval = int(self.env.freq.replace('Min', ''))
        else:
            raise Exception('bar size %s not implemented...' %
                            self.env.freq)

        if self.env.asset_class == 'FX':
            typelist = ['bid', 'ask', 'mid']
        else:
            typelist = ['bid', 'ask', 'mid', 'trade']

        res, dt = mongo_ohlc(self.env, mode='bbo', type_list=typelist,
                             last_periods=last_periods, step_min=interval)

        self.bid = res['bid']
        self.ask = res['ask']
        self.mid = res['mid']
        if 'trade' in typelist:
            self.trade = res['trade']

    def load_prices_intraday(self, last_periods=None,
                             cross_validate=False, cv_window=30):
        if self.use_db:
            return self.load_db(last_periods)

        self.load5(last_periods, 'BID')
        self.load5(last_periods, 'ASK')

        # removing leading missing values
        finite_count = np.sum(np.isfinite(self.bid['close']), axis=1).values
        finite_max = max(finite_count)
        first_max = np.where(finite_count == finite_max)[0][0]
        self.bid['close'] = self.bid['close'].iloc[first_max:]

        common_dates = self.bid['close'].index & self.ask['close'].index
        common_symbols = self.bid['close'].columns & self.ask['close'].columns
        for item in ['bid', 'ask']:
            print 'reshaping using common indices for %s...' % item
            tmp = getattr(self, item)
            for key in tmp:
                tmp[key] = tmp[key].loc[common_dates, common_symbols]

        # eliminate crossed bid/ask
        for item in self.bid:
            hit = self.bid[item] > self.ask[item]
            self.bid[item][hit] = float('nan')
            self.ask[item][hit] = float('nan')

        # reshaping trade matrix after bid/ask row/col labels are confirmed
        if self.env.asset_class != 'FX':
            self.load5(last_periods, 'TRADE')
            for item in self.trade:
                print 'reshaping trade indices on %s...' % item
                mat = self.bid[item].copy()
                mat[:] = float('nan')
                common_dates = mat.index & self.trade[item].index
                common_symbols = mat.columns & self.trade[item].columns
                mat.loc[common_dates, common_symbols] = self.trade[
                    item].loc[common_dates, common_symbols]
                self.trade[item] = mat

            if cross_validate:
                for key in ['open', 'high', 'low', 'close']:
                    print ('cross validating bid/ask on'
                           ' %s using trade prices...') % key
                    ref = self.trade[key].values

                    for item in ['bid', 'ask']:
                        tmp = getattr(self, item)[key]
                        hit = np.abs(tmp.values / ref - 1) > 0.10
                        tmp.values[hit] = float('nan')

        self.mid = {x: (self.ask[x] + self.bid[x]) / 2 for x in self.ask}

        tmp = (self.mid['open'] + self.mid['close']) / 2
        missing = ~np.isfinite(self.mid['vwap'])
        self.mid['vwap'][missing] = tmp[missing]

        self.mid = pd.Panel(self.mid)
        self.bid = pd.Panel(self.bid)
        self.ask = pd.Panel(self.ask)
        self.trade = pd.Panel(self.trade)

    def load_prices_tick(self, h5file='h:/cache/tick/CL1_RI.h5'):

        data = pd.read_hdf(h5file, 'data')
        freq = self.env.freq

        ohlc = data.trade.ffill().resample(freq, 'ohlc')
        v = data.vol.resample(freq, 'sum')
        ohlc['open'] = ohlc['close'].shift(1)  # previous month's close
        ohlc['vwap'] = ohlc.open * 1 / 3 + ohlc.close * 2 / 3
        ohlc['volume'] = v.loc[ohlc.index]
        ohlc['value'] = ohlc.volume * ohlc.vwap

        # risk-free rate from FRED
        import pandas.io.data as web
        rf = self.env.pivots['rf']
        rf_data = web.DataReader(rf, "fred",
                                 start=self.env.mkt_start_date)
        rf_data = rf_data.resample(freq).loc[ohlc.index]

        self.mid = {}

        for item in ohlc:
            mat = pd.concat((ohlc[item], rf_data), axis=1)
            mat.columns = ['CL1', rf]
            self.mid[item] = mat
            # self.trade[item] = mat

        self.mid = pd.Panel.from_dict(self.mid)
        self.bid = self.mid.copy()
        self.ask = self.mid.copy()

    def load5(self, last_periods=None, type_='TRADE', processes=1):
        if processes > 1:
            raise Exception('Pytables not threading safe...use only 1 thread')

        resample = self.env.freq if \
            self.env.freq != '1Min' else None
        result = []
#        pool = ThreadPool(processes=processes)

        h5path = self.env.pricing_path
        print 'loading %s prices in %d thread(s)...' % (type_, processes)

        result = {}
        for h5file in sorted(os.listdir(h5path)):
            if h5file.find(type_) > -1:
                ticker, ex = h5file.split('_')[:2]
                if len(ticker) == 1:
                    ticker = '_'.join((ticker, ex))

                ticker = ticker.replace('.', '/')

                if ticker in self.env.read_univ():
                    load5_file(result, h5path, h5file,
                               last_periods, resample)

#        pool.close()
#        pool.join()
        print 'retrieving index...'
        union_index = None
        union_ticker = []

        for ticker in result:
            df = result[ticker]
            union_index = df.index.union(
                union_index) if union_index is not None else df.index
            union_ticker.append(ticker)

        union_index = union_index.unique()
        union_ticker = sorted(set(union_ticker))

        # using only trading hours....
        local_index = [
            x.replace(tzinfo=pytz.UTC).astimezone(self.env.tz)
            for x in union_index]
        if self.env.morning_end_time is not None:
            local_index = [x for x in local_index if
                           x.time() >= self.env.mkt_start_time and
                           x.time() <= self.env.morning_end_time or
                           x.time() >= self.env.afternoon_start_time and
                           x.time() <= self.env.mkt_end_time]
        else:
            local_index = [x for x in local_index if x.time(
            ) >= self.env.mkt_start_time and x.time() <= self.env.mkt_end_time]

        if self.env.weekday_only:
            local_index = [x for x in local_index if x.weekday() < 5]
        union_index = [
            x.astimezone(pytz.UTC).replace(tzinfo=None) for x in local_index]

        print 'combining results...'
        res = {}
        for ticker in result:
            print 'processing %s...' % ticker
            df = result[ticker]

            for metric in df.columns:
                if metric != 'ticker':
                    if metric not in res:
                        res[metric] = pd.DataFrame(index=union_index)
                    res[metric][ticker] = df[metric]

        # using shifted close as open...
        res['open'].values[1:] = res['close'].values[:-1]

        mid = (res['open'] + res['close']) / 2
        try:
            res['vwap'] = res['value'] / res['volume']
        except ZeroDivisionError:
            res['vwap'] = mid.copy()

        missing = ~np.isfinite(res['vwap'])
        res['vwap'][missing] = mid[missing]

        ######
        if self.env.tz_convert:
            for metric in res:
                res[metric].index = [
                    x.replace(tzinfo=pytz.UTC).astimezone(self.env.tz)
                    for x in res[metric].index]

        assign = {'TRADE': 'trade', 'BID': 'bid', 'ASK': 'ask'}
        if type_ in assign:
            setattr(self, assign[type_], res)
        else:
            raise Exception('unrecogized price type %s...' % type_)

    def spd_process(self, spd, close, mid, rf):

        raise SystemError
        rel_spd = spd / close
        # filling missing bid/asks
        for i in range(rel_spd.shape[0]):
            row = rel_spd.values[i, :]
            hit = np.isnan(row)
            if all(hit):
                row[:] = np.nan
            else:
                row[hit] = np.nanmedian(row)  # to be conservative

        if False:
            self.bid = {}
            self.ask = {}
            for item in mid:
                if item in ['open', 'high', 'low', 'close', 'vwap']:
                    self.bid[item] = mid[item] - rel_spd * mid['close'] / 2
                    self.ask[item] = mid[item] + rel_spd * mid['close'] / 2
                else:
                    self.bid[item] = mid[item]
                    self.ask[item] = mid[item]

        # one-month average of bid/ask spread
        print 'smoothing bid/ask spread and inferring bid/ask for OHLC ...'
        rel_spd = mid['spd'] / mid['close']

        # filling missing bid/asks
        for i in range(rel_spd.shape[0]):
            row = rel_spd.values[i, :]
            hit = np.isfinite(row)
            if np.sum(hit) > 0:
                row[~hit] = np.nanmedian(row)  # to be conservative

        rel_spd = rel_spd.bfill()

        # no bid/ask for treasury
        rel_spd[rf] = 0

        self.rel_spd = rel_spd
        del mid['spd']
