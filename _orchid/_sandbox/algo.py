# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

import pandas as pd
from model import pca
import time
from datetime import datetime
from data import db

from msg.msgpk import stream

pd.set_option('display.notebook_repr_html', False)
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 7)
pd.set_option('display.width', 120)


class algobeat(stream):

    def __init__(self, silence=False, interval=1, **kwargs):

        stream.__init__(self, **kwargs)

        self.freq = interval   # every 10 sec
        self.bars = 30  # 30
        self.sd_window = 30  # self.bars
        self.nfactors = 15
        self.last_rows = 1
        self.silence = silence

        # initial alpha/sharpe needed for ignition
        self.df = db.sql_ohlc(dbfile=db.TICK_DB_FILE,
                              bars=self.bars + self.sd_window + self.last_rows,
                              freq=self.freq, overhead=20)

        self.df.columns = [x.encode('ascii', 'ignore')
                           for x in self.df.columns]
        required = self.bars + self.sd_window + 1
        if self.df.shape[0] < required:
            raise Exception('insufficient kickstarting data, %d rows more needed' % (
                required - self.df.shape[0]))

        self.df.ffill(inplace=True)

        self.model = pca.PCAModel(
            self.df, self.nfactors, self.bars, method='nipals')
        self.alpha = self.model.generate_signal()
        self.alpha_mat = pd.DataFrame(columns=self.df.columns)
        self.alpha_mat[self.alpha.columns] = self.alpha

        # sharpe of trailing alpha signal
        sd_mat = pd.rolling_std(
            self.alpha_mat, self.sd_window, min_periods=3).replace(0, float('nan'))
        self.sharpe_mat = self.alpha_mat / sd_mat
        self.sharpe_mat = self.sharpe_mat.iloc[-self.bars:]
        self.sharpe_mat = self.sharpe_mat.replace(float('nan'), 0)

        print 'sharpe mat:'
        print self.sharpe_mat.tail(10)

        self.prepare()
        self.listening()

    def action(self, msg):

        # incoming msg
        mid = {x: (msg[x]['ask'] + msg[x]['bid']) / 2 for x in msg}
        # print pd.DataFrame(test)
        if len(mid) < 1:
            return
#        raw = pd.DataFrame(msg)
#        if raw.shape[0] < 1:
#            return

        hit = list(set(mid) & set(self.df.columns))
        ts = datetime.utcnow()
        # mid = (raw.loc['ask'] + raw.loc['bid']) / 2    ### using mid prices
        self.df.loc[ts, hit] = [mid[x] for x in hit]  # mid[self.df.columns]

        t0 = time.time()

        # rolling shape
        self.df.ffill(inplace=True)
        extra = self.df.shape[0] - (self.bars + self.sd_window + 1)
        if extra > 0:
            self.df.drop(self.df.index[:extra], inplace=True)
        elif extra < 0:
            print 'waiting for df size: %d' % self.df.shape[0]
            return

        # fitting PCA model
        model = pca.PCAModel(self.df, self.nfactors, self.bars, method='nipals', last_rows=self.last_rows,
                             silence=self.silence)
        signal = model.generate_signal()
        alpha = pd.DataFrame(columns=self.df.columns)
        alpha[signal.columns] = signal

        # one extra line of alpha and sharpe
        for ts in alpha.index[-self.last_rows:]:
            self.alpha_mat.loc[ts] = alpha.loc[ts]
            sd = self.alpha_mat[-self.sd_window:
                                ].std(axis=0).replace(0, float('nan'))
            self.sharpe_mat.loc[ts] = (
                alpha.loc[ts] / sd).replace(float('nan'), 0)

        # send alpha slice via msgpack
        alpha_out = self.sharpe_mat[-1:]
        stamp = alpha_out.index.format()
        alpha_out = alpha_out.to_dict('records')[0]
        alpha_out['stamp'] = stamp  # add a timestamp

        for link in self.downlink:
            link.send(self.packer.pack({'alpha': alpha_out, 'mid': mid}))

        # timing
        t1 = time.time()
        if not self.silence:
            print self.sharpe_mat.tail(10)
            print 'total time used for db & alpha generation: %.2f seconds' % (t1 - t0)
            print 'current time: %s' % datetime.utcfromtimestamp(t1)

        # trimming alpha and sharpe mat; dropping old values from beginning
        extra = self.alpha_mat.shape[0] - (self.bars + self.sd_window + 1)
        if extra > 0:
            self.alpha_mat.drop(self.alpha_mat.index[:extra], inplace=True)

        extra = self.sharpe_mat.shape[0] - (self.bars + 1)
        if extra > 0:
            self.sharpe_mat.drop(self.sharpe_mat.index[:extra], inplace=True)


if __name__ == "__main__":

    from optparse import OptionParser

    usage = "usage: %prog [options] <up_port=8002> <down_port=8004>"
    parser = OptionParser(usage)
    parser.add_option("-s", "--silence", dest="silence", action="store_true",
                      help="hide streaming data")
    parser.add_option("-i", '--interval', dest="interval", type="int",
                      help="interval in seconds for heartbeating (default: %default)",
                      metavar="barInterval",
                      default=1)

    up_port = 8002
    down_port = 8004
    silence = False

    (options, args) = parser.parse_args()
    interval = options.interval

    if len(args) > 2:
        parser.error("incorrect number of arguments")
    if len(args) == 2:
        up_port = int(args[0])
        down_port = int(args[1])
    # else:
    #    raise Exception('must specify both uplink and downlink ports')
    if options.silence:
        silence = True

    algobeat(up_port=[up_port], down_port=[down_port],
             interval=interval, silence=silence)
