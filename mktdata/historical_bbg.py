#!/usr/bin/env python
"""
Created on Mon Jun 23 11:16:21 2014

@author: han.yan
"""

from api import bbg
import pandas as pd
from multiprocessing import Pool
from time import sleep
from utils.timedate import timeflag
from datetime import datetime
from environ.wrapper import get_env

pd.set_option('display.notebook_repr_html', False)
pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 10)


def parseCmdLine():
    from optparse import OptionParser
    usage = "usage: %prog [options] <environ>"
    parser = OptionParser(
        usage=usage, description="Retrieve historical tick/bar data.")

    parser.add_option("-b", "--bar", dest="bar",
                      action="store_true", default=False,
                      help="request bar data instead of tick data")

    parser.add_option("-t",
                      dest="hrs_step",
                      type="int",
                      help="step length in hrs (default: %default)",
                      metavar="hrsStep",
                      default=12)
    parser.add_option("-i",
                      dest="bar_interval",
                      type="int",
                      help="bucket interval (default: %default)",
                      metavar="barInterval",
                      default=None)

    parser.add_option("-s",
                      dest="pricing_source",
                      help="pricing source (default: %default)",
                      metavar="pricingSource",
                      default='')

    parser.add_option("-e",
                      dest="event",
                      help="requested event (for IntradayBarRequest only)",
                      metavar="Event",
                      default=None)

    parser.add_option("-v",
                      dest="verbose",
                      help="detailed output during downloading...",
                      metavar="verbose",
                      action="store_true",
                      default=False)

    parser.add_option("-d",
                      dest="daily",
                      help="daily mode for historical download",
                      metavar="daily",
                      action="store_true",
                      default=False)

    parser.add_option("-x",
                      dest="start_date",
                      help="starting date for the request (default: %default)",
                      metavar="startDate",
                      default='1970-01-01')

    parser.add_option("-y",
                      dest="end_date",
                      help="ending date for the request (default: %default)",
                      metavar="endDate",
                      default='2049-01-01')

    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)
    parser.add_option("--me",
                      dest="maxEvents",
                      type="int",
                      help="stop after this many events (default: %default)",
                      metavar="maxEvents",
                      default=1e99)

    options, args = parser.parse_args()
    return options, args, parser


if __name__ == "__main__":

    # defaults for debugging
    events = ['TRADE', 'BEST_BID', 'BEST_ASK']
    req_type = 'IntradayTickRequest'
    options, args, parser = parseCmdLine()

    # options post-processing
    if options.bar:
        req_type = 'IntradayBarRequest'
        options.hrs_step = 12
        options.bar_interval = 1

    if options.event is not None:
        events = [options.event]

    if len(args) != 1:
        parser.error('must specify environment...')
    else:
        name = args[0]
        name = name + '-1D' if options.daily else name + '-1Min'
        env = get_env(name)
        universe = env.read_univ(update=False, full_ticker=True,
                                    pivots=True)

    def test(x):
        print(x)
        sleep(2)

    t0 = timeflag()
    cutoff_time = datetime.utcnow()

    try:
        pool = Pool(processes=5)
        res = []

        if options.daily:  # pivot index
            fields = ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST',
                      'PX_BID', 'PX_ASK',
                      'PX_VOLUME', 'TURNOVER', 'EQY_SH_OUT', 'CUR_MKT_CAP']
            start_datetime = env.mkt_start_date
            end_datetime = cutoff_time

        for security in universe:

            # CDX specification
            if env.asset_class == 'CDX':
                raise Exception('CDX not implemented....')

            if ' ' not in security:
                print 'skipping %s ...' % security
                continue

            ticker, source, asset = bbg.bbg_split(security)
            if env.history_source != [] and \
                    source not in env.history_source:
                print 'skipping source %s for %s' % (source, security)
                continue

            if options.daily:
                start_datetime = env.mkt_start_date
                end_datetime = env.mkt_end_date

                args = (security, fields, env.hdf5_path,
                        start_datetime, end_datetime, True, options.verbose)
                result = pool.apply_async(bbg.download_daily, args=args)
                res.append(result)

            else:
                # arguments for downloading
                for event in events:
                    args = (security,
                            event,
                            options.hrs_step,
                            options.bar_interval,
                            env.hdf5_path,
                            options.start_date,
                            options.end_date,
                            req_type,
                            cutoff_time,
                            True,
                            options.verbose)

                    result = pool.apply_async(bbg.download_intraday, args=args)
                    res.append(result)

        # retriving results (process won't start to run until here)
        tmp = [x.get(timeout=1e6) for x in res]

    except Exception as e:
        print 'error: %s' % e
        import pdb
        pdb.set_trace()

    except KeyboardInterrupt:
        print 'Ctrl+C detected, exiting...'

    delta = (timeflag() - t0)
    print 'grid time: %f sec' % (delta)
