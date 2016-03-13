#!/usr/bin/env python

"""
Limit order book simulation for Indian security exchange.
"""

import _lob

import gzip
import logging
import os
import pandas as pd
#import sys
import time


pd.set_option('display.notebook_repr_html',False)
pd.set_option('display.max_rows',10)
pd.set_option('display.max_columns',6)
pd.set_option('display.width', 120)


if __name__ == '__main__':
    
    from optparse import OptionParser

    usage = "Usage: %prog <ticker> <output directory> <input file names>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest = "verbose", action = "store_true", 
                        default = False, help = "showing detailed info")
    
    (options, args) = parser.parse_args()

    args = ['EXAMPLE','./', 'EXAMPLE-orders-sameday.csv']
    firm_name, output_dir = args[:2]
    file_name_list = args[2:]

    # Suppress log generation when not in debug mode:
    DEBUG = True
    if DEBUG:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    format = '%(asctime)s %(name)s %(levelname)s [%(funcName)s] %(message)s'
    logging.basicConfig(level=level, format=format)

    # Remove root log handlers:
    for h in logging.root.handlers:
        logging.root.removeHandler(h)

    # Set up output files:
    events_log_file = os.path.join(output_dir, 'events-' + firm_name + '.log')
    daily_stats_log_file = os.path.join(output_dir, 'daily_stats-' + firm_name + '.log')
    
    # Instantiate simulation:
    lob = _lob.LimitOrderBook(show_output=False, sparse_events=True,
                              events_log_file=events_log_file,
                              stats_log_file=None,
                              daily_stats_log_file=daily_stats_log_file)

    # Only create log file when in debug mode:
    if DEBUG:
        log_file = os.path.join(output_dir, 'lob-' + firm_name + '.log')
        fh = logging.FileHandler(log_file, 'w')
        fh.setFormatter(logging.Formatter(format))
        lob.logger.addHandler(fh)

    # Process all available files; assumes that the files are named in
    # a way such that their sort order corresponds to the
    # chronological order of their respective contents:    
        
    start = time.time()

    for file_name in sorted(file_name_list):

        # Check whether input file is compressed:
        with gzip.open(file_name, 'rb') as f:
            try:
                f.read(1)
            except IOError:
                compression = None
            else:
                compression = 'gzip'

        tp = pd.read_csv(file_name,
                             names=_lob.col_names,
                             iterator=True,
                             compression=compression)
        while True:
            try:
                data = tp.get_chunk(500)
            except StopIteration:
                break
            else:
                # Process orders that occurred before a certain cutoff time:
                #if data.irow(0)['trans_time'] > '09:25:00.000000':
                #    break        
                for row in data.iterrows():
                    #raise SystemExit
                    lob.process(row)

    lob.record_daily_stats(lob.day)
    lob.print_daily_stats()
    print 'Processing time:              ', (time.time()-start)
