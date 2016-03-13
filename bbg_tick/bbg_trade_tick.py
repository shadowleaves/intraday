# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 15:06:04 2015

@author: han.yan
"""


import os
#import gzip
#import re
import pytz
import pandas as pd
import numpy as np
# from datetime import datetime
from ciso8601 import parse_datetime
from utils.utils import timeflag, chunking
from multiprocessing import Pool

def parsing(csvpath, files, label):

#    tz = pytz.timezone('EST5EDT')
    mapping = {'T': 'trade', 'S': 'settle', 'B': 'bid', 'A': 'ask'}
#    columns = ['timestamp', 'type_', 'px', 'qty', 'tag']
    total = 0

    #name_str = '_'.join((files[0].split('.')[1], files[-1].split('.')[1]))
    h5file = 'h:/cache/tick/%s.h5' % label
    
    try:
        os.remove(h5file)
    except:
        pass
    
    for csvfile in files:
        #print csvfile
        t0 = timeflag()
        df = pd.read_csv(csvpath + csvfile)
        #if df.shape[0] < 1e5:
        #    continue
        
        t1 = timeflag()
        df = df[(df.side=='T') & (df.px!=0)][::-1]
        if df.shape[0] < 1:
            continue
        
        df.index = pd.DatetimeIndex(df.datetime)
        df = df.drop(['datetime','tag'], axis=1)
#        import pdb
#        pdb.set_trace()
        t2 = timeflag()
        try:
            store = pd.HDFStore(h5file, 'a')
            store.append('data', df)
            store.close()
        except Exception as e:
            print 'error: %s' % e
            import pdb
            pdb.set_trace() ##df = pd.DataFrame(columns=columns)
        
        
        parsing = t1 - t0
        pivoting = t2 - t1
        saving = timeflag() - t2
        total += 1
        print ('%s, day %s, parsing %.1f sec, filtering %.1f sec, '
               'saving %.1f sec' ) % (label, df.index[0].date(),
                                      parsing, pivoting, saving)
                                        
    

if __name__ == '__main__':
    
    import random
    
    csvpath = 'h:/cache/perl/'
    files = os.listdir(csvpath)
    
    if True:
        parsing(csvpath, files, 'trade')
        raise SystemExit
    
    random.seed()
    random.shuffle(files)    
    
    chunk_size = 350
    n_proc = int(len(files)/chunk_size)
    print 'using %d parallel processes...' % n_proc
    
    chunks = chunking(files, chunk_size)
    pool = Pool(processes=n_proc)
    
    res = []
    i = 0
    for block in chunks:
        label = 'block_%d' % i
        result = pool.apply_async(parsing, args=(csvpath, block, label))
        res.append(result)
        i += 1
    
    # retriving results (process won't start to run until here)
    tmp = [x.get(timeout=1e6) for x in res]
