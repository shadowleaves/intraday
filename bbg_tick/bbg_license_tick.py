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
        
        px = df.pivot_table(index='datetime', columns='side', values='px', aggfunc=np.nanmean)
        vol = df.pivot_table(index='datetime', columns='side', values='qty', aggfunc=np.nansum)
        
        if px.shape != vol.shape:
            print 'mismatched px/vol shape...'
            import pdb
            pdb.set_trace()
        
        px.index = pd.DatetimeIndex([parse_datetime(x) for x in px.index])
        vol.index = px.index
        px[px==0] = np.nan
        
        if len(px.columns) < 4:
            continue
        
        px.columns = [mapping[x] for x in px.columns]
        vol.columns = [mapping[x] for x in vol.columns]
        #import pdb
        #pdb.set_trace()
        
        t2 = timeflag()
        try:
            store = pd.HDFStore(h5file, 'a')
            store.append('tick', px)
            store.append('volume', vol)
            store.close()
        except:
            import pdb
            pdb.set_trace() ##df = pd.DataFrame(columns=columns)
        
        
        parsing = t1 - t0
        pivoting = t2 - t1
        saving = timeflag() - t2
        total += 1
        print ('%s, day %s, parsing %.1f sec, pivoting %.1f sec, '
               'saving %.1f sec' ) % (label, px.index[0].date(),
                                      parsing, pivoting, saving)
                                        
    

if __name__ == '__main__':
    
    import random
    
    csvpath = 'h:/cache/perl/'
    files = os.listdir(csvpath)
    
    if True:
        parsing(csvpath, files, 'test')
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
