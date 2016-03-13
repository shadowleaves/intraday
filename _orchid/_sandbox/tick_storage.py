# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

from datetime import datetime
import sqlite3 as sql
from optparse import OptionParser
from msg.msgpk import stream
from db.db import TICK_DB_FILE
import time
#import numpy as np

cnt = 0
t_ = time.clock(); timestamp_ = time.time()  ### don't use this for microsec

class receiver(stream):    
    def __init__(self, db_rows_limit = 1e6, overhead = 1e5, silence = False, **kwargs):
        
        self.cache = []
        self.cache_limit = 50
        self.db_rows_limit = db_rows_limit
        self.overhead = overhead
        self.silence = silence
        
        self.db_init()
        stream.__init__(self, **kwargs)
        self.prepare()
        self.listening()
        
    ##############
    def db_init(self):
        self.table_name = 'flow2'
        #### initializing database
        
        self.con = sql.connect(TICK_DB_FILE)   
        self.cur = self.con.cursor()
        self.cur.execute('drop table if exists %s '% self.table_name)
        self.cur.execute('''create table if not exists %s
                    (instrument text, ask real, bid real, quote_time real, db_time real
                    , primary key (instrument asc, quote_time desc, db_time desc))''' % self.table_name)
 
        
    def action(self, msg):
        global cnt
        inst = msg['instrument']
        ask = msg['ask']
        bid = msg['bid']
        quote_time = datetime.utcfromtimestamp(msg['time'])
        rec_time = datetime.utcfromtimestamp(msg['stamp'])
        cpu_time = datetime.utcfromtimestamp(time.clock() - t_ + timestamp_) #datetime.utcnow() #parser.parse(msg['stamp']) #datetime.utcnow()
        db_time = datetime.utcnow()
        
        lag0 = (db_time - cpu_time).total_seconds()  ### this is the real shift. don't use
        lag1 = (db_time-rec_time).total_seconds()
        lag2 = (rec_time-quote_time).total_seconds()  ### meaningless because BBG doesn't provide accurate trade time
        
#        if (np.isnan(ask) or np.isnan(bid)) and inst == 'GBPJPY':
#            print msg
#        
        if not self.silence and cnt % 10 == 0:
             print '%9f %9f %9f %s %4s %12f %12f %s' % (lag0, lag1, lag2, msg['instrument'], msg['dealer'], msg['ask'], msg['bid'], datetime.fromtimestamp(msg['stamp']))
            
        cnt = cnt + 1
        
        return
             
        self.cache.append((inst, bid, ask, rec_time, db_time))  ### need to use rec_time as it's more accurate
                
        if len(self.cache) > self.cache_limit: #### do not commit too often as this will slow down streaming
            self.cur.executemany('insert into %s values (?,?,?,?,?)' % self.table_name, self.cache)
            
            while len(self.cache) > 0:
                try:
                    self.con.commit()
                except sql.OperationalError:
                    print 'database locked, try again...'
                else:
                    print 'commit !!!!!'
                    self.cache = []
                    #### truncating earlier stream
                    start, end = self.cur.execute('select min(rowid), max(rowid) from %s' % self.table_name).fetchall()[0]
                    if end - start > self.db_rows_limit + self.overhead:
                        self.cur.execute('delete from %s where rowid < %d' % (self.table_name, end - self.db_rows_limit))
                        print 'truncating !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
                    break
    


if __name__ == "__main__":

### options
    usage = "usage: %prog [options] [port=8003]"
    op = OptionParser(usage)
    op.add_option("-s", "--silence", dest = "silence", action = "store_true", 
                        help = "hide streaming data")

    port = 8003  #### tick db stream
    silence = False
    
    (options, args) = op.parse_args()
    if len(args) > 1:
        op.error("incorrect number of arguments")
    if len(args) == 1:
        port = int(args[0])
    if options.silence:
        silence = True
    
  #### msgpack
    receiver(up_port = [port], silence = silence, sleep_time = 0)
