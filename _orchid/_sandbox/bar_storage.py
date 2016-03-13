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

class receiver(stream):    
    def __init__(self, db_rows_limit = 1e6, overhead = 1e5, silence = False, **kwargs):
        
        self.cache = []
        #self.cache_limit = 50
        self.db_rows_limit = db_rows_limit
        self.overhead = overhead
        self.silence = silence
        
        self.db_init()
        stream.__init__(self, **kwargs)
        self.prepare()
        self.listening()
        
    ##############
    def db_init(self):
        self.table_name = 'flow'
        #### initializing database
        
        self.con = sql.connect(TICK_DB_FILE)   
        self.cur = self.con.cursor()
        self.cur.execute('drop table if exists flow')
        self.cur.execute('''create table if not exists %s
                    (instrument text, ask real, bid real, quote_time real, db_time real
                    , primary key (instrument asc, quote_time desc, db_time desc))''' % self.table_name)
 
        
    def action(self, msg):
        
        db_time = datetime.utcnow()
        
        for inst in msg:
            ask = msg[inst]['ask']
            bid = msg[inst]['bid']
            quote_time = datetime.utcfromtimestamp(msg[inst]['quote_time'])
            self.cache.append((inst, bid, ask, quote_time, db_time))  ### need to use rec_time as it's more accurate
        
        if not self.silence:
            print 'db time: %s' % db_time
                
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
    receiver(up_port = [port], silence = silence, sleep_time = 0.001)
