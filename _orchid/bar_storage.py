# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

from datetime import datetime
import sqlite3 as sql
from optparse import OptionParser
from msg.mcast import multicast
from data.db import INTRADAY_DB_FILE
#import timed

class bar_collect(multicast):
    def __init__(self, db_rows_limit = 5e5, overhead = 1e4, verbose = False, **kwargs):
        
        multicast.__init__(self, **kwargs)
        self.type_ = 'db'
        self.cache = []
        #self.cache_limit = 50
        self.db_rows_limit = db_rows_limit
        self.overhead = overhead
        self.verbose = verbose
        self.db_init()

    ##############
    def db_init(self):
        self.table_name = 'flow'
        #### initializing database
        print INTRADAY_DB_FILE
        self.con = sql.connect(INTRADAY_DB_FILE)   
        self.cur = self.con.cursor()
        #self.cur.execute('drop table if exists flow')
        self.cur.execute('''create table if not exists %s
                    (instrument text, bid real, ask real, trade real, quote_time datetime, db_time datetime
                    , primary key (instrument asc, db_time desc, quote_time desc))
                    ''' % self.table_name)
 #
        
    def process(self, msg, port):
        if 'BEAT' not in port:
            return
            
        db_time = datetime.utcnow()

        for inst in msg:
            ask = msg[inst]['ask']
            bid = msg[inst]['bid']
            trade = msg[inst]['trade']
            quote_time = datetime.utcfromtimestamp(msg[inst]['time'])
            row = (inst, bid, ask, trade, quote_time, db_time)
            self.cache.append(row)  ### need to use rec_time as it's more accurate
        
        if self.verbose:
            print 'db time: %s' % db_time

        self.cur.executemany('insert into %s values (?,?,?,?,?,?)' % self.table_name, self.cache)
        
        while len(self.cache) > 0:
            try:
                self.con.commit()
            except sql.OperationalError:
                print 'database locked, try again...'
            else:
                self.cache = []
                #### truncating earlier stream
                start, end = self.cur.execute('select min(rowid), max(rowid) from %s' % self.table_name).fetchall()[0]
                if end - start > self.db_rows_limit + self.overhead:
                    self.cur.execute('delete from %s where rowid < %d' % (self.table_name, end - self.db_rows_limit))
                    print 'truncating !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
                
                print 'db time: %s, commit... # of rows: %d' % (db_time, end-start)
                break



if __name__ == "__main__":

    usage = "usage: %prog [options] <up_port=MASTER|BEAT> <down_port=REPORT>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest = "verbose", action = "store_true", 
                        help = "showing detailed info")
 
    (options, args) = parser.parse_args()
    up_port = ['BEAT']
    down_port = []
    
    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
         parser.error("incorrect number of arguments")
        
    db = bar_collect(up_port = up_port, down_port = down_port, 
                     verbose = options.verbose)
    db.prepare()
    db.listening()
