# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 14:29:06 2014

@author: han.yan
"""

from datetime import datetime
from tempo import sched
from optparse import OptionParser
from msg.mcast import multicast
from utils.utils import miu, timeflag

import logging
logging.basicConfig()
       
class heartbeat(multicast):
    def __init__(self, interval = 1, **kwargs):        
        self.latest = {}
        multicast.__init__(self, **kwargs)
        
        if interval < 60:
            if 60 % interval != 0:
                raise Exception('60 seconds must be divisible by interval %d (seconds)' % interval)
            int_str = ','.join([str(x * interval) for x in range(0,60/interval)])
            print int_str
            self.beat = sched.cronbeats(second = int_str, hFun = self.send_msg, 
                                max_instances = 10)
        else:
            if interval % 60 != 0:
                raise Exception('interval %d (seconds) must be divisible by 60' % interval)
            interval = interval / 60
            int_str = ','.join([str(x * interval) for x in range(0,60/interval)])
            print int_str
            self.beat = sched.cronbeats(minute = int_str, hFun = self.send_msg, 
                                max_instances = 10)
            
        self.type_ = 'beat'
        
        self.prepare()
        self.beat.start()
        self.listening()
            
    def process(self, msg, port):
        if port != 'TOB':
            return
            
        inst = msg['ticker']
        if not self.latest.has_key(inst):
            self.latest[inst] = {}
            
        self.latest[inst]['ask'] = msg['ask']
        self.latest[inst]['bid'] = msg['bid']
        self.latest[inst]['ask.src'] = msg['ask.src']
        self.latest[inst]['bid.src'] = msg['bid.src']
        self.latest[inst]['trade'] = msg['trade']
        self.latest[inst]['time'] = msg['timestamp']
        
       # print self.latest
    def send_msg(self):
        ts = datetime.now()
        t0 = timeflag()
        if self.signal != 'pause':
            for portname in self.down_portname:
                if portname != 'REPORT':
                    self.sending(self.latest, portname)
        delta = (timeflag() - t0) * 1e6
        print 'heartbeat: %s, delay: %f %ss' % (ts, delta, miu)

if __name__ == "__main__":

    usage = "usage: %prog [options] <up_port=MASTER|TOB> <down_port=BEAT>"
    parser = OptionParser(usage)
#    parser.add_option("-v", "--verbose", dest = "verbose", action = "store_true", 
#                        help = "showing streaming data")
    parser.add_option("-i",
                  dest="interval", type="int", help="cronbeating interval in seconds (default: %default)",
                  metavar="interval", default=5)
    
    (options, args) = parser.parse_args()
    up_port = ['TOB']
    down_port = ['BEAT']
    
    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
         parser.error("incorrect number of arguments")
        
    beats = heartbeat(up_port = up_port, down_port = down_port, interval = options.interval)
