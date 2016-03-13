# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""


from datetime import datetime
from tempo import sched

from msg.msgpk import stream

       
class heartbeat(stream):
    def __init__(self, silence = False, interval = 1, **kwargs):        
        self.latest = {}
        self.silence = silence
        stream.__init__(self, **kwargs)
        self.prepare()
        
        int_str = ','.join([str(x * interval) for x in range(0,60/interval)])
        print int_str
        self.beat = sched.cronbeats(second = int_str, hFun = self.send_msg, 
                                max_runs = 1e6, max_instances = 10)
        self.beat.start()
        self.listening()
        
    def action(self, msg):
        inst = msg['instrument']
        if not self.latest.has_key(inst):
            self.latest[inst] = {}
            
        self.latest[inst]['ask'] = msg['ask']
        self.latest[inst]['bid'] = msg['bid']
        self.latest[inst]['quote_time'] = msg['time']
        
    def send_msg(self):
        for link in self.downlink:
            link.send(self.packer.pack(self.latest))
        if not self.silence:
            print datetime.utcnow()


if __name__ == "__main__":

    from optparse import OptionParser
    
    usage = "usage: %prog [options] <up_port=8001> <down_port=8002|8003>"
    parser = OptionParser(usage)
    parser.add_option("-s", "--silence", dest = "silence", action = "store_true", 
                        help = "hide streaming data")
    parser.add_option("-i", '--interval', dest="interval", type="int",
                      help="interval in seconds for heartbeating (default: %default)",
                      metavar="barInterval",
                      default=1)

    up_port = 8001
    down_port = 8002
    silence = False
    
    (options, args) = parser.parse_args()
    interval = options.interval
    
    if len(args) > 2:
        parser.error("incorrect number of arguments")
    if len(args) == 2:
        up_port = [int(x) for x in args[0].split('|')]
        down_port = [int(x) for x in args[1].split('|')]
    else:
        raise Exception('must specify both uplink and downlink ports')
    if options.silence:
        silence = True
        
    heartbeat(up_port = up_port, down_port = down_port, interval = interval, silence = silence)
