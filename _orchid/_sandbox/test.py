# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:45:38 2014

@author: han.yan
"""

#import msgpack
# from time import sleep
#import random
#import socket
# from datetime import datetime
# from dateutil import parser
#import sqlite3 as sql
# from optparse import OptionParser

from msg.msgpk import streaming


class receiver(streaming):
    def action(self, msg):
        print msg
#    def __init(self, *args, **kwargs):
#        streaming.init(args, kwargs)

        
        
