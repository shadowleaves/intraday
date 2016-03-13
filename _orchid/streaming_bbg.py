# -*- coding: utf-8 -*-
"""
Created on Sun Aug 24 23:05:57 2014

@author: han.yan
"""
import blpapi
import thread
import nanotime

# from datetime import datetime
from optparse import OptionParser
from msg.mcast import stream
from api.bbg import parse_BBG_stream
from utils.utils import to_timestamp, miu, timeflag
from cache import environ


class bbg_stream(stream):

    def __init__(self, verbose=False, interval=1, universe=[], **kwargs):
        stream.__init__(self, **kwargs)

        self.universe = universe
        self.session = None
        self.verbose = verbose

    def read_univ(self):
        return self.universe

    def connect(self, service='//blp/mktdata', host='localhost', port=8194):
        """
            connect to BBG Desktop API streaming service
        """
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)

        # Create a Session
        session = blpapi.Session(sessionOptions)

        # start the session
        if not session.start():
            raise Exception("Failed to start session.")
            return None

        if session is None:
            raise Exception('BBG session start failure')

        # opening service
        if not session.openService(service):
            print "Failed to open", service
            return

        self.session = session

    def subscribe(self, fields='LAST_TRADE,BID,ASK'):

        self.connect()

        subscriptions = blpapi.SubscriptionList()
        for security in self.universe:
            subscriptions.add(security,
                              # any changes to the fields will trigger update
                              fields,
                              "",
                              blpapi.CorrelationId(security))

        self.session.subscribe(subscriptions)

        # Process received events
        while self.signal != 'stop':
            # We provide timeout to give the chance to Ctrl+C handling:
            event = self.session.nextEvent(timeout=500)  # 0.5 second timeout
            if self.signal == 'pause':
                continue

            for msg in event:
#                if event.eventType() == blpapi.Event.SUBSCRIPTION_STATUS:
                if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:

                    # get the timestamp first
                    rec_time = nanotime.now().timestamp()
                    t0 = timeflag()
                    dt = parse_BBG_stream(msg, mode='dict')

                    if dt is None:
                        continue

                    dt['quote_time'] = to_timestamp(dt['quote_time'])
                    dt['timestamp'] = rec_time

                    delta = (timeflag() - t0) * 1e6
                    if self.verbose:
                        print 'STREAM latency: %f %ss' % (delta, miu)
                    self.sending(dt, 'TICK')

if __name__ == "__main__":

    usage = "usage: %prog [options] <up_port=MASTER> <down_port=TICK> <ASSET_CLASS>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      help="showing streaming data")
    parser.add_option(
        "-e", "--env", dest="env", default='ALL', help="environments")

    (options, args) = parser.parse_args()

    up_port = ['MASTER']
    down_port = ['TICK']

    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
        parser.error("incorrect number of arguments")

    if options.env == 'ALL':
        options.env = 'ETF|FX|CDX|Comdty'

    universe = []
    for env in options.env.split('|'):
        env = environ.get_env(env + '_1Min')
        instruments = env.read_univ(update=False, full_ticker=True)
        universe += instruments

    tick = bbg_stream(verbose=options.verbose, universe=sorted(set(universe)),
                      up_port=up_port, down_port=down_port)
    tick.prepare()  # mcast channel preparation
    # separate thread for listening
    thread.start_new_thread(tick.listening, ())
    tick.subscribe()
