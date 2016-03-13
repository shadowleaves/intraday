# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 11:16:21 2014

@author: han.yan
"""

import pandas as pd
import re
import requests
import json
import thread

from time import mktime
import nanotime
from optparse import OptionParser
from streaming.msg.mcast import stream
from dateutil.parser import parse

token = '48042022074230dd51070716d3622a8c-19b1c2461152dab2e9a61007eab477f9'


class oad_stream(stream):

    def __init__(self, verbose=False, interval=1, **kwargs):
        stream.__init__(self, **kwargs)

        # self.domain = 'fxpractice.oanda.com'
        self.domain = 'sandbox.oanda.com'
        self.access_token = token
        self.account_id = '4960345'
        self.verbose = verbose

        self.universe = self.load_univ()

    def load_univ(self):
        univ = list(self.load_univ_details().instrument)
        return [x.encode('ascii', 'ignore') for x in univ
                if 'XXX' not in x and re.match('^\w\w\w_\w\w\w$', x)
                and not re.match('BCO|XPD|XPT', x)]

    def load_univ_details(self):

        s, resp = self.connect(service="/v1/instruments", streaming=False)
        df = pd.DataFrame(resp.json()['instruments'])
        df = df.convert_objects(convert_numeric=True)

        return df

    def connect(self, service, streaming=False, instruments=None,
                close_session=False):
        """

        Environment           <Domain>
        fxTrade               stream-fxtrade.oanda.com
        fxTrade Practice      stream-fxpractice.oanda.com
        sandbox               stream-sandbox.oanda.com
        """
        # instruments = string.join(self.universe,sep=',')
        try:
            s = requests.Session()
            params = {'accountId': self.account_id}
            if instruments is not None:
                params['instruments'] = ','.join(instruments)

            if 'sandbox' in self.domain:
                protocol = 'http://'
                headers = None
            else:
                protocol = 'https://'
                headers = {'Authorization': 'Bearer ' + self.access_token,
                           # 'X-Accept-Datetime-Format' : 'unix'
                           }

            prefix = 'stream' if streaming else 'api'
            url = protocol + prefix + '-' + self.domain + service

            req = requests.Request('GET', url=url,
                                   headers=headers, params=params)
            pre = req.prepare()
            resp = s.send(pre, stream=streaming, verify=False)
        except Exception as e:
            s.close()
            print "Caught exception when connecting to stream\n" + str(e)

        if not streaming:
            s.close()
        return s, resp

    def subscribe(self):

        # unlimited json stream from oanda
        s, resp = self.connect(service="/v1/prices", instruments=self.universe,
                               streaming=True)
        if resp.status_code != 200:
            print resp.text
            raise SystemExit

        for line in resp.iter_lines(1):
            if self.signal == 'stop':
                break
            elif self.signal == 'pause':
                continue
            if line:
                try:
                    msg = json.loads(line)
                except KeyboardInterrupt:
                    print "Ctrl+C pressed. Stopping..."
                except Exception as e:
                    print ('Caught exception when converting '
                           'message into json\n' + str(e))
                    raise SystemExit
                if 'tick' in msg:
                    msg = msg['tick']
                if 'instrument' in msg:

                    dt = {}
                    dt['ticker'] = msg['instrument'].replace('_', '')
                    dt['timestamp'] = nanotime.now().timestamp()
                    dt['quote_time'] = mktime(
                        parse(msg['time']).utctimetuple())
                    dt['size'] = 0
                    dt['type_'] = 'QUOTE'
                    for subtype in ('ask', 'bid'):
                        dt['price'] = msg[subtype]
                        dt['subtype'] = subtype.upper()
                        dt['source'] = 'sandbox'
                        dt['asset'] = 'Curncy'
                        dt['dir'] = 0
                        dt['code'] = ''
                        print nanotime.now()
                        self.sending(dt, 'TICK')  # send
                        if self.verbose:
                            print msg


if __name__ == "__main__":

    usage = "usage: %prog [options] <up_port=MASTER> <down_port=TICK|REPORT>"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      default=False, help="showing streaming data")

    (options, args) = parser.parse_args()

    up_port = ['MASTER']
    down_port = ['TICK']

    if len(args) > 0:
        up_port = args[0].split('|')
    if len(args) > 1:
        down_port = args[1].split('|')
    if len(args) > 2:
        parser.error("incorrect number of arguments")

    oad = oad_stream(
        verbose=options.verbose, up_port=up_port, down_port=down_port)
    oad.prepare()
    thread.start_new_thread(oad.listening, ())  # separate thread for listening
    oad.subscribe()
