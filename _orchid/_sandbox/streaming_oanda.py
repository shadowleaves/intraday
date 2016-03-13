"""
Demonstrates streaming feature in OANDA open api

To execute, run the following command:

python streaming.py [options]

To show heartbeat, replace [options] by -b or --displayHeartBeat
"""

import pandas as pd
import string
import requests
import json

from datetime import datetime
from optparse import OptionParser

from msg.msgpk import stream

domain = 'stream-fxpractice.oanda.com'
access_token = '48042022074230dd51070716d3622a8c-19b1c2461152dab2e9a61007eab477f9'
account_id = '4960345'

def get_all_instruments():
    s = requests.Session()
    #url = "https://" + domain + "/v1/instruments"
    url = 'https://api-fxpractice.oanda.com/v1/instruments'
    headers = {'Authorization' : 'Bearer ' + access_token,
                   # 'X-Accept-Datetime-Format' : 'unix'
                  }
    params = {'accountId' : account_id}
    req = requests.Request('GET', url, headers = headers, params = params)
    pre = req.prepare()
    resp = s.send(pre, stream = False, verify = False)
    s.close()
    
    df = pd.DataFrame(resp.json()['instruments'])
    df = df.convert_objects(convert_numeric = True)

    return df
    
def connect_to_stream(instruments):
    """

    Environment           <Domain>
    fxTrade               stream-fxtrade.oanda.com
    fxTrade Practice      stream-fxpractice.oanda.com
    sandbox               stream-sandbox.oanda.com
    """

    try:
        s = requests.Session()
        url = "https://" + domain + "/v1/prices"
        headers = {'Authorization' : 'Bearer ' + access_token,
                   # 'X-Accept-Datetime-Format' : 'unix'
                  }
        params = {'instruments' : instruments, 'accountId' : account_id}
        req = requests.Request('GET', url, headers = headers, params = params)
        pre = req.prepare()
        resp = s.send(pre, stream = True, verify = False)
        return resp
    except Exception as e:
        s.close()
        print "Caught exception when connecting to stream\n" + str(e) 


def streaming(ports, silence = True):
    
    df = get_all_instruments()
    instruments = string.join(df.instrument,sep=',')    

    #### calling custom stream class for sending outbound msgpacks
    outbound = stream(down_port = ports)  
    outbound.prepare()  ### establishing connection(s)
    
    #### unlimited json stream from oanda
    response = connect_to_stream(instruments)
    if response.status_code != 200:
        print response.text
        exit
    
    for line in response.iter_lines(1):
        if line:
            try:
                msg = json.loads(line)
            except KeyboardInterrupt:
                print "Ctrl+C pressed. Stopping..."
            except Exception as e:
                print "Caught exception when converting message into json\n" + str(e)
                exit
                
            if msg.has_key('tick'):
                msg = msg['tick']
            if msg.has_key("instrument"):
                msg['stamp'] = datetime.utcnow().isoformat()
                outbound.sending(msg)  ## send
                if not silence: print msg
            

if __name__ == "__main__":
    
    usage = "usage: %prog [options] [port1=8001] [port2=....] [port3=....] ..."
    parser = OptionParser(usage)
    parser.add_option("-s", "--silence", dest = "silence", action = "store_true", 
                        help = "hide streaming data")

    ports = [8001]
    silence = False
    
    (options, args) = parser.parse_args()
    if len(args) >= 1:
        ports = [int(x) for x in args]
    if options.silence:
        silence = True

    streaming(ports, silence = silence)
