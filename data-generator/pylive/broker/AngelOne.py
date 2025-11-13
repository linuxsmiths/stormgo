import os, sys, csv
import json
import time
import requests
import shutil
import pandas as pd
import config as cfg
from helpers import *

# Keep it after helpers import else that overrides datetime.
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

#
# /usr/local/lib/python3.10/dist-packages/SmartApi/smartConnect.py:SmartConnect
# class has static code in the class which connects to some API endpoint,
# hence this import sometimes takes time, hence the logs around this and other
# similar ones, to catch that.
#
PYPInfo("Before import SmartConnect")
from SmartApi import SmartConnect
PYPInfo("SmartConnect imported")

PYPInfo("Before import SmartWebSocket")
from SmartApi import SmartWebSocket
PYPInfo("SmartWebSocket imported")

#
# Need to copy SmartApi folder from
# https://github.com/angel-one/smartapi-python/tree/AMX-3393-Smart-Api-Python-Library/SmartApi
# to /usr/local/lib/python3.10/dist-packages/, followed by some bug fixes in
# smartWebSocketV2.py.
#
PYPInfo("Before import SmartWebSocketV2")
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
PYPInfo("SmartWebSocketV2 imported")

import pyotp
import multiprocessing

#
# API doc @ https://github.com/angel-one/smartapi-python
# https://smartapi.angelbroking.com/docs/Orders#place
#

instruments_file = os.path.join(cfg.pylivedir, "angelone_instruments.json")

#
# We keep a copy of the angelone_instruments.json checked in to our repo
# just in case AngelOne download API starts returning empty file on a day
# (I have seen it happen). In such case, it's better to use the (potentially)
# stale instruments_file.
# We try to update the ref file in our repo every day after after downloading
# that way the ref file is guaranteed to be uptodate.
#
instruments_file_ref = os.path.join(cfg.pyhistorical_dir, "angelone_instruments.json.ref")
symbol_to_instrument_token = None
instrument_token_to_symbol = None
api_key = None
refreshToken = None
authToken = None
feedToken = None
userProfile = None
loginTimestamp = None
rmsLimit = None
allHolding = None
allPosition = None
gttList = None
lastKnownOrderbook = None
clientCode = None
sub_token = None
token_list = None
ss = None
ss2 = None
obj = None
tick_cb = None
initialized = None
# Are we connected to websocket and getting live streams?
websocket_running = False
stop_websocket_called = False

stocks = get_stocks_list()

#
# Some internal server error returns we've seen.
#
# {
#     "message": "Internal Error",
#     "errorcode": "AB2001",
#     "status": false,
#     "data": null
# }
#
# {
#     "message": "Something Went Wrong, Please Try After Sometime",
#     "errorcode": "AB1004",
#     "status": false,
#     "data": null
# }
#
#
def _SAFEAPI(func, *args):
    ''' Use this for calling AngelOne API functions to protect them from occassional
        'Access denied because of exceeding access rate' errors.

        Returns None on failure and appropriate json object on success.

        Note: We also retry on failed status too. The reason is that mostly we
              won't be making failing API calls, so if it fails it's likely due to
              some internal server error and retrying may help. In this case we
              retry with more wait to help the server.
    '''
    resp = None
    for i in range(1, 10):
        try:
            if len(args) == 0:
                resp = func()
            elif len(args) == 1:
                resp = func(args[0])
            elif len(args) == 2:
                resp = func(args[0], args[1])
            elif len(args) == 3:
                resp = func(args[0], args[1], args[2])
            else:
                ASSERT(False, "Cannot call %s with %d arguments" % (func, len(args)))

            if resp is not None:
                if resp['status'] != True:
                    PYPWarn("API (%s) returned failed status: %s" %
                            (func, json.dumps(resp, indent=4)))
                    # Additional wait for such "internal server" errors.
                    time.sleep(1.0)
                    raise Exception("resp['status'] is not true!")
            else:
                #
                # None return from the API call is mostly deliberate failure
                # and we shouldn't retry in that case.
                #
                PYPWarn("API (%s) returned None" % (func))

            return resp
        except Exception as e:
            PYPWarn("API (%s) failed with exception: %s" % (func, e))
            #
            # Most likely cause for failure would be "too frequent API calls",
            # so wait before retrying. Minimum allowed Req/sec is 1 as per
            # https://smartapi.angelbroking.com/docs/RateLimit, so we might
            # need to sleep for max 1sec, which means we might have more than
            # 1 iteration of this loop.
            #
            time.sleep(0.5)
    PYPError("API (%s) failed even after 10 retries!" % (func))
    return None

def _refresh_instruments():
    ''' Download instruments for AngelOne from their standard url.
    '''
    if cfg.skip_load_instruments_json:
        # copy ref file to the instruments_file.
        dest = shutil.copyfile(instruments_file_ref, instruments_file)
        PYPWarn("*** Skipped downloading AngelOne instruments file, using ref file %s -> %s" %
                (instruments_file_ref, dest))
        return

    # Don't download instruments_file if it's already recent enough.
    if os.path.exists(instruments_file):
       stat_buf = os.stat(instruments_file)
       instruments_file_mtime = pd.Timestamp(stat_buf.st_mtime,
                                             unit='s',
                                             tz='Asia/Kolkata').tz_localize(None)
       now = pd.Timestamp.now()
       td = now - instruments_file_mtime
       if td.days == 0:
           PYPWarn("Skipping download of %s as it is less than 1 day old" %
                   (instruments_file))
           return

    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

    PYPInfo("[AngelOne] Downloading instruments from %s" % url)

    #
    # TODO: For resilience we need to handle the case where the download fails
    #       We will have to handle the exception below and then decide whether
    #       its safer to bail out than use an older instruments.json file.
    #
    start_get = pd.Timestamp.now().timestamp()
    data = requests.get(url).json()
    end_get = pd.Timestamp.now().timestamp()

    start_dump = pd.Timestamp.now().timestamp()
    with open(instruments_file, "w+") as f:
        json.dump(data, f, indent = 2)
    end_dump = pd.Timestamp.now().timestamp()

    #
    # Verify that downloaded file is not empty, if so use the ref file.
    #
    stat_buf = os.stat(instruments_file)
    if stat_buf.st_size > 0:
        PYPPass("[AngelOne] Successfully downloaded AngelOne instruments data into %s "
                "[Time taken: download=%.2f secs, dump=%.2f secs]" %
                (instruments_file, (end_get - start_get), (end_dump - start_dump)))
    else:
        # copy ref file to the instruments_file.
        dest = shutil.copyfile(instruments_file_ref, instruments_file)
        PYPWarn("[AngelOne] Downloaded AngelOne instruments file is empty, using ref file %s -> %s" %
                (instruments_file_ref, dest))


def _populate_symbol_to_instrument_token_map():
    ''' Populate the symbol to instrument token using the data obtained from
        _refresh_instruments() to get AngelOne specific tokens for every symbol.
        This also populates the reverse map for token->symbol lookup.
    '''
    global symbol_to_instrument_token
    global instrument_token_to_symbol

    # Must be called only once.
    ASSERT(symbol_to_instrument_token is None)
    symbol_to_instrument_token = {}

    ASSERT(instrument_token_to_symbol is None)
    instrument_token_to_symbol = {}

    #
    # One row in AngelOne's instrument data looks like this-
    # {
    #   "token":"3045",
    #   "symbol":"SBIN-EQ",
    #   "name":"SBIN",
    #   "expiry":"",
    #   "strike":"-1.000000",
    #   "lotsize":"1",
    #   "instrumenttype":"",
    #   "exch_seg":"NSE",
    #   "tick_size":"5.000000"
    # }
    # AngelOne identifies NIFTY symbols with an additional "-EQ", hence adding here.
    #
    # For non-equity symbols, we don't need to add "-EQ" since they are
    # not looked up from AngelOne, and instead they are looked up from
    # Yahoo Finance.
    #
    #angelone_symbols = [(x + "-EQ") for x in stocks]
    with open(instruments_file) as f:
        angelone_instruments_data = json.load(f)

    #
    # Let's add all equity symbols not just ones present in 'stocks' list.
    # This shouldn't be needed as we won't generate orders for symbols outside
    # 'stocks' but it helps in case we place manual orders.
    #
    for row in angelone_instruments_data:
        #if row["symbol"] in angelone_symbols:
        if row["symbol"].endswith("-EQ"):
            #
            # Removing the "-EQ" which was added earlier to obtain standard stock symbol.
            # Some symbols for which historical data is not downloaded from angelone, but
            # from Yahoo Finance (or elsewhere), they don't have "-EQ" in the end, use
            # them as-is.
            # Examples are YF.NIFTY50/YF.NIFTYBANK/YF.NIFTY100/YF.NIFTYNEXT50/YF.RELIANCE.
            # They have a "YF." prefix to highlight that they are downloaded from Yahoo
            # Finance.
            #
            symbol = row["symbol"][:-3] if row["symbol"].endswith("-EQ") else row["symbol"]
            symbol_to_instrument_token[symbol] = row["token"]
            instrument_token_to_symbol[row["token"]] = symbol

    PYPPass("Successfully populated %d AngleOne symbol to instrument tokens." %
            (len(symbol_to_instrument_token)))
    PYPInfo(symbol_to_instrument_token)
    return

def is_initialized():
    ''' Helper method to verify that broker has been initialized.
        Users may call this to check before calling a broker method.
    '''
    return initialized

def is_valid_symbol(symbol):
    ''' Test if symbol is a valid symbol recognized by AngleOne.
    '''
    return symbol in symbol_to_instrument_token

def is_valid_token(token):
    ''' Test if token is a valid AngleOne token corresponding to some symbol.
    '''
    return token in instrument_token_to_symbol

def token_to_symbol(token):
    ''' Return symbol corresponding to the broker specific token id.
    '''
    return instrument_token_to_symbol[token]

def symbol_to_token(symbol):
    ''' Return broker specific token corresponding to the symbol.
    '''
    return symbol_to_instrument_token[symbol]

def tick2tick(tick):
    ''' Convert broker specific tick to our broker-agnostic tick object.
        Common code works on the broker-agnostic tick object.
    '''
    #
    # XXX Add generic tick class and perform the conversion here.
    #     Till then the caller is not broker agnostic.
    #
    return tick

def register_tickcb(cb):
    ''' Register callback method to be called on each tick.
        As Websocket receives tick from broker, this will be called for
        each tick received.
    '''
    global tick_cb

    # Must be called once.
    ASSERT(tick_cb is None)

    # cb must be a callable.
    ASSERT(cb is not None)
    ASSERT(callable(cb))

    tick_cb = cb

def login():
    ''' Perform login to AngelOne account for performing API calls.
    '''
    # We must be here only if broker selection is angelone.
    ASSERT(cfg.broker['selection'] == 'angelone')

    # AngelOne specific account details.
    account = cfg.broker['angelone']['account']

    global api_key
    api_key = account['api_key']

    global obj
    obj = SmartConnect(api_key=api_key)

    while True:
        totp = pyotp.TOTP(account['token'])
        time_remaining = totp.interval - (datetime.now().timestamp() % totp.interval)
        otp = totp.now()

        try:
            PYPInfo("AngelOne login, using user_name=%s, password=%s, api_key=%s, OTP=%s "
                    "time before OTP expires = %.3f seconds" %
                    (account['user_name'], account['password'], api_key, otp, time_remaining))

            data = obj.generateSession(account['user_name'], account['password'], otp)
            #
            # Once I've seen this return None and not throw an exception.
            #
            # refreshToken = data['data']['refreshToken']
            # TypeError: 'NoneType' object is not subscriptable
            #
            # Not clear whether data is none or data['data'] is None.
            #
            # Q: Can this possibly happen when we were not able to make a successful
            #    POST request to the login endpoint
            #    /rest/auth/angelbroking/user/v1/loginByPassword ?
            # A: No, in that case generateSession() should throw an exception.
            #
            # Update:
            # Now I know that when login fails, data is not None but data['data'] is None.
            #
            # In case of failed login, obj.generateSession() returns following
            # login response json:
            # {
            #   'status': False,
            #   'message': 'Invalid totp',
            #   'errorcode': 'AB1050',
            #   'data': None
            # }
            #
            # When getProfile() fails due to an unknown error then
            # generateSession() returns None and we deliberately fail the following
            # ASSERT.
            #
            ASSERT(data is not None)

            #
            # If loginResponse['status'] is not true that indicates login
            # failure.
            #
            if data['status'] != True:
                PYPError("login failed, login response: %s" % json.dumps(data, indent=4))
                raise Exception("loginResponse['status'] is not true, login failed!")

            # In success case, data['data'] must be a valid json.
            ASSERT(data['data'] is not None, ("data is %s" % json.dumps(data, indent=4)))
            break;
        except Exception as e:
            PYPError("Failed generating session with exception: %s" % (e))
            time.sleep(1.5)

    PYPInfo("AngelOne login successful for user %s!" % (account['user_name']))

    #
    # In case of successful login, obj.generateSession() above returns the
    # following user profile json.
    #
    # {
    #   "status":true,
    #   "message":"SUCCESS",
    #   "errorcode":"",
    #   "data":{
    #           "clientcode":"YOUR_CLIENT_CODE",
    #           "name":"YOUR_NAME",
    #           "email":"",
    #           "mobileno":"",
    #           "exchanges":"['bse_cm', 'nse_cm']",
    #           "products":"['BO', 'NRML', 'CO', 'CNC', 'MIS', 'MARGIN']",
    #           "lastlogintime":"",
    #           "broker":"",    <-- doc says brokerid but we get broker
    #           "jwtToken":..., <-- these token attributes are not returned in user
    #                               profile response but generateSession() additionally
    #                               packs them from login response
    #           "refreshToken":...,
    #           "feedToken":...
    #   }
    # }
    PYPInfo("Returned user data: %s" % (json.dumps(data, indent=4)))

    global loginTimestamp
    global refreshToken
    global authToken
    global feedToken
    global userProfile
    global clientCode
    global rmsLimit
    global allHolding
    global allPosition
    global gttList

    #
    # Note the login time.
    #
    loginTimestamp = pd.Timestamp.now()

    #
    # Make sure clientCode matches ours.
    # If this ASSERT fails this got to be a bug in AngelOne API.
    #
    clientCode = account['user_name']
    ASSERT(data['data']['clientcode'] == clientCode,
           ("[AngelOne BUG] clientcode must be %s, got %s" %
            (clientCode, data['data']['clientcode'])))

    # fetch refreshToken.
    refreshToken = data['data']['refreshToken']
    PYPInfo("Got refreshToken = %s" % refreshToken)

    # fetch authToken.
    authToken = data['data']['jwtToken']
    PYPInfo("Got authToken = %s" % authToken)

    # fetch the feedtoken.
    feedToken = obj.getfeedToken()
    PYPInfo("Got feedToken = %s" % feedToken)

    # fetch User Profile.
    userProfile = _SAFEAPI(obj.getProfile, refreshToken)
    if userProfile is not None:
        PYPInfo("Got userProfile = %s" % json.dumps(userProfile, indent=4))
        ASSERT(userProfile['status'] == True)
        # Add timestamp for reference (in UTC).
        # SUTCWTALT.
        userProfile['timestamp'] = pd.Timestamp.now().timestamp()

    #
    # If we cannot get userProfile then there's something wrong, don't
    # proceed.
    #
    ASSERT(userProfile is not None)

    # fetch funds info.
    rmsLimit = _SAFEAPI(obj.rmsLimit)
    if rmsLimit is not None:
        PYPInfo("Got rmsLimit = %s" % json.dumps(rmsLimit, indent=4))
        ASSERT(rmsLimit['status'] == True)
        # Add timestamp for reference (in UTC).
        # SUTCWTALT.
        rmsLimit['timestamp'] = pd.Timestamp.now().timestamp()

    # fetch holdings info.
    allHolding = _SAFEAPI(obj.allholding)
    if allHolding is not None:
        PYPInfo("Got allholding = %s" % json.dumps(allHolding, indent=4))
        ASSERT(allHolding['status'] == True)
        # Add timestamp for reference (in UTC).
        # SUTCWTALT.
        allHolding['timestamp'] = pd.Timestamp.now().timestamp()

    # fetch positions info.
    allPosition = _SAFEAPI(obj.position)
    if allPosition is not None:
        PYPInfo("Got position = %s" % json.dumps(allPosition, indent=4))
        ASSERT(allPosition['status'] == True)
        # Add timestamp for reference (in UTC).
        # SUTCWTALT.
        allPosition['timestamp'] = pd.Timestamp.now().timestamp()

    #
    # fetch list of GTTs placed.
    # Ref https://smartapi.angelbroking.com/docs/Gtt#gttrulelist for request
    # and response type.
    #
    gttList = get_gtt_list()
    if gttList is not None:
        PYPInfo("Got gttList = %s" % json.dumps(gttList, indent=4))
        ASSERT(gttList['status'] == True)
        # Add timestamp for reference (in UTC).
        # SUTCWTALT.
        gttList['timestamp'] = pd.Timestamp.now().timestamp()

        # Test get_gtt_details() works fine.
        if gttList['status'] == True:
            PYPInfo("Has %d GTTs" % len(gttList['data']))
            for gtt in gttList['data']:
                ruleid = gtt['id']
                gtt_details = get_gtt_details(ruleid)
                PYPInfo("GTT details for ruleid %s: %s" %
                        (ruleid, json.dumps(gtt_details, indent=4)))

    #
    # Test get_order_status() works fine.
    # XXX To test it, set uniqueorderid to a valid value corresponding to an
    #     outstanding order, else it'll return "order not found".
    #
    if False:
        uniqueorderid = "f93b47d7-4ff0-48f1-9f22-63aa14dffa3b"
        order_status = get_order_status(uniqueorderid)
        if order_status is not None:
            PYPInfo("get_order_status(%s) = %s" %
                    (uniqueorderid, json.dumps(order_status, indent=4)))

    orderBook = get_order_book()
    if orderBook is not None:
        PYPInfo("get_order_book() = %s" % (json.dumps(orderBook, indent=4)))
        ASSERT(orderBook['status'] == True)

    tradeBook = get_trade_book()
    if tradeBook is not None:
        PYPInfo("get_trade_book() = %s" % (json.dumps(tradeBook, indent=4)))
        ASSERT(tradeBook['status'] == True)

def on_open1(ws):
    PYPInfo("websocket1:on_open")
    task="mw"   # mw|sfi|dp
    PYPInfo("Subscribing token list: %s" % sub_token)
    ss.subscribe(task, sub_token)

def on_message1(ws, message):
    PYPInfo("websocket1:Ticks: {}".format(message))

def on_error1(ws, error):
    PYPError(error)

def on_close1(ws):
    PYPWarn("Close")

def ws_init():
    if not stocks:
        PYPError("No stocks to subscribe!")
        #
        # Why would we want to run w/o any stocks to subscribe,
        # catch unintended bad usage.
        #
        ASSERT(False)
        return

    global sub_token
    sub_token = ("nse_cm|%s" % symbol_to_instrument_token[stocks[0]])

    # Concat rest of the stocks with an ampersand.
    for stock in stocks[1:]:
        token = symbol_to_instrument_token[stock]
        sub_token = ("%s&nse_cm|%s" % (sub_token, token))

    global ss
    ss = SmartWebSocket(feedToken, clientCode)

    # Assign the callbacks.
    ss._on_open = on_open1
    ss._on_message = on_message1
    ss._on_error = on_error1
    ss._on_close = on_close1

    #
    # TODO: ss.connect() is a blocking call that waits till websocket is
    #       connected. Need to put this in another process.
    #
    PYPInfo("Before connect")
    ss.connect()
    PYPInfo("After connect")

def on_open2(ws2):
    # Details @ https://smartapi.angelbroking.com/docs/WebSocket2
    PYPInfo("websocket2:on_open")
    PYPInfo("Subscribing token list: %s" % token_list)

    #
    # 1 (LTP)
    # 2 (Quote)
    # 3 (Snap Quote)
    #
    # TODO: Snap Quote is bigger than Quote so I suspect that may cause
    #       websocket tick drops (it's just an hypothesis at this time).
    #       For now we don't need bid/ask depth data. Later if we need it make
    #       sure the ticks are not dropped o/w we might have to create
    #       multiple websockets. Currently multiple websockets are not
    #       working.
    #
    mode = 2

    #
    # A 10 character alphanumeric ID client may provide which will be
    # returned by the server in error response to indicate which request
    # generated error response.  Clients can use this optional ID for
    # tracking purposes between request and corresponding error response.
    #
    correlation_id = "tomar ws2"
    ss2.subscribe(correlation_id, mode, token_list)
    PYPInfo("Subscribe done!")

def on_data2(wsapp, message):
    #
    # This logs the entire tick data.
    #
    #pretty_formatted_msg = json.dumps(message, indent=2)
    #print("websocket2:Ticks (%s): %s" %
    #                (instrument_token_to_symbol[message["token"]], pretty_formatted_msg))

    #
    # This is tied to CandleGenerator.on_tick()
    #
    ASSERT(tick_cb is not None)
    ASSERT(callable(tick_cb))

    tick_cb(message)

def on_error2(ws, error):
    PYPError("websocket2:on_error2")
    PYPError(error)

def on_close2(ws):
    PYPWarn("websocket2:on_close2")
    PYPWarn("Close")

def ws2_init():
    if not stocks:
        PYPError("No stocks to subscribe!")
        #
        # Why would we want to run w/o any stocks to subscribe,
        # catch unintended bad usage.
        #
        ASSERT(False)
        return

    global token_list
    #
    # From https://smartapi.angelbroking.com/docs/WebSocket2
    # exchangeType has following possible values
    #
    # 1 (nse_cm)
    # 2 (nse_fo)
    # 3 (bse_cm)
    # 4 (bse_fo)
    # 5 (mcx_fo)
    # 7 (ncx_fo)
    # 13 (cde_fo)
    #
    token_list = [{"exchangeType": 1, "tokens": []}]

    for stock in stocks:
        token = symbol_to_instrument_token[stock]
        token_list[0]["tokens"] += [token]

    global ss2
    # login() must have been called.
    ASSERT(clientCode is not None)
    ASSERT(feedToken is not None)
    ss2 = SmartWebSocketV2(authToken, api_key, clientCode, feedToken)

    # Assign the callbacks.
    ss2.on_open = on_open2
    ss2.on_data = on_data2
    ss2.on_error = on_error2
    ss2.on_close = on_close2

    #
    # We will not start getting ticks as yet.
    # To start getting ticks we have to call ss2.connect() for connecting the
    # websocket. See start_websocket().
    #

def get_historical():
    global obj
    # Sample code for calling historic api.
    try:
        historicParam = {
                "exchange": "NSE",
                "symboltoken": "3045",
                "interval": "ONE_MINUTE",
                "fromdate": "2023-07-10 09:15",
                "todate": "2023-07-10 15:30"
        }
        hd = _SAFEAPI(obj.getCandleData, historicParam)
        if hd is None:
            raise Exception("obj.getCandleData failed!")
        PYPInfo("hd = %s" % json.dumps(hd, indent=4))
    except Exception as e:
        PYPError("Historic Api failed: %s" % (e))

def init():
    global initialized
    # Must be called only once.
    ASSERT(initialized is None)

    PYPInfo("Starting init for AngelOne...")

    _refresh_instruments()
    _populate_symbol_to_instrument_token_map()
    login()

    # Initialise websocket2 for getting feed data for subscribed stocks.
    ws2_init()

    initialized = True
    PYPPass("AngelOne init done!")

def start_websocket(cb):
    ''' Call this function to start getting ticks.
        register_tickcb() MUST have been called before calling this.
    '''
    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    global tick_cb
    tick_cb = cb
    ASSERT(tick_cb is not None)
    ASSERT(callable(tick_cb))

    global ss2

    #
    # ss.connect() is a blocking call that waits till websocket is
    # connected. Caller should arrange to call this from a thread context
    # different from the main thread.
    #
    PYPWarn("Before connect")

    global websocket_running

    # MUST be called only once.
    ASSERT(websocket_running == False)
    websocket_running = True

    # stop_websocket_called must be False before starting the websocket.
    ASSERT(stop_websocket_called == False)

    ss2.connect()

    # It comes out when stop_websocket() calls close_connection().
    ASSERT(websocket_running)
    websocket_running = False

    #
    # XXX MUST NEVER COME HERE UNLESS stop_websocket() IS CALLED XXX
    # If websocket terminates for some reason, SmartWebSocketV2::_on_error()
    # callback will get called which will reconnect+resubscribe, so we should
    # only come here when stop_websocket() is called.
    #
    PYPWarn("After connect")
    ASSERT(stop_websocket_called == True)

def stop_websocket():
    ''' Call this function to close websocket created by start_websocket().
    '''
    # Must be called only once, when websocket is connected.
    ASSERT(websocket_running)

    global stop_websocket_called
    ASSERT(stop_websocket_called == False)

    stop_websocket_called = True

    PYPInfo("Before close_connection")
    ss2.close_connection()
    PYPInfo("After close_connection")

    #
    # XXX This has been seen to come here after ss2.connect() returns in
    #     start_websocket(). I've added the ASSERT to know if/when it
    #     doesn't honour that.
    #     Added an additional sleep to make sure ss2.connect() returns first.
    #
    time.sleep(1)
    ASSERT(websocket_running == False)

def get_profile():
    ''' Return a json object containing user profile details.
    '''
    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    #
    # Sample json object returned
    # {
    #     "timestamp": 1703851691.763173,
    #     "status": true,
    #     "message": "SUCCESS",
    #     "errorcode": "",
    #     "data": {
    #         "clientcode": "XXXXXXX",
    #         "name": "XXXXX XXXXX",
    #         "email": "",
    #         "mobileno": "",
    #         "exchanges": [
    #             "bse_cm",
    #             "nse_cm"
    #         ],
    #         "products": [
    #             "BO",
    #             "NRML",
    #             "CO",
    #             "CNC",
    #             "MIS",
    #             "MARGIN"
    #         ],
    #         "lastlogintime": "",
    #         "broker": ""
    #     }
    # }
    #
    ASSERT(userProfile['status'] == True)
    return userProfile

def get_funds():
    ''' Return a json object containing funds details, funds, p/l, etc.
    '''
    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    global rmsLimit

    #
    # rmsLimit() API has been seen to fail many times with the following
    # error.
    #
    # {
    #     "message": "Something Went Wrong, Please Try After Sometime",
    #     "errorcode": "AB1004",
    #     "status": false,
    #     "data": null
    # }
    #
    # Sometimes the retry helps but sometimes it exhausts all retries and
    # _SAFEAPI() returns failure.
    #
    rmsLimit = _SAFEAPI(obj.rmsLimit)
    if rmsLimit is None:
        return None

    #
    # Add timestamp for reference (in UTC).
    # We add it for error cases too so that caller can also note the time when
    # the bad value was returned.
    # SUTCWTALT.
    #
    rmsLimit['timestamp'] = pd.Timestamp.now().timestamp()

    if rmsLimit['status'] != True:
        PYPError("[get_funds] Got rmsLimit = %s" % json.dumps(rmsLimit, indent=4))
        ASSERT(False)

    #
    # Sample json object returned
    # {
    #     "timestamp": 1703851691.763173,
    #     "status": true,
    #     "message": "SUCCESS",
    #     "errorcode": "",
    #     "data": {
    #         "net": "19948.7700",
    #         "availablecash": "19948.7700",
    #         "availableintradaypayin": "0.0000",
    #         "availablelimitmargin": "0.0000",
    #         "collateral": "0.0000",
    #         "m2munrealized": "0.0000",
    #         "m2mrealized": "-3.0500",
    #         "utiliseddebits": "29.0700",
    #         "utilisedspan": null,
    #         "utilisedoptionpremium": null,
    #         "utilisedholdingsales": null,
    #         "utilisedexposure": null,
    #         "utilisedturnover": null,
    #         "utilisedpayout": "19948.7700"
    #     }
    # }
    #
    return rmsLimit

def get_holdings():
    ''' Return a json object containing holdings details.
    '''
    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    global allHolding

    allHolding = _SAFEAPI(obj.allholding)
    if allHolding is None:
        return None

    #
    # Add timestamp for reference (in UTC).
    # We add it for error cases too so that caller can also note the time when
    # the bad value was returned.
    # SUTCWTALT.
    #
    allHolding['timestamp'] = pd.Timestamp.now().timestamp()

    if allHolding['status'] != True:
        PYPError("[get_holdings] Got allHolding = %s" % json.dumps(allHolding, indent=4))
        ASSERT(False)

    #
    # Sample json object returned
    # {
    #      "timestamp": 1703851691.763173,
    #      "status": true,
    #      "message": "SUCCESS",
    #      "errorcode": "",
    #      "data": {
    #           "holdings": [
    #                {
    #                     "tradingsymbol": "TATASTEEL-EQ",
    #                     "exchange": "NSE",
    #                     "isin": "INE081A01020",
    #                     "t1quantity": 0,
    #                     "realisedquantity": 2,
    #                     "quantity": 2,
    #                     "authorisedquantity": 0,
    #                     "product": "DELIVERY",
    #                     "collateralquantity": null,
    #                     "collateraltype": null,
    #                     "haircut": 0,
    #                     "averageprice": 111.87,
    #                     "ltp": 130.15,
    #                     "symboltoken": "3499",
    #                     "close": 129.6,
    #                     "profitandloss": 37,
    #                     "pnlpercentage": 16.34
    #                },
    #                {
    #                     "tradingsymbol": "PARAGMILK-EQ",
    #                     "exchange": "NSE",
    #                     "isin": "INE883N01014",
    #                     "t1quantity": 0,
    #                     "realisedquantity": 2,
    #                     "quantity": 2,
    #                     "authorisedquantity": 0,
    #                     "product": "DELIVERY",
    #                     "collateralquantity": null,
    #                     "collateraltype": null,
    #                     "haircut": 0,
    #                     "averageprice": 154.03,
    #                     "ltp": 201,
    #                     "symboltoken": "17130",
    #                     "close": 192.1,
    #                     "profitandloss": 94,
    #                     "pnlpercentage": 30.49
    #                },
    #                {
    #                     "tradingsymbol": "SBIN-EQ",
    #                     "exchange": "NSE",
    #                     "isin": "INE062A01020",
    #                     "t1quantity": 0,
    #                     "realisedquantity": 8,
    #                     "quantity": 8,
    #                     "authorisedquantity": 0,
    #                     "product": "DELIVERY",
    #                     "collateralquantity": null,
    #                     "collateraltype": null,
    #                     "haircut": 0,
    #                     "averageprice": 573.1,
    #                     "ltp": 579.05,
    #                     "symboltoken": "3045",
    #                     "close": 570.5,
    #                     "profitandloss": 48,
    #                     "pnlpercentage": 1.04
    #                }
    #           ],
    #           "totalholding": {
    #                "totalholdingvalue": 5294,
    #                "totalinvvalue": 5116,
    #                "totalprofitandloss": 178.14,
    #                "totalpnlpercentage": 3.48
    #           }
    #      }
    # }
    #
    return allHolding

def get_positions():
    ''' Return a json object containing positions details.
    '''
    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    global allPosition

    allPosition = _SAFEAPI(obj.position)
    if allPosition is None:
        return None

    #
    # Add timestamp for reference (in UTC).
    # We add it for error cases too so that caller can also note the time when
    # the bad value was returned.
    # SUTCWTALT.
    #
    allPosition['timestamp'] = pd.Timestamp.now().timestamp()

    if allPosition['status'] != True:
        PYPError("[get_positions] Got allPosition = %s" % json.dumps(allPosition, indent=4))
        ASSERT(False)

    #
    # Sample json object returned
    # {
    #      "timestamp": 1703851691.763173,
    #      "status": true,
    #      "message": "SUCCESS",
    #      "errorcode": "",
    #      "data": [
    #           {
    #                "exchange": "NSE",
    #                "symboltoken": "2885",
    #                "producttype": "DELIVERY",
    #                "tradingsymbol": "RELIANCE-EQ",
    #                "symbolname": "RELIANCE",
    #                "instrumenttype": "",
    #                "priceden": "1",
    #                "pricenum": "1",
    #                "genden": "1",
    #                "gennum": "1",
    #                "precision": "2",
    #                "multiplier": "-1",
    #                "boardlotsize": "1",
    #                "buyqty": "1",
    #                "sellqty": "0",
    #                "buyamount": "2235.80",
    #                "sellamount": "0",
    #                "symbolgroup": "EQ",
    #                "strikeprice": "-1",
    #                "optiontype": "",
    #                "expirydate": "",
    #                "lotsize": "1",
    #                "cfbuyqty": "0",
    #                "cfsellqty": "0",
    #                "cfbuyamount": "0",
    #                "cfsellamount": "0",
    #                "buyavgprice": "2235.80",
    #                "sellavgprice": "0",
    #                "avgnetprice": "2235.80",
    #                "netvalue": "- 2235.80",
    #                "netqty": "1",
    #                "totalbuyvalue": "2235.80",
    #                "totalsellvalue": "0",
    #                "cfbuyavgprice": "0",
    #                "cfsellavgprice": "0",
    #                "totalbuyavgprice": "2235.80",
    #                "totalsellavgprice": "0",
    #                "netprice": "2235.80"
    #           }
    #      ]
    # }
    #
    return allPosition

def place_order(order_json):
    global obj

    PYPInfo("[AngelOne] place_order: %s" % json.dumps(order_json, indent=4))

    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    #
    # order_json is created by Order::dump_generated().
    # Perform sanity check.
    #
    ASSERT(order_json["producttype"] == "DELIVERY" or order_json["producttype"] == "BO")
    ASSERT(order_json["transactiontype"] == "BUY" or order_json["transactiontype"] == "SELL")
    ASSERT(order_json["ordertype"] == "MARKET" or order_json["ordertype"] == "LIMIT")
    ASSERT(order_json["quantity"] > 0)
    # For market orders price must be 0, for limit orders it must be non-zero.
    ASSERT((order_json["price"] > 0) == (order_json["ordertype"] == "LIMIT"))
    # Only bracket intraday orders have squardoff/stoploss price set.
    ASSERT((order_json["squareoff"] > 0) == (order_json["producttype"] == "BO"))
    # Either both or none of squareoff and stoploss must be set.
    ASSERT((order_json["squareoff"] > 0) == (order_json["stoploss"] > 0))

    #
    # engine doesn't generate SL/SL-M orders so we shouldn't have a trigger
    # price.
    # GTT orders do have triggerprice but they shouldn't come here, they
    # should go to create_gtt().
    #
    ASSERT(order_json["triggerprice"] == 0)

    # place order.
    stock = order_json["tradingsymbol"]
    try:
        token = symbol_to_instrument_token[stock]
    except Exception as e:
        PYPError("symbol_to_instrument_token[%s] failed: %s" % (stock, e))
        # We should never generate order for a stock which we don't know.
        ASSERT(False)
        return None

    squareoff = 0
    stoploss = 0
    # User should not set price for market orders, if set ignore it.
    price = float(order_json["price"] if (order_json["ordertype"] == "LIMIT") else 0)

    #
    # We don't generate market orders for intraday (only limit BO). Only delivery
    # orders can be market orders and infact delivery orders are only market
    # orders (see following if check).
    #
    ASSERT((order_json["ordertype"] != "MARKET") or
           (order_json["producttype"] == "DELIVERY"))

    if order_json["producttype"] == "DELIVERY":
        # We only support market delivery orders and not SL/SL-M orders.
        ASSERT(order_json["ordertype"] == "MARKET")

        #
        # Delivery orders.
        # XXX For margin delivery where we use the Margin Trading Facility
        #     (MTF) provided by AngelOne, we must set the producttype to
        #     "MARGIN". Currently we don't use that.
        #
        variety = "NORMAL"
        producttype = "DELIVERY"
    elif order_json["squareoff"] > 0:
        #
        # Intraday bracket orders, must have both squareoff (target price)
        # and stoploss set.
        #
        ASSERT(order_json["stoploss"] > 0)
        variety = "ROBO"
        producttype = "BO"
    else:
        #
        # Intraday market orders.
        # Engine executes these for squaring off our bracket orders before market
        # close to avoid auto-squareoff broker charges.
        #
        # XXX No auto-squareoff is done by cancelling the ROBO order.
        #     This is not used currently. Assert, till we have a valid usage.
        #
        variety = "NORMAL"
        producttype = "INTRADAY"
        ASSERT(False)

    #
    # INTRADAY orders are always placed as bracket orders and for bracket
    # orders we must have both squareoff and stoploss.
    # Also note that AngelOne expects squareoff and stoploss as a price
    # difference and not as an absolute price so engine must generate
    # likewise.
    #
    if order_json["producttype"] == "BO" and order_json["squareoff"] > 0:
        ASSERT(order_json["squareoff"] > 0)
        ASSERT(order_json["stoploss"] > 0)
        squareoff = float(order_json["squareoff"])
        stoploss = float(order_json["stoploss"])
        ASSERT(squareoff > 0)
        ASSERT(stoploss > 0)

        # If it's more than 10% then probably caller is passing squareoff and
        # stoploss as absolute values, catch that.
        ASSERT(((squareoff*100.0)/price) < 10.0)
        ASSERT(((stoploss*100.0)/price) < 10.0)

    try:
        #
        # API details @ https://smartapi.angelbroking.com/docs/Orders#place
        #
        # variety
        # ~~~~~~~
        # Currently we only place orders with following 'variety':
        # 1. NORMAL - These orders have just one leg (buy or sell) and
        #             we use these for placing delivery orders.
        # 2. ROBO   - These orders have two legs (first leg is buy or sell
        #             and then second leg is squareoff or stoploss).
        #             We use these for intraday orders as intraday orders
        #             generated by engine are bracket orders.
        #
        # Following 'variety' of orders are allowed by AngelOne but we don't
        # use them:
        # 3. STOPLOSS
        # 4. AMO
        #
        # XXX Do we need to consider duration=IOC (Immediate Or Cancel)?
        #
        orderparams = {
            "variety": variety,
            "tradingsymbol": "%s-EQ" % order_json["tradingsymbol"],
            "symboltoken": token,
            "transactiontype": order_json["transactiontype"],
            "exchange": "NSE",
            "ordertype": order_json["ordertype"],
            "producttype": producttype,
            "duration": "DAY",
            "price": price,
            "squareoff": squareoff,
            "stoploss": stoploss,
            "quantity": order_json["quantity"]
        }

        PYPPass("[AngelOne] Placing order %s" % (json.dumps(orderparams, indent=4)))

        orderResponse = _SAFEAPI(obj.placeOrderFullResponse, orderparams)
        if orderResponse is not None:
            if orderResponse['status'] == True:
                PYPPass("Successfully placed order: %s" % (json.dumps(orderResponse, indent=4)))
                orderid = orderResponse['data']['orderid']
                uniqueorderid = orderResponse['data']['uniqueorderid']
                return {"orderid": orderid, "uniqueorderid": uniqueorderid}
            else:
                PYPError("obj.placeOrderFullResponse() returned failure status: %s" %
                         (json.dumps(orderResponse, indent=4)))
                return None
        else:
            raise Exception("obj.placeOrderFullResponse returned None")
    except Exception as e:
        PYPError("Order placement failed: %s" % (e))
        return None

def modify_order(order_json):
    '''
    '''
    global obj

    PYPPass("[AngelOne] Modifying order %s" % (json.dumps(order_json, indent=4)))

    orderResponse = _SAFEAPI(obj.modifyOrder, order_json)
    if orderResponse is not None:
        if orderResponse['status'] == True:
            PYPPass("Successfully modified order: %s" % (json.dumps(orderResponse, indent=4)))
            orderid = orderResponse['data']['orderid']
            return {"orderid": orderid}
        else:
            PYPError("obj.modifyOrder() returned failure status: %s" %
                     (json.dumps(orderResponse, indent=4)))
            return None
    else:
        PYPError("obj.modifyOrder() returned None: %s" % (json.dumps(order_json, indent=4)))
        return None

def cancel_order(orderid, variety):
    '''
    '''
    global obj

    PYPPass("[AngelOne] Cancelling order {orderid=%s, variety=%s}" % (orderid, variety))

    orderResponse = _SAFEAPI(obj.cancelOrder, orderid, variety)
    if orderResponse is not None:
        if orderResponse['status'] == True:
            PYPPass("Successfully cancelled order: %s" % (json.dumps(orderResponse, indent=4)))
            orderid = orderResponse['data']['orderid']
            return {"orderid": orderid}
        else:
            PYPError("obj.cancelOrder() returned failure status: %s" %
                     (json.dumps(orderResponse, indent=4)))
            return None
    else:
        PYPError("obj.cancelOrder(orderid=%s, variety=%s) returned None" %
                 (orderid, variety))
        return None

def get_order_book():
    ''' Return order book, containing data about all our orders.
    '''
    global obj
    global lastKnownOrderbook

    orderBook = _SAFEAPI(obj.orderBook)
    if orderBook is None:
        #
        # Return last known order book if we have, it's better than having no
        # orderbook. This is good for resilience when we have to place some
        # order and orderBook API is failing for some reason.
        #
        if lastKnownOrderbook:
            PYPWarn("[get_order_book] obj.orderBook failed, returning lastKnownOrderbook\n:%s" %
                    json.dumps(lastKnownOrderbook, indent=4))
            return lastKnownOrderbook
        return None

    #
    # Add timestamp for reference (in UTC).
    # We add it for error cases too so that caller can also note the time when
    # the bad value was returned.
    # SUTCWTALT.
    #
    orderBook['timestamp'] = pd.Timestamp.now().timestamp()

    if orderBook['status'] != True:
        PYPError("[get_order_book] Got orderBook = %s" % json.dumps(orderBook, indent=4))
        ASSERT(False)

    #
    # Sample json object returned
    # {
    #     "timestamp": 1704605637.249141
    #     "status": true,
    #     "message": "SUCCESS",
    #     "errorcode": "",
    #     "data": [
    #         {
    #             "variety": "NORMAL",
    #             "ordertype": "LIMIT",
    #             "producttype": "INTRADAY",
    #             "duration": "DAY",
    #             "price": 10.55,
    #             "triggerprice": 0.0,
    #             "quantity": "1",
    #             "disclosedquantity": "0",
    #             "squareoff": 0.0,
    #             "stoploss": 0.0,
    #             "trailingstoploss": 0.0,
    #             "tradingsymbol": "IOC-EQ",
    #             "transactiontype": "BUY",
    #             "exchange": "NSE",
    #             "symboltoken": "1624",
    #             "ordertag": "",
    #             "instrumenttype": "",
    #             "strikeprice": -1.0,
    #             "optiontype": "",
    #             "expirydate": "",
    #             "lotsize": "1",
    #             "cancelsize": "0",
    #             "averageprice": 0.0,
    #             "filledshares": "0",
    #             "unfilledshares": "1",
    #             "orderid": "231229000008865",
    #             "text": "You are trying to place the order with a price which is outside circuit limits.Try placing order within the circut limits.",
    #             "status": "rejected",
    #             "orderstatus": "rejected",
    #             "updatetime": "29-Dec-2023 09:01:02",
    #             "exchtime": "",
    #             "exchorderupdatetime": "",
    #             "fillid": "",
    #             "filltime": "",
    #             "parentorderid": "",
    #             "uniqueorderid": "f93b47d7-4ff0-48f1-9f22-63aa14dffa3b"
    #         }
    #     ]
    # }
    #
    lastKnownOrderbook = orderBook
    return orderBook

def get_trade_book():
    ''' Return trade book, containing data about executed orders.
    '''
    global obj

    tradeBook = _SAFEAPI(obj.tradeBook)
    if tradeBook is None:
        return None

    #
    # Add timestamp for reference (in UTC).
    # We add it for error cases too so that caller can also note the time when
    # the bad value was returned.
    # SUTCWTALT.
    #
    tradeBook['timestamp'] = pd.Timestamp.now().timestamp()

    if tradeBook['status'] != True:
        PYPError("[get_trade_book] Got tradeBook = %s" % json.dumps(tradeBook, indent=4))
        ASSERT(False)

    #
    # Sample json object returned
    # {
    #     "timestamp": 1704605637.249141
    #     "status": true,
    #     "message": "SUCCESS",
    #     "errorcode": "",
    #     "data": [
    #         {
    #             "variety": "NORMAL",
    #             "ordertype": "LIMIT",
    #             "producttype": "INTRADAY",
    #             "duration": "DAY",
    #             "price": 10.55,
    #             "triggerprice": 0.0,
    #             "quantity": "1",
    #             "disclosedquantity": "0",
    #             "squareoff": 0.0,
    #             "stoploss": 0.0,
    #             "trailingstoploss": 0.0,
    #             "tradingsymbol": "IOC-EQ",
    #             "transactiontype": "BUY",
    #             "exchange": "NSE",
    #             "symboltoken": "1624",
    #             "ordertag": "",
    #             "instrumenttype": "",
    #             "strikeprice": -1.0,
    #             "optiontype": "",
    #             "expirydate": "",
    #             "lotsize": "1",
    #             "cancelsize": "0",
    #             "averageprice": 0.0,
    #             "filledshares": "0",
    #             "unfilledshares": "1",
    #             "orderid": "231229000008865",
    #             "text": "You are trying to place the order with a price which is outside circuit limits.Try placing order within the circut limits.",
    #             "status": "rejected",
    #             "orderstatus": "rejected",
    #             "updatetime": "29-Dec-2023 09:01:02",
    #             "exchtime": "",
    #             "exchorderupdatetime": "",
    #             "fillid": "",
    #             "filltime": "",
    #             "parentorderid": "",
    #             "uniqueorderid": "f93b47d7-4ff0-48f1-9f22-63aa14dffa3b"
    #         }
    #     ]
    # }
    #
    return tradeBook

#
# orderstatus seen:
#
# "orderstatus": "AMO SUBMITTED",
# "orderstatus": "rejected",
# "orderstatus": "trigger pending",
# "orderstatus": "open",
# "orderstatus": "complete",
# "orderstatus": "cancelled",
#
# From https://www.angelone.in/knowledge-center/online-share-trading/difference-between-order-book-and-trade-book
# The status of an order can be ‘requested’, ‘queued’, ‘ordered’, ‘executed’, ‘part executed’, ‘expired’, ‘canceled’ or ‘rejected’.
#
def get_order_status(uniqueorderid):
    ''' Return details about a specific order.
    '''
    global obj
    #
    # XXX Doc says we must pass uniqueorderid which is a GUID but
    #     place/modify/etc return an integer.
    #
    # Update: uniqueorderid is treated as a string so we can pass
    #         whatever is returned.
    #
    return _SAFEAPI(obj.individual_order_details, uniqueorderid)

def gttCreateRuleFullResponse(createRuleParams):
    ''' Like gttCreateRule() but returns the complete response.
        Just like placeOrderFullResponse() is to placeOrder().
        Since smartapi doesn't provide this, we implement the same using
        the exported postRequest() API.
    '''
    global obj

    params = createRuleParams
    for k in list(params.keys()):
        if params[k] is None:
            del(params[k])

    createGttRuleResponse = _SAFEAPI(obj.postRequest, "api.gtt.create", params)
    return createGttRuleResponse

def gttModifyRuleFullResponse(modifyRuleParams):
    global obj

    params = modifyRuleParams
    for k in list(params.keys()):
        if params[k] is None:
            del(params[k])

    modifyGttRuleResponse = _SAFEAPI(obj.postRequest, "api.gtt.modify", params)
    return modifyGttRuleResponse

def create_gtt(order_json):
    ''' Create a GTT order.
        Returns {"orderid": ruleid} on success and None on failure.
    '''
    global obj

    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    #
    # GTT orders have the special producttype GTT.
    # Note that this is not something that AngelOne APIs understand, they need
    # GTT APIs to have producttype set as DELIVERY, but the type GTT helps us
    # distinguish GTT from non-GTT delivery orders.
    #
    ASSERT(order_json["producttype"] == "GTT")
    # Can be buy or sell.
    ASSERT(order_json["transactiontype"] == "BUY" or order_json["transactiontype"] == "SELL")
    # triggerprice must be valid.
    ASSERT(order_json["triggerprice"] > 0)
    # price must be valid.
    ASSERT(order_json["price"] > 0)
    # quantity must be valid.
    ASSERT(order_json["quantity"] > 0)
    #
    # Must be limit order.
    # Techically a GTT can be a market order so it gets executed as soon as
    # the triggerprice is breached, but we set a limit price to be honored
    # after triggerprice breach. Catch unintended wrong usage.
    # XXX Remove this if we want to place market GTT orders.
    #
    ASSERT(order_json["ordertype"] == "LIMIT")

    # place order.
    stock = order_json["tradingsymbol"]
    try:
        token = symbol_to_instrument_token[stock]
    except Exception as e:
        PYPError("symbol_to_instrument_token[%s] failed: %s" % (stock, e))
        # We should never generate order for a stock which we don't know.
        ASSERT(False)
        return None

    # gtt rule creation
    try:
        gttCreateParams = {
            "tradingsymbol": "%s-EQ" % order_json["tradingsymbol"],
            "symboltoken": token,
            "exchange" : "NSE",
            "producttype" : "DELIVERY",
            "transactiontype": order_json["transactiontype"],
            "price": order_json["price"],
            "triggerprice" : order_json["triggerprice"],
            "qty": order_json["quantity"],
            "disclosedqty": order_json["quantity"],
            "timeperiod" : 365
        }

        PYPPass("[AngelOne] Placing GTT order %s" % (json.dumps(gttCreateParams, indent=4)))

        createGttRuleResponse = gttCreateRuleFullResponse(gttCreateParams)
        if createGttRuleResponse is None:
            raise Exception("gttCreateRuleFullResponse return None")

        if createGttRuleResponse['status'] == True:
            #
            # AngelOne returns GTT ruleid as an integer, but we treat it
            # similar to the orderid which is a string, so we convert ruleid
            # also to string.
            # Note that the API doc example shows ruleid returned as a string
            # but in reality it's returned as an integer, f.e,
            #
            # "id": 1857148
            #
            rule_id = str(createGttRuleResponse['data']['id'])
            PYPPass("Successfully placed GTT order with ruleid %s" % (rule_id))
            return {"orderid": rule_id}
        else:
            raise Exception("obj.gttCreateRule failed: %s" %
                (json.dumps(createGttRuleResponse, indent=4)))
    except Exception as e:
        PYPError("GTT Rule creation failed: %s" % (e))
        return None

def modify_gtt(order_json):
    ''' Modify a GTT order.
        Returns {"orderid": ruleid} on success and None on failure.
    '''
    global obj

    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    #
    # broker_orderid attribute conveys the ruleid of the GTT to be modified, it's a
    # MUST. We use broker_orderid for both the orderid used for orders and
    # ruleid used for GTTs and hence we use the common type str for both.
    #
    ASSERT('broker_orderid' in order_json)
    ASSERT(isinstance(order_json['broker_orderid'], str))
    ASSERT(len(order_json['broker_orderid']) > 0)

    # GTT orders must have special type of GTT. See create_gtt().
    ASSERT(order_json["producttype"] == "GTT")
    # Can be buy or sell.
    ASSERT(order_json["transactiontype"] == "BUY" or order_json["transactiontype"] == "SELL")
    # Must be limit order. See create_gtt().
    ASSERT(order_json["ordertype"] == "LIMIT")
    # triggerprice must be valid.
    ASSERT(order_json["triggerprice"] > 0)
    # price must be valid.
    ASSERT(order_json["price"] > 0)
    # quantity must be valid.
    ASSERT(order_json["quantity"] > 0)

    # place order.
    stock = order_json["tradingsymbol"]
    try:
        token = symbol_to_instrument_token[stock]
    except Exception as e:
        PYPError("symbol_to_instrument_token[%s] failed: %s" % (stock, e))
        # We should never generate order for a stock which we don't know.
        ASSERT(False)
        return None

    # gtt rule creation
    try:
        gttModifyParams = {
            "id": order_json["broker_orderid"],
            "symboltoken": token,
            "exchange" : "NSE",
            "price": order_json["price"],
            "qty": order_json["quantity"],
            "triggerprice" : order_json["triggerprice"],
            "disclosedqty": order_json["quantity"],
            "timeperiod" : 365
        }

        PYPPass("[AngelOne] Modifying GTT order %s" % (json.dumps(gttModifyParams, indent=4)))

        modifyGttRuleResponse = gttModifyRuleFullResponse(gttModifyParams)
        if modifyGttRuleResponse is None:
            raise Exception("gttModifyRuleFullResponse returned None")

        if modifyGttRuleResponse['status'] == True:
            rule_id = str(modifyGttRuleResponse['data']['id'])
            #
            # ruleid of old and modified GTT must be same, since the same GTT
            # is modified and a new one is not created.
            #
            PYPPass("Successfully modified GTT order with ruleid %s, new ruleid %s" %
                    (gttModifyParams['id'], rule_id))
            ASSERT(gttModifyParams['id'] == rule_id)
            return {"orderid": rule_id}
        else:
            raise Exception("obj.gttModifyRule failed: %s" %
                (json.dumps(modifyGttRuleResponse, indent=4)))
    except Exception as e:
        PYPError("GTT Rule modify failed: %s" % (e))
        return None

def cancel_gtt(order_json):
    ''' Cancel a GTT order.
        Returns {"orderid": ruleid} on success and None on failure.
    '''
    global obj

    # Must be called only after broker is initialized.
    ASSERT(is_initialized())

    #
    # broker_orderid attribute conveys the ruleid of the GTT to be cancelled, it's a
    # MUST.
    #
    ASSERT('broker_orderid' in order_json)
    ASSERT(isinstance(order_json['broker_orderid'], str))
    ASSERT(len(order_json['broker_orderid']) > 0)

    # place order.
    stock = order_json["tradingsymbol"]
    try:
        token = symbol_to_instrument_token[stock]
    except Exception as e:
        PYPError("symbol_to_instrument_token[%s] failed: %s" % (stock, e))
        # We should never generate order for a stock which we don't know.
        ASSERT(False)
        return None

    # gtt rule creation
    try:
        gttCancelParams = {
            "id": order_json["broker_orderid"],
            "symboltoken": token,
            "exchange" : "NSE",
        }

        PYPPass("[AngelOne] Cancelling GTT order %s" %
                (json.dumps(gttCancelParams, indent=4)))

        cancelGttRuleResponse = obj.gttCancelRule(gttCancelParams)
        if cancelGttRuleResponse is None:
            raise Exception("obj.gttCancelRule returned None")

        PYPWarn("cancelGttRuleResponse = %s" % (json.dumps(cancelGttRuleResponse, indent=4)))
        if cancelGttRuleResponse['status'] == True:
            # GTT ruleid is returned as an integer, unlike orderid.
            rule_id = str(cancelGttRuleResponse['data']['id'])

            #
            # Cancel response MUST return the ruleid of the to-be-cancelled GTT
            # order.
            #
            ASSERT(gttCancelParams['id'] == rule_id)
            PYPPass("Successfully cancelled GTT order with ruleid %s" %
                    (gttCancelParams['id']))
            return {"orderid": rule_id}
        else:
            raise Exception("obj.gttCancelRule failed: %s" %
                    (json.dumps(cancelGttRuleResponse, indent=4)))
    except Exception as e:
        PYPError("GTT Rule cancel failed: %s" % (e))
        return None

def get_gtt_details(ruleid):
    ''' Return details about a specific GTT rule.
    '''
    global obj
    return _SAFEAPI(obj.gttDetails, ruleid)

def get_gtt_list():
    ''' Return list of GTT orders placed.
    '''
    global obj
    status = [ "NEW", "CANCELLED", "ACTIVE", "SENTTOEXCHANGE", "FORALL" ]
    # XXX Understand signifiance of page and count.
    gtt_list = _SAFEAPI(obj.gttLists, status, 1, 10)
    if gtt_list is not None:
        # Add timestamp for reference (in UTC).
        # SUTCWTALT.
        gtt_list['timestamp'] = pd.Timestamp.now().timestamp()
    return gtt_list

def is_robo_main_order(order):
    ''' Is the given order a main ROBO order?
    '''
    return ((order['variety'] == "ROBO") and
            (order['ordertype'] == 'LIMIT') and
            (order['producttype'] == "BO") and
            (order['parentorderid'] == ""))

def is_robo_sub_order(order):
    ''' Does the given order json correspond to a ROBO order's target or stoploss
        sub-order?
    '''
    #
    # The stoploss sub-order will have ordertype STOPLOSS_LIMIT if the ROBO
    # order is not fully executed else it may have ordertype LIMIT.
    # See get_stoploss_suborder_for_robo() for details.
    #
    return ((order['variety'] == "ROBO") and
            ((order['ordertype'] == 'STOPLOSS_LIMIT') or (order['ordertype'] == 'LIMIT')) and
            (order['producttype'] == "BO") and
            (order['parentorderid'] != ""))

def is_gtt_rule(json):
    ''' Does the given json correspond to a GTT rule (and not an order)?

        Some functions operate on both GTT rule and order and they would
        want to act differently, so they can use this.
    '''
    is_gtt = ('clientid' in json)

    if is_gtt:
        ASSERT('id' in json)
        ASSERT('orderid' not in json)
        ASSERT('variety' not in json)
        ASSERT('orderstatus' not in json)
        ASSERT('uniqueorderid' not in json)
        ASSERT(json['producttype'] == "DELIVERY")
        ASSERT(json['triggerprice'] > 0)
        ASSERT((json['status'] == "NEW") or
               (json['status'] == "ACTIVE") or
               (json['status'] == "CANCELLED") or
               (json['status'] == "SENTTOEXCHANGE"))
    else:
        ASSERT('id' not in json)
        ASSERT('orderid' in json)
        ASSERT('variety' in json)
        ASSERT('orderstatus' in json)
        ASSERT('uniqueorderid' in json)

    return is_gtt

def get_stoploss_suborder_for_robo(parentorderid, orderbook):
    ''' For every ROBO order (bracket order) AngelOne creates two additonal
        sub-orders one for exiting with the target/profit and another for exiting
        at stoploss.
        Given the parentorderid this function returns the corresponding stoploss
        order.

        Here are the 3 sample orders from the order book.

        ==> Stoploss order.
            This has the parentorderid equal to the original orderid.
        {
            "variety": "ROBO",
            "ordertype": "STOPLOSS_LIMIT",
            "producttype": "BO",
            "duration": "DAY",
            "price": 134.9,
            "triggerprice": 131.95,
            "quantity": "1",
            "disclosedquantity": "1",
            "squareoff": 0.0,
            "stoploss": 0.0,
            "trailingstoploss": 0.0,
            "tradingsymbol": "IOC-EQ",
            "transactiontype": "BUY",
            "exchange": "NSE",
            "symboltoken": "1624",
            "ordertag": "",
            "instrumenttype": "",
            "strikeprice": -1.0,
            "optiontype": "",
            "expirydate": "",
            "lotsize": "1",
            "cancelsize": "0",
            "averageprice": 0.0,
            "filledshares": "0",
            "unfilledshares": "1",
            "orderid": "231229000075477",
            "text": "",
            "status": "trigger pending",
            "orderstatus": "trigger pending",
            "updatetime": "29-Dec-2023 09:21:33",
            "exchtime": "29-Dec-2023 09:21:33",
            "exchorderupdatetime": "29-Dec-2023 09:21:33",
            "fillid": "",
            "filltime": "",
            "parentorderid": "231229000075475",
            "uniqueorderid": "231229000075477"
        },

        ==> Target order
            This has the parentorderid equal to the original orderid.
        {
            "variety": "ROBO",
            "ordertype": "LIMIT",
            "producttype": "BO",
            "duration": "DAY",
            "price": 129.95,
            "triggerprice": 0.0,
            "quantity": "1",
            "disclosedquantity": "1",
            "squareoff": 0.0,
            "stoploss": 0.0,
            "trailingstoploss": 0.0,
            "tradingsymbol": "IOC-EQ",
            "transactiontype": "BUY",
            "exchange": "NSE",
            "symboltoken": "1624",
            "ordertag": "",
            "instrumenttype": "",
            "strikeprice": -1.0,
            "optiontype": "",
            "expirydate": "",
            "lotsize": "1",
            "cancelsize": "0",
            "averageprice": 0.0,
            "filledshares": "0",
            "unfilledshares": "1",
            "orderid": "231229000075476",
            "text": "",
            "status": "open",
            "orderstatus": "open",
            "updatetime": "29-Dec-2023 09:21:33",
            "exchtime": "29-Dec-2023 09:21:33",
            "exchorderupdatetime": "29-Dec-2023 09:21:33",
            "fillid": "",
            "filltime": "",
            "parentorderid": "231229000075475",
            "uniqueorderid": "231229000075476"
        },

        ==> Actual SELL order.
            This must be "complete" only then the other two orders will be created.
        {
            "variety": "ROBO",
            "ordertype": "LIMIT",
            "producttype": "BO",
            "duration": "DAY",
            "price": 130.5,
            "triggerprice": 0.0,
            "quantity": "1",
            "disclosedquantity": "0",
            "squareoff": 0.0,
            "stoploss": 0.0,
            "trailingstoploss": 0.0,
            "tradingsymbol": "IOC-EQ",
            "transactiontype": "SELL",
            "exchange": "NSE",
            "symboltoken": "1624",
            "ordertag": "",
            "instrumenttype": "",
            "strikeprice": -1.0,
            "optiontype": "",
            "expirydate": "",
            "lotsize": "1",
            "cancelsize": "0",
            "averageprice": 130.95,
            "filledshares": "1",
            "unfilledshares": "0",
            "orderid": "231229000075475",
            "text": "",
            "status": "complete",
            "orderstatus": "complete",
            "updatetime": "29-Dec-2023 09:21:33",
            "exchtime": "29-Dec-2023 09:21:33",
            "exchorderupdatetime": "29-Dec-2023 09:21:33",
            "fillid": "",
            "filltime": "",
            "parentorderid": "",
            "uniqueorderid": "49261896-2322-4977-9862-168a10ed6621"
        }
    '''
    main_order = get_main_order_for_robo(parentorderid, orderbook)
    ASSERT(main_order is not None)

    for order in orderbook['data']:
        ASSERT((order['transactiontype'] == "BUY") or
               (order['transactiontype'] == "SELL"))

        # MUST condition.
        if order['parentorderid'] != parentorderid:
            continue

        # parentorderid is used only to identify ROBO sub-orders.
        ASSERT((order['variety'] == "ROBO") and (order['producttype'] == "BO"))

        # Sub-order must have the opposite transactiontype from main order.
        ASSERT(order['transactiontype'] != main_order['transactiontype'])

        # Sub-order price cannot be same as main order.
        ASSERT(float(order['price']) != float(main_order['price']))

        #
        # "if" is the case where the stoploss sub-order has not triggered and
        # hence the ordertype is STOPLOSS_LIMIT, while "elif" is the case
        # where the stoploss sub-order has triggered and hence the ordertype
        # has become LIMIT. In the latter case we need to distinguish stoploss
        # from target sub-order since both now have ordertype as LIMIT. We do
        # that by checking the price. The stoploss sub-order will trade at a
        # loss while target sub-order will trade at a profit.
        #
        if order['ordertype'] == 'STOPLOSS_LIMIT':
            if order['transactiontype'] == "SELL":
                ASSERT(float(order['price']) < float(main_order['price']))
            else:
                ASSERT(float(order['price']) > float(main_order['price']))
            return order
        elif order['ordertype'] == 'LIMIT':
            # Can be stoploss or target sub-order.
            if order['transactiontype'] == "SELL":
                # Stoploss sub-order will sell at a lower price than the main order.
                if float(order['price']) < float(main_order['price']):
                    return order
            else:
                # Stoploss sub-order will buy at a higher price than the main order.
                if float(order['price']) > float(main_order['price']):
                    return order
        else:
            # Cannot be anything else.
            ASSERT(False)

    return None

def get_target_suborder_for_robo(parentorderid, orderbook):
    ''' Given the parentorderid this function returns the target order.
        See get_stoploss_suborder_for_robo() for details.
    '''
    main_order = get_main_order_for_robo(parentorderid, orderbook)
    ASSERT(main_order is not None)

    for order in orderbook['data']:
        ASSERT((order['transactiontype'] == "BUY") or
               (order['transactiontype'] == "SELL"))

        # MUST condition.
        if order['parentorderid'] != parentorderid:
            continue

        # parentorderid is used only to identify ROBO sub-orders.
        ASSERT((order['variety'] == "ROBO") and (order['producttype'] == "BO"))

        # Sub-order must have the opposite transactiontype from main order.
        ASSERT(order['transactiontype'] != main_order['transactiontype'])

        # Sub-order price cannot be same as main order.
        ASSERT(float(order['price']) != float(main_order['price']))

        # Can be either STOPLOSS_LIMIT or LIMIT.
        ASSERT((order['ordertype'] == 'LIMIT') or (order['ordertype'] == 'STOPLOSS_LIMIT'))

        #
        # If stoploss sub-order has triggered it will also have the ordertype
        # as LIMIT so we need to distinguish that from target sub-order.
        # A target sub-order will trade at a profit while stoploss sub-order
        # will trade at a loss.
        # See get_stoploss_suborder_for_robo() for details.
        #
        if order['ordertype'] == 'LIMIT':
            if order['transactiontype'] == "SELL":
                # Target sub-order will sell at a higher price than the main order.
                if float(order['price']) > float(main_order['price']):
                    return order
            else:
                # Target sub-order will buy at a lower price than the main order.
                if float(order['price']) < float(main_order['price']):
                    return order
    return None

def get_main_order_for_robo(parentorderid, orderbook):
    ''' Given the parentorderid this function returns the main ROBO order.
        See get_stoploss_suborder_for_robo() for details.
    '''
    for order in orderbook['data']:
        if ((order['variety'] == "ROBO") and
            (order['ordertype'] == 'LIMIT') and
            (order['producttype'] == "BO") and
            (order['orderid'] == parentorderid)):
            # Main order MUST have parentorderid as "".
            ASSERT(order['parentorderid'] == "")
            return order
    return None

def is_fully_completed_robo_order(broker_orderid, orderbook):
    ''' Is this a fully completed ROBO order?
        A fully completed ROBO order is one which has both entered and exited
        (target or stop-loss) the trade.
    '''
    main_order = get_main_order_for_robo(broker_orderid, orderbook)
    if main_order is None:
        PYPError("No ROBO order with broker_orderid %s" % (broker_orderid))
        return False

    #
    # A ROBO order can be fully completed only if the main order's orderstatus
    # is complete.
    #
    main_order_completed = (main_order['orderstatus'] == "complete")
    if not main_order_completed:
        PYPInfo("Main ROBO orderstatus is '%s'" % (main_order['orderstatus']))
        return False

    #
    # OK, ROBO order is partially completed for sure, see if it's fully
    # completed. A partially completed ROBO order will have the following:
    # 1. stoploss sub-order in "trigger pending" state.
    # 2. target sub-order in "open" state.
    #
    # while a fully completed ROBO order will have the following properties:
    # 1. Either the target or the stoploss sub-order must have completed and
    #    the other sub-order would be cancelled.
    # 2. If stoploss sub-order has completed, it can only happen when the
    #    stoploss price was triggered and the order was placed at the
    #    Exchange, which means the ordertype will no longer be STOPLOSS_LIMIT
    #    but instead it'll be LIMIT.
    # 3. If target sub-order has completed, the stoploss sub-order must be
    #    cancelled. In this case the ordertype can remain as STOPLOSS_LIMIT.
    #    XXX Verify that the ordertype does remain STOPLOSS_LIMIT.
    #
    sl_order = get_stoploss_suborder_for_robo(broker_orderid, orderbook)
    ASSERT(sl_order is not None)
    tgt_order = get_target_suborder_for_robo(broker_orderid, orderbook)
    ASSERT(tgt_order is not None)

    # target/stoploss sub-order has completed/cancelled?
    tgt_so_completed = (tgt_order['orderstatus'] == "complete")
    sl_so_completed = (sl_order['orderstatus'] == "complete")
    tgt_so_cancelled = (tgt_order['orderstatus'] == "cancelled")
    sl_so_cancelled = (sl_order['orderstatus'] == "cancelled")

    # Both cannot be completed.
    ASSERT(not (tgt_so_completed and sl_so_completed))

    #
    # Both cannot be cancelled.
    # Technically AngelOne could have decided to cancel both in case of
    # auto-squareoff, but in case of auto-squareoff when we cancel the main ROBO
    # order, I've seen that AngelOne will complete the stoploss sub-order.
    # It considers auto-squareoff as "selling at a loss since target wasn't hit".
    #
    # XXX If AngelOne changes behaviour, change this.
    #
    ASSERT(not (tgt_so_cancelled and sl_so_cancelled))

    return tgt_so_completed or tgt_so_cancelled

def is_partially_completed_robo_order(broker_orderid, orderbook):
    ''' Is this a partially completed ROBO order?
        A partially completed ROBO order is one which has completed the main
        order but the sub-orders (target and stop-loss) are still open.
    '''
    main_order = get_main_order_for_robo(broker_orderid, orderbook)
    if main_order is None:
        PYPError("No ROBO order with broker_orderid %s" % (broker_orderid))
        return False

    #
    # A ROBO order can be partially completed only if the main order's orderstatus
    # is complete.
    #
    main_order_completed = (main_order['orderstatus'] == "complete")
    if not main_order_completed:
        PYPInfo("Main ROBO orderstatus is '%s'" % (main_order['orderstatus']))
        return False

    #
    # OK, ROBO order is either partially or fully completed.
    # A partially completed ROBO order will have neither sub-orders completed
    # or cancelled.
    # See details in is_fully_completed_robo_order().
    #
    sl_order = get_stoploss_suborder_for_robo(broker_orderid, orderbook)
    ASSERT(sl_order is not None)
    tgt_order = get_target_suborder_for_robo(broker_orderid, orderbook)
    ASSERT(tgt_order is not None)

    # target/stoploss sub-order has completed/cancelled?
    tgt_so_completed = (tgt_order['orderstatus'] == "complete")
    sl_so_completed = (sl_order['orderstatus'] == "complete")
    tgt_so_cancelled = (tgt_order['orderstatus'] == "cancelled")
    sl_so_cancelled = (sl_order['orderstatus'] == "cancelled")

    # Both cannot be completed.
    ASSERT(not (tgt_so_completed and sl_so_completed))

    #
    # Both cannot be cancelled.
    # Technically AngelOne could have decided to cancel both in case of
    # auto-squareoff, but in case of auto-squareoff when we cancel the main ROBO
    # order, I've seen that AngelOne will complete the stoploss sub-order.
    # It considers auto-squareoff as "selling at a loss since target wasn't hit".
    #
    # XXX If AngelOne changes behaviour, change this.
    #
    ASSERT(not (tgt_so_cancelled and sl_so_cancelled))

    return (not tgt_so_completed) and (not tgt_so_cancelled)

def squareoff_robo_order(broker_orderid, orderbook):
    ''' Given the broker_orderid for an open ROBO order, squareoff the order.

        Unfortunately AngelOne doesn't provide a simple API to do this.
        We have to do all of the following:
        1. Cancel the stoploss sub-order created by AngelOne.
        2. Cancel the target sub-order created by AngelOne.
        3. Place a market order to squareoff whatever units we have.

        Update: We don't have to do all the above instead we just need to cancel
                the parent ROBO order (which has a status of "complete").
                Note that though the order is complete, we can still cancel it.
                Since it's a ROBO order, cancel has a special meaning that it
                closes the order (aka squareoff).

        Returns the tuple (cancelled, squaredoff) with two bools where the first
        bool is true if the order is cancelled and second bool is true if the
        order is squaredoff. If neither is true this means some error occurred.
        If both are true that has a special meaning that the ROBO order was already
        "fully complete" (fully complete means that the main ROBO order was completed
        and also either the target or stoploss sub-order was completed/cancelled
        and the other sub-order cancelled (because of OCO)). In such case we don't
        need to do anything.
    '''
    PYPInfo("squareoff_robo_order(%s)" % broker_orderid)

    # Legit orderbook with at least one order.
    ASSERT(orderbook['status'] == True)
    ASSERT(len(orderbook['data']) >= 1)

    #
    # Find the main ROBO order. We trust the caller that they will call us for
    # valid ROBO orders, hence we MUST see the main ROBO order in the orderbook,
    # else it's a AngelOne BUG.
    # If we do not find the main ROBO order, there's nothing we can do.
    #
    main_order = get_main_order_for_robo(broker_orderid, orderbook)
    if main_order is None:
        PYPWarn("[AngelOne BUG] Did not find main ROBO order for broker_orderid %s" %
                broker_orderid)
        # cancelled, squaredoff
        return False, False

    PYPInfo("Main order for ROBO order %s has orderstatus '%s'" %
            (broker_orderid, main_order['orderstatus']))

    main_order_completed = (main_order['orderstatus'] == "complete")

    #
    # If main order has not completed (aka not yet entered) then just cancel
    # the ROBO order and we are done. orderstatus will be "open" for ROBO orders
    # which have not yet completed. For such orders we won't have the stoploss
    # and target sub-orders created, cancelling the open ROBO order is all we need.
    #
    if not main_order_completed:
        PYPInfo("Cancelling not-yet-complete ROBO order (orderstatus=%s): "
                "orderid=%s variety=%s" %
                (main_order['orderstatus'], broker_orderid, "ROBO"))
        cancel_order_resp = cancel_order(broker_orderid, "ROBO")
        PYPInfo("Cancel not-yet-complete ROBO order response: %s" %
                json.dumps(cancel_order_resp, indent=4))
        # cancelled, squaredoff
        return True, False

    #
    # If main order has completed, it could mean two things:
    # 1. Main ROBO order was completed and the two sub-orders (stoploss and
    #    target) were created and one of them completed (either target or
    #    stoploss was hit) causing the other to be cancelled (One Cancels
    #    Other).
    #    We call this case "Fully Completed" to distinguish it from the other
    #    completed case where only the main order is completed and sub-orders
    #    are still open (see #2).
    # 2. Main ROBO order was completed and the two sub-orders (stoploss and
    #    target) were created but neither stoploss nor target were hit.
    #    This is our case of interest where we would want to squareoff.
    #
    if is_fully_completed_robo_order(broker_orderid, orderbook):
        PYPInfo("ROBO order %s **FULLY COMPLETED**, nothing to squareoff: %s" %
                (broker_orderid, json.dumps(main_order, indent=4)))
        #
        # cancelled, squaredoff
        # We use the special combination cancelled=True and squaredoff=True to
        # convey that the ROBO order is fully completed.
        #
        return True, True

    sl_order = get_stoploss_suborder_for_robo(broker_orderid, orderbook)
    ASSERT(sl_order is not None)
    tgt_order = get_target_suborder_for_robo(broker_orderid, orderbook)
    ASSERT(tgt_order is not None)

    #
    # Stoploss sub-order must not be triggered.
    # If not, it's likely some AngelOne bug since we got all three orders in
    # the orderbook (the main ROBO order and the stoploss and target sub-orders).
    # The main ROBO order status is "complete", which means the main order has
    # completed but the sub-orders are still open.
    #
    if sl_order['orderstatus'] != "trigger pending":
        PYPError("[AngelOne BUG] stoploss sub-order for broker_orderid %s not pending, "
                 "has orderstatus '%s': %s" %
                 (broker_orderid, sl_order['orderstatus'],
                  json.dumps(sl_order, indent=4)))
        # cancelled, squaredoff
        return False, False

    # Target sub-order must be open.
    if tgt_order['orderstatus'] != "open":
        PYPError("[AngelOne BUG] target sub-order for broker_orderid %s not open, "
                 "has orderstatus '%s': %s" %
                 (broker_orderid, tgt_order['orderstatus'],
                  json.dumps(tgt_order, indent=4)))
        # cancelled, squaredoff
        return False, False

    #
    # In the following discussion AngelOne claims that we need to cancel the
    # ROBO order and not each individual sub-orders.
    # https://smartapi.angelbroking.com/topic/1209/exit-robo-order-manually/2
    #
    # This seems to work too!
    #
    PYPInfo("Cancelling ROBO order: orderid=%s variety=%s" % (broker_orderid, "ROBO"))
    cancel_order_resp = cancel_order(broker_orderid, "ROBO")
    PYPInfo("Cancel ROBO order response: %s" % json.dumps(cancel_order_resp, indent=4))
    if cancel_order_resp is None:
        PYPError("[AngelOne BUG] Cancel ROBO order %s failed!" % broker_orderid)
        # cancelled, squaredoff
        return False, False

    PYPPass("Successfully squared-off ROBO order %s!" % broker_orderid)
    # cancelled, squaredoff
    return False, True
