import os
import pandas as pd
import numpy as np
import datetime
import logging
from pprint import *
from colorama import Fore, Back, Style

import config as cfg

#
# Use this wherever we want to use a "high enough" value to discourage
# some action. This should be larger than any valid values involved,
# else things may break.
#
# Note: math.inf is treated as NaN by pandas.DataFrame.Rolling, so we
#       cannot use that to indicate a "large value", else all the
#       aggregation methods like min()/max() return NaN even when we
#       have enough window worth of values.
#       Ref: https://stackoverflow.com/questions/60766199/pandas-rolling-returns-nan-when-infinity-values-are-involved
#
#INFINITY = 999999999.99
INFINITY = 99999999999
#TS_INFINITY = pd.Timestamp('2051-01-01 15:29:00+05:30')
TS_INFINITY = pd.Timestamp('2051-01-01 15:29:00')


# Error logs go to ERROR_LOG in addition to the configured log destination.
ERROR_LOG = "/tmp/pyt.error"

# Truncate the existing log files.
err_f = open(ERROR_LOG, "w")
assert err_f is not None

def PYPLog(logstr, console=None, logfile=False):
    ''' Normal logging.
        It'll log to console, if:
            1. Caller has passed console=True
            2. Caller has not explicitly passed console=False and
               cfg.log_to_console is True.
    '''
    if cfg.skip_logging:
        return

    log_to_console = (console or (console is None and cfg.log_to_console))

    dlogstr = ("[%s][%d][   LOG] %s" %
                (datetime.datetime.now(), os.getpid(), logstr))

    logging.info(dlogstr)

    if log_to_console:
        print(dlogstr)

def PYPDebug(logstr, console=None):
    ''' Normal logging.
    '''
    if cfg.skip_logging or not cfg.verbose:
        return

    log_to_console = (console or (console is None and cfg.log_to_console))

    dlogstr = ("[%s][%d][ DEBUG] %s" %
                (datetime.datetime.now(), os.getpid(), logstr))

    logging.info(dlogstr)

    if log_to_console:
        print(dlogstr)

def PYPInfo(logstr, console=None, logfile=False):
    ''' Bright.
    '''
    if cfg.skip_logging:
        return

    log_to_console = (console or (console is None and cfg.log_to_console))

    dlogstr = ("[%s][%d][  INFO] %s" %
            (datetime.datetime.now(), os.getpid(), logstr))

    logging.info(dlogstr)

    if log_to_console:
        print(Style.BRIGHT + dlogstr + Style.RESET_ALL)

def PYPError(logstr, console=None):
    ''' Bright red.
    '''
    # Always log errors to console. This helps in catching assert failures.
    log_to_console = True

    dlogstr = ("[%s][%d][ ERROR] %s" %
                (datetime.datetime.now(), os.getpid(), logstr))

    logging.error(dlogstr)

    if log_to_console:
        print(Style.BRIGHT + Fore.RED + dlogstr + Fore.RESET + Style.RESET_ALL)

    # Send errors to error log too.
    err_f.write(dlogstr + "\n")
    err_f.flush()

def PYPWarn(logstr, console=None, logfile=True):
    ''' Bright yellow.
    '''
    #
    # XXX
    # When directing logs to a file/null it's useful to have warning logs come
    # on console.
    # XXX
    #log_to_console = (console or (console is None and cfg.log_to_console))
    log_to_console = True

    dlogstr = ("[%s][%d][  WARN] %s" %
                (datetime.datetime.now(), os.getpid(), logstr))

    logging.warn(dlogstr)

    if log_to_console:
        print(Style.BRIGHT + Fore.YELLOW + dlogstr +
              Fore.RESET + Style.RESET_ALL)

def PYPPass(logstr, console=None, logfile=True):
    ''' Logging for some successful action.
    '''
    if cfg.skip_logging:
        return

    log_to_console = (console or (console is None and cfg.log_to_console))

    dlogstr = ("[%s][%d][  PASS] %s" %
            (datetime.datetime.now(), os.getpid(), logstr))

    logging.info(dlogstr)

    if log_to_console:
        print(Style.BRIGHT + Fore.GREEN + dlogstr +
              Fore.RESET + Style.RESET_ALL)

def PYPFail(logstr, console=None):
    ''' Logging for some failed action.
    '''
    # Just like PYPError(), log all failures to console.
    log_to_console = True

    dlogstr = ("[%s][%d][  FAIL] %s" %
                (datetime.datetime.now(), os.getpid(), logstr))

    logging.error(dlogstr)

    if log_to_console:
        print(Fore.RED + dlogstr + Fore.RESET + Style.RESET_ALL)

    err_f.write(dlogstr + "\n")
    err_f.flush()

def guess_candle_size(df):
    ''' Given an OHLC dataframe, guess the candle size it corresponds to.
    '''
    i = 1
    td0 = df.index[i]   - df.index[i-1]
    td1 = df.index[i+1] - df.index[i]
    td2 = df.index[i+2] - df.index[i+1]

    # 3 consecutive matching timedeltas should be accurate enough.
    while td0 != td1 or td1 != td2:
        i += 1
        td0 = df.index[i]   - df.index[i-1]
        td1 = df.index[i+1] - df.index[i]
        td2 = df.index[i+2] - df.index[i+1]

    # The first three candles itself should be compliant with the candle size.
    assert(i == 1)

    seconds = td0.total_seconds()
    assert(seconds >= 60)
    return seconds

def is_market_start(ts):
    ''' Given a Timestamp, return True if it corresponds to the market start time.
    '''
    assert(isinstance(ts, pd.Timestamp))
    return (ts.hour == 9 and ts.minute == 15 and ts.second == 0)
