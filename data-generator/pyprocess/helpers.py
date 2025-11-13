import os, sys, traceback
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

def guess_candle_size(df, csvfile):
    ''' Given an OHLC dataframe, guess the candle size it corresponds to.

        'csvfile' argument is just for useful logging in case of some issue.
    '''

    #
    # When processing Nifty100 or lower stocks many stocks don't have clean
    # data so we see many stocks with many missing 1Min candles which cause
    # the assert in this function to fail. if you see many such failures,
    # uncomment this to disable candle size detection and hardcode to 1Min
    # which is the case.
    #
    # return 60

    #
    # We need at least 3 ticks to guess candle size.
    # In case of live tick processing this can happen when we just start getting
    # live ticks and we have only one or two of them.
    # Since 1Min is the common tick size we read (infact we don't read anything
    # other than 1Min) we assume 1Min in such case.
    #
    if (len(df.index) <= 3):
        print("Not enough ticks (only %d) in %s, for guessing candle size, assuming 1Min!" %
                (len(df.index), csvfile))
        # This should only happen when processing live ticks.
        assert(cfg.process_live_data)
        return 60

    i = 1

    assert(len(df.index) > i+2)
    td0 = df.index[i]   - df.index[i-1]
    td1 = df.index[i+1] - df.index[i]
    td2 = df.index[i+2] - df.index[i+1]

    # 3 consecutive matching timedeltas should be accurate enough.
    while td0 != td1 or td1 != td2:
        i += 1
        assert(len(df.index) > i+2)
        td0 = df.index[i]   - df.index[i-1]
        td1 = df.index[i+1] - df.index[i]
        td2 = df.index[i+2] - df.index[i+1]

    #
    # The first four candles itself should be compliant with the candle size.
    # For some data like the NIFTY_50 daily candles, if there's a holiday in
    # the beginning we might have to be iterate a little to get to 4
    # consecutive candles, e.g., the following data only gets 4 consecutive
    # candles at i==4.
    #
    # 2013-01-02 09:15:00+05:30,5982.60,6006.05,5982.00,5993.25,0.00
    # 2013-01-03 09:15:00+05:30,6015.80,6017.00,5986.55,6009.50,0.00
    # 2013-01-04 09:15:00+05:30,6011.95,6020.75,5981.55,6016.15,0.00
    # 2013-01-07 09:15:00+05:30,6042.15,6042.15,5977.15,5988.40,0.00
    # 2013-01-08 09:15:00+05:30,5983.45,6007.05,5964.40,6001.70,0.00
    # 2013-01-09 09:15:00+05:30,6006.20,6020.10,5958.45,5971.50,0.00
    # 2013-01-10 09:15:00+05:30,5998.80,6005.15,5947.30,5968.65,0.00
    #
    # To be safe let's assert for <= 4 for daily candles.
    #
    if td0 == pd.Timedelta("1D"):
        if (i > 4):
                print("\n*** [%s] i=%d df.index[%d] = %s ***\n" % (csvfile, i, i, df.index[i]))
        assert(i <= 4)
    else:
        #
        # Intraday candles should mostly not need a retry, but sometimes I've
        # seen live candles won't be formed fully and then this may fail.
        # We don't want to increase it to hide those error, rather try to find
        # out why live data is not getting all the ticks for all the stocks.
        #
        # XXX There was a bug in pylive where we were not dumping candles
        #     which did not have the first tick received in the 0th sec, while
        #     some 1Min candles were easily having their first tick received
        #     in 1st or 2nd sec.
        #     Now we should not see this, so we get this assert back.
        #
        if (i != 1):
                print("\n*** [%s] i=%d df.index[%d] = %s ***\n" % (csvfile, i, i, df.index[i]))
        assert(i == 1)

    seconds = td0.total_seconds()
    assert(seconds >= 60)
    return seconds

def is_market_start(ts):
    ''' Given a Timestamp, return True if it corresponds to the market start time.
    '''
    assert(isinstance(ts, pd.Timestamp))
    return (ts.hour == 9 and ts.minute == 15 and ts.second == 0)

def ASSERT(cond, msg=None, exitcode=None):
    ''' Kill the program if the assertion condition 'cond' fails.
        This is useful especially to be called from code that is called by the
        websocket hook since asserts called from there are absorbed and result
        in a meaningless message.
    '''
    if cond == True:
        return

    #
    # Unfortunately we cannot log the condition, so the best we can do is
    # print filename and line number.
    #
    PYPError("Line %d @ %s: Assertion failed, exiting!" %
                    (sys._getframe(1).f_lineno,
                     sys._getframe(1).f_code.co_filename))

    stack_trace = ""
    for line in traceback.format_stack():
        stack_trace += line.strip()

    PYPError("\n\n-----[ Traceback (most recent call last) ]----------\n\n%s" % stack_trace)

    if msg is not None:
        PYPError("\n*** Dying statement *** %s\n" % msg)
    #
    # XXX sys.exit() just raises a SystemExit exception and hence it just
    #     causes the current thread to exit. In order to exit the entire
    #     program including all threads we need to call os._exit(), but
    #     os._exit() doesn't flush the stdio buffers, so we call sys.exit() to
    #     flush the stdio buffers and then catch the exception and call
    #     os._exit().
    #
    try:
        sys.stdout.flush()
        sys.exit(exitcode if exitcode is not None else 1)
    except SystemExit:
        os._exit(exitcode if exitcode is not None else 1)

def get_seconds_since_market_start(ts):
    ''' Given a timestamp, return number of seconds elapsed since market start.
    '''
    tstart = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    td = ts - tstart
    assert(td.total_seconds() >= 0)
    return int(td.total_seconds())
