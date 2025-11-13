############################################################################
# This is the common helper module to be used by all python programs.
# It can be used after adding the following boilerplate code.
#
# #
# # This file is cppbacktester/src/pylive/common/helpers.py
# # srcdir is cppbacktester/src.
# #
# srcdir = str(Path(__file__).resolve().parent.parent.parent)
# pylivedir = os.path.join(srcdir, "pylive")
# commondir = os.path.join(pylivedir, 'common')
#
# sys.path.append(commondir)
#
# from helpers import *
#
# TODO: If programs other than pylive use it, how do we set the config?
#       Sort this out before using it outside pylive.
############################################################################

import sys, os, csv, json, atexit, traceback
import multiprocessing
import pandas as pd
import numpy as np
import datetime
import logging
import threading
import pytz
import functools
from pprint import *
from colorama import Fore, Back, Style
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
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

holiday_map = {}
holiday_list_loaded = False

# Callback to be called on exit.
exit_cb = None

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
    #
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

def register_exitcb(cb):
    ''' Register callback to be called on program exit.
        Unfortunately atexit handlers are not called by os._exit() and sys.exit()
        doesn't terminate all the threads, and we need both.
        So we use our own exit handler.
    '''
    global exit_cb

    # Must be called once.
    ASSERT(exit_cb is None)
    ASSERT(cb is not None)
    ASSERT(callable(cb))

    #
    # atexit.register() callback is called when we exit because of say Ctrl-C
    # or calling sys.exit().
    # exit_cb() is called when we exit due to ASSERT() failure.
    #
    atexit.register(cb)
    exit_cb = cb

def get_srcdir():
    ''' Return source directory.
        This is useful to access resources relative to the source dir path.
    '''
    return Path(__file__).resolve().parent.parent.parent

def get_stocks_list():
    ''' Load list of stocks to trade.
        This can be an entire Nifty index as downloaded from
        https://www.niftyindices.com/, or it can be a handpicked list of
        stocks prepared externally using some heuristics (f.e. most traded
        stocks). Usually such handpicked stock list is either prepared manually
        or it's prepared using some other pre-processing program before the
        main pytrader program is run.

        The stock list file must be a csv file, but each of these type of
        files have different format. The index and handpicked files have the
        following headers:

        1. Company Name,Industry,Symbol,Series,ISIN Code
        2. Company Name,Symbol
    '''
    # CSV file containing list of stocks to process.
    stocklist = cfg.tld + "/NSE/" + cfg.stocklist
    assert(os.path.isfile(stocklist))

    PYPInfo("Loading stocks list from %s" % stocklist)
    stocks = {}

    with open(stocklist) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        num_cols = 0

        for row in csv_reader:
            #
            # Skip commented lines.
            # This provides an easy way to drop some symbol from NIFTY_50 f.e,
            # if it has got unclean data.
            # Also useful for adding comments.
            #
            if len(row[0]) > 0 and row[0][0] == '#':
                PYPWarn("Skipping commented line starting with: %s" % row[0])
                continue

            line_count += 1

            #
            # Assert the format of the CSV file to ensure we correctly parse
            # it. We support two forms. See function description.
            #
            if line_count == 1:
                # Header line.
                num_cols = len(row)
                assert(num_cols == 5 or num_cols == 2)

                if num_cols == 5:
                    assert(row[0] == 'Company Name')
                    assert(row[1] == 'Industry')
                    assert(row[2] == 'Symbol')
                    assert(row[3] == 'Series')
                    assert(row[4] == 'ISIN Code')
                elif num_cols == 2:
                    assert(row[0] == 'Company Name')
                    assert(row[1] == 'Symbol')
                else:
                    assert(False)
                continue

            # All rows MUST have the same number of columns as the header.
            assert(len(row) == num_cols)

            if num_cols == 5:
                company_name    = row[0]
                industry        = row[1]
                symbol          = row[2]
                series          = row[3]
                # For now, only equity.
                assert(series == 'EQ')
                assert(symbol not in stocks.keys())
                stocks[symbol] = {'company_name': company_name,
                                  'industry': industry }
            elif num_cols == 2:
                company_name    = row[0]
                symbol          = row[1]
                assert(symbol not in stocks.keys())
                stocks[symbol] = {'company_name': company_name,
                                  'industry': "Not Known" }
            else:
                assert(False)

    assert(len(stocks) > 0)
    PYPInfo("Using %d stock(s): %s" % (len(stocks), stocks))

    return list(stocks.keys())

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
    PYPError("Line %d @ %s: Assertion failed, exiting with exitcode=%s!" %
                    (sys._getframe(1).f_lineno,
                     sys._getframe(1).f_code.co_filename,
                     exitcode))

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
    # XXX Following sys.exit() doesn't cause atexit handlers to be called, probably
    #     the exit handlers are run from the SystemExit handler.
    #     So we use our own exit handler.
    #
    if exit_cb is not None:
        exit_cb()

    try:
        sys.stdout.flush()
        sys.exit(exitcode if exitcode is not None else 1)
    except SystemExit:
        os._exit(exitcode if exitcode is not None else 1)

def LINE():
    ''' Return line number of the callsite where it's called from.
    '''
    return sys._getframe(1).f_lineno

def FILE():
    ''' Return file name of the callsite where it's called from.
    '''
    return sys._getframe(1).f_code.co_filename

def pid_running(pid):
    ''' Check if given PID is running.

        XXX This may not work on non-Unix machines.
    '''
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

def pid_process_name(pid):
    ''' Given a pid return the process name.

        XXX This will work only one Linux.
    '''
    cmdline_file = os.path.join("/proc", str(pid), "cmdline")
    try:
        with open(cmdline_file, "r") as f:
            process_name = f.read()
            return os.path.basename(process_name.rstrip('\x00'))
    except Exception as e:
        PYPError("pid_process_name(%d): Failed with exception: %s" % (pid, e))

    return None

def ensure_only_one_instance(rundir, process_name):
    ''' Ensure there's only one instance of the given process running.
        It looks for the file <rundir>/<process_name>.pid and if present reads an
        integer from the file. This is the pid of the earlier process instance that
        is running. It checks if the given pid is running and its process name is
        'process_name'. An earlier instance is supposed to be running if:
        1. <rundir>/<process_name>.pid exists, and
        2. pid present in the above file belongs to a currently running process, and
        3. /proc/<pid>/cmdline matches 'process_name'.

        If either of the above conditions is false, then no prior instance is considered
        to be running and it goes ahead and stamps the pid returned by os.getpid() in
        <rundir>/<process_name>.pid for checks by subsequent runs. It also creates
        <rundir>/<process_name>.startepoch and writes the local timezone epoch (not UTC)
        when the above pid file was created, i.e., when the process was started.

        Else, if a prior instance is running it asserts and bails out. User has
        to fix it.

        rundir          => Directory name where the pidfile is created.
        process_name    => Name of the process to look for.
    '''
    ASSERT(os.path.isdir(rundir))
    pidfile = os.path.join(rundir, "%s.pid" % process_name)
    sefile = os.path.join(rundir, "%s.startepoch" % process_name)

    try:
        if os.path.exists(pidfile):
            with open(pidfile, "r") as f:
                pid = int(f.read())

            if (pid_running(pid) and (pid_process_name(pid) == process_name)):
                PYPError("File %s present, '%s' is likely already running!" %
                         (pidfile, process_name))
                PYPError("If not running, delete the file and then try again!")
                # Cause the program to bail out.
                ASSERT(False)
                return
            else:
                PYPInfo("Stale pidfile (%s (%d)), deleting!" % (pidfile, pid))
                os.remove(pidfile)

        #
        # Stamp pid.
        # We don't need to register atexit() method to delete the file as we
        # can correctly handle stale files.
        #
        with open(pidfile, "w") as f:
            #register_exitcb(lambda: os.remove(pidfile) if os.path.exists(pidfile) else None)
            f.write("%s" % os.getpid())
            f.flush()

        with open(sefile, "w") as f:
            f.write("%s" % int(pd.Timestamp.now().tz_localize('Asia/Kolkata').timestamp()))
            f.flush()
    except Exception as e:
        PYPError("ensure_only_one_instance(%s): Failed with exception: %s" %
                 (pidfile, e))
        ASSERT(False)

def kill_all_children():
    ''' Kill all multiprocessing processes started by the main thread.
        Since those are children of the main thread, only main thread can call
        this.

        w/o this errors (mostly assert failures) in one process can be hidden
        in the heap of logs from other processes. This causes entire processing
        to stop immediately thus making the error easy to spot.
    '''
    # get all active child processes.
    active = multiprocessing.active_children()
    PYPWarn('Killing %s active children' % len(active))

    # terminate all active children.
    for child in active:
        child.terminate()

def get_engine_pid():
    ''' Return pid of engine (backtester) from pylive/backtester.pid which is
        stamped by engine, and does staleness validations.

        If engine is not running, returns -1.
    '''
    pidfile = os.path.join(cfg.pylivedir, "backtester.pid")

    #
    # Engine MUST be running in live mode, but it's possible that we haven't
    # yet started it, return 0 .
    #
    if not os.path.exists(pidfile):
        PYPWarn("File %s not present, engine is likely not running!" % pidfile)
        return -1

    with open(pidfile, "r") as f:
        pid = int(f.read())

    process_name = pid_process_name(pid)
    if process_name != "backtester":
        if process_name is not None:
            PYPWarn("Backtester pid %d is stale (process has name %s)!" %
                    (pid, process_name))
        else:
            PYPWarn("Backtester pid %d is stale (process does not exist)!" % (pid))
        return -1

    return pid

def get_engine_startepoch():
    ''' Return epoch at which the current incarnation of engine started.
        The epoch returned is in local timezone not UTC.

        If engine is not running, returns -1.
    '''
    #
    # If engine not running, let caller know.
    #
    if get_engine_pid() == -1:
        PYPWarn("Cannot find startepoch, engine not running!")
        return -1

    epochfile = os.path.join(cfg.pylivedir, "backtester.startepoch")

    #
    # Engine MUST be running in live mode, but it's possible that we haven't
    # yet started it, return 0 to indicate that.
    #
    if not os.path.exists(epochfile):
        PYPWarn("File %s not present, engine is likely not running!" % epochfile)
        return -1

    epoch = None
    with open(epochfile, "r") as f:
        epoch = int(f.read())

    ASSERT(epoch is not None)
    ASSERT(epoch > 1708132275)

    return epoch

def get_pylivebroker_startepoch():
    ''' Return epoch at which the current incarnation of pylive.broker started.
        The epoch returned is in local timezone not UTC.

        If engine is not running, returns -1.
    '''
    #
    # Note: We don't need to do "is running" check since this function will be
    #       called from pylive.broker, so it must be running.
    #

    epochfile = os.path.join(cfg.pylivedir, "pylive.broker.startepoch")

    # If pylive.broker is running, startepoch file MUST be present.
    ASSERT(os.path.exists(epochfile))

    epoch = None
    with open(epochfile, "r") as f:
        epoch = int(f.read())

    ASSERT(epoch is not None)
    ASSERT(epoch > 1708132275)

    return epoch

def set_pd_display_options():
    ''' Call this to set pandas options for printing full dataframe.
    '''
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

def reset_pd_display_options():
    ''' Call this to reset pandas options to their defaults.
    '''
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.max_colwidth')

def get_current_time():
    ''' Returns a datetime.datetime object.
        This can be automatically converted to a pd.Timestamp object if
        the caller wants.
        To get the time in usec precision, use .timestamp, for other
        components use the .year, .month, .day, .hour, .minute, .second,
        etc.
    '''
    #return pd.Timestamp('2000-01-01 00:00:00+0530').now()
    # Using datetime.now() is 15X faster than above.
    # We return a tz-aware time to have uniformity with time read from csv.
    #tz = pytz.timezone('Asia/Kolkata')
    #
    # PERF:
    # tz or no-tz should match stockcache.read_csv() and read_ohlcv().
    #
    #tz = pytz.FixedOffset(330)
    #return tz.localize(datetime.datetime.now())
    return datetime.datetime.now()

def is_market_open(dt=None):
    ''' Given a Timestamp, return if the markets are open at that time.
        Caller usually wants to start a candle at this time, so we return
        false if dt matches 15:30.
    '''
    # If not timestamp provided by caller, check current time.
    if dt is None:
        dt = datetime.datetime.now()

    assert(isinstance(dt, datetime.datetime))

    # Markets are closed on Saturday/Sunday.
    # TODO: Take care of public holidays?
    if dt.weekday() == 5 or dt.weekday() == 6:
        return False

    sec = dt.hour*3600 + dt.minute*60
    return sec >= 9*3600+15*60 and sec < 15*3600+30*60

def after_market_close(dt):
    ''' Given a Timestamp, return if the markets are closed at that time.
    '''
    assert(isinstance(dt, pd.Timestamp))
    # We should be called for a working day.
    assert(dt.weekday() != 5 and dt.weekday() != 6)

    # After market close?
    if dt.time() > datetime.time(15, 30):
        return True

    return False

def get_next_day_market_start(ts):
    ''' Given a Timestamp, return the Timestamp for the next day's market start.
        This does not check if the day returned was a trading day. If the caller
        wants to ensure that, he will typically pass this timestamp to get_index().
    '''
    assert(isinstance(ts, pd.Timestamp))

    #
    # PERF: numpy.timedelta64(1, 'D') runs faster than pd.Timedelta('1D'),
    #       and save creating one more pd.Timestamp pbject.
    #       We can even do better.
    #       If ts.day < 28, we know adding 1D to it will not change month
    #       and year, so we can simply avoid adding 1D to ts and creating a
    #       new TS.
    #

    if ts.day < 28:
        day = ts.day + 1
        month = ts.month
        year = ts.year
    else:
        ts_nxt = ts + np.timedelta64(1, 'D')
        day = ts_nxt.day
        month = ts_nxt.month
        year = ts_nxt.year

    return pd.Timestamp(year=year,
                        month=month,
                        day=day,
                        hour=9,
                        minute=15,
                        second=0,
                        microsecond=0)

def get_prev_day_market_start(ts):
    ''' Given a Timestamp, return the Timestamp for the prev day's market start.
        This does not check if the day returned was a trading day. If the caller
        wants to ensure that, he will typically pass this timestamp to get_index().
    '''
    assert(isinstance(ts, pd.Timestamp))
    #
    # PERF: numpy.timedelta64 runs faster than pd.Timedelta,
    #       and save creating one more pd.Timestamp pbject.
    #       We can even do better.
    #       If ts.day > 1, we know subtracting 1D from it will not change month
    #       and year, so we can simply avoid subtracting 1D to ts and creating a
    #       new TS.
    #

    if ts.day > 1:
        day = ts.day - 1
        month = ts.month
        year = ts.year
    else:
        ts_prev = ts - np.timedelta64(1, 'D')
        day = ts_prev.day
        month = ts_prev.month
        year = ts_prev.year

    return pd.Timestamp(year=year,
                        month=month,
                        day=day,
                        hour=9,
                        minute=15,
                        second=0,
                        microsecond=0)

#@functools.lru_cache(maxsize=2)
def get_prev_day_market_close(ts):
    ''' Given a Timestamp, return the Timestamp for the prev day's market close.
        This does not check if the day returned was a trading day. If the caller
        wants to ensure that, he will typically pass this timestamp to get_index().
    '''
    assert(isinstance(ts, pd.Timestamp))

    #
    # PERF: numpy.timedelta64 runs faster than pd.Timedelta,
    #       and save creating one more pd.Timestamp pbject.
    #
    if ts.day > 1:
        day = ts.day - 1
        month = ts.month
        year = ts.year
    else:
        ts_prev = ts - np.timedelta64(1, 'D')
        day = ts_prev.day
        month = ts_prev.month
        year = ts_prev.year

    return pd.Timestamp(year=year,
                        month=month,
                        day=day,
                        hour=15,
                        minute=30,
                        second=0,
                        microsecond=0)

def get_same_day_market_start(ts):
    ''' Given a Timestamp, return the Timestamp for the same day's market start.
        This does not check if the day returned was a trading day. If the caller
        wants to ensure that, he will typically pass this timestamp to get_index().
    '''
    assert(isinstance(ts, pd.Timestamp))
    return ts.replace(hour=9, minute=15, second=0, microsecond=0)

def get_seconds_since_market_start(ts):
    ''' Given a timestamp, return number of seconds elapsed since market start.
    '''
    tstart = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    td = ts - tstart
    assert(td.total_seconds() >= 0)
    return int(td.total_seconds())

def is_market_start(ts):
    ''' Given a Timestamp, return True if it corresponds to the market start time.
    '''
    assert(isinstance(ts, pd.Timestamp))
    return (ts.hour == 9 and ts.minute == 15 and ts.second == 0)

def must_auto_squareoff_now():
    ''' Should we auto-squareoff now?
        We do it at 3:05PM to avoid the broker auto-squareoff at 3:10PM.
    '''
    ts = pd.Timestamp.now()
    return (ts.hour == 15 and ts.minute >= 5)

def ts_before(ts, h, m, s):
    ''' Test if the given Timestamp is before h:m:s for the day.
    '''
    return ts < ts.replace(hour=h, minute=m, second=s, microsecond=0)

def ts_after(ts, h, m, s):
    ''' Test if the given Timestamp is after h:m:s for the day.
    '''
    return not ts_before(ts, h, m, s)

def file_updated_after_market_start(file_path):
    ''' Returns True if the given file was updated after market start today.
        Files created before market start may be considered "stale" for various
        purposes and hence this function may be useful for finding that.
    '''
    stat_buf = os.stat(file_path)
    file_mtime = pd.Timestamp(stat_buf.st_mtime,
                              unit='s',
                              tz='Asia/Kolkata').tz_localize(None)
    market_start = get_same_day_market_start(pd.Timestamp.now())
    return file_mtime >= market_start

def guess_candle_size(df, csvfile):
    ''' Given an OHLC dataframe, guess the candle size it corresponds to.

        'csvfile' argument is just for useful logging in case of some issue.
    '''
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
        assert(False)
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
        if (i != 1):
                print("\n*** [%s] i=%d df.index[%d] = %s ***\n" % (csvfile, i, i, df.index[i]))
        assert(i == 1)

    seconds = td0.total_seconds()
    assert(seconds >= 60)
    return seconds

def load_holiday_json():
    ''' Load holiday_list.nse.equities.<year>.
    '''
    now = pd.Timestamp.now()
    holiday_list_file = os.path.join(cfg.pylivedir,
                                        ("holiday_list.nse.equities.%d" % now.year))
    #
    # If file doesn't exist log a warning and continue.
    # We must have it when we run pylive/broker as pylive/cron would have
    # fetched it.
    #
    if not os.path.exists(holiday_list_file):
        PYPWarn("*** Holiday list file %s not found, won't handle holidays ***" %
                (holiday_list_file))
        return

    stat_buf = os.stat(holiday_list_file)
    assert(stat_buf.st_size > 0)

    global holiday_map

    with open(holiday_list_file) as f:
        holidays = json.load(f)

    for h in holidays["tradingDate"].values():
        holiday_map[pd.Timestamp(h)] = True

    PYPPass("Holiday list loaded: %s" % holiday_map.keys())

def today_is_holiday():
    ''' Returns true if today is trading holiday.
        Don't call this before pylive/cron downloads the holiday list.
    '''
    global holiday_list_loaded

    if not holiday_list_loaded:
        load_holiday_json()
        holiday_list_loaded = True

    return pd.Timestamp.now() in holiday_map

class AtomicCounter:
    def __init__(self, initial=0):
        self.value = initial
        self._lock = threading.Lock()

    def inc(self, num=1):
        with self._lock:
            self.value += num
            return self.value

    def dec(self, num=1):
        with self._lock:
            self.value -= num
            return self.value

