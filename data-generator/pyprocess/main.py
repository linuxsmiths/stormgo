#!/usr/bin/env python3

import sys, setproctitle
import signal
import argparse
import config as cfg
import stockprocessor as sp
from helpers import *

#
# Send SIGHUP to engine which would be waiting to reload the latest live data.
#
def wakeup_engine():
    pidfile = cfg.pylivedir + "/backtester.pid"

    #
    # Engine MUST be running in live mode, but it's possible that we haven't
    # yet started it, skip.
    #
    if not os.path.exists(pidfile):
        PYPWarn("File %s not present, backtester is likely not running!" % pidfile)
        return

    with open(pidfile, "r") as f:
        pid = int(f.read())

    PYPPass("Sending SIGHUP to backtester (pid=%d)" % pid)
    try:
        os.kill(pid, signal.SIGHUP)
    except ProcessLookupError:
        #
        # This can happen f.e., if backtester crashes or we are running live
        # pyprocess for debugging.
        #
        PYPWarn("Failed, probably backtester is not running!")
    except Exception as e:
        # Any other exception, rethrow, so that we know.
        PYPError("os.kill(%d, SIGHUP) got exception: %s" % (pid, e))
        raise

#
# Send SIGSTOP to engine to pause it while we are updating the aggregate
# candle data, to avoid engine reading half-written files causing parsing
# issues.
#
def pause_engine():
    pidfile = cfg.pylivedir + "/backtester.pid"

    #
    # Engine MUST be running in live mode, but it's possible that we haven't
    # yet started it, skip.
    #
    if not os.path.exists(pidfile):
        PYPWarn("File %s not present, backtester is likely not running!" % pidfile)
        return

    with open(pidfile, "r") as f:
        pid = int(f.read())

    PYPPass("Sending SIGSTOP to backtester (pid=%d)" % pid)
    try:
        os.kill(pid, signal.SIGSTOP)
    except ProcessLookupError:
        #
        # This can happen f.e., if backtester crashes or we are running live
        # pyprocess for debugging.
        #
        PYPWarn("Failed, probably backtester is not running!")
    except Exception as e:
        # Any other exception, rethrow, so that we know.
        PYPError("os.kill(%d, SIGSTOP) got exception: %s" % (pid, e))
        raise

#
# Resume engine (using SIGCONT) paused by pause_engine().
#
def resume_engine():
    pidfile = cfg.pylivedir + "/backtester.pid"

    #
    # Engine MUST be running in live mode, but it's possible that we haven't
    # yet started it, skip.
    #
    if not os.path.exists(pidfile):
        PYPWarn("File %s not present, backtester is likely not running!" % pidfile)
        return

    with open(pidfile, "r") as f:
        pid = int(f.read())

    PYPPass("Sending SIGCONT to backtester (pid=%d)" % pid)
    try:
        os.kill(pid, signal.SIGCONT)
    except ProcessLookupError:
        #
        # This can happen f.e., if backtester crashes or we are running live
        # pyprocess for debugging.
        #
        PYPWarn("Failed, probably backtester is not running!")
    except Exception as e:
        # Any other exception, rethrow, so that we know.
        PYPError("os.kill(%d, SIGCONT) got exception: %s" % (pid, e))
        raise

#
# This is the pyprocess program that reads the historical stock data from csv
# files, processes it and adds new columns containing useful aggregated info
# (like X-<day|min> SMA/EMA, RSI, 25-day-low-price, and many others) that is
# needed for backtesting various algorithms. It outputs the existing OHLCV data
# and the computed columns as a new csv file which will be used by the C++
# backtester. We do this in Python as there are already lot of existing code
# that can be used and also pandas dataframes are quite fast.
# It reads the config file backtester.json for various configuration settings.
#
def main():
    start = pd.Timestamp.now()

    # Let main process be called "pyp.main".
    setproctitle.setproctitle("pyp.main");

    # Initialize parser.
    parser = argparse.ArgumentParser()

    #
    # Add optional argument for processing live data.
    # Note that live data means just the $stock.prelive.csv and $stock.live.csv
    # candles, and in that case the processed data is updated in $stock.final.live.XMin.csv.
    #
    # Usually pylive/broker will invoke with --live option after it adds a new
    # live 1Min candle data to live file.
    #
    parser.add_argument("--live", help="Process live data", action="store_true")

    #
    # Add optional argument for specifying the csv files containing list of
    # stocks which must be processed.
    #
    # XXX Use one.csv only for testing or for processing index data, f.e.,
    #     when backtester gives the error lhsod_epoch is not matching with the
    #     last trading day.
    #
    parser.add_argument("--stocklistcsv",
                        type=str,
                        choices=["NIFTY_50.csv", "NIFTY_100.csv", "NIFTY_200.csv", "one.csv"],
                        help="csv file containing stocks to process")

    # Read arguments from command line
    args = parser.parse_args()

    # Anything before this log is due to modules getting imported.
    PYPPass('==> Starting %spyprocess for %s... [logfile=%s]' %
        ("live " if args.live else "",
         args.stocklistcsv if args.stocklistcsv else cfg.stocklist,
         cfg.logfile))

    #
    # -l/--live commandline is same as setting process_live_data in
    # backtester.json. It's better to not set process_live_data in the config
    # and use the -l/--live command line option as that allows us to use both
    # modes without making config changes.
    #
    if args.live:
        cfg.process_live_data = True
        cfg.candles = ["1Min", "3Min", "5Min", "10Min", "15Min"]
        PYPPass("[Live] Forcing intraday candles (%s), ignoring config.candles!" %
                cfg.candles)

    if args.stocklistcsv:
        # This will update the cfg.stocklist when used in stockprocessor.py.
        cfg.stocklist = cfg.tld + "/NSE/" + args.stocklistcsv
        assert(os.path.isfile(cfg.stocklist))
        PYPPass("Forcing stocklistcsv=%s!" % cfg.stocklist)

    #
    # Pause the engine before we start making any changes to the live data
    # files to avoid parsing issues caused by engine parsing partially written
    # files.
    #
    if cfg.process_live_data:
        pause_engine()

    sp.init()
    sp.start()
    sp.join()

    #
    # If we just finished processing live data, send SIGHUP to engine which
    # would want to reload the latest finalized data.
    # Before that we need to resume it since we have pasused it above.
    #
    if cfg.process_live_data:
        resume_engine()
        wakeup_engine()

    end = pd.Timestamp.now()
    PYPPass('Done pyprocess, took %s' % (end - start))

if __name__ == '__main__':
    main()
