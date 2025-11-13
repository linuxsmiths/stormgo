#!/usr/bin/env python3

import sys, os, time, setproctitle, signal
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

import config as cfg
from helpers import *

if cfg.broker['selection'] == "angelone":
    import AngelOne as broker
else:
    # Till we support other brokers.
    assert(False)

import CandleGenerator as CandleGenerator
import OrderPlacer as OrderPlacer
import OrderTracker as OrderTracker

#
# This is the pylive broker program. It has all the logic of "live" interaction
# with the broker. This involves monitoring the src/pylive/orders/ directories
# and performing the required interaction with the broker. To be precise, it
# does the following:
# 1. Monitor src/pylive/orders/generated/ directory for newly generated orders
#    by the engine and places these orders with the broker. After successful
#    placement moves the orders to src/pylive/orders/placed/ directory. Failed
#    orders (insufficient funds or something else) are moved to
#    src/pylive/orders/failed/ directory.
# 2. Monitor orders in src/pylive/orders/placed/, checking their status with
#    the broker, and if an order is executed moves it to orders/executed/
#    directory. At 3:15 PM checks all the placed orders and if not executed
#    auto-squares off the orders and moves them to orders/squaredoff/
#    directory.
# 3. It queryies live ticks from the broker and saves 1Min ticks in
#    $tld/NSE/historical/$stock/$stock.live.csv. After saving every new
#    1Min tick it'll run pyprocess in "live" mode where it just goes over the
#    live.csv files for each stock and creates/updates the following files:
#    $tld/NSE/historical/$stock/$stock.final.live.1Min.csv
#    $tld/NSE/historical/$stock/$stock.final.live.5Min.csv
#    $tld/NSE/historical/$stock/$stock.final.live.10Min.csv
#    $tld/NSE/historical/$stock/$stock.final.live.15Min.csv
#    After updating these files it'll send a SIGHUP to the engine asking to reload
#    live aggregated data so that it has uptodate intraday aggregates it may need
#    for its strategy analysis. Note that the 1D aggregates have already been
#    updated in Step2 so engine must have the latest daily aggregates. It just
#    needs to update the intraday aggregates.
#    Since generating aggregates needs some historical data, and for intraday
#    aggregates we need at most 1 day worth of 1Min candles, pyprocess (live)
#    will need previous days 1Min candle data too. See Step2 how that data is
#    available in $stock.prelive.csv file.
#    When engine gets the SIGHUP it loads the data from $stock.final.live.xMin.csv
#    files. It carefully updates the BTDataFrame so that only the new entries are
#    added.
#

#
# Send SIGHUP to engine asking it to terminate.
#
def wakeup_engine():
    pid = get_engine_pid()
    if pid == -1:
        PYPError("wakeup_engine: Could not send *last* SIGHUP to backtester, not found!")
        return

    PYPPass(("Sending *last* SIGHUP to backtester (pid=%d), signalling termination!" % pid),
            console=True)
    try:
        os.kill(pid, signal.SIGHUP)
        PYPPass(("os.kill(%d, SIGHUP) successful!" % (pid)), console=True)
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

def terminate_after_market_close():
    ''' This thread calls stop() method on all pylive modules to stop them, after
        market closes.
    '''
    # Look for market close only after it opens.
    PYPWarn("terminate_after_market_close: waiting for market open!")
    while not is_market_open():
        time.sleep(5)

    # Now market is open, keep checking for close.
    PYPWarn("terminate_after_market_close: market open, now waiting for close!")
    while is_market_open():
        time.sleep(5)

    PYPWarn("terminate_after_market_close: market closed, waiting for 1Min before terminating!")
    time.sleep(60)
    PYPWarn("terminate_after_market_close: now terminating...")

    CandleGenerator.stop()
    OrderPlacer.stop()
    OrderTracker.stop()

    sys.stdout.flush()
    #os._exit(0)
    # Send "last SIGHUP" to engine asking it to terminate.
    wakeup_engine()
    PYPWarn("After wakeup_engine!")

def main():
    setproctitle.setproctitle("pylive.broker");

    ensure_only_one_instance(cfg.pylivedir, "pylive.broker")

    #
    # Anything before this log is due to modules getting imported.
    # Logs from main() are forced to console as they are few and important.
    #
    PYPPass(('==> Starting pylive.broker... [logfile=%s]' % (cfg.logfile)), console=True)

    #
    # Quietly bail out on stock market holidays so that systemd doesn't restart us.
    #
    if today_is_holiday():
        PYPWarn("Today is holiday, exiting!")
        return

    #
    # Broker initialization. Much of this would be common to all brokers, but
    # some of it may be broker specific.
    # f.e., it will download instruments file (for stock to token mapping),
    # login to the broker, setup websocket for getting ticks, etc.
    #
    # This should be the first thing as the remaining methods may call the
    # broker methods.
    #
    # This will login and do other necessary initializations, stock ticks will
    # start coming only after broker.start_websocket() is called which must be
    # done later.
    #
    PYPInfo(("==> Initializing broker %s" % cfg.broker['selection']), console=True)
    assert(not broker.is_initialized())
    broker.init()
    assert(broker.is_initialized())
    PYPInfo(("==> Broker (%s) initialized successfully!" % cfg.broker['selection']),
            console=True)

    #
    # Start a thread to monitor market timings and terminate pylive when
    # market closes.
    #
    termthr = threading.Thread(target=terminate_after_market_close, args=(), daemon=False)
    termthr.start()

    # Call init() for each component.
    PYPInfo(("==> Start .init()"), console=True)
    CandleGenerator.init()
    OrderPlacer.init()
    OrderTracker.init()
    PYPInfo(("==> Done .init()"), console=True)

    # Call start() for each component.
    PYPInfo(("==> Start .start()"), console=True)
    OrderPlacer.start()
    OrderTracker.start()
    CandleGenerator.start()
    PYPInfo(("==> Done .start()"), console=True)

    # Call join() for each component.
    PYPInfo(("==> Start .join()"), console=True)
    CandleGenerator.join()
    OrderPlacer.join()
    OrderTracker.join()
    termthr.join()
    PYPInfo(("==> Done .join()"), console=True)

    PYPPass(('Exiting pylive.broker'), console=True)

if __name__ == '__main__':
    main()
