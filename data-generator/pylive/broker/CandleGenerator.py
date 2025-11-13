import os, sys, csv
from pathlib import Path
import subprocess
import json
import queue
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

import threading
import pandas as pd
import config as cfg
from helpers import *

if cfg.broker['selection'] == "angelone":
    import AngelOne as broker
else:
    # Till we support other brokers.
    ASSERT(False)

cg = None

# Total ticks received over websocket.
ticks_received = 0

#
# Ticks for prev minute (or older) received.
# This should only come at the starting minute and never afterwards.
#
stale_ticks_received = 0

exit_now = False
ws_thread = None
dequeue_thread = None
historical_thread = None

# Time when pylive started.
start_time = None

#
# LTP details (one per symbol) are placed in this directory and updated as we
# get new ticks.
#
ltp_dir = os.path.join(cfg.srcdir, "pylive/orders/ltp")
ASSERT(os.path.exists(ltp_dir))

#
# Absolute minute at which CandleGenerator started.
# We ignore candles that start on the same minute as they could be incomplete.
#
#start_minute = None

class Candle:
    def __init__(self, instrument):
        # token whose tick data this Candle holds.
        self.instrument = instrument
        self.symbol = broker.token_to_symbol(instrument)
        ASSERT(len(self.symbol) > 1);

        # start-of-candle timestamp.
        self.dt = None

        #
        # Boolean flag to indicate if this is a partial candle.
        # Partial candles are not dumped to avoid corrupting our live candle
        # data.
        # Q: What is a partial candle?
        # A: Candles formed on the same minute as when this program is
        #    started. Since they may not have some ticks at the start of the
        #    minute we don't dump these.
        self.partial = None

        # OHLCV values.
        self.o = None
        self.h = None
        self.l = None
        self.c = None
        self.v = None

        #
        # Running value of volume_trade_for_the_day (cumulative daily volume).
        # This is set from tick["volume_trade_for_the_day"] as new ticks are
        # received. When a candle is sealed and dumped at the end of the
        # minute, this value from the outgoing candle is copied in to the new
        # incoming candle's volume_trade_for_the_day_at_soc.
        #
        self.volume_trade_for_the_day = 0

        #
        # Cumulative daily volume at the start of candle.
        # This is only set to non-zero for candles that start at exact minute
        # boundary. Usually the very first candle for a token may not start on
        # a minute boundary and hence we don't save that candle to avoid
        # inaccurate signal generation.
        #
        self.volume_trade_for_the_day_at_soc = 0

        #
        # Set and create initial LTP file which will be updated as we receive
        # ticks for this symbol.
        #
        self.ltp_file = os.path.join(ltp_dir, self.symbol)

        initial_ltp = {
                "sequence_number": 0,
                "exchange_timestamp": 0,
                "last_traded_price": 0,
        }

        try:
            with open(self.ltp_file, 'w') as f:
                json.dump(initial_ltp, f, indent=4)
                PYPPass("Created initial LTP file %s" % (self.ltp_file))
        except Exception as e:
            PYPError("Failed to create initial LTP file %s" % (self.ltp_file))
            # Treat this as fatal as engine needs LTP for correctly placing
            # orders.
            ASSERT(False)
        ASSERT(os.path.exists(self.ltp_file))

    def on_tick(self, tick):
        ''' Update candle with the newly received tick.
            Sample tick looks like this:

            {
                    "subscription_mode": 2,
                    "exchange_type": 1,
                    "token": "11630",
                    "sequence_number": 6950681,
                    "exchange_timestamp": 1714629059000,
                    "last_traded_price": 36790,
                    "subscription_mode_val": "QUOTE",
                    "last_traded_quantity": 3,
                    "average_traded_price": 36653,
                    "volume_trade_for_the_day": 9745238,
                    "total_buy_quantity": 926745.0,
                    "total_sell_quantity": 1683466.0,
                    "open_price_of_the_day": 36320,
                    "high_price_of_the_day": 36930,
                    "low_price_of_the_day": 36320,
                    "closed_price": 36320
            }

        '''

        if cfg.verbose:
            PYPInfo("Candle::on_tick(%s)" % broker.token_to_symbol(tick["token"]))
        ASSERT(tick["token"] == self.instrument,
               "%s != %s" % (tick["token"], self.instrument))

        # tick_timestamp is in milliseconds.
        tick_timestamp = tick["exchange_timestamp"]
        tick_timestamp //= 1000

        #
        # Sanity check. Must be greater than "1 Jan 2023".
        #
        ASSERT(tick_timestamp > 1672511400, "tick_timestamp=%d" % tick_timestamp)

        # Price traded in the last tick.
        ltp = tick["last_traded_price"]

        # Cumulative daily volume till now.
        volume_trade_for_the_day = tick["volume_trade_for_the_day"]

        #
        # First tick of the candle.
        # At start of every minute we dump the previous 1Min candle and start
        # a new Candle, so at the start of every minute we will get the first
        # tick of the candle.
        #
        if self.dt is None:
            # Nothing should be set yet.
            ASSERT(self.o is None)
            ASSERT(self.h is None)
            ASSERT(self.l is None)
            ASSERT(self.c is None)
            ASSERT(self.v is None)

            # Starting time.
            self.dt = pd.to_datetime(tick_timestamp, unit='s').tz_localize('UTC').tz_convert('Asia/Kolkata')

            #
            # If this tick was formed in the same minute that we started
            # collecting live data we are not sure if we have seen all the
            # ticks for the minute, play safe, mark it partial and don't dump
            # it.
            #
            tick_minute = (self.dt.hour * 60) + self.dt.minute

            #
            # Now we wait for start of minute so don't have to check for partial
            # candles. Infact if we check we may end up marking the first
            # candle as always partial even though it may start on the 0th
            # second.
            #
            #self.partial = (tick_minute <= start_minute)
            self.partial = False

            #
            # We deal in liquid stocks and hence we expect to get tick at the
            # 0th second of the minute, if not, then let us know.
            # Note that we don't drop ticks just because we don't have ticks
            # at the 0th second, but it's still important to know if it
            # happens too frequently.
            #
            # Note: I've seen some less liquid stocks (even in NIFTY50) do not
            #       get ticks necessarily at the 0th second, so we leave a
            #       margin of 2 seconds to avoid noise.
            #
            if self.dt.second >= 2:
                PYPWarn("[%s] First tick of the minute not received till 2 seconds: %s" %
                        (broker.token_to_symbol(tick["token"]), self.dt))

            #
            # Starting time, rounded to the starting minute. Note that we do
            # it even if there were no ticks received in the 0th second. This
            # is required as we want to dump 1Min ticks starting clean at 0th
            # second in order to not confuse our consumers.
            #
            self.dt = self.dt.replace(second=0)

            self.o = ltp
            self.h = ltp
            self.l = ltp
            self.c = ltp
            self.v = 0

            #
            # If not set by Instrument::dump(), set it here.
            # This will be true only for the very first candle after program
            # start.
            #
            # TODO: This must be set only if the tick_timestamp is 09:15:00,
            #       else we may risk creating incomplete candles.
            #
            if self.volume_trade_for_the_day_at_soc == 0:
                self.volume_trade_for_the_day_at_soc = volume_trade_for_the_day
            else:
                ASSERT(volume_trade_for_the_day >= self.volume_trade_for_the_day_at_soc,
                       ("%d < %d" %
                        (volume_trade_for_the_day, self.volume_trade_for_the_day_at_soc)))

        self.h = max(self.h, ltp)
        self.l = min(self.l, ltp)

        #
        # Always set self.c so that the last value set becomes the actual
        # closing price.
        #
        self.c = ltp

        # Cumulative daily volume.
        self.volume_trade_for_the_day = volume_trade_for_the_day

        #
        # Now dump the tick into self.ltp_file.
        # Note that we dump LTP on every tick so ltp_file will always contain
        # the latest tick. self.ltp_file must have been created in the
        # constructor so we must have it here.
        #
        ASSERT(os.path.exists(self.ltp_file)
        try:
            with open(self.ltp_file, 'w') as f:
                json.dump(tick, f, indent=4)
                if cfg.verbose:
                    PYPInfo("Dumped LTP into file %s:\n%s" %
                            (self.ltp_file, json.dumps(tick, indent=4)))
        except Exception as e:
            PYPError("Failed to dump LTP into file %s:\n%s" %
                     (self.ltp_file, json.dumps(tick, indent=4)))
            #
            # Treat this as fatal as engine needs LTP for correctly placing
            # orders.
            #
            ASSERT(False)

    def asrow(self):
        ASSERT(self.dt is not None)
        ASSERT(self.o is not None)
        ASSERT(self.h is not None)
        ASSERT(self.l is not None)
        ASSERT(self.c is not None)
        ASSERT(self.v is not None)

        return [self.dt, self.o/100, self.h/100, self.l/100, self.c/100, self.v]

    def __str__(self):
        return ("%s,%.2f,%.2f,%.2f,%.2f,%d" %
                (self.dt, self.o, self.h, self.l, self.c, self.v))


class Instrument:
    ''' This class holds tick info for one token.
    '''
    def __init__(self, token):
        self.token = token
        # Total ticks received for this token.
        self.ticks_received = 0
        self._1Min_ticks_dumped = 0

        #
        # While pyhistorical is writing today's candles to $stock.live.csv we
        # hold off and not dump collected ticks. We store them in this list
        # and later dump them once CandleGenerator::historical_data_refreshed
        # becomes True.
        #
        # Note: This may result in duplicate ticks to be added to $stock.live.csv
        #       as pyhistorical might get the same tick as what pylive may
        #       generate, depending on when exactly they run, how much time
        #       pyhistorical takes, etc.
        #       THIS IS NOT AN ISSUE AS pyprocess duly removes duplicate ticks
        #       before aggregating.
        #       NO, this can be an issue, see [AVOID_DUP_HIST_AND_LIVE].
        #
        self.pending_rows = []
        self.ongoing_candle = Candle(token)
        self.symbol = broker.token_to_symbol(token)
        self.csv_file = os.path.join(cfg.historicaldir,
                                     self.symbol,
                                     "%s.live.csv" % self.symbol)

    def on_tick(self, tick):
        self.ticks_received += 1
        if cfg.verbose:
            PYPInfo("[%s] Instrument::on_tick(%s)" %
                            (self.ticks_received, broker.token_to_symbol(tick["token"])))
        ASSERT(tick["token"] == self.token,
               "%s != %s" % (tick["token"], self.token))
        self.ongoing_candle.on_tick(tick)

    def dump(self, historical_data_refreshed):
        ''' Returns True if it has dumped ticks in self.csv_file, else returns False.
        '''
        #
        # If no tick received for this stock yet, don't bother dumping.
        #
        # Q: How can we come here with no ticks received for a stock?
        # A: We dump when is_new_minute() returns True which checks if
        #    there is at least one tick received for atleast one stock in the
        #    previous minute and now we have received at least one tick for at
        #    least one stock (need not be the same as the prev one).
        #    There could be some stocks for which no ticks are received.
        #
        if (self.ongoing_candle.dt is None):
                return False

        PYPDebug("[%s] dt = %s" %
                (broker.token_to_symbol(self.token), self.ongoing_candle.dt))

        ASSERT(self.ongoing_candle.partial is not None)
        if self.ongoing_candle.partial:
            PYPWarn("Not dumping partial candle for (%s / %s): dt=%s" %
                    (broker.token_to_symbol(self.token), self.token, self.ongoing_candle.dt))

        #
        # While pyhistorical was dumping ticks-till-the-last-minute in
        # $stock.live.csv we stored our live ticks in pending_rows. Now if
        # pyhistorical is done we should dump those before adding new ticks.
        # This may result in duplicate ticks to be added. See Instrument
        # constructor to see why it's not an issue.
        #
        # [AVOID_DUP_HIST_AND_LIVE]
        # NO, it is a problem sometimes as duplicate rows may prevent
        # guess_candle_size() from correctly parsing the live.csv file and it
        # may bail out.
        #
        # f.e.
        #
        # 2023-11-17 09:15:00+05:30,682.5,682.5,679.75,680.6,205381
        # 2023-11-17 09:16:00+05:30,680.9,681.25,680.9,681.15,47699
        # 2023-11-17 09:15:00+05:30,680.55,680.55,680.55,680.55,0
        # 2023-11-17 09:16:00+05:30,680.55,682.05,680.55,681.85,92157
        # 2023-11-17 09:17:00+05:30,682.1,683.55,681.45,683.25,130116
        #
        # guess_candle_size() fails for the above as we don't have 4 candles
        # with fixed time difference.
        #
        # To avoid this we make sure that we don't repeat the candles in
        # live.csv.
        #
        dumped = False

        if (len(self.pending_rows) > 0) and historical_data_refreshed:
            #
            # Read the live.csv file and skip already present rows.
            #
            dtypes = {
                'Open': np.float64,
                'High': np.float64,
                'Low': np.float64,
                'Close': np.float64,
                'Volume': np.int64
            }

            # Columns are in the following order.
            columns = ['Open', 'High', 'Low', 'Close', 'Volume']

            live_df = None
            if os.path.exists(self.csv_file):
                stat_buf = os.stat(self.csv_file)
                if stat_buf.st_size > 0:
                    # live.csv doesn't have a header.
                    live_df = pd.read_csv(
                            self.csv_file,
                            engine="pyarrow",
                            names=columns,
                            index_col=0,
                            parse_dates=True,
                            dtype=dtypes,
                            header=None)
                    #
                    # pd.read_csv() always sets tz to UTC, though it correctly
                    # takes into account the time offset mentioned, so we just
                    # need to convert it to India timezone.
                    #
                    assert(live_df.index[0].tz == pytz.UTC)
                    live_df.index = live_df.index.tz_convert("Asia/Kolkata")
            else:
                PYPWarn("*** %s not found ***" % (self.csv_file))

            outfile = open(self.csv_file, 'a')
            csvwriter = csv.writer(outfile)

            PYPWarn("Dumping %d pending rows to %s" %
                    (len(self.pending_rows), self.csv_file))

            for row in self.pending_rows:
                # Don't dump duplicate rows in live.csv.
                if live_df is not None and (row[0] in live_df.index):
                    PYPWarn("Skipping already present live row for (%s / %s): %s" %
                            (broker.token_to_symbol(self.token), self.token, row))
                    continue

                PYPWarn("Dumping row for (%s / %s): %s" %
                        (broker.token_to_symbol(self.token), self.token, row))
                csvwriter.writerow(row)
                dumped = True

            # Dump only once.
            self.pending_rows.clear()
            outfile.close()

        #
        # XXX
        # Dump ongoing_candle latest 1Min data into appropriate file.
        # See if we can dump inline or if it's too slow and will hold the
        # caller unnecessarily. If so we can add these to some queue and from
        # there some other process can dump it.
        # We will have to see how many ticks can be queued by the websocket
        # before it starts dropping them. All the ticks queued while we are
        # holding the websocket callback, will be passed once the websocket
        # callback returns.
        #
        if ((not self.ongoing_candle.partial) and
            (self.ongoing_candle.volume_trade_for_the_day_at_soc != 0)):

                ASSERT(self.ongoing_candle.volume_trade_for_the_day != 0)
                self.ongoing_candle.v = (self.ongoing_candle.volume_trade_for_the_day -
                                self.ongoing_candle.volume_trade_for_the_day_at_soc)
                ASSERT(self.ongoing_candle.v >= 0)

                row = self.ongoing_candle.asrow()
                #
                # XXX Do this from a thread to reduce block time for the
                #     websocket handler.
                #     If we do this, we need to make sure finalize_live_data()
                #     runs only after this thread and all such threads
                #     complete. That may be hard!!
                #
                if historical_data_refreshed:
                    # Get a csv writer handle on the live.csv file.
                    outfile = open(self.csv_file, 'a')
                    csvwriter = csv.writer(outfile)

                    PYPPass("Dumping new row for (%s / %s): %s, to %s" %
                            (broker.token_to_symbol(self.token), self.token,
                             row, self.csv_file))

                    csvwriter.writerow(row)
                    #outfile.flush()
                    outfile.close()
                    dumped = True
                else:
                    self.pending_rows += [row]
                    PYPWarn("Historical data is not ready yet, cannot dump new "
                            "tick to %s, saved in pending row (total=%d): %s" %
                            (self.csv_file, len(self.pending_rows), row))

                self._1Min_ticks_dumped += 1

        #
        # Fresh candle for the next minute.
        # Take volume_trade_for_the_day from the outgoing candle and set that
        # as volume_trade_for_the_day_at_soc for the new candle starting now.
        #
        volume_trade_for_the_day = self.ongoing_candle.volume_trade_for_the_day
        self.ongoing_candle = Candle(self.token)
        self.ongoing_candle.volume_trade_for_the_day_at_soc = volume_trade_for_the_day
        return dumped

class CandleGenerator:
    instantiated = False
    def __init__(self):

        # Enforce singleton.
        ASSERT(not CandleGenerator.instantiated)
        CandleGenerator.instantiated = True

        #
        # CandleGenerator tracks ongoing candles for each instrument for which
        # we are getting ticks. self.instruments has one entry for each
        # instrument, keyed by the instrument token. Note that token is
        # specific to the Broker unlike symbol name which is fixed.
        # Entries will be added to this as we start getting ticks.
        #
        self.instruments = {}

        # How many times dump_all_instruments() was called.
        self.dump_count = 0

        # How many times finalize_live_data() was called.
        self.finalize_count = 0

        #
        # Since we can start at any time of the day we need to fetch the 1Min
        # candles till the last 1Min candle. We do this using historical APIs
        # and save those to $stock.live.csv for each stock in cfg.stocklist.
        # Once this is done the live candle generator then starts adding live
        # ticks beyond that.
        # Till this is set the live ticks should not be dumped to
        # $stock.live.csv but kept in memory and later dumped once this is
        # True.
        #
        self.historical_data_refreshed = False

        #
        # Last tick timestamp stored as datetime. This is the timestamp of the
        # last tick (corresponding to any token) received.
        # Whenever we get a tick whose minute value is different from
        # last_tick_dt, it means that this tick is the beginning of a new
        # minute and we should close all the instrument candles for the
        # previous minute and dump them.
        #
        self.last_tick_dt = None

        #
        # Make an infinite queue where the websocket's on_data() handler will
        # add ticks as soon as it is called. A dedicated thread will dequeue
        # from this queue and perform the actual on_tick() handling.
        # The reason for using a queue is to free the websocket callback
        # promptly to avoid any tick drops.
        #
        self.q = queue.Queue(maxsize=0)

        #
        # Create and start the dequeue thread for processing ticks added by
        # the websocket on_data() callback self.enqueue().
        #
        global dequeue_thread
        dequeue_thread = threading.Thread(target=self.dequeue, args=(), daemon=False)
        dequeue_thread.start()

    def enqueue(self, tick):
        ''' This is the websocket on_data() callback.
            It just adds the tick to the thread-safe queue and returns.
            The idea is to free the websocket callback promptly in case that
            causes tick drops.

            Note: It's not confirmed yet if the ticks are being dropped by the
                  websocket or AngelOne itself is not sending all ticks.
                  Note that it does add extra overhead of adding the ticks to
                  the queue and then dequeuing from the queue. This may cause
                  more CPU usage. Need to check.
        '''
        self.q.put(tick)
        PYPInfo("After enqueue (%s), self.q.qsize: %d" %
                (broker.token_to_symbol(tick["token"]), self.q.qsize()))
        if self.q.qsize() > 100:
            PYPWarn("After enqueue (%s), self.q.qsize: %d" %
                    (broker.token_to_symbol(tick["token"]), self.q.qsize()))

    def dequeue(self):
        while True:
            #
            # Read next tick queued by Websocket on_data() callback.
            # It'll block here if the queue is empty, once the enqueue()
            # thread adds new ticks, it'll be woken up.
            #
            PYPInfo("Before dequeue, self.q.qsize: %d" % (self.q.qsize()))
            if self.q.qsize() > 100:
                PYPWarn("Before dequeue, self.q.qsize: %d" % (self.q.qsize()))

            #
            # I've seen problems where websocket can get stuck, it won't
            # received anything from the broker for long periods and it
            # doesn't disconnect for 15+ minutes. Sometimes restarting the
            # pylive process may help, so do that after few seconds of
            # no-tick-data.
            # Since we can come here before the market starts, we have to
            # explicitly check for that.
            #
            while True:
                try:
                    tick = self.q.get(timeout=30)
                    break
                except queue.Empty:
                    if is_market_open():
                        PYPError("Did not see any tick for 30 seconds!")
                        PYPError("Websocket may be stuck, restarting pylive!")
                        #
                        # ASSERT causes non-zero exit which causes systemd to
                        # restart.
                        #
                        ASSERT(False)

            # stop() enqueues the None sentinel value for asking it to stop.
            if tick is None:
                PYPWarn("dequeue: exiting on receiving sentinel value!");
                break

            #
            # Call the tick handler.
            # When dumping tick data (at the start of a new minute) this can
            # take more time and and that's when the queue might build up.
            #
            self.on_tick(tick)

    def is_new_minute(self, tick_timestamp):
        ''' Check if tick_timestamp (corresponding to a newly received tick)
            marks the beginning of a new minute.
        '''
        #
        # If this is the first tick received over websocket, we have no tick
        # data to seal, so return false.
        #
        if self.last_tick_dt is None:
            return False

        #
        # Timestamp received is in IST timezone so use that for correctness.
        # Note that we don't really need to do that as all we need to find is
        # if tick_timestamp belongs to a new minute than the last timestamp
        # stored in last_tick_dt.
        #
        dt = pd.to_datetime(tick_timestamp,
                            unit='s').tz_localize('UTC').tz_convert('Asia/Kolkata')

        if cfg.verbose:
            PYPDebug("is_new_minute: dt=%s last_tick_dt=%s" % (dt, self.last_tick_dt))

        # Absolute minute of this tick and the last tick received.
        dt_minute = dt.hour*60 + dt.minute
        last_tick_dt_minute = self.last_tick_dt.hour*60 + self.last_tick_dt.minute

        #
        # Ideally we should get ticks in increasing order of timestamp, i.e.,
        # we should not get a tick with an older timestamp after a tick with a
        # newer timestamp, but we accomodate some stray ticks which may get
        # reordered.
        #
        # Let us note stray ticks and ticks that skip an entire minute.
        # We deal with liquid stocks and indices so one minute skip should not
        # really happen.
        #
        # XXX Sometimes I've seen the "Future tick received" assert fail.
        #     What happened is that the websocket had some issue, it got stuck
        #     for ~3minutes, then it automatically restarted, on_open2() got
        #     called, it subscribed again and started getting ticks again, but
        #     this tick was +3mins from last_tick_dt and hencce the assertion
        #     failed. The fundamental reason for this seems to be n/w issue
        #     where websocket is not able to get ticks from the AngelOne
        #     server!!
        #     In such a case we should restart pylive so that it can then
        #     query historical ticks and start properly.
        #
        #     Another problem is that this assert failure caused the dequeue()
        #     thread to exit while the enqueue() thread kept on enqueueing
        #     more ticks and dequeue() thread wasn't there to dump the ticks
        #     so no instrument got new ticks.
        #     Restarting pylive in this case is the best option. This means
        #     any assert failure MUST cause entire program restart.
        #
        if dt_minute < last_tick_dt_minute:
            PYPWarn("Reordered tick received %s (last_tick_dt = %s)" %
                    (dt, self.last_tick_dt))
        elif dt_minute > last_tick_dt_minute+1:
            PYPWarn("Future tick received %s (last_tick_dt = %s)" %
                    (dt, self.last_tick_dt))

        ASSERT(dt_minute >= (last_tick_dt_minute-1))
        ASSERT(dt_minute <= (last_tick_dt_minute+1))

        #
        # This tick starts a new minute?
        #
        return dt_minute > last_tick_dt_minute

    def dump_all_instruments(self):
        ''' Dump candles for all instruments.
            Returns the count of instruments actually dumped. Note that some
            instruments may not have any ticks or they may have partial candle,
            those won't be dumped.
        '''
        #
        # If we are called that means we have at least one tick, which means
        # we have at least one instrument.
        #
        ASSERT(len(self.instruments) > 0)

        #
        # Count how many times dump_all_instruments() is called.
        # Not all of these calls will result in data getting dumped to
        # files.
        #
        self.dump_count += 1

        dumped = 0
        for _, instrument in self.instruments.items():
            PYPWarn("Dumping instrument (%s / %s), dumped=%d, "
                    "historical_data_refreshed=%s" %
                    (broker.token_to_symbol(instrument.token),
                     instrument.token,
                     dumped,
                     self.historical_data_refreshed))

            #
            # instrument.dump() will return true only if it dumps at least one
            # tick in the $stock.live.csv file.
            # Only if at least one stock has dumped we would want to call
            # pyprocess to generate aggregate tick data.
            #
            if instrument.dump(self.historical_data_refreshed):
                dumped += 1

            #
            # Every time we dump, we must dump all instruments.
            # If an instrument misses any 1min tick, shout out.
            #
            if instrument._1Min_ticks_dumped != self.dump_count:
                PYPError("[%s] 1Min ticks dumped (%d) != total dump count (%d)" %
                        (broker.token_to_symbol(instrument.token),
                         instrument._1Min_ticks_dumped, self.dump_count))
        #
        # Spin a sub-process to process the latest live tick collected and
        # process it to generate finalized data (with various aggregates
        # that we need). This will create files of the form
        # $stock.final.live.<XMin>.csv.
        # These will then be loaded by the engine to update its historical
        # data. It'll also run the analysis after the last collected tick
        # data to see if any new signal needs to be generated.
        #
        # Note: finalize_live_data() will also arrange to send SIGHUP to
        #       engine for processing the updated live data.
        #
        # Note: We call finalize_live_data() only if we are able to dump
        #       at least one instrument.
        #
        if dumped > 0:
            self.finalize_live_data()
            #self.finalize_count += 1
            # Touch pylive_running to indicate liveness to engine.
            Path(cfg.pylivedir + "/pylive_running.xxx").touch()
        self.finalize_count += 1

        return dumped

    def finalize_live_data(self):
        ''' Run pyprocess to finalize prelive+live data to get intraday aggregate data
            till the last tick.
            This will create/update files of the form $stock.final.live.<XMin>.csv
        '''
        if not self.ok_to_finalize():
            PYPWarn("Not finalizing live data yet as we don't have enough live candles!")
            return

        cwd = cfg.srcdir + "/pyprocess"
        exe = cwd + "/main.py"

        # Add marker to ease log identification in the common logfile.
        PYPWarn("\n-----[pyprocess start]----------------------------")

        PYPInfo("Finalizing live data for %s using %s" % (cfg.stocklist, exe))

        #
        # Use stocklist from engine/backtest.json as that's what
        # pylive/main.py collects live ticks for.
        #
        # check=True will cause it to raise an exception if the subprocess
        # fails.
        #
        # Note: pyprocess will also send SIGHUP to engine for reloading newly
        #       added data after it's done generating the finalized data.
        #
        # Note: os.spawnl() will cause pyprocess to be spawned in the
        #       background and the control comes here immediately.
        #       This is preferred as we don't want to block this thread while
        #       pyprocess is generating aggregate data (followed by sending
        #       SIGHUP to engine). This runs in the on_tick handler and we want
        #       it to get going with processing live ticks.
        #
        # Note: pyprocess MUST send a SIGSTOP to backtester so that it doesn't
        #       read the aggregate files while pyprocess is writing them as
        #       that causes issues where backtester may read half written
        #       files and get syntax errors.
        #       Once done it should send a SIGCONT.
        #
        os.spawnl(os.P_NOWAIT, exe, "pyprocess/main.py",
                  "--stocklistcsv", cfg.stocklist, "--live")

        #result = subprocess.run([exe, "--stocklistcsv", cfg.stocklist, "--live"], cwd=cwd,
        #                        timeout=60, capture_output=False, text=True, check=True)
        #result.check_returncode()

        PYPPass("Scheduled pyprocess/main.py for finalizing live data!")
        PYPWarn("-----[pyprocess end]----------------------------\n")

    def refresh_historical_data(self):
        ''' Run pyhistorical to fetch today's 1Min data from start of day till the
            last 1Min tick. This data is stored in $stock.live.csv and then the live
            candle generator starts adding new ticks after that.
        '''
        ASSERT(self.historical_data_refreshed == False)
        cwd = cfg.srcdir + "/" + "pyhistorical"
        exe = cwd + "/main.py"

        # Add marker to ease log identification in the common logfile.
        PYPWarn("\n-----[pyhistorical start]----------------------------")

        PYPInfo("Downloading today's 1Min data for %s using %s" % (cfg.stocklist, exe))

        #
        # Hopefully it won't take more than 2 hours to download the incremental
        # historical stock data. If this is run after a long time you may want to
        # update this.
        #
        start = pd.Timestamp.now()
        result = subprocess.run([exe, "--stocklistcsv", cfg.stocklist, "--live"], cwd=cwd,
                                timeout=300, check=False, capture_output=True, text=True)
        PYPWarn("<pyhistorical output> %s </pyhistorical output>" % result.stdout)
        result.check_returncode()
        end = pd.Timestamp.now()

        #
        # Mark historical_data_refreshed so that Instrument::dump() can start
        # dumping live ticks to $stock.live.csv.
        #
        self.historical_data_refreshed = True

        PYPPass("Finished downloading today's 1Min data, took %s" % (end - start))
        PYPWarn("-----[pyhistorical end]----------------------------\n")

    def wait_for_new_minute(self):
        ''' Wait till a new minute starts.
        '''
        PYPWarn("Waiting for new minute @ %s" % pd.Timestamp.now())
        while True:
            now = pd.Timestamp.now()
            if now.second >= 0 and now.second <= 2:
                break;
            # Touch pylive_running to indicate liveness to engine.
            Path(cfg.pylivedir + "/pylive_running.xxx").touch()
            time.sleep(0.1)
        PYPWarn("New minute started @ %s" % pd.Timestamp.now())

    def ok_to_finalize(self):
        ''' Returns true if it's ok to finalize now.
            We don't finalize till we have enough 1min ticks to get a new aggregate
            candle for 3,5,10,15 Mins, i.e., we are 15Min past 09:15AM (09:30AM).
        '''
        assert(start_time is not None)

        now = pd.Timestamp.now()
        #
        # We do not finalize (i.e. run pyprocess) till 15Min after pylive is
        # restarted (if it's restarted in the middle of the trading day).
        # This is to make sure that we have at least one new candle of each
        # size (1/3/5/10/15 Min) to write in $stock.final.live.XMin.csv and
        # hence BTDataFrame::read_csv() doesn't assert that there is no new
        # tick in $stock.final.live.XMin.csv as compared to $stock.final.XMin.csv.
        #
        # See [ALOR] in BTDataFrame.cpp.
        #
        td_since_start = now - start_time
        assert(td_since_start > pd.Timedelta('1Sec'))
        assert(td_since_start < pd.Timedelta('1D'))

        sec = now.hour*3600 + now.minute*60
        return ((sec >= 9*3600+30*60 and sec < 15*3600+30*60) and
                (td_since_start >= pd.Timedelta('15Min')))

    def on_tick(self, tick):
        ''' on_tick handler for the CandleGenerator class.
            Every tick received over the websocket must be fed to this.

            Note: Do not assert in this function and any function called from
                  this function as that doesn't result in program to stop,
                  but instead it causes a meaningless message from websocket
                  claiming the callback failed.
                  Use ASSERT().
        '''
        global ticks_received
        global stale_ticks_received

        #
        # Log for the first 1000 ticks received to confirm things are working,
        # or if verbose is set in config.
        #
        if cfg.verbose or (ticks_received < 1000):
            PYPInfo("[%d] CandleGenerator::on_tick(%s)" %
                            (ticks_received, broker.token_to_symbol(tick["token"])))

        tick = broker.tick2tick(tick)

        #
        # XXX This is AngelOne specific, make it generic.
        #
        mode = tick["subscription_mode"]
        ASSERT(mode == 1 or mode == 2 or mode == 3, ("mode=%d" % mode))

        token = tick["token"]

        # AngelOne tick timestamp is in milliseconds.
        tick_timestamp = tick["exchange_timestamp"]
        tick_timestamp //= 1000

        dt = pd.to_datetime(tick_timestamp, unit='s').tz_localize('UTC').tz_convert('Asia/Kolkata')
        #
        # Ignore ticks received outside market hours.
        #
        if ((dt.hour < 9) or
            (dt.hour == 9 and dt.minute < 15) or
            (dt.hour > 15) or
            (dt.hour == 15 and dt.minute > 30)):
            pretty_tick = json.dumps(tick, indent=2)
            PYPWarn("Tick (@ %s) generated outside market hours, ignoring: %s" %
                            (dt, pretty_tick))
            return

        if self.last_tick_dt is None and dt.second > 2:
            #
            # when we start pylive on minute boundary, I see that candles for
            # the previous minute also arrive, since websocket is not done
            # sending all of them. Ignore those!
            # f.e.,
            # 2023-08-21 09:52:59+05:30
            #
            pretty_tick = json.dumps(tick, indent=2)
            PYPWarn("Previous minute tick (@ %s), ignoring: %s" %
                            (dt, pretty_tick))
            return

        #
        # Any tick received with minute less than the current minute must be
        # considered stale and dropped, else we may mess up the tick data.
        #
        # XXX We need to check if stale ticks are common or does Angelone take
        #     care of publishing ticks in strictly increasing time order, so
        #     if we get one tick at T other ticks will only be greater than T.
        #
        # Update: Stale ticks are only seen in the first minute after pylive
        #     is restarted and not seen during the run, which is good!
        #

        if self.last_tick_dt is not None:
            dt_minute = dt.hour*60 + dt.minute
            last_tick_dt_minute = self.last_tick_dt.hour*60 + self.last_tick_dt.minute
            if dt_minute < last_tick_dt_minute:
                stale_ticks_received += 1
                pretty_tick = json.dumps(tick, indent=2)
                PYPError("[%d] Stale tick (@ %s), last_tick_dt=%s, ignoring: %s" %
                                (stale_ticks_received, dt, self.last_tick_dt, pretty_tick))
                return

        # Only count ticks received in market hours.
        ticks_received += 1

        #
        # First tick that starts a new minute causes current ongoing candles
        # (for the current minute) to be sealed and dumped.
        #
        # XXX Hopefully anything after this tick will be later than this and
        #     hence won't be for the previous minute. In case we get some tick
        #     for the older minute we should ignore that with the risk that we
        #     will lose some accuracy.
        #
        is_new_minute = self.is_new_minute(tick_timestamp)

        #
        # After we are done processing this tick, store the tick_timestamp as
        # last_tick_dt, signifying the timestamp of the last tick received.
        # Whenever a tick is received whose minute value is different from
        # last_tick_dt that means it's time to seal all the minute candles and
        # dump them.
        #
        # Sometimes I've seen that AngelOne will send older ticks later (maybe
        # they arrive like that on websocket). This is possible since
        # self.last_tick_dt tracks the latest tick received for all stocks, but
        # some stock may be getting older starting tick.
        #
        # Ensure last_tick_dt should not move  backwards.
        #
        if self.last_tick_dt is None:
            self.last_tick_dt = dt
        else:
            self.last_tick_dt = max(self.last_tick_dt, dt)

        if is_new_minute:
            PYPWarn("Got new minute tick [%s], dumping all instruments!" %
                    pd.to_datetime(tick_timestamp, unit='s').
                    tz_localize('UTC').tz_convert('Asia/Kolkata'))
            #
            # dump_all_instruments() will also call finalize_live_data() to
            # generate live aggregate data after the latest 1Min candle data
            # received.
            #
            self.dump_all_instruments()

        #
        # Very first tick received for this token, create a new empty Instrument,
        # we will populate it immediately afterwards.
        #
        if token not in self.instruments:
            self.instruments[token] = Instrument(token)

        # Call on_tick() handler for the token.
        instrument = self.instruments[token]
        instrument.on_tick(tick)

        #
        # Every time we call dump_all_instruments(), at the end we must call
        # finalize_live_data(), if not then something is wrong, shout out.
        #
        if self.finalize_count != self.dump_count:
            PYPError("finalize_count (%d) != dump_count (%d)" %
                    (self.finalize_count, self.dump_count));

    def start(self):
        #
        # Clear ticks_received, just in case.
        # Note: Since we assert for singleton in CandleGenerator, this is
        #       superfluous for now.
        #
        global start_time
        start_time = pd.Timestamp.now()

        global ticks_received
        if ticks_received != 0:
            PYPWarn("Clearing ticks_received (%d)" % (ticks_received));
            ticks_received = 0
        #
        # Connect to websocket passing self.on_tick as the callback which will
        # be called for every tick received over the websocket.
        # This is done in a separate thread as the start_websocket() call is a
        # blocking call.
        #
        global ws_thread
        ws_thread = threading.Thread(target=broker.start_websocket,
                                     args=(self.enqueue,),
                                     daemon=False)
        ws_thread.start()

    def stop(self):
        # Stop websocket to not get any more ticks.
        broker.stop_websocket()

        #
        # Ask processing thread to stop after processing all queued ticks.
        # Though not needed, we add a sleep here just to make doubly sure that
        # the sentinel None tick doesn't get queued before any valid tick that
        # the websocket might queue.
        #
        time.sleep(1)
        self.q.put(None)

def init():
    ''' This runs in the context of the main process.
    '''
    # Must have already been initialized by main.
    ASSERT(broker.is_initialized())

    #
    # Touch pylive_running to indicate liveness to engine.
    # This is the first time this marker file is created, and later it's
    # updated everytime we dump live candle data or from other places like
    # while waiting for market to open or new minute to start, etc.
    #
    Path(cfg.pylivedir + "/pylive_running.xxx").touch()

    PYPInfo('CandleGenerator: init() start')
    # Create the singleton CandleGenerator object.
    global cg
    cg = CandleGenerator()
    PYPInfo('CandleGenerator: init() end')

def start():
    ''' This runs in the context of the main process.
    '''
    PYPInfo('CandleGenerator: start() start')

    #
    # Set start_minute in the beginning so that we don't consider partial
    # candles when we started before market hours
    #
    # XXX Now we always start at the start of a minute, hence we cannot have
    #     any partial candles.
    #
    #now = pd.Timestamp.now()
    #global start_minute
    #start_minute = (now.hour * 60) + now.minute

    global cg

    #
    # This will always start on a minute boundary hence Candle::partial is not
    # needed.
    #
    # XXX If this ever starts on a non-minute boundary we need to set
    #     Candle::partial
    #
    if not is_market_open():
        PYPWarn("Waiting for market open @ %s" % pd.Timestamp.now())

        while not is_market_open():
            # Touch pylive_running to indicate liveness to engine.
            Path(cfg.pylivedir + "/pylive_running.xxx").touch()
            time.sleep(0.1)

        PYPWarn("OK, market open now @ %s" % pd.Timestamp.now())

        #
        # No need to refresh historical data as we are getting ticks right
        # from the start of the day, mark it refreshed.
        #
        cg.historical_data_refreshed = True
    else:
        #
        # Once refresh_historical_data() successfully refreshes the historical
        # 1Min candles for the day, it'll set historical_data_refreshed to
        # True.
        #
        ASSERT(cg.historical_data_refreshed == False)

        #
        # We wait for a new minute to start so that the live tick can get all
        # the ticks for the minute and in the meantime historical 1Min ticks
        # for the day are also collected.
        #
        cg.wait_for_new_minute()

        #
        # Before we start collecting live data we should fill the missing
        # candles of the day by making historical APIs, so that no matter when
        # we start pylive it always gets all the live candles of the day.
        #
        # XXX We don't currently explicitly stop this thread as it runs for a
        #     very short time anyways.
        #
        global historical_thread
        historical_thread = threading.Thread(target=cg.refresh_historical_data,
                                             args=(),
                                             daemon=False)
        historical_thread.start()

    cg.start()
    PYPInfo('CandleGenerator: start() end')

def stop():
    ''' This runs in the context of the main process.
    '''
    PYPInfo('CandleGenerator: stop() start')

    cg.stop()
    global exit_now
    exit_now = True

    PYPInfo('CandleGenerator: stop() end')

def join():
    ''' This runs in the context of the main process.
    '''
    PYPInfo('CandleGenerator: join() start')

    global historical_thread
    global ws_thread
    global dequeue_thread

    # join() MUST be called after start().
    assert(ws_thread is not None)
    assert(dequeue_thread is not None)

    #
    # historical_thread is only created when pylive/broker is started after
    # trading starts.
    #
    if historical_thread is not None:
        historical_thread.join()
        PYPInfo('CandleGenerator: historical_thread exited!')

    ws_thread.join()
    PYPInfo('CandleGenerator: ws_thread exited!')

    dequeue_thread.join()
    PYPInfo('CandleGenerator: dequeue_thread exited!')

    PYPInfo('CandleGenerator: join() end')
