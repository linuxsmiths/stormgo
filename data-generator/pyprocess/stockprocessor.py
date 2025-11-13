import os,sys,time,csv,setproctitle,re
import platform
import subprocess
import pandas as pd
import config as cfg
from helpers import *
import multiprocessing
import time
from finta.finta import TA as ta

import pyarrow as pa
import pyarrow.csv as pacsv

#
# Copy-on-write is going to be the default in Pandas 3.0
# With CoW pandas will make a copy of the underlying dataframe when let's say
# two dataframe references refer to the same underlying dataframe (one may be
# a different view of the dataframe) and one of them is updated.
# This makes the behaviour more umambiguous.
#
# https://pandas.pydata.org/docs/user_guide/copy_on_write.html#copy-on-write
#
pd.options.mode.copy_on_write = True

#
# List of runner processes, one runner process handles one stock.
#
runners = []

#
# How many stocks should we process in parallel.
# Note that stock processing takes both CPU and Memory resources, so this
# should be set to no more than number of CPU cores and such that it doesn't
# cause memory thrashing.
#
# TODO: Auto determine based on available CPU and Memory resources.
#       Till then manually set it to the number of logical CPU cores.
#
parallelism = 4

#
# Offset in seconds
#
tzoffset = 0

class StockProcessor(object):
    ''' The StockProcessor class handles processing of a single stock's data.
        It involves reading the historical stock data from csv file(s) and
        processing it to add more columns. Its final goal is to dump the
        existing OHLCV columns along with the newly added columnds into a
        csv file. This csv file can then be ingested by the C++ backtester
        program that needs to use the historical data.
    '''
    #
    # intraday aggregates. Calculated for various intraday candle sizes.
    #
    # e.g.
    # "3-SMA" => 3 period SMA. Period is as per candle size, so for 15Min
    #            candle, 3-SMA is the SMA of the last 3x15Min candles.
    #
    # Note: We need {2-20}-RSI for correctly implementing is_overbought()
    #       and is_oversold(), but these make prepped history files bigger and
    #       slower to load. Uncomment these when we really start using those.
    #
    # Note: For faster computation of live aggregates, we have commented the
    #       following. Uncomment them only if being used in the code.
    #       DEMA
    #       TEMA
    #       VSMA
    #       VWAP
    #
    aggregates_i  = ("3-SMA",   "5-SMA",   "10-SMA",   "15-SMA",   "20-SMA")
    aggregates_i += ("3-EMA",   "5-EMA",   "10-EMA",   "15-EMA",   "20-EMA")
    #aggregates_i += ("3-DEMA",  "5-DEMA",  "10-DEMA",  "15-DEMA",  "20-DEMA")
    #aggregates_i += ("3-TEMA",  "5-TEMA",  "10-TEMA",  "15-TEMA",  "20-TEMA")
    aggregates_i += ("3-VSMA",  "5-VSMA",  "10-VSMA",  "15-VSMA",  "20-VSMA")
    #aggregates_i += ("3-VEMA",  "5-VEMA",  "10-VEMA",  "15-VEMA",  "20-VEMA")
    aggregates_i += ("3-RSI",   "5-RSI",   "10-RSI",   "15-RSI",   "20-RSI")
    #aggregates_i += ("2-RSI",   "3-RSI",   "4-RSI",    "5-RSI",    "6-RSI")
    #aggregates_i += ("7-RSI",   "8-RSI",   "9-RSI",    "10-RSI",   "11-RSI")
    #aggregates_i += ("12-RSI",  "13-RSI",  "14-RSI",   "15-RSI",   "16-RSI")
    #aggregates_i += ("17-RSI",  "18-RSI",  "19-RSI",   "20-RSI",   "21-RSI")
    #aggregates_i += ("3-VWAP",  "5-VWAP",  "10-VWAP",  "15-VWAP",  "20-VWAP")
    aggregates_i += ("3-High",  "5-High",  "10-High",  "15-High",  "20-High")
    aggregates_i += ("3-Low",   "5-Low",   "10-Low",   "15-Low",   "20-Low")

    #
    # Interday aggregates. Calculated for 1D and above candles only.
    #
    # e.g.
    # 5-High   => Weekly highest price.
    # 21-High  => Monthly highest price.
    # 65-High  => Quarterly highest price.
    # 260-High => Yearly highest price.
    #
    # Note: Since the numerical part in the aggregate name determines the
    #       number of samples taken, we use 5 for week's aggregate data and
    #       not 7, since we don't have samples for the weekends.
    #
    aggregates_I =  ("2-SMA",  "3-SMA",   "4-SMA",    "5-SMA",   "6-SMA")
    aggregates_I += ("7-SMA",  "8-SMA",   "9-SMA",    "10-SMA",  "11-SMA")
    aggregates_I += ("12-SMA", "13-SMA",  "14-SMA",   "15-SMA",  "16-SMA")
    aggregates_I += ("17-SMA", "18-SMA",  "19-SMA",   "20-SMA",  "21-SMA")
    aggregates_I += ("22-SMA", "23-SMA",  "24-SMA",   "25-SMA",  "26-SMA")
    aggregates_I += ("27-SMA", "28-SMA",  "29-SMA",   "30-SMA",  "31-SMA")
    aggregates_I += ("50-SMA", "100-SMA", "200-SMA",)
    #aggregates_I  = ("3-SMA",  "5-SMA",   "10-SMA",   "15-SMA",  "20-SMA", "200-SMA")
    aggregates_I += ("3-EMA",  "5-EMA",   "10-EMA",   "15-EMA",  "20-EMA")
    aggregates_I += ("3-VSMA", "5-VSMA",  "10-VSMA",  "15-VSMA", "20-VSMA")
    aggregates_I += ("3-VEMA", "5-VEMA",  "10-VEMA",  "15-VEMA", "20-VEMA")
    aggregates_I += ("2-RSI",  "3-RSI",   "4-RSI",    "5-RSI",   "6-RSI")
    aggregates_I += ("7-RSI",  "8-RSI",   "9-RSI",    "10-RSI",  "11-RSI")
    aggregates_I += ("12-RSI", "13-RSI",  "14-RSI",   "15-RSI",  "16-RSI")
    aggregates_I += ("17-RSI", "18-RSI",  "19-RSI",   "20-RSI",  "21-RSI")
    aggregates_I += ("22-RSI", "23-RSI",  "24-RSI",   "25-RSI",  "26-RSI")
    aggregates_I += ("27-RSI", "28-RSI",  "29-RSI",   "30-RSI",  "31-RSI")
    #aggregates_I += ("3-RSI",   "5-RSI",   "10-RSI",   "15-RSI",  "20-RSI")
    aggregates_I += ("2-High",  "3-High",  "4-High",   "5-High",  "6-High")
    aggregates_I += ("7-High",  "8-High",  "9-High",   "10-High", "11-High")
    aggregates_I += ("12-High", "13-High", "14-High",  "15-High", "16-High")
    aggregates_I += ("17-High", "18-High", "19-High",  "20-High", "21-High")
    aggregates_I += ("22-High", "23-High", "24-High",  "25-High", "26-High")
    aggregates_I += ("27-High", "28-High", "29-High",  "30-High", "31-High")
    # Add columns for finding monthly High.
    aggregates_I += ("43-High", "64-High", "86-High",  "107-High","129-High")
    aggregates_I += ("151-High","173-High","194-High", "216-High","237-High", "260-High")

    aggregates_I += ("2-Low",   "3-Low",  "4-Low",   "5-Low",  "6-Low")
    aggregates_I += ("7-Low",   "8-Low",  "9-Low",   "10-Low", "11-Low")
    aggregates_I += ("12-Low",  "13-Low", "14-Low",  "15-Low", "16-Low")
    aggregates_I += ("17-Low",  "18-Low", "19-Low",  "20-Low", "21-Low")
    aggregates_I += ("22-Low",  "23-Low", "24-Low",  "25-Low", "26-Low")
    aggregates_I += ("27-Low",  "28-Low", "29-Low",  "30-Low", "31-Low")
    # Add columns for finding monthly Low.
    aggregates_I += ("43-Low",  "64-Low", "86-Low",  "107-Low","129-Low")
    aggregates_I += ("151-Low", "173-Low","194-Low", "216-Low","237-Low", "260-Low")

    aggregates_I += ("3-ATR",  "5-ATR",  "14-ATR", "21-ATR",  "65-ATR",   "260-ATR")

    #
    # In "live" mode we will be only creating aggregate intraday candles, so
    # we won't be referencing aggregates_I, but just for correctness ensure
    # aggregates_I is empty.
    #
    if cfg.process_live_data:
        aggregates_I = ()

    #
    # Lookup table to efficiently get seconds corresponding to candle size.
    # Note: Though we don't have NaN candles for weekends we still use candle
    #       sizes as per the calendar time. This is because pandas resample()
    #       method does the resampling based on actual calendar time and not
    #       based on the number of samples.
    #       See comment in process().
    #
    candle_size_to_seconds = {
            '1Min': 60,
            '3Min': 60*3,
            '5Min': 60*5,
            '10Min': 60*10,
            '15Min': 60*15,
            '1H': 60*60,
            '1D': 60*60*24,         # Daily candle.
            '7D': 60*60*24*7,       # Weekly candle.
            '30D': 60*60*24*30,     # Monthly candle.
            '90D': 60*60*24*90,     # Quarterly candle.
            '365D': 60*60*24*365,   # Yearly candle.
    }

    def is_intraday_candle(candle):
        return (pd.Timedelta(candle) < pd.Timedelta('1D'))

    def __init__(self, stock):

        # One stockprocessor for every stock.
        self.stock = stock

        self.candles_fields = {
                '1Min': None,
                '3Min': None,
                '5Min': None,
                '10Min': None,
                '15Min': None,
                '1D': None,
                '7D': None,
                '30D': None,
                '90D': None,
                '365D': None,
        }

        #
        # Columns info used for pyarrow schema.
        # These are the static columns, info on dynamic columns is added
        # when the columns are added.
        #
        for candle in self.candles_fields:
            self.candles_fields[candle] = [
                    pa.field('Epoch', pa.int64()),
                    pa.field('Date', pa.timestamp('s')),
                    pa.field('Open', pa.float64()),
                    pa.field('High', pa.float64()),
                    pa.field('Low', pa.float64()),
                    pa.field('Close', pa.float64()),
                    pa.field('Volume', pa.int64()),
            ]

        #
        # Candles of various sizes.
        # Each of these is infact the entire dataframe with all the original
        # OHLCV columns read from the csv file(s) (or the resampled OHLCV
        # values) and the aggregate columns we later add.
        #
        # 'Tick' corresponds to the raw tick data received. This is the 'most
        # granular' price data. All the other candles are formed by resampling
        # this approximately.
        #
        # Note: candles['1Min'] and candles['1D'] dataframes are special.
        #       We use them to calculate various intraday and interday trends,
        #       averages etc, irrespective of the candle size being used for
        #       analysis.
        #       Obviously 5Min and 15Min candles are important as they are
        #       used by stockcache.analyze() to process each tick (5Min/15Min).
        #
        # Note: '1D' dataframe has entries only for actual trading days.
        #
        # >>> dfd
        #                              Open         Epoch     High      Low    Close   Volume
        # Date
        # 2020-01-01 09:15:00+05:30  2418.00  1.577850e+09  2438.50  2409.00  2430.55   942828
        # 2020-01-02 09:15:00+05:30  2430.00  1.577937e+09  2472.75  2422.20  2469.90  1698145
        # 2020-01-03 09:15:00+05:30  2455.00  1.578023e+09  2463.80  2441.80  2453.50  1954948
        # >> 2020-01-04 and 2020-01-05 are weekends and hence not included.
        #
        # 2020-01-06 09:15:00+05:30  2428.00  1.578282e+09  2439.35  2371.40  2389.95  2628735
        # 2020-01-07 09:15:00+05:30  2401.25  1.578369e+09  2427.85  2380.20  2420.00  3761017
        #
        # But when we calculate aggregates, f.e., 1WH we make sure we
        # calculate them for the calendar week, so 1WH is the highest price
        # for the last 1 calendar week (or 5 trading days aka 5 samples in the
        # daily candle).
        # See process(), especially the asfreq() part.
        #
        self.candles = {
                'Tick': pd.DataFrame(),
                '1Min': None,
                '3Min': None,
                '5Min': None,
                '10Min': None,
                '15Min': None,
                '1D': None,
                '7D': None,
                '30D': None,
                '90D': None,
                '365D': None,
        }

        #
        # For index stocks use only 1D+ candles, else use whatever is
        # specified in the config.
        #
        if (stock == "YF.NIFTY50" or stock == "YF.NIFTYBANK" or stock == "YF.RELIANCE" or
            stock == "YF.NIFTY100" or stock == "YF.NIFTYNEXT50"):
                self.cfg_candles = ["1D", "7D", "30D", "90D"]
        else:
                self.cfg_candles = cfg.candles

        #
        # How big is each candle in candle['Tick'] dataframe.
        # This is the lowest common denominator. All other candle sizes are
        # made by resampling this. It's typically 1 minute.
        #
        self.tick_candle_duration_secs = None

        # csv final pathname for various candle sizes.
        self.csvfinal = {
                '1Min': None,
                '3Min': None,
                '5Min': None,
                '10Min': None,
                '15Min': None,
                '1D': None,
                '7D': None,
                '30D': None,
                '90D': None,
                '365D': None,
        }

        assert(os.path.isdir(cfg.tld))
        self.csvdir = cfg.tld + "/NSE/historical/" + stock
        assert(os.path.isdir(self.csvdir))

        for candle in self.csvfinal:
            if cfg.process_live_data:
                    self.csvfinal[candle] = (self.csvdir + "/" +
                                             stock + (".final.live.%s.csv" % candle))
            else:
                    self.csvfinal[candle] = (self.csvdir + "/" +
                                             stock + (".final.%s.csv" % candle))

        #
        # List all available csv files containing the stock's historical data.
        # We depend on listdir() to fail in case of any problems
        # (ENOEXIST, EPERM, ...) with csvdir.
        #
        self.csvfiles = os.listdir(self.csvdir)
        PYPDebug('%d files found in %s: %s' %
                 (len(self.csvfiles), self.csvdir, self.csvfiles))

        #
        # Not very precise, but enough to filter unwanted non-csv files,
        # f.e. vim swap files.
        # In "live" mode we only load $stock.prelive.csv and $stock.live.csv,
        # the prelive csv contains previous trading day's full 1Min candle
        # data while live csv contains today's candle data as it's getting
        # generated.
        #
        if cfg.process_live_data:
                #
                # If $stock.live.csv file is not present and only
                # $stock.prelive.csv is present then processing it'll result
                # in $stock.final.live.<XMin>.csv files to have no new data as
                # compared to $stock.final.<XMin>.csv.
                # This causes problems when backtester attempts to load
                # live data. BTDataFrame::read_csv() expects that in live mode
                # $stock.final.live.<XMin>.csv must contain some new data on
                # top of $stock.final.<XMin>.csv. With just prelive.csv it'll
                # not be any new data so avoid generating the aggregate data
                # files.
                #
                # Not only this, even if $stock.live.csv may be present but it
                # may not have enough ticks for generating at least one
                # aggregate tick for XMin then also we should not create
                # aggregate file as it'll not have any new data.
                # pylive ensures that it doesn't call pyprocess till 15min
                # from market start so that we have enough ticks.
                #
                # Update: Now BTDataFrame::read_csv() correctly skips
                #         $stock.final.live.<XMin>.csv if it has not changed
                #         from the last time it was added.
                #
                live_csv_file = self.csvdir + "/" + ('%s.live.csv' % stock)
                if not os.path.exists(live_csv_file):
                    PYPError("%s not present, skipping live pyprocess!" % live_csv_file)
                    # Let caller know by setting self.csvfiles to None.
                    self.csvfiles = None
                    return None

                regex = re.compile('^%s.(pre)?live.csv$' % stock)
        else:
                regex = re.compile('^%s_20[0-9]{2}.*.csv$' % stock)

        self.csvfiles = list(filter(regex.search, self.csvfiles))

        #
        # Sort for loading the historical data in strict time order.
        # Note that this sorting is just for hygiene. process() does the
        # sorting of the accumulated ticks anyways so even if we load in
        # unorted order, it'll get sorted eventually.
        # Infact for live mode this sorting doesn't help since live.csv comes
        # after prelive.csv.
        #
        self.csvfiles.sort()

        # Don't proceed if nothing to process.
        if not self.csvfiles:
            raise ValueError("%s: No csv files to process!" % self.csvdir)

        PYPInfo("Using %d csv files for %s: %s" %
                (len(self.csvfiles), stock, self.csvfiles))

    def read_ohlcv(self, csvfile):
        ''' Read ohlcv data from csvfile into a pandas dataframe and return it.
            Each line in the csv file must be of the sample form:
            2015-02-02 09:15:00+05:30,171.95,172.4,171.2,172.35,54661
        '''
        #
        # Note: Some csv files have a header line to tell about the various
        #       fields. We handle that by forcing dtype=dtypes to read_csv(),
        #       so that it throws an exception if some line in csv does not
        #       comply. If we get that exception, we assume that it's because
        #       of the header line and we do one more pass this time asking
        #       read_csv() to treat first line as the header.
        #       If there is any other error in csv, it will fail the second
        #       time and then we will bail out.
        #
        dtypes = {
                    'Open': np.float64,
                    'High': np.float64,
                    'Low': np.float64,
                    'Close': np.float64,
                    'Volume': np.int64
        }

        #
        # We assume columns are in the following order.
        # Some csvs have extra columns which we ignore as of now.
        #
        # TODO: Some csvs already contain technical data like various moving
        #       averages etc, if so, make use of that.
        #
        columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        try:
            #
            # TODO: Use pyarrow.csv.read_csv()
            #
            # Actually pandas 1.4 has added support for pyarrow and it can be
            # specified using engine=pyarrow parameter. That way we can use
            # the exact same pd.read_csv() without any changes.
            #
            # Note: pyarrow engine gives more than 2x speedup over the c
            #       engine.
            #
            df = pd.read_csv(
                    csvfile,
                    #engine="c",
                    engine="pyarrow",
                    names=columns,
                    index_col=0,
                    parse_dates=True,
                    dtype=dtypes)
        except (ValueError, TypeError):
            PYPWarn(
                "pd.read_csv() threw ValueError/TypeError while parsing "
                "csvfile %s, most probably the first row is the header, "
                "re-trying with header=0. Consider removing the header to "
                "avoid double parsing in future!" % csvfile)
            df = pd.read_csv(
                    csvfile,
                    #engine="c",
                    engine="pyarrow",
                    names=columns,
                    index_col=0,
                    parse_dates=True,
                    dtype=dtypes,
                    header=0)

        #
        # Directory containing the csv file.
        # This should be same for all csv files sent for loading.
        #
        csvdir = os.path.dirname(csvfile)
        assert(len(self.csvdir) > 0)
        assert(csvdir == self.csvdir)

        #
        # PERF:
        # Remove timezone info.
        # This is unnecessary and hurts performance.
        #
        # TODO: Remove tz info from the csv files, to avoid this extra step.
        #       2015-02-02 09:15:00+05:30 -> 2015-02-02 09:15:00
        #
        df.index = df.index.tz_convert("Asia/Kolkata")
        df.index = df.index.tz_localize(None)

        #
        # Since we are removing the timezone we store the offset to be used
        # for timestamp() in tzoffset. This allows us to correctly generate
        # Epoch value for the local timezone (for which the historical data
        # is present).
        #
        # XXX Open this up if you want the Epoch column to contain
        #     time-since-epoch in IST, o/w we generate it in UTC.
        #     Currently we want the timezone to be UTC and not the local
        #     timezone, that keeps many things simple.
        #
        #global tzoffset
        #tzoffset = -(5.5 * 3600)

        #
        # Perform required cleanups on the loaded dataframe.
        # It fixes some well known issues with historical tick data.
        #
        df = self.clean_ohlcv(csvfile, df)
        if len(df) == 0:
                PYPWarn("No valid tick left after cleanup: %s" % (csvfile))
                return

        # Must have 5 columns.
        assert(df.shape[1] == 5)
        # At least one row.
        assert(df.shape[0] != 0)
        # Index must be of type pd.Timestamp.
        assert(df.index.inferred_type == 'datetime64')
        # Each other column must be of type float.
        assert(type(df['Open'][0]) == np.float64)
        assert(type(df['High'][0]) == np.float64)
        assert(type(df['Low'][0]) == np.float64)
        assert(type(df['Close'][0]) == np.float64)
        assert(type(df['Volume'][0]) == np.float64 or
               type(df['Volume'][0]) == np.int64)

        #
        # Make sure all csv files have the same candle size data, else it
        # causes nothing but confusion.
        #
        if self.tick_candle_duration_secs is not None:
            assert(guess_candle_size(df, csvfile) == self.tick_candle_duration_secs)
        else:
            self.tick_candle_duration_secs = guess_candle_size(df, csvfile)

        #
        # For live mode we deal only with 1Min candles.
        #
        if cfg.process_live_data:
            assert(self.tick_candle_duration_secs == 60)

        #
        # Append this new csv to the ticks data.
        # Note that we don't post-process the data like sorting, removing
        # duplicates, etc, now. We do it all at once in process() once we
        # have the complete ticks.
        #
        self.candles['Tick'] = pd.concat((self.candles['Tick'], df))
        #print("%s\n", self.candles['Tick'])

        return

    def load(self):
        ''' Read all the csv files from the sorted list and add candles to
            candles['Tick']. Once this returns we have all the tick data
            loaded in self.candles['Tick']. This can then be post-processed
            to clean it and generate additional aggregate columns.
        '''
        for csvfile in self.csvfiles:
            csv_abspath = self.csvdir + '/' + csvfile
            PYPInfo("Processing %s" % csv_abspath)
            self.read_ohlcv(csv_abspath)
            #if csvfile == self.stock + ".live.csv":
            PYPInfo("[%s] Total ticks now: %d" % (csvfile, len(self.candles['Tick'])))

    def clean_ohlcv(self, csvfile, df_tick):
        ''' Cleanup OHLCV data. Basically this removes any extra tick that is
            sometimes present in the historical data.

            IT MUST ONLY BE CALLED FOR 1MIN TICK DATA.

            Param csvfile is only used for logging.
        '''
        #
        # Perform cleanup for known anomalies in historical dataset.
        # THIS MUST BE DONE AFTER CONVERTING THE TIMEZONE SINCE MANY OF THESE
        # CLEANUPS ARE DONE BASED ON TIME.
        #
        # AngelOne historical APIs have been seen to return some invalid ticks
        # for 09:14.
        #
        # 2022-01-03 15:28:00+05:30,1469.35,1470.5,1467.71,1470.5,1602
        # 2022-01-04 09:14:00+05:30,1475.92,1482.16,1473.95,1479.73,6918 <-- BAD
        # 2022-01-04 09:15:00+05:30,1479.53,1481.15,1476.55,1479.42,5757
        #
        # and some invalid ticks after 15:29
        # Some of these invalid ticks are actually Deepawali Muhurat trading.
        # We are not equipped to handle candles outside regular trading hours,
        # so better drop them.
        #
        # 2022-10-21 15:29:00+05:30,846.9,847.9,844.3,847.9,3552
        # 2022-10-24 18:07:00+05:30,855.0,855.0,855.0,855.0,886 <-- BAD
        # 2022-10-24 18:15:00+05:30,855.05,857.0,847.9,851.95,7329 <-- BAD
        #
        # 2018-11-06 15:29:00+05:30,43.15,43.5,43.1,43.1,21405
        # 2018-11-07 17:15:00+05:30,43.5,43.5,43.5,43.5,0
        # 2018-11-07 17:22:00+05:30,44.15,44.15,44.15,44.15,6710
        #
        df_outside_trading_hours = (
            df_tick[(df_tick.index.hour < 9) |
                     ((df_tick.index.hour == 9) & (df_tick.index.minute < 15)) |
                    (df_tick.index.hour > 15) |
                     ((df_tick.index.hour == 15) & (df_tick.index.minute > 29))])

        if len(df_outside_trading_hours) > 0:
                PYPWarn("Dropping bad rows (outside trading time) from csvfile %s:\n%s" %
                                (csvfile, df_outside_trading_hours.to_string()))
                df_tick.drop(df_outside_trading_hours.index, inplace=True)

        #
        # Drop data for (Diwali) Muhurat Trading.
        # This is outside the usual trading hours and we cannot handle that.
        # TODO: Add muhurat trading days for more years.
        #
        muhurat = [
                "2015-11-11",
                "2016-10-30",
                "2017-07-10", # This is not muhurat, but some technical glitch
                              # in NSE prevented trading till 12:30PM and hence
                              # there's no data for that period.
                "2017-10-19",
                "2018-11-07",
                "2019-10-27",
                "2020-02-01", # This is union budget.
                "2020-11-14",
                "2021-02-24", # This is not muhurat, but some technical glitch
                              # in NSE prevented trading for almost entire day
                              # and hence there's no data for that period.
                "2021-11-04",
                "2022-10-24",
                "2023-11-12",
        ]

        muhurat_df = pd.DataFrame()
        for md in muhurat:
                ymd = md.split('-')
                this_muhurat = (
                        df_tick[(df_tick.index.year == int(ymd[0])) &
                                (df_tick.index.month == int(ymd[1])) &
                                (df_tick.index.day == int(ymd[2]))])
                muhurat_df = pd.concat([muhurat_df, this_muhurat])

        if len(muhurat_df) > 0:
                PYPWarn("Dropping muhurat rows from csvfile %s:\n%s" %
                                (csvfile, muhurat_df.to_string()))
                df_tick.drop(muhurat_df.index, inplace=True)

        #
        # Some stocks have known cleanliness issues for given dates, exclude
        # those dates.
        #
        per_stock_unclean = {
                #
                # TATASTEEL split 1 to 10 on Jul 29 2022, AngelOne has fixed
                # the historical data but data for 26th and 27th Jul is not
                # fixed.
                # XXX TATASTEEL has more issues, exclude it using exclude.csv.
                #
                "TATASTEEL": ["2022-07-26", "2022-07-27"]

                # TORNTPHARM has a split on 2018-04-13 which appears like a fall.
                # exclude using exclude.csv.

        }

        if self.stock in per_stock_unclean:
                psu_df = pd.DataFrame()
                for psud in per_stock_unclean[self.stock]:
                        ymd = psud.split('-')
                        this_psu = (
                                df_tick[(df_tick.index.year == int(ymd[0])) &
                                        (df_tick.index.month == int(ymd[1])) &
                                        (df_tick.index.day == int(ymd[2]))])
                        psu_df = pd.concat([psu_df, this_psu])

                if len(psu_df) > 0:
                        PYPWarn("Dropping (per-stock) unclean rows from csvfile %s:\n%s" %
                                        (csvfile, psu_df.to_string()))
                        df_tick.drop(psu_df.index, inplace=True)

        #
        # If the volume is -ve, fix it, f.e., BRITANNIA/BRITANNIA_2024.partial.csv has
        # a -ve volume (-2044).
        # 2024-03-02 11:18:00+05:30,4927.05,4927.05,4927.05,4927.05,-2044
        #
        if np.any(df_tick['Volume'] < 0):
                PYPWarn("[%s] Volume < 0:\n%s" % (csvfile,
                        df_tick[df_tick['Volume'] < 0].to_string()))
                df_tick['Volume'] = np.maximum(df_tick['Volume'], 0)

        #
        # Fix for all Open, High, Low, Close.
        # If <= 0, set them to 0.01, as <= 0 is not a valid value for price.
        #
        if np.any(df_tick['Open'] <= 0):
                PYPWarn("[%s] Open <= 0:\n%s" % (csvfile,
                        df_tick[df_tick['Open'] <= 0].to_string()))
                df_tick['Open'] = np.maximum(df_tick['Open'], 0.01)

        if np.any(df_tick['High'] <= 0):
                PYPWarn("[%s] High <= 0:\n%s" % (csvfile,
                        df_tick[df_tick['High'] <= 0].to_string()))
                df_tick['High'] = np.maximum(df_tick['High'], 0.01)

        if np.any(df_tick['Low'] <= 0):
                PYPWarn("[%s] Low <= 0:\n%s" % (csvfile,
                        df_tick[df_tick['Low'] <= 0].to_string()))
                df_tick['Low'] = np.maximum(df_tick['Low'], 0.01)

        if np.any(df_tick['Close'] <= 0):
                PYPWarn("[%s] Close <= 0:\n%s" % (csvfile,
                        df_tick[df_tick['Close'] <= 0].to_string()))
                df_tick['Close'] = np.maximum(df_tick['Close'], 0.01)

        #
        # Sometimes I've seen that historical data will have High and Low not
        # set correctly. It may be wrong data but the best we can do is to
        # clean it so that Low and High columns have the lowest and highest
        # calue among all the OHLC columns.
        #
        # Example of a bad row with H < O.
        # DRREDDY
        # 2023-03-03 09:15:00+05:30,4412.6,4409.95,4380.05,4380.8,3406
        #
        if np.any((df_tick['High'] < df_tick['Open']) |
                  (df_tick['High'] < df_tick['Close'])):
                PYPWarn("[%s] High < Open:\n%s" % (csvfile,
                        df_tick[df_tick['High'].lt(df_tick['Open'])].to_string()))
                PYPWarn("[%s] High < Close:\n%s" % (csvfile,
                        df_tick[df_tick['High'].lt(df_tick['Close'])].to_string()))

                df_tick['High'] = df_tick[['Open', 'Close']].max(axis=1)

        if np.any((df_tick['Low'] > df_tick['Open']) |
                  (df_tick['Low'] > df_tick['Close'])):
                PYPWarn("[%s] Low > Open:\n%s" % (csvfile,
                        df_tick[df_tick['Low'].gt(df_tick['Open'])].to_string()))
                PYPWarn("[%s] Low > Close:\n%s" % (csvfile,
                        df_tick[df_tick['Low'].gt(df_tick['Close'])].to_string()))

                df_tick['Low'] = df_tick[['Open', 'Close']].min(axis=1)


        #
        # Following 09:15 rows are known to be missing from most stocks.
        # Copy them from the next (09:16) row. We do this since just this one
        # row makes the entire day unusable as we remove days with 09:15
        # candle missing.
        #
        copy_from_next = [
                "2020-11-23 09:15:00",
                "2020-11-24 09:15:00",
                "2020-11-25 09:15:00",
                "2020-11-26 09:15:00",
                "2020-11-27 09:15:00"
        ]

        added = False
        for d in copy_from_next:
                #
                # d+1Min row must be present and d row must not be present.
                #
                if pd.Timestamp(d) + pd.Timedelta("1Min") not in df_tick.index:
                        continue
                if pd.Timestamp(d) in df_tick.index:
                        continue
                PYPWarn("[%s] Copying %s -> %s" %
                        (csvfile,
                         pd.Timestamp(d) + pd.Timedelta("1Min"),
                         pd.Timestamp(d)))
                df_tick.loc[pd.Timestamp(d)] = df_tick.loc[pd.Timestamp(d) + pd.Timedelta("1Min")]
                added = True

        if added:
                df_tick.sort_index()

        #
        # 15:29 rows are known to be missing from some stocks.
        # Copy them from the prev (15:28) row. Since we repeat the previous
        # row there will be some inaccuracy but hopefully it won't be too
        # different from reality.
        #
        stock_1529_missing = [
                "UPL"
        ]

        if self.stock in stock_1529_missing:
                added = False
                for d in df_tick.index:
                        ts = pd.Timestamp(d)
                        if ts.hour != 15 or ts.minute != 28:
                                continue
                        ts29 = ts.replace(hour=15, minute=29)

                        # If 15:29 row is already present, continue.
                        if ts29 in df_tick.index:
                                continue

                        # else, copy it from 15:28.
                        PYPWarn("[%s] Copying %s -> %s" % (csvfile, ts, ts29))
                        df_tick.loc[ts29] = df_tick.loc[ts]
                        added = True

                if added:
                        df_tick.sort_index()

        #
        # If it's still not clean, this is probably the case of some
        # missing/extra ticks.
        # Remove days with incomplete data. Remove entire day if at least
        # one tick is missing for the day. "Clean days" will have 375 ticks.
        #
        # XXX Since this will cause entire day to be removed thus will affect
        #     grouping of days in a week. Let's think more if we should clean
        #     or not. Open the following check if we want to clean.
        #
        # XXX We should not do this for live mode where we are generating just
        #     intraday candles.
        #     Not actually, see XXX below.
        #
        if (len(df_tick) / 375) != (len(df_tick) // 375):
                PYPWarn("Dataframe for csvfile %s still not clean (has %d rows)!" %
                        (csvfile, len(df_tick)))
                #
                # Drop days which do not have the starting (09:15) candle.
                # These show up as starting time !=09:15 in the final.1D csv.
                # This causes assert failure in StockInfo::candle_from_epoch().
                #
                # APOLLOHOSP has missing 09:15 candle for 2020-11-24 to
                # 2020-11-27, this causes backtests ending in one of these
                # dates to fail with 'm_orderstatus[i].m_buy_inflight == 0'
                # assertion if a Buy order is placed on 23rd Nov.
                #
                # Most stocks have missing 09:15 candle for 2020-11-23 to
                # 2020-11-27.
                #
                # TODO: See if we should fix these by copying the OHLCV data
                #       from 09:16 to 09:15.
                #
                # XXX   In live mode if we don't have the initial candles we
                #       end up skipping the entire live data.
                #       Yes, but now we get historical data for the day from
                #       AngelOne if we restart pylive at any time during the
                #       day, so we should not have this case!
                #
                df_915 = df_tick[((df_tick.index.hour == 9) & (df_tick.index.minute == 15))]
                clean_days = df_915.set_index(df_915.index.year*365+df_915.index.dayofyear).index
                df_tick = df_tick[(df_tick.index.year*365+df_tick.index.dayofyear).isin(clean_days)]

                if cfg.omit_partial_days:
                    PYPWarn("Omitting partial days!")
                    count = df_tick.set_index([df_tick.index, df_tick.index.year*365+df_tick.index.dayofyear]).groupby(level=1).count()
                    clean_days = count[count.Open == 375].index
                    df_tick = df_tick[(df_tick.index.year*365+df_tick.index.dayofyear).isin(clean_days)]
                    # Must be clean now.
                    assert(len(df_tick)/375 == len(df_tick)//375)

        return df_tick

    def add_aggregate(self, candle, aggr):
        #
        # aggr is of the form <N>-<aggregate> where N is the number of
        # candles (of size 'candle') over which 'aggregate' is computed.
        # It calculates the aggregate column and adds it to the dataframe
        # self.candles[candle].
        #
        # XXX This is deprecated as it results in the following warning
        #
        #     "PerformanceWarning: DataFrame is highly fragmented.  This is
        #      usually the result of calling `frame.insert` many times, which
        #      has poor performance.  Consider joining all columns at once using
        #      pd.concat(axis=1) instead. To get a de-fragmented frame, use
        #      `newframe = frame.copy()`"
        #
        #      The reason is because it adds every aggregate column separately
        #      causing the DataFrame to become fragmented.
        #
        #      Use get_aggregate_column() instead.
        #
        tokens = aggr.split('-')
        assert(len(tokens) == 2)
        assert(type(int(tokens[0])) == int)

        #
        # TODO: pa.float32 gives 4 decimal places.
        #       See if we can get 2 decimal places to further save space in
        #       final.csv.
        #
        self.candles_fields[candle] += [
                pa.field(('%s-%s' % (tokens[0], tokens[1])), pa.float32())]

        # We should compute aggregate for 3Min and above candles.
        assert(pd.Timedelta(candle) >= pd.Timedelta('3Min'))

        df = self.candles[candle]

        #
        # TODO: Replace with match-case when python is upgraded to 3.10+.
        #
        #
        # Note: Since we treat the entire 1Min dataframe as one and find the
        #       rolling max/min for that this will result in valid values to be
        #       incorrectly set for rows where it doesn't make sense, f.e.,
        #       1hH doesn't make sense for first 59 candles of the day, but
        #       we will calculate it based on the last 59 candles of previous
        #       day.
        #       Caller (get_lastN_high_low()) should be aware of this and not
        #       return meaningless values to the caller.
        # TODO: See if we can set those to NaN in the first place.
        #
        #
        # Replace NaN with an inert value.
        # For calculating max() an inert value is -INFINITY as it'll
        # not change the outcome, similarly for min(), INFINITY is the
        # inert value.
        #
        # asfreq('1D') adds rows for holidays with the column value set to
        # NaN. With that, one week high actually represents the high for the
        # trading week (5 week days + 2 week end holidays).
        #
        # Note: For all aggregate candles fill NaNs with 0s. This is required
        #       as the C++ DataFrame read() method uses nan_policy::dont_pad_with_nans,
        #       which results in these aggregate vectors to not be padded with
        #       nans and instead it skips those rows, resulting in aggregate
        #       vectors to be of smaller size causing offsets to be messed up.
        #
        # TODO: Fix it to use pd.concat(axis=1).
        #       https://stackoverflow.com/questions/74360872/how-can-i-use-pd-concat-to-join-all-columns-at-once-instead-of-calling-frame-i
        #
        if tokens[1] == 'High':
            #dfc = df.asfreq(candle).Close.fillna(-INFINITY)
            dfc = df.Close.fillna(-INFINITY)
            df[aggr] = dfc.rolling(window=int(tokens[0])).max().fillna(0)
        elif tokens[1] == 'Low':
            #dfc = df.asfreq(candle).Close.fillna(INFINITY)
            dfc = df.Close.fillna(INFINITY)
            df[aggr] = dfc.rolling(window=int(tokens[0])).min().fillna(0)
        elif tokens[1] == 'ATR':
            df[aggr] = ta.ATR(df, period=int(tokens[0])).fillna(0)
        elif tokens[1] == 'SMA':
            df[aggr] = ta.SMA(df, period=int(tokens[0])).fillna(0)
        elif tokens[1] == 'EMA':
            df[aggr] = ta.EMA(df, period=int(tokens[0])).fillna(0)
        elif tokens[1] == 'DEMA':
            df[aggr] = ta.DEMA(df, period=int(tokens[0])).fillna(0)
        elif tokens[1] == 'TEMA':
            df[aggr] = ta.TEMA(df, period=int(tokens[0])).fillna(0)
        elif tokens[1] == 'RSI':
            df[aggr] = ta.RSI(df, period=int(tokens[0])).fillna(0)
        elif tokens[1] == 'VWAP':
            df[aggr] = ta.VWAPN(df, period=int(tokens[0])).fillna(0)
        elif tokens[1] == 'VEMA':
            df[aggr] = ta.EMA(df, period=int(tokens[0]), column="volume").fillna(0)
        elif tokens[1] == 'VSMA':
            df[aggr] = ta.SMA(df, period=int(tokens[0]), column="volume").fillna(0)
        else:
            assert False, ("Unsupported aggregate %s" % tokens[1])

    def get_aggregate_column(self, candle, aggr):
        ''' Given an aggr string of the form <N>-<aggregate> where N is the
            number of candles (of size 'candle') over which 'aggregate' is
            computed, return the corresponding pandas Series (column).

            This should be used in place of add_aggregate() as that results in
            fragmentation and performance issues.
        '''

        #
        # aggr is of the form <N>-<aggregate> where N is the number of
        # candles (of size 'candle') over which 'aggregate' is computed.
        #
        tokens = aggr.split('-')
        assert(len(tokens) == 2)
        assert(type(int(tokens[0])) == int)

        #
        # TODO: pa.float32 gives 4 decimal places.
        #       See if we can get 2 decimal places to further save space in
        #       final.csv.
        #
        self.candles_fields[candle] += [
                pa.field(('%s-%s' % (tokens[0], tokens[1])), pa.float32())]

        # We should compute aggregate for 3Min and above candles.
        assert(pd.Timedelta(candle) >= pd.Timedelta('3Min'))

        df = self.candles[candle]

        #
        # TODO: Replace with match-case when python is upgraded to 3.10+.
        #
        # Note: Since we treat the entire 1Min dataframe as one and find the
        #       rolling max/min for that this will result in valid values to be
        #       incorrectly set for rows where it doesn't make sense, f.e.,
        #       1hH doesn't make sense for first 59 candles of the day, but
        #       we will calculate it based on the last 59 candles of previous
        #       day.
        #       Caller (get_lastN_high_low()) should be aware of this and not
        #       return meaningless values to the caller.
        # TODO: See if we can set those to NaN in the first place.
        #
        # Replace NaN with an inert value.
        # For calculating max() an inert value is -INFINITY as it'll
        # not change the outcome, similarly for min(), INFINITY is the
        # inert value.
        #
        # asfreq('1D') adds rows for holidays with the column value set to
        # NaN. With that, one week high actually represents the high for the
        # trading week (5 week days + 2 week end holidays).
        #
        # Note: For all aggregate candles fill NaNs with 0s. This is required
        #       as the C++ DataFrame read() method uses nan_policy::dont_pad_with_nans,
        #       which results in these aggregate vectors to not be padded with
        #       nans and instead it skips those rows, resulting in aggregate
        #       vectors to be of smaller size causing offsets to be messed up.
        #
        # Note: The .rename(aggr) in the end is needed to make sure that the
        #       Pandas Series that we return has the correct name, so that
        #       when the caller concats it to the df it has the correct name.
        #
        if tokens[1] == 'High':
            #dfc = df.asfreq(candle).Close.fillna(-INFINITY)
            dfc = df.Close.fillna(-INFINITY)
            return dfc.rolling(window=int(tokens[0])).max().fillna(0).rename(aggr)
        elif tokens[1] == 'Low':
            #dfc = df.asfreq(candle).Close.fillna(INFINITY)
            dfc = df.Close.fillna(INFINITY)
            return dfc.rolling(window=int(tokens[0])).min().fillna(0).rename(aggr)
        elif tokens[1] == 'ATR':
            return ta.ATR(df, period=int(tokens[0])).fillna(0).rename(aggr)
        elif tokens[1] == 'SMA':
            return ta.SMA(df, period=int(tokens[0])).fillna(0).rename(aggr)
        elif tokens[1] == 'EMA':
            return ta.EMA(df, period=int(tokens[0])).fillna(0).rename(aggr)
        elif tokens[1] == 'DEMA':
            return ta.DEMA(df, period=int(tokens[0])).fillna(0).rename(aggr)
        elif tokens[1] == 'TEMA':
            return ta.TEMA(df, period=int(tokens[0])).fillna(0).rename(aggr)
        elif tokens[1] == 'RSI':
            return ta.RSI(df, period=int(tokens[0])).fillna(0).rename(aggr)
        elif tokens[1] == 'VWAP':
            return ta.VWAPN(df, period=int(tokens[0])).fillna(0).rename(aggr)
        elif tokens[1] == 'VEMA':
            return ta.EMA(df, period=int(tokens[0]), column="volume").fillna(0).rename(aggr)
        elif tokens[1] == 'VSMA':
            return ta.SMA(df, period=int(tokens[0]), column="volume").fillna(0).rename(aggr)
        else:
            assert False, ("Unsupported aggregate %s" % tokens[1])

    def process(self):
        ''' Perform post processing on the loaded historical tick data.
            This will create candles of various sizes and add required
            aggregate columns (technical indicators).
        '''
        #
        # Dataframe corresponding to the "ticks" candle.
        # process() must be called only after historical tick data has
        # been loaded using read_csv().
        # This MUST have been populated when process() is called.
        #
        df_tick_raw = self.candles['Tick']
        assert(not df_tick_raw.empty)
        assert(df_tick_raw.index.inferred_type == 'datetime64')

        #
        # tick_candle_duration_secs MUST have been set by read_ohlcv().
        # Most common historical data has 1Min candles.
        # We also need to process some 1D candles like NIFTY_50 index.
        #
        assert(self.tick_candle_duration_secs is not None)
        assert(self.tick_candle_duration_secs == 60 or
               self.tick_candle_duration_secs == 24*60*60)

        #
        # Clean the tick data.
        # 1. Remove duplicate ticks.
        # 2. Sort in ascending order.
        #
        if df_tick_raw.index.has_duplicates:
            df_tick_raw = df_tick_raw[~df_tick_raw.index.duplicated(keep='first')]
            assert(not df_tick_raw.index.has_duplicates)
        if not df_tick_raw.index.is_monotonic_increasing:
            df_tick_raw = df_tick_raw.sort_index(ascending=True)
            assert(df_tick_raw.index.is_monotonic_increasing)

        #
        # Add unix timestamp for faster search of required row based on
        # timestamp.
        # Note that this gives a false SettingWithCopyWarning, hence we disable
        # it for the duration of this call.
        #
        # See detailed comment in dump_holiday_json().
        #
        pd.options.mode.chained_assignment = None

        #
        # Unless directed by config, calculate Epoch before aggregation.
        # See details in config.py.
        #
        if not cfg.calculate_epoch_after_aggregation:
                df_tick_raw['Epoch'] = df_tick_raw.index.map(
                                        mapper=(lambda x: int(x.timestamp())+tzoffset))

        pd.options.mode.chained_assignment = 'warn'

        # Make sure all columns have equal rows.
        assert(df_tick_raw['Open'].size == df_tick_raw.index.size)
        assert(df_tick_raw['High'].size == df_tick_raw.index.size)
        assert(df_tick_raw['Low'].size == df_tick_raw.index.size)
        assert(df_tick_raw['Close'].size == df_tick_raw.index.size)
        assert(df_tick_raw['Volume'].size == df_tick_raw.index.size)
        if not cfg.calculate_epoch_after_aggregation:
                assert(df_tick_raw['Epoch'].size == df_tick_raw.index.size)

        #
        # Create candles of all required sizes by downsampling the 'Tick'
        # candles to the desired candle size.
        # origin='start' is needed to make sure that aggregated candles start
        # from 09:15 instead of the default 09:00 for '1H' or 00:00 for '1D'.
        #
        for candle in self.cfg_candles:
            #
            # We can only downsample from a smaller tick to a larger candle.
            # Upsampled candles contain inaccurate extrapolated info.
            # Don't fool the strategy engines with inaccurate upsampled data.
            #
            if (self.candle_size_to_seconds[candle] <
                self.tick_candle_duration_secs):
                PYPError("[%s] Refusing to upsample from ticks of %d seconds "
                         "to %s candle!" %
                         (self.stock,
                          self.tick_candle_duration_secs,
                          candle))
                # Better to bail out, as this is unexpected.
                assert(False)
                continue

            #
            # We do not want df_tick.resample() below to generate resampled
            # candles w/ incomplete groups, i.e., if suppose we have 1Min
            # ticks for 09:15 to 09:22, we only want the full 5Min candle from
            # 09:15 to 09:19 to be created but we don't want the imcomplete
            # 1Min candle @ 09:20 to be created yet, which will yield a
            # complete 5Min candle only after we have the last tick 09:24.
            #
            # resample() doesn't have any option to not resample if incomplete
            # group data is present, so we remove the extra ticks from the
            # end to make sure we always have completed 1Min ticks for which
            # the correct resampled candles can be formed.
            #
            # This is specially a problem with live data since partial candles
            # will make the engine believe that the new aggregate candle is ready.
            #
            # Another way of doing this could be as described here:
            # https://stackoverflow.com/questions/51063353/pandas-resample-skip-incomplete-groups-at-the-start
            #
            # XXX Another problem is like this
            # After properly dropping the ticks we are guaranteed to not
            # generate incomplete aggregate candles but we still have this
            # problem. Let's say at 2:21 we get a new 1min tick and we run
            # pyprocess. This will correctly remove all ticks till 2:15 so
            # that we don't wrongly generate the 15Min tick starting at 2:15
            # (till 2:30), but it nevertheless generates 15Min aggregates too
            # and saves it in $stock.final.live.15Min.csv, though there's no
            # net new data since last time, but the files mtime changes which
            # causes BTDataFrame::read_csv() to believe that something
            # changed and it tries to load it which then fails as there's
            # nothing new.
            #
            # Update: BTDataFrame::read_csv() now correctly skips
            #         $stock.final.live.<XMin>.csv if it has not changed since
            #         the last time it was added.
            #
            if cfg.process_live_data and candle != "1Min":
                # How many minutes in the resampled candle.
                candle_minute = self.candle_size_to_seconds[candle] // 60
                assert(candle_minute >= 3)

                last_tick_ts = df_tick_raw.index[-1]
                # Last tick's minute-since-market-start.
                last_tick_minute_sms = get_seconds_since_market_start(last_tick_ts) // 60

                #
                # +1 because 5Min candle spans from 09:15 to 09:19 and the
                # next one starts from 09:20, so if the last tick we have is
                # the one starting @ 09:19 (and ending at 09:20), last_tick_minute_sms
                # is 4, then we have complete rows for a 5Min candle and we don't want
                # to drop anything.
                #
                extra_rows = (last_tick_minute_sms + 1) % candle_minute

                if extra_rows != 0:
                    #
                    # we need to drop one or more ticks, make a deep copy and
                    # then we will make inplace changes to that.
                    #
                    df_tick = df_tick_raw.copy(deep=True)

                    #
                    # We drop one row at a time and not remove extra_rows
                    # rows at once. This is because there might be some
                    # missing tick so extra_rows is not the correct number we
                    # want to drop.
                    #
                    # Note that while we drop incomplete groups, but later
                    # when the incomplete group is not the last group we will
                    # still form the aggregate (albeit with lesser component
                    # ticks). f.e., in the following when process() is called
                    # at 11:45, the last 1Min tick for BPCL is the one at
                    # 11:42, so it will skip and won't generate the 5Min
                    # aggregate at 11:40. But when it's called at 11:50, it'll
                    # form two new 5Min candles, 11:40 and 11:45.
                    #
                    # 2024-04-02 11:39:00+05:30,616.8,617.3,616.8,617.15,8182
                    # 2024-04-02 11:40:00+05:30,617.15,617.2,616.8,617.05,3002
                    # 2024-04-02 11:41:00+05:30,617.05,617.15,616.75,617.0,3886
                    # 2024-04-02 11:42:00+05:30,617.0,617.1,616.85,616.9,998
                    # 2024-04-02 11:45:00+05:30,616.55,616.8,616.25,616.5,19262
                    # 2024-04-02 11:46:00+05:30,616.5,616.7,616.2,616.4,3087
                    # 2024-04-02 11:47:00+05:30,616.4,616.65,616.3,616.35,4709
                    # 2024-04-02 11:48:00+05:30,616.35,616.9,616.3,616.9,4315
                    # 2024-04-02 11:49:00+05:30,616.9,616.9,616.65,616.65,2484
                    #
                    # Also see candle_from_epoch(), search for [RNIDXOMC].
                    #
                    while True:
                        PYPWarn("[%s] Dropping extra tick %s for %s candle" %
                            (self.stock, df_tick.index[-1], candle))
                        df_tick.drop(df_tick.tail(1).index, inplace=True)

                        # Check if we still have incomplete groups.
                        last_tick_ts = df_tick.index[-1]
                        last_tick_minute_sms = (
                            get_seconds_since_market_start(last_tick_ts) // 60)
                        extra_rows = (last_tick_minute_sms + 1) % candle_minute
                        if extra_rows == 0:
                            break
                else:
                    # extra_rows == 0
                    df_tick = df_tick_raw
                #
                # Assert that we don't pass this point with incomplete groups.
                #
                last_tick_ts = df_tick.index[-1]
                last_tick_minute_sms = get_seconds_since_market_start(last_tick_ts) // 60
                extra_rows = (last_tick_minute_sms + 1) % candle_minute
                assert(extra_rows == 0)
            else:
                # not cfg.process_live_data or candle == "1Min"
                df_tick = df_tick_raw

            #
            # If aggregate size same as the tick size, use the tick df
            # directly, no need to resample.
            #
            # Note on resample candle size
            # ============================
            # When resampling the candle size MUST be as per the calendar
            # time, i.e. '7D' to get weekly candle and not '5D'. This is so
            # because the resample() method actually looks at the date column
            # and decides how many rows to include based on the calendar time
            # and NOT BASED ON THE NUMBER OF ROWS.
            #
            # This is unlike the aggregate calculation done in add_aggregate()
            # where the period is the actual number of rows to include, so to
            # get a week's aggregate data we will use period=5 for a '1D'
            # candle data.
            #
            if (self.tick_candle_duration_secs ==
                self.candle_size_to_seconds[candle]):
                self.candles[candle] = df_tick.copy(deep=True)
                #
                # If calculate_epoch_after_aggregation is True and the tick
                # candle matches the desired candle size, then we skip the
                # following resample code path where we set the 'Epoch'
                # column, do it here.
                #
                if cfg.calculate_epoch_after_aggregation:
                        assert('Epoch' not in self.candles[candle])
                        dftmp = self.candles[candle]
                        dftmp['Epoch'] = dftmp.index.map(
                                                mapper=(lambda x: int(x.timestamp())+tzoffset))
            else:
                agg_dict = {
                        'Open': 'first',
                        'High': 'max',
                        'Low': 'min',
                        'Close': 'last',
                        'Volume': 'sum'
                }

                #
                # If Epoch column already present, need to direct aggregator
                # to set it in the aggregated data by picking the first one.
                #
                if not cfg.calculate_epoch_after_aggregation:
                        assert('Epoch' in df_tick)
                        agg_dict['Epoch'] = 'first'
                else:
                        assert('Epoch' not in df_tick)

                self.candles[candle] = df_tick.resample(candle,
                                                        origin='start').agg(agg_dict).dropna()

                #
                # If Epoch not already calculated, calculate now after
                # aggregation.
                #
                if cfg.calculate_epoch_after_aggregation:
                        assert('Epoch' not in self.candles[candle])
                        dftmp = self.candles[candle]
                        dftmp['Epoch'] = dftmp.index.map(
                                                mapper=(lambda x: int(x.timestamp())+tzoffset))
                else:
                        assert('Epoch' in self.candles[candle])

            PYPPass("[%s] %d candles of %s" %
                    (self.stock,
                     self.candles[candle].shape[0],
                     candle))

            #
            # Basic cleanliness assertions.
            #

            #
            # One of the most common bugs that can silently slip in is that
            # the start of the candles are not aligned to start-of-day.
            #
            #print("candle=%s df_tick.index[0]=%s ts = %s" % (candle, df_tick.index[0], self.candles[candle].index[0]))

            assert(is_market_start(self.candles[candle].index[0]))
            assert(self.candles[candle].index.is_monotonic_increasing)
            assert(not self.candles[candle].index.has_duplicates)
            #assert(self.candles[candle].index.is_all_dates)
            assert(self.candles[candle].index.inferred_type == 'datetime64')
            #
            # origin='start' argument to resample() ensures that all resampled
            # candles have the same start time (09:15).
            #
            assert(df_tick.index[0] == self.candles[candle].index[0])

        #
        # If the sort and duplicates removal above caused df_tick to be a
        # copy of the original series, save it back to candle['Tick'].
        #
        if (id(self.candles['Tick']) != id(df_tick)):
            self.candles['Tick'] = df_tick

        #
        # Now we have candle data of various sizes.
        # Calculate required aggregates for various different candle sizes.
        #
        for candle in self.cfg_candles:
            #
            # Add aggregates for candles greater than 1Min.
            # 1Min candle is special, it is the "tick" candle and it won't
            # be used by Strategy routines for analysis, hence doesn't need
            # the aggregate columns, just the OHLCV columns enough for running
            # backtest (need to pass to emulated Exchange and Broker) for
            # correct order execution.
            #
            if (pd.Timedelta(candle) > pd.Timedelta('1Min')):
                    if pd.Timedelta(candle) < pd.Timedelta('1D'):
                        aggregates = self.aggregates_i
                    else:
                        assert(not cfg.process_live_data)
                        aggregates = self.aggregates_I

                    #
                    # add_aggregate() adds one column at a time causing
                    # Dataframe to become fragmented which caused perf warning
                    # and slowness.
                    # Use pd.concat() to add all aggregate columns
                    # simultaneously, get_aggregate_column() returns the
                    # requested columns.
#if 0
                    #for aggr in aggregates:
                    #    self.add_aggregate(candle, aggr)
#else
                    A = pd.concat([self.get_aggregate_column(candle, aggr)
                                    for aggr in aggregates], axis=1)
                    self.candles[candle] = pd.concat((self.candles[candle], A), axis=1)
#endif

                    #
                    # Ensure all desired columns added successfully.
                    #
                    for aggr in aggregates:
                        assert(aggr in self.candles[candle].keys())

            # Change index to Epoch.
            self.candles[candle]['Date'] = self.candles[candle].index
            self.candles[candle].set_index('Epoch', inplace=True)

            #print("----> columns[%s] = %s" % (candle, self.candles[candle].columns))

            # Rename columns as per the C++ DataFrame name format.
            cols = list(self.candles[candle].columns)
            numrows = self.candles[candle].shape[0]
            newcols = []
            self.candles[candle].index.rename('INDEX:%d:<double>' % numrows, inplace=True)
            for col in cols:
                if col == "Date":
                    newcols += ['Date:%d:<string>' % numrows]
                elif col == "Volume":
                    newcols += ['Volume:%d:<double>' % numrows]
                else:
                    newcols += ['%s:%d:<double>' % (col, numrows)]

            self.candles[candle].columns = newcols

            #
            # Update pyarrow fields.
            #
            newfields = []
            for paf in self.candles_fields[candle]:
                if paf.name == "Epoch":
                    newfields += [paf.with_name('INDEX:%d:<double>' % numrows)]
                elif paf.name == "Date":
                    newfields += [paf.with_name('Date:%d:<string>' % numrows)]
                elif paf.name == "Volume":
                    newfields += [paf.with_name('Volume:%d:<double>' % numrows)]
                else:
                    newfields += [paf.with_name('%s:%d:<double>' % (paf.name, numrows))]
            self.candles_fields[candle] = newfields

        return

    def dump(self):
        assert(not self.candles['Tick'].empty)

        #pd.set_option('display.float_format','{:.2f}'.format)

        #
        # TODO: Shall we dump the Tick csv too?
        #
        for candle in self.cfg_candles:
            csvfinal = self.csvfinal[candle]
            assert(len(csvfinal) > 0)

            #print(self.candles[candle].index)
            #self.candles[candle].index = self.candles[candle].index.astype(str)
            #print(self.candles[candle].index)

            #
            # Uncomment the following 2 lines for using pandas to_csv()
            # method for printing csv. It's much slower than pyarrow'
            # write_csv() but it has some nice properties like allowing the
            # float precision.
            #
            #self.candles[candle].to_csv(csvfinal, float_format='%.2f')
            #continue

            #self.candles[candle].to_hdf(csvfinal, key='pd', mode='w')

            df = self.candles[candle]

            #for col in df.columns:
            #    if df[col].dtype == "float64":
            #        df[col] = df[col].fillna(1).map(lambda x: int(x*100))


            #df['Open'] = df['Open'].map(lambda x: int(x*100))

            my_schema = pa.schema(self.candles_fields[candle])

            out = pa.Table.from_pandas(self.candles[candle], schema=my_schema)
            del self.candles[candle]
            wo = pacsv.WriteOptions(batch_size=1024)
            pacsv.write_csv(out, csvfinal, write_options=wo)

            #
            #
            # pyarrow dumps the header line with quotes, remove them as the
            # C++ program doesn't like them.
            #
            # The second sed expression changes all float values with more than
            # 2 digit precision to 2-digits. Gives ~10% reduction in file sizes,
            # but takes a lot of time.
            # w/o this NIFTY100 stocks take ~4mins, with this ~18mins.
            # XXX: Uncomment the first sed_string to save space.
            # XXX: This is not needed once we fix the above code to not dump
            #      floats with >2 digit precision.
            #
            #sed_string='s#"##g; s#\([0-9]\+\.[0-9]\{2\}\)[0-9]\+#\\1#g'
            sed_string='s#"##g'

            # Mac's sed expects a mandatory argument after -i.
            if platform.system() == 'Darwin':
                subprocess.call(['sed', '-i', '', sed_string, csvfinal])
            else:
                subprocess.call(['sed', '-i', sed_string, csvfinal])

            # Dump some useful data helpful in debugging.
            stat_buf = os.stat(csvfinal)
            csv_mtime = pd.Timestamp(stat_buf.st_mtime,
                                     unit='s',
                                     tz='Asia/Kolkata').tz_localize(None)
            PYPInfo("Dumped %s (size=%d, mtime=%s, lastrow=[%s,%s])" %
                    (csvfinal, stat_buf.st_size, csv_mtime,
                     df.index[-1], df.iloc[-1].tolist()))


    def ensure(self):
        if self.is_uptodate():
            PYPPass("Final csv(s) uptodate for %s" % self.stock)
            return
        #
        # Load 'Tick' candle.
        # For non-live mode these are $stock_<year>.csv files, while for live
        # mode these are $stock.{pre}live.csv files.
        #
        self.load()

        #
        # Resample and compute to get other candles and aggregate columns.
        # For non-live mode it'll generate $stock.final.<XMin|YDay>.csv and
        # for live mode it'll generate $stock.final.live.<XMin>.csv files.
        #
        self.process()

        #
        # Dump all columns in the $stock.final(.live).<XMin>.csv files.
        #
        self.dump()

    def is_uptodate(self):
        #
        # Force re-evaluation if config.force is set.
        #
        if cfg.force:
            return False

        #
        # If we don't have any of the desired "final" candle, then we are not
        # uptodate. Following loop also stores the mtime of the oldest final
        # csv in final_mtime. If we have any csv file newer than final_mtime,
        # we need to recompute final csvs.
        #
        # TODO: Calculate hash of aggregates_i and aggregates_I and store it
        #       in a file, if the hash changes that means we need different
        #       columns than what is stored in the final csv(s), so we need
        #       to recompute.
        #
        final_mtime = None
        for candle in self.cfg_candles:
            if not os.path.exists(self.csvfinal[candle]):
                PYPWarn("[Not Uptodate] Final csv %s not present" %
                        self.csvfinal[candle])
                return False
            stat_buf = os.stat(self.csvfinal[candle])
            this_mtime = pd.Timestamp(stat_buf.st_mtime,
                                      unit='s',
                                      tz='Asia/Kolkata').tz_localize(None)
            if final_mtime is None or this_mtime < final_mtime:
                final_mtime = this_mtime

        # Will come here only if at least one final csv is present.
        assert(final_mtime is not None)

        for csvfile in self.csvfiles:
            csv_abspath = self.csvdir + '/' + csvfile
            stat_buf = os.stat(csv_abspath)
            csv_mtime = pd.Timestamp(stat_buf.st_mtime,
                                     unit='s',
                                     tz='Asia/Kolkata').tz_localize(None)

            if csv_mtime > final_mtime:
                PYPWarn("[Not Uptodate] csv file (%s) with mtime %s is newer "
                        "than final csv mtime %s, will reload data from csv "
                        "file(s)" % (csv_abspath, csv_mtime, final_mtime))
                for candle in self.cfg_candles:
                    if os.path.exists(self.csvfinal[candle]):
                        os.remove(self.csvfinal[candle])
                return False

        return True

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
    PYPInfo("Loading stocks list from %s" % cfg.stocklist)
    stocks = {}

    with open(cfg.stocklist) as csv_file:
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

    #
    # Add other non-equity symbols which are not present in the csv file.
    # Not needed for live mode since in live mode we are only interested in
    # intraday 1Min candles and these are all daily candles.
    #
    if not cfg.process_live_data:
        if "YF.NIFTY50" not in stocks.keys():
            stocks["YF.NIFTY50"] = {}
        if "YF.NIFTYNEXT50" not in stocks.keys():
            stocks["YF.NIFTYNEXT50"] = {}
        if "YF.NIFTY100" not in stocks.keys():
            stocks["YF.NIFTY100"] = {}
        # XXX Make sure NIFTYBANK tick data is clean.
        if "YF.NIFTYBANK" not in stocks.keys():
            stocks["YF.NIFTYBANK"] = {}
        #if "YF.RELIANCE" not in stocks.keys():
        #    stocks["YF.RELIANCE"] = {}

    PYPInfo("Using %d stock(s): %s" % (len(stocks), stocks))

    return list(stocks.keys())

def process_stock(stock):
    ''' Process one stock, right from loading the csv(s), postprocessing to
        add additional aggregate columns and then dumping the entire dataframe
        into a final csv file.
        This is run by a worker process.
    '''
    setproctitle.setproctitle("pyp.%s" % stock)

    PYPInfo("Processing stock %s" % stock)
    sp = StockProcessor(stock)
    if sp.csvfiles is not None:
        sp.ensure()
        PYPInfo("Done processing stock %s" % stock)
    else:
        PYPError("No csvfiles to process for stock %s" % stock)

def num_running():
    ''' Returns number of stocks that are currently being processed.
    '''
    global runners
    assert(len(runners) > 0)
    cnt = 0
    for runner in runners:
        # Count all runners that have "started but not exited".
        if runner.pid is not None and runner.exitcode is None:
            cnt += 1
    return cnt

def kill_all_children():
    ''' Kill all multiprocessing processes started by the main thread.
        Since those are children of the main thread, only main thread can run
        this code.

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

def init():
    PYPInfo('stockprocessor.init() start')

    #
    # Create a worker process for each stock.
    # We will later start these as appropriate, i.e., we will not start all of
    # these together but only enough to fully utilize available CPU and
    # Memory resources.
    #
    for stock in get_stocks_list():
        p = multiprocessing.Process(target=process_stock,
                                    name=("pyp.%s" % stock),
                                    args=(stock,))
        assert(p is not None)
        runners.append(p)

    PYPInfo('stockprocessor.init() end (count=%d)' % len(runners))

def start():
    PYPInfo('stockprocessor.start() start [parallelism=%d]' % (parallelism))

    # Start per-stock runners, not more than 'parallelism' at a time.
    for runner in runners:
        # If already enough runner running, wait for at least one to complete.
        while num_running() == parallelism:
            time.sleep(0.01)

        PYPInfo("Starting runner %s" % runner.name)
        assert(runner.pid is None)
        runner.start()
        assert(runner.pid is not None)
        assert(num_running() <= parallelism)

    PYPInfo('stockprocessor.start() end')

def join():
    PYPInfo('stockprocessor.join() start')

    for runner in runners:
        # join() MUST be called after start().
        assert(runner.pid != None)

        PYPWarn('Waiting for %s (pid=%d)' % (runner.name, runner.pid))
        runner.join()
        PYPWarn('%s (pid=%d) completed with exitcode %d' %
                (runner.name, runner.pid, runner.exitcode))
        #
        # If it fails to process some file due to error, fail it to the
        # caller so that pyprocess doesn't silently complete.
        # This is not enough, though this helps to have a non-zero exit status
        # for the program but the actual error (mostly assertion failure
        # stack) is hidden in the huge logs from other processes, so we need
        # to kill other processes.
        #
        if runner.exitcode != 0:
                PYPError('FAILED while processing %s (pid=%d)' % (runner.name, runner.pid))
                kill_all_children()
                assert(False)

    PYPInfo('stockprocessor.join() end')
