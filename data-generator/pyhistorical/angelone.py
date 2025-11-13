import json
import requests
import csv
import os
import sys
import shutil
import pathlib
from datetime import datetime
import pandas as pd
import helpers as ch
from SmartApi import SmartConnect
import pyotp
import time
from pathlib import Path

from brokerbase import BrokerBase

def get_linecount(filename):
    ''' Return number of lines in the given file.
    '''
    lines = 0
    with open(filename, 'rb') as f:
        buf_size = 1024 * 1024
        read_f = f.raw.read

        buf = read_f(buf_size)
        while buf:
            lines += buf.count(b'\n')
            buf = read_f(buf_size)

    return lines

class AngelOne(BrokerBase):
    instruments_file = os.path.join(ch.pyhistorical_dir, "angelone_instruments.json")

    #
    # We keep a copy of the angelone_instruments.json checked in to our repo
    # just in case AngelOne download API starts returning empty file on a day
    # (I have seen it happen). In such case, it's better to use the (potentially)
    # stale instruments_file.
    # We try to update the ref file in our repo every day after after downloading
    # that way the ref file is guaranteed to be uptodate.
    #
    instruments_file_ref = os.path.join(ch.pyhistorical_dir, "angelone_instruments.json.ref")

    #
    # Generic YF.XXX index symbol name to AngelOne specific index symbol names.
    # Note that we use generic YF.XXX style names for indices.
    #
    angelone_index_symbol = {
        "YF.NIFTY50":       "Nifty 50",
        "YF.NIFTYNEXT50":   "Nifty Next 50",
        "YF.NIFTY100":      "Nifty 100",
        "YF.NIFTY200":      "Nifty 200",
        "YF.NIFTYBANK":     "Nifty Bank",
    }

    def __init__(self):
        # For ensuring we login only once.
        self.obj = None

    def ensure_login(self, cfg):
        ''' Log in to AngelOne, if not already logged in.
            If already logged in, it's a no-op.
        '''
        # If already logged in, pass.
        if self.obj:
            return

        # AngelOne specific account details.
        angelone = cfg['broker']['angelone']['account']
        self.obj = SmartConnect(api_key=angelone['api_key'])
        totp = pyotp.TOTP(angelone['token'])
        time_remaining = totp.interval - (datetime.now().timestamp() % totp.interval)
        otp = totp.now()

        while True:
            try:
                print("AngelOne login, using user_name=%s, password=%s, api_key=%s, OTP=%s, "
                      "time before OTP expires = %.3f seconds" %
                      (angelone['user_name'], angelone['password'],
                       angelone['api_key'], otp, time_remaining))

                self.obj.generateSession(
                        angelone['user_name'], angelone['password'], totp=otp)
                break
            except Exception as e:
                print("[pyhistorical] Login failed with exception: {}".format(e))
                #
                # Most likely cause for failure would be "too frequent API
                # calls", so wait before retrying.
                # Anyways now we don't login on every call so this is not
                # an issue.
                #
                time.sleep(0.5)
        print("\n[pyhistorical] Logged in successfully!\n")

    def download_instruments(self, cfg):
        """
        Download instruments for AngelOne from their standard url.
        """

        if cfg["skip_load_instruments_json"] == "True":
            # copy ref file to the instruments_file.
            dest = shutil.copyfile(self.instruments_file_ref, self.instruments_file)
            print("[pyhistorical] *** Skipped downloading AngelOne instruments file, using ref file %s -> %s" %
                    (self.instruments_file_ref, dest))
            return

        # Don't download instruments_file if it's already recent enough.
        if os.path.exists(self.instruments_file):
                stat_buf = os.stat(self.instruments_file)
                instruments_file_mtime = pd.Timestamp(stat_buf.st_mtime,
                                                      unit='s',
                                                      tz='Asia/Kolkata').tz_localize(None)
                now = pd.Timestamp.now()
                td = now - instruments_file_mtime
                if td.days == 0:
                        print("[pyhistorical] Skipping download of %s as it is less than 1 day old" %
                                        (self.instruments_file))
                        return

        print("[pyhistorical] Downloading instruments from angelone...")
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        data = requests.get(url).json()
        with open(self.instruments_file, "w+") as f:
            json.dump(data, f, indent = 2)

        #
        # Verify that downloaded file is not empty, if so use the ref file.
        #
        stat_buf = os.stat(self.instruments_file)
        if stat_buf.st_size > 0:
            print("[pyhistorical] Successfully downloaded AngelOne instruments data into %s" %
                            (self.instruments_file))
        else:
            # copy ref file to the instruments_file.
            dest = shutil.copyfile(self.instruments_file_ref, self.instruments_file)
            print("[pyhistorical] *** Downloaded AngelOne instruments file is empty, using ref file %s -> %s" %
                    (self.instruments_file_ref, dest))

    def populate_symbol_to_instrument_token_for_stocklist(self):
        """
        Populate the symbol to instrument token using the data obtained from
        download_instruments() to get AngelOne specific tokens for every symbol.
        """
        # Must be called only once.
        assert(self.symbol_to_instrument_token is None)
        self.symbol_to_instrument_token = {}

        #
        # One row in AngelOne's instrument data looks like this-
        # {"token":"3045","symbol":"SBIN-EQ","name":"SBIN","expiry":"","strike":"-1.000000",
        # "lotsize":"1","instrumenttype":"","exch_seg":"NSE","tick_size":"5.000000"}
        # AngelOne identifies NIFTY symbols with an additional "-EQ", hence adding here.
        #
        # If not equity, it's index symbols and for them we use
        # angelone_index_symbol map for mappping the generic YF.XXX index
        # symbol names to the AngelOne index symbol names.
        #
        # XXX Note that this is not used currently as AngelOne doesn't provide
        #     historical data for index symbols!
        #
        for x in self.stock_symbols:
            # Either equity or index, cannot be both.
            assert(self.is_equity(x) != self.is_index(x));
            # Index (and only index) must be present in angelone_index_symbol.
            assert(self.is_index(x) == (x in self.angelone_index_symbol))

        angelone_symbols = [(x + "-EQ") if self.is_equity(x) else self.angelone_index_symbol[x] for x in self.stock_symbols]

        with open(self.instruments_file) as f:
            angelone_instruments_data = json.load(f)

        for row in angelone_instruments_data:
            if row["symbol"] in angelone_symbols:
                #
                # Removing the "-EQ" which was added earlier to obtain standard stock symbol.
                # Some symbols for which historical data is not downloaded from angelone, but
                # from Yahoo Finance (or elsewhere), they don't have "-EQ" in the end, use
                # them as-is.
                # Examples are YF.NIFTY50/YF.NIFTYBANK/YF.RELIANCE.
                # They have a "YF." prefix to highlight that they are downloaded from Yahoo
                # Finance.
                #
                symbol = row["symbol"][:-3] if row["symbol"].endswith("-EQ") else row["symbol"]
                self.symbol_to_instrument_token[symbol] = row["token"]

        print("[pyhistorical] Successfully populated AngleOne symbol to instrument tokens.")
        print(self.symbol_to_instrument_token)
        return

    def get_instrument_token_for_symbol(self, symbol):
        ''' Returns AngleOne instrument token (used for AngleOne APIs) corresponding
            to the symbol.
        '''
        #
        # For index symbols we use the generic YF.XXXX name, we need to
        # convert those to AngleOne symbol names before we convert them to
        # instrument tokens.
        #
        symbol = self.angelone_index_symbol[symbol] if symbol in self.angelone_index_symbol else symbol
        assert(self.symbol_to_instrument_token is not None)
        assert(len(self.symbol_to_instrument_token) > 0)
        return self.symbol_to_instrument_token[symbol]

    def load_daily_historical_data(self, symbol, year, filename, cfg):
        ''' Load 1D candle data for the given index symbol.

            XXX   This does not work currently as AngleOne historical APIs do
                  not work for index symbols, they work only for equity and futures, and
                  load_daily_historical_data() is mostly meant for index symbols.
                  See https://smartapi.angelbroking.com/topic/602/how-do-i-get-historical-index-data-for-nifty-sensex
                  In future if AngleOne provides historical data for index symbols
                  this can be used.
        '''
        #
        # Don't call this till AngleOne starts supporting historical data for indices!
        # AngleOne started supporting historical data for indices from Dec 13 2023.
        # https://smartapi.angelbroking.com/topic/602/how-do-i-get-historical-index-data-for-nifty-sensex/7
        # XXX But it returns volume as 0, so it's still not usable.
        #     Revisit this after sometime to see if they started returning
        #     volume correctly.
        #
        #assert(False)

        # Must be called only for index symbols.
        assert(self.is_index(symbol))

        # If not already logged in, login to the AngelOne account.
        self.ensure_login(cfg)

        # Query historical 1D data for the entire year.
        first_day_of_year = pd.Timestamp('%s-01-01 09:15:00' % year)
        last_day_of_year = pd.Timestamp('%s-12-31 09:15:00' % year)

        #
        # AngelOne instrument token for the given symbol.
        # This is what AngelOne APIs understand.
        #
        instrument_token = self.get_instrument_token_for_symbol(symbol)

        while True:
            try:
                #
                # AngelOne can supply all 1D candles for a year in one call,
                # so ask for all.
                #
                historicParam = {
                    "exchange": "NSE",
                    "symboltoken": "{}".format(instrument_token),
                    "interval": "ONE_DAY",
                    "fromdate": datetime.strftime(first_day_of_year, '%Y-%m-%d %H:%M'),
                    "todate": datetime.strftime(last_day_of_year, '%Y-%m-%d %H:%M')
                }
                hd = self.obj.getCandleData(historicParam)['data']
                print("Got historical_data %s" %
                                (json.dumps(self.obj.getCandleData(historicParam), indent=2)))
                break
            except Exception as e:
                print("[pyhistorical] Historic Api failed: {}".format(e))
                # w/o sleep it performs better, as the server itself
                # throttles the APIs.
                time.sleep(0.5)

        print("[pyhistorical] {} historical daily data received for {} from AngelOne, "
              "between {} -> {}." .format(
              len(hd), symbol, first_day_of_year, last_day_of_year))

        # Truncate existing file since we got the entire data to be written.
        outfile = open(filename, 'w')
        csvwriter = csv.writer(outfile)

        # Write historical data to csv file.
        for r in hd:
            r[0] = r[0].replace("T", " ")
            #
            # AngelOne returns time as "2024-04-10 00:00:00+05:30" whereas we
            # need "2024-04-10 09:15:00+05:30"
            #
            r[0] = pd.Timestamp(r[0]) + pd.Timedelta("9H15Min")
            csvwriter.writerow(r)

        outfile.close()

    def topup_live_candles_till_the_last_minute(self, cfg):
        """
        Topup historical candle data from the last day/minute in $stock.live.csv
        till the last completed 1Min candle.
        This can be used as a faster way to update historical data than load_historical()
        which checks for all the years and is hence more complete albeit little slower.
        """
        assert(cfg["topup_live_historical"] == "True")

        if not ch.is_market_open():
            print("[pyhistorical] \n*** Market not open, cannot topup live candles ***")
            return

        for i, symbol in enumerate(self.stock_symbols):
            self.load_historical_for_year_for_symbol(datetime.now().year, symbol, cfg)

        print("[pyhistorical] Topped up data for [%d/%d] stocks from %s!\n" %
                (i + 1, len(self.stock_symbols), cfg['stocklist_csv']))

    def load_historical(self, cfg):
        """
        Load historical data based on configuration provided in historical.json.
        """
        start_year = int(cfg['startyear'])
        end_year = datetime.now().year
        assert(end_year >= start_year)

        force_update = False
        # If `historical.json` says that we should forcefully update data between
        # force_startyear & force_endyear, then we overwrite all the historical data
        # in that period, irrespective of whether it was complete or partial.
        if cfg.get('force_startyear') and cfg.get('force_endyear'):
            force_startyear = int(cfg['force_startyear'])
            force_endyear = int(cfg['force_endyear'])
            assert(force_endyear >= force_startyear)
            force_update = True

        for i, symbol in enumerate(self.stock_symbols):
            if cfg.get("candle"):
                datadir = ("%s/NSE/historical/%s/%s" %
                                (cfg["tld"], cfg["candle"], symbol))
            else:
                datadir = ("%s/NSE/historical/%s" % (cfg["tld"], symbol))

            # If datadir doesn't exist, create it recursively.
            if not os.path.exists(datadir):
                print("[pyhistorical] Creating symbol dir %s" % (datadir))
                pathlib.Path(datadir).mkdir(parents=True, exist_ok=True)
            assert(os.path.isdir(datadir))

            #
            # For all the requested years, fetch historical data.
            #
            for year in range(start_year, end_year + 1):
                filename = ("%s/%s_%d.csv" % (datadir, symbol, year))
                partial_filename = ("%s/%s_%d.partial.csv" % (datadir, symbol, year))
                # If json says to forcefully update the file for that year then
                # delete the existing file (complete or partial) and recreate.
                if force_update and force_startyear <= year <= force_endyear:
                    if os.path.isfile(filename):
                        # Only one of these can be present.
                        assert(not os.path.isfile(partial_filename))
                        print("[pyhistorical] Deleting existing (full) historical file %s" %
                                        (filename))
                        os.remove(filename)
                    elif os.path.isfile(partial_filename):
                        print("[pyhistorical] Deleting existing (partial) historical file %s" %
                                        (partial_filename))
                        os.remove(partial_filename)
                    print(
                        "Forcefully updating historical data for %s in %d" % (symbol, year))

                #
                # If there's already a file containing "full" historical data for
                # that year, skip and proceed with the next year(s).
                #
                # Note: Since we take care when closing the file, if we have
                #       the full file we know we have the full data.
                #
                if os.path.isfile(filename):
                    print("[pyhistorical] %s already present, skipping!" % (filename))
                    continue

                self.load_historical_for_year_for_symbol(year, symbol, cfg)

            print("[pyhistorical] Historical data updated for [%d/%d] stocks from %s!\n" %
                            (i + 1, len(self.stock_symbols), cfg['stocklist_csv']))

    def load_historical_for_year_for_symbol(self, year, symbol, cfg):
        """
        Load historical data for the given year, for the given symbol.
        The data is loaded in the csv file
        ('%s/NSE/historical/%s/%s_%s.partial.csv' % (tld, symbol, symbol, year)) for
        non-live mode and ("%s/%s.live.csv" % (datadir, symbol)) for live mode.

        If it gets data for the entire year, then it closes the file
        by moving it to a new file with ".partial" removed from the name.
        """
        if cfg.get("candle"):
                #
                # We don't need to download arbitrary sized candles, we only need
                # 1Min candles. Catch incorrect usage. In case there's a need
                # to download a special sized candle, comment this assert.
                #
                assert(False)
                datadir = ("%s/NSE/historical/%s/%s" %
                                (cfg["tld"], cfg["candle"], symbol))
        else:
                datadir = ("%s/NSE/historical/%s" % (cfg["tld"], symbol))

        # load_historical() must have created datadir before calling us.
        assert(os.path.isdir(datadir))

        # Last 1Min candle of the year.
        last_candle_year = pd.Timestamp('%s-12-31 15:29:00' % year)

        # Last 1Min candle of the day.
        now_notz = pd.Timestamp(datetime.now()).tz_localize(None)
        last_candle_today = now_notz.replace(hour=15, minute=29, second=0, microsecond=0)

        # First 1Min candle of the day.
        first_candle_today = now_notz.replace(hour=9, minute=15, second=0, microsecond=0)

        #
        # Mostly last_candle_today < last_candle_year, but it'll be equal for
        # the last day of the year.
        #
        if cfg["topup_live_historical"] == "True":
            assert(last_candle_today <= last_candle_year)

        #
        # The completed_year variable is needed to track whether we can promote the
        # partial file to a complete/full file or not, after historical download is
        # done.
        #
        completed_year = (last_candle_today >= last_candle_year)

        #
        # This is the final filename after we have collected historical data
        # for the entire year. This should not already exist, or else we would
        # not be called.
        #
        completed_filename = ("%s/%s_%d.csv" % (datadir, symbol, year))
        assert(not os.path.exists(completed_filename))

        #
        # Till we have all the candles for the year, we store them in a
        # file with ".partial.csv" suffix.
        # Partial file indicates that it does not have data for the entire year yet,
        # but has all the data entries in order till specific date & without any
        # in-between missing entries.
        #
        partial_filename = ("%s/%s_%d.partial.csv" % (datadir, symbol, year))

        #
        # For --live mode we store in $stock.live.csv.
        #
        live_filename = ("%s/%s.live.csv" % (datadir, symbol))

        #
        # For index stocks we need just the 1D data.
        #
        if self.is_index(symbol):
            #
            # We shouldn't be downloading any of the index stocks in live
            # mode.
            #
            assert(cfg["topup_live_historical"] != "True")

            # XXX Yahoo historical data is not clean.
            #return self.load_daily_historical_data_from_yahoo(symbol, year,
            #        completed_filename if completed_year else partial_filename)

            #
            # XXX AngelOne doesn't yet support historical data for indices.
            # XXX They started supporting historical data for indices from Dec
            #     2023, but the volume comes as 0, so it's still not usable.
            #
            #return self.load_daily_historical_data(symbol, year,
            #        completed_filename if completed_year else partial_filename, cfg)
            
            #
            # Download index 1D historical data from NSE.
            # This is the best we have currently as it gets data from NSE
            # directly.
            #
            return self.load_daily_data_from_nse(symbol, year,
                    completed_filename if completed_year else partial_filename)

        #
        # This is the last candle that we need to fetch in 'year'. For the
        # ongoing year this will be last candle of the day while for completed
        # years it'll be the last candle of the year.
        #
        # AngelOne's historical data APIs returns max 500 x 1min candle data in
        # one request, and gives the latest/recent 500 entries in any range of
        # from_date and to_date provided. Hence, the idea is to go backward from
        # the last date of the year to the start date till which we need to get
        # historical data.
        # We will reduce to_date in each iteration till it touches start_date.
        #
        to_date = min(last_candle_year, last_candle_today)

        #
        # If topping up live data, we want to get data upto the last candle of
        # today. Infact we want 1Min candle upto the last minute, but we can
        # simply ask upto the last candle of the day and the historical API
        # will correctly return upto the last minute.
        #
        if cfg["topup_live_historical"] == "True":
            assert(to_date == last_candle_today)

        #
        # Get a csv writer handle on the output csv file,
        # partial_filename for the non-live case and live_filename for the
        # --live case. For live case we want to overwrite the file as it may
        # have some partial data and we want to reset it and have all the data
        # from the start of day till the last completed 1Min candle.
        #
        if cfg["topup_live_historical"] == "True":
            outfile = open(live_filename, 'w')
        else:
            outfile = open(partial_filename, 'a')

        csvwriter = csv.writer(outfile)

        #
        # AngelOne instrument token for the given symbol.
        # This is what AngelOne APIs understand.
        #
        instrument_token = self.get_instrument_token_for_symbol(symbol)

        if cfg["topup_live_historical"] == "True":
            print("[pyhistorical] \n[AngelOne] Topping up live data for symbol %s token %s in year %d" %
                    (symbol, instrument_token, year))
        else:
            print("[pyhistorical] \n[AngelOne] Loading historical data for symbol %s token %s in year %d" %
                    (symbol, instrument_token, year))

        #
        # Setting the initial date for this year from where historical data needs
        # to be requested. If partial file exists, start from the next minute after
        # the last minute's data we have in the partial file, else start from
        # the 1st candle of the year.
        #
        # For live mode, we want to just have candles from the beginning of
        # today.
        #
        if cfg["topup_live_historical"] == "True":
            from_date_initial = first_candle_today
        elif (os.path.isfile(partial_filename) and
                os.stat(partial_filename).st_size != 0):
            #
            # Number of rows to skip when reading csv.
            # This is an optimization else reading the entire partial csv takes
            # time, whereas all we need is to find out the last row so that we can
            # query subsequent rows from the broker. We read just the last line
            # from the csv (we read the last 2, just in case the first line is
            # a comment or header.
            #
            skip_first_n = get_linecount(partial_filename) - 2
            print("[pyhistorical] %s -> %d" % (partial_filename, skip_first_n))
            df = ch.read_ohlcv(partial_filename, skip_first_n)
            from_date_initial = df.index[-1].tz_localize(None) + pd.Timedelta('1m')
        else:
            from_date_initial = pd.Timestamp('%s-01-01 09:15:00' % year)

        #
        # If not already logged in, login to the AngelOne account.
        #
        self.ensure_login(cfg)

        #
        # This stores the last received candle for logging.
        #
        max_last_datetime = from_date_initial

        # Maintain historical data (hd) in list, to be written in csv eventually.
        hd_list = []

        start_date = from_date_initial

        #
        # Unless o/w specified in the config, download 1Min candles.
        #
        interval = "ONE_MINUTE"
        if cfg.get("candle"):
                if cfg["candle"] == "5Min":
                        interval = "FIVE_MINUTE"
                elif cfg["candle"] == "10Min":
                        interval = "TEN_MINUTE"
                else:
                        print("[pyhistorical] Unsupported historical candle size: %s\n" %
                                        (cfg["interval"]))
                        assert(False)

        #
        # We need historical candles from start_date to to_date.
        #
        # In case of partial csv file, start_date is the next candle after the
        # last candle in the csv file (we assume partial csv file has all the
        # candles upto a point). If partial csv file is not present,
        # start_date is the very first candle of the year.
        #
        # to_date is today's last candle or for completed years to_date is the
        # last candle of the year.
        #
        num_rows_added = 0
        while True:
            if to_date < start_date:
                print("[pyhistorical] [%s] Done getting historical data, "
                      "to_date=%s start_date=%s" % (symbol, to_date, start_date))
                break

            historicParam = {}
            while True:
                try:
                    #
                    # We ask for tne entire range of candles that we need.
                    # AngelOne will not return all the candles, instead it
                    # will return the most recent 5000x1Min candles in the
                    # range, then we ask again with 'todate' updated to the
                    # first candle received (- 1Min) which will get us the
                    # previous 500x1Min candles and so on.
                    #
                    # Hence every time we receive a bunch of candles we add it
                    # to the beginning of hd_list, that way at the end hd_list
                    # will contain the list of all the candles in ascending
                    # order.
                    #
                    historicParam = {
                        "exchange": "NSE",
                        "symboltoken": "{}".format(instrument_token),
                        "interval": interval,
                        "fromdate": datetime.strftime(start_date, '%Y-%m-%d %H:%M'),
                        "todate": datetime.strftime(to_date, '%Y-%m-%d %H:%M')
                    }
                    hd = self.obj.getCandleData(historicParam)['data']
                    break

                except Exception as e:
                    print("[pyhistorical] [%s] Historical Api failed (%s): %s" %
                            (symbol, historicParam, e))
                    # w/o sleep it performs better, as the server itself
                    # throttles the APIs.
                    time.sleep(0.5)

            if hd is None or len(hd) == 0:
                if hd is None:
                    print("[pyhistorical] [%s] Angelone historical API returned "
                          "hd=None for [%s -> %s]" % (symbol, start_date, to_date))
                else:
                    print("[pyhistorical] [%s] Angelone historical API returned "
                          "no entries for [%s -> %s]" % (symbol, start_date, to_date))
                break

            #
            # Prepend this new lot of candles received into hd_list.
            #
            hd_list[:0] = hd

            last_datetime_in_hd = datetime.strptime(hd[-1][0], '%Y-%m-%dT%H:%M:%S+05:30')
            first_datetime_in_hd = datetime.strptime(hd[0][0], '%Y-%m-%dT%H:%M:%S+05:30')

            # Update "last candle received" for logging.
            max_last_datetime = max(max_last_datetime, last_datetime_in_hd)

            # Update to_date for next iteration. Subtracting 1 min to ensure that
            # duplicate entries are avoided.
            to_date = first_datetime_in_hd - pd.Timedelta('1m')

            print("[pyhistorical] {} historical data entries received for {} from AngelOne, "
                  "between {} -> {}." .format(
                  len(hd_list), symbol, first_datetime_in_hd, last_datetime_in_hd))

        # Write historical data to csv file.
        for r in hd_list:
            r[0] = r[0].replace("T", " ")
            csvwriter.writerow(r)
            num_rows_added += 1

        outfile.close()

        if completed_year:
            if os.stat(partial_filename).st_size != 0:
                os.rename(partial_filename, completed_filename)
            else:
                # If a company is listed after 2015, then there won't be any
                # candles for the entire year.
                print("[pyhistorical] Deleting empty partial file: {}".format(partial_filename))
                os.unlink(partial_filename)

        if cfg["topup_live_historical"] == "True":
            print("[pyhistorical] [{}] Collected today's live 1Min data from [{} -> {}] in {} ({} new rows added)".format
                  (symbol, from_date_initial, max_last_datetime, live_filename, num_rows_added))
        else:
            print("[pyhistorical] [{}] Collected historical_data from [{} -> {}] in {} ({} new rows added)"
                  " [completed={}] " .format
                  (symbol, from_date_initial, max_last_datetime,
                   partial_filename, num_rows_added, completed_year))
