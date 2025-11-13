import abc
import csv
import pandas as pd
import helpers as ch
#
# Yahoo finance package used for downloading daily NIFTY index data, as AngelOne
# only supports historical data for (NIFTY) Equity and Futures.
#
import yfinance as yf
import nselib
from nselib import capital_market

class BrokerBase(metaclass=abc.ABCMeta):
    instruments_file = None
    stock_symbols = None
    symbol_to_instrument_token = None

    #
    # Generic YF.XXX index symbol name to NSE specific index symbol names.
    # Note that we use generic YF.XXX style names for indices.
    #
    nse_index_symbol = {
        "YF.NIFTY50":       "NIFTY 50",
        "YF.NIFTYNEXT50":   "NIFTY NEXT 50",
        "YF.NIFTY100":      "NIFTY 100",
        "YF.NIFTY200":      "NIFTY 200",
        "YF.NIFTYBANK":     "NIFTY BANK",
    }

    @abc.abstractmethod
    def ensure_login(self, cfg):
        """
        Login to the broker account if not already logged in.
        Should be a no-op if already logged in.
        """
        pass

    @abc.abstractmethod
    def download_instruments(self):
        """
        Download instruments (i.e. data for mapping stock symbols to tokens)
        into json or csv files.
        """
        pass

    @abc.abstractmethod
    def populate_symbol_to_instrument_token_for_stocklist(self):
        """
        Populate the symbol to instrument token using the data obtained from
        download_instruments() to get broker specific tokens for every symbol.
        """
        pass

    @abc.abstractmethod
    def get_instrument_token_for_symbol(self, symbol):
        """
        Every broker uses token names for symbols and the token names must be
        passed in the API calls to identify the symbols.
        This method must get the broker specific token for the given symbol name.
        """
        pass

    @abc.abstractmethod
    def topup_live_candles_till_the_last_minute(self, cfg):
        """
        Topup historical candle data from the last day/minute in <current year>_partial.csv
        till the last completed 1Min candle.
        This can be used as a faster way to update historical data that load_historical()
        which checks for all the years and is hence more complete albeit little slower.
        """
        pass

    @abc.abstractmethod
    def load_historical(self, cfg):
        """
        Load historical data based on configuration provided in historical.json.
        """
        pass

    @abc.abstractmethod
    def load_historical_for_year_for_symbol(self, year, symbol, cfg):
        """
        Load historical data for the given year, for the given symbol.
        The data is loaded in the csv file
        ('%s/NSE/historical/%s/%s_%s.partial.csv' % (tld, symbol, symbol, year)).

        If it gets data for the entire year, then it closes the file
        by moving it to a new file with ".partial" removed from the name.
        """
        pass

    def test_and_get_updated_symbol(self, symbol):
        '''
        Some symbols have their names updated from what they are in NIFTY_xx.csv.
        The broker recognizes them with the new name, return the new name.
        '''
        updated_dict = {
            "CADILAHC": "ZYDUSLIFE",
            "LTI": "LTIM",
            "MOTHERSUMI": "MOTHERSON"
        }
        if symbol in updated_dict:
            return updated_dict[symbol]
        return symbol

    def is_skip_symbol(self, symbol):
        skip_list = [
            "MINDTREE", # MINDTREE is not present in instruments file.
            "SRTRANSFIN"
        ]
        return symbol in skip_list

    def is_equity(self, symbol):
        ''' Is the given symbol an equity symbol?
            If not equity, it will be index.

            Actually now we have included some equity symbols in it too.
            The more accurate distinction now is that all symbols for which
            is_equity() return True, these are downloaded from Yahoo Finance
            as 1D symbols.
            The reason we have included RELIANCE in this list is because NIFTY50
            data from Yahoo Finance is not very clean, there are many missing
            candles, so it cannot be reliably used for doing checks like whether
            market was open on a given day, what was the previous/next trading
            day, etc. We use RELIANCE for doing those checks.

            XXX
            Update: Even RELIANCE data from Yahoo Finance is not completely
                    clean, it also has many missing days of data, so it doesn't
                    correctly work for finding prev/next trading day.

                    Since we can never guarantee completely clean data, we use
                    NIFTY50 itself as the source of truth for "which days were
                    trading days" and whatever days are missing from there we
                    don't backtest for those days. This might result in slight
                    inaccuarcy but we have to live with that.

                    If we can get historical NIFTY50 index data from investing.com
                    that might be cleaner, but currently they don't provide
                    APIs.
            XXX
        '''
        non_equity = [
            "YF.NIFTY50",
            "YF.NIFTY100",
            "YF.NIFTYNEXT50",
            "YF.NIFTYBANK",
            #"YF.RELIANCE"
        ]
        return symbol not in non_equity

    def is_index(self, symbol):
        ''' Is the given symbol an index symbol?
        '''
        index = [
            "YF.NIFTY50",
            "YF.NIFTY100",
            "YF.NIFTYNEXT50",
            "YF.NIFTYBANK",
        ]
        return symbol in index

    def populate_list_of_stock_symbols(self, cfg):
        '''
        Obtains list of stock symbols from stocklist_csv provided in historical.json.
        This can be an entire Nifty index as downloaded from
        https://www.niftyindices.com/, or it can be a handpicked list of
        stocks.
        The stock list file must be a csv file, but each of these type of
        files have different format. The index and handpicked files have the
        following headers:

        1. Company Name,Industry,Symbol,Series,ISIN Code
        2. Company Name,Symbol
        '''
        tld = cfg['tld']
        stocklist_csv = cfg['stocklist_csv']

        # Must be called only once.
        assert(self.stock_symbols is None)

        stocks_file = ("%s/NSE/%s" % (tld, stocklist_csv))

        print("[pyhistorical] Loading stocks list from %s" % stocks_file)

        with open(stocks_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            num_cols = 0
            self.stock_symbols = []

            for row in csv_reader:
                #
                # Skip commented lines.
                # This provides an easy way to drop some symbol from NIFTY_50 f.e,
                # if it has got unclean data.
                # Also useful for adding comments.
                #
                if len(row[0]) > 0 and row[0][0] == '#':
                    print("[pyhistorical] Skipping commented line starting with: %s" % row[0])
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
                    # For now, only equity.
                    assert(row[3] == 'EQ')
                    symbol = row[2]
                elif num_cols == 2:
                    symbol = row[1]
                else:
                    assert(False)

                #
                # Some symbols may have their names changed in the Exchange
                # from what we have in NIFTY_50.csv (and other aggregate
                # files). We need to query AngelOne with the new name.
                #
                symbol = self.test_and_get_updated_symbol(symbol)
                # No duplicates.
                assert(symbol not in self.stock_symbols)

                #
                # AngelOne doesn't have some symbols in the instruments file.
                # XXX This is a bug in AngelOne, but we proceed with skipping
                #     those symbols.
                #
                if self.is_skip_symbol(symbol):
                    continue
                self.stock_symbols += [symbol]

        #
        # Add other non-equity symbols which are not present in the csv file
        # Don't add them if topup_live_historical is true since for these
        # index stocks we only get daily candles and we only top up 1Min
        # candles when topup_live_historical is true.
        #
        if cfg["topup_live_historical"] != "True":
            self.stock_symbols += ["YF.NIFTY50", "YF.NIFTY100", "YF.NIFTYNEXT50",
                                   "YF.NIFTYBANK",
                                   #"YF.RELIANCE"
                                   ]

        print("[pyhistorical] Loaded %d symbol(s) from %s" % (len(self.stock_symbols), stocks_file))
        print(self.stock_symbols)
        assert(len(self.stock_symbols) > 0)
        return

    @abc.abstractmethod
    def load_daily_historical_data(self, symbol, year, filename):
        """ Load 1D candle data for the given index symbol.
        """
        pass

    def load_daily_data_from_nse(self, symbol, year, filename):
        """ Load 1D candle data from NSE new site using nselib.
            This is useful for downloading index data as AngleOne currently
            only supports historical data for (Nifty) Equity and Futures
            and Yahoo Finance data is not clean, it keeps breaking very
            frequently.

            symbol must be a valid NSE symbol as used in Index dropdown in
            https://www.nseindia.com/reports-indices-historical-index-data
            and not the YF.XXX (YF.NIFTY50 f.e.) index symbols that we use.

            The data downloaded is for 'year' (till date) and is stored in
            'filename'.
        """
        print("[pyhistorical] \n[nselib] Loading 1D historical data for symbol %s "
              "for year %d and saving in file %s" % (symbol, year, filename))

        #
        # Convert YF.XXX generic symbol name to actual NSE symbol name.
        # f.e. YF.NIFTY50 becomes "Nifty 50"
        #
        symbol = self.nse_index_symbol[symbol]
        from_date = ("01-01-%d" % year)

        #
        # NSE data for the following period is missing Volume info:
        # Aug 2nd to Dec 31st 2019
        # Jul 21st 2023
        #
        # NSE data for the following period is missing OHLC info:
        # Jul 15,16,17,20 2020
        # Oct 1st to Dec 31st 2020
        #
        # Use yahoo finance data for the range.
        # We do it for the entire range from first invalid to the end of
        # year to avoid multiple YF calls.
        #
        if year == 2019:
            to_date = ("01-08-%d" % year)
            yf_from_date = "2019-08-02"
            yf_to_date = "2019-12-31"
        elif year == 2020:
            to_date = ("14-07-%d" % year)
            yf_from_date = "2020-07-15"
            yf_to_date = "2020-12-31"
        elif year == 2023:
            to_date = ("20-07-%d" % year)
            yf_from_date = "2023-07-21"
            yf_to_date = "2023-12-31"
        else:
            to_date = ("31-12-%d" % year)
            yf_from_date = None
            yf_to_date = None

        print("[pyhistorical] Making nselib call to get historical data for symbol %s with "
              "parameters (from_date=%s, to_date=%s)" % (symbol, from_date, to_date))

        df = nselib.capital_market.index_data(index=symbol,
                                              from_date=from_date,
                                              to_date=to_date)

        df.set_index("TIMESTAMP", inplace=True)
        df.index = pd.to_datetime(df.index, format="%d-%m-%Y")
        df.index = (i + pd.Timedelta("9H15Min") for i in df.index)
        df.index = df.index.tz_localize("Asia/Kolkata")

        column_remap = {
            'OPEN_INDEX_VAL': 'Open',
            'HIGH_INDEX_VAL': 'High',
            'LOW_INDEX_VAL': 'Low',
            'CLOSE_INDEX_VAL': 'Close',
            'TRADED_QTY': 'Volume',
        }
        df.rename(columns=column_remap, inplace=True)

        # Drop extra columns.
        df = df[["Open", "High", "Low", "Close", "Volume"]]

        #
        # Clean the tick data.
        # 1. Remove duplicate ticks.
        # 2. Sort in ascending order.
        #
        # Unfortunately NSE data is also not always clean! It has duplicates
        # too.
        #
        if df.index.has_duplicates:
            df = df[~df.index.duplicated(keep='first')]
            assert(not df.index.has_duplicates)
        if not df.index.is_monotonic_increasing:
            df = df.sort_index(ascending=True)
            assert(df.index.is_monotonic_increasing)

        if yf_from_date is not None:
            assert(yf_to_date is not None)

            nse2yahoo = {
                "NIFTY 50": "^NSEI",
                "NIFTY NEXT 50": "^NSMIDCP",
                "NIFTY 100": "^CNX100",
                "NIFTY BANK": "^NSEBANK",
            }
            #
            # Get from yahoo finance for the period where NSE data is not
            # clean. Once NSE fixes it, we can remove this special casing.
            #
            df_rest = self.load_daily_data_from_yahoo(symbol=nse2yahoo[symbol],
                                                      from_date=yf_from_date,
                                                      to_date=yf_to_date)
            df = pd.concat([df, df_rest])

        #
        # XXX We expect NSE data to be clean
        #     If these asserts are seen to fail we need to clean the data.
        #
        assert(not df.index.has_duplicates)
        assert(df.index.is_monotonic_increasing)

        df.to_csv(filename, float_format='%.2f')

    def load_daily_data_from_yahoo(self, symbol, year=None, filename=None,
                                   from_date=None, to_date=None):
        """ Load 1D candle data from yahoo finance.
            This is useful for downloading index data as AngleOne currently
            only supports historical data for (Nifty) Equity and Futures.

            symbol must be a yahoo finance symbol as confirmed in
            https://finance.yahoo.com/lookup
            The data downloaded is for 'year' (till date) and is stored in
            'filename'.

            'from_date' and 'to_date' can be provided in yyyy-mm-dd format and if
            provided then it returns a dataframe containing data in the requested
            range. 'year' and 'filename' are ignored in this case and MUST not be
            passed.
        """
        # Either both should be None or both should be valid.
        assert((from_date is None) == (to_date is None))
        # Either from_date/to_date or year must be specified.
        assert((from_date is None) != (year is None))
        #
        # If from_date is provided we don't dump to a file but instead return
        # the df, hence filename must not be provided, else it must be provided.
        #
        assert((from_date is None) != (filename is None))

        #
        # Validate range has at least 1 day.
        # Also from_date and to_date must be in the same year.
        #
        if from_date is not None:
            assert(pd.Timestamp(from_date) < pd.Timestamp(to_date))
            assert(pd.Timestamp(from_date).year == pd.Timestamp(to_date).year)
            assert(year is None)
            year = pd.Timestamp(from_date).year

        range_query = (from_date is not None)

        if not range_query:
            print("[pyhistorical] \n[Yahoo Finance] Loading 1D historical data for symbol %s "
                  "for year %d and saving in file %s" % (symbol, year, filename))
        else:
            print("[pyhistorical] \n[Yahoo Finance] Loading 1D historical data for symbol %s "
                  "in range [%s, %s]" % (symbol, from_date, to_date))

        # yf ticker.
        ticker = yf.Ticker(symbol)

        #
        # Get 1D time interval historical data for the entire year to date.
        # back_adjust: Back-adjusted data to mimic true historical prices
        # rounding: Round values to 2 decimal places?
        #
        from_date = ("%d-01-01" % year) if from_date is None else from_date

        now = pd.Timestamp.now()
        sec = now.hour*3600 + now.minute*60

        # Not Saturday/Sunday and within the trading time.
        is_market_open = (now.weekday() != 5 and now.weekday() != 6 and
                          (sec >= 9*3600+15*60 and sec < 15*3600+30*60))

        if pd.Timestamp(from_date).year < now.year or not is_market_open:
                end=("%d-12-31" % pd.Timestamp(from_date).year)
        else:
                #
                # If we are running pyhistorical on a trading day then exclude
                # the current day else that one is incomplete.
                #
                # Update: It seems yahoo finance has fixed the API and now it
                #         doesn't return the current day's historical data so
                #         we don't need to subtract 1D.
                #
                # TODO:   Watch it, if it changes again!
                #         Yes, it started failing again. It was returning an
                #         empty entry with NaN for all values.
                #         2023-08-21 00:00:00+05:30 NaN NaN NaN NaN NaN
                #
                #         To be safe, if the current time is less than the
                #         market close time then subtract 1 day.
                #
                yday = now
                #if is_market_open:
                #    yday = now - pd.Timedelta("1D")
                end=("%d-%d-%d" % (yday.year, yday.month, yday.day))

        if to_date is None:
            to_date = end
        else:
            #
            # XXX
            # Don't set to_date in future else during the trading day YF
            # returns the current day's data even though current day is not
            # complete yet.
            # XXX
            #
            # we have to set to_date depending on the current time.
            # If it's before market start we can set it to today else set it
            # to y'day, else today's entry will come which is not valid.
            #
            # Unfortunately YF keeps breaking this. Sometimes it returns new
            # incomplete data! Still don't have a foolproof way of getting the
            # correct data at all times.
            #
            td1 = min(pd.Timestamp(to_date), pd.Timestamp(end))
            to_date=("%d-%d-%d" % (td1.year, td1.month, td1.day))

        print("[pyhistorical] Making yfinance call to get historical data for symbol %s with "
              "parameters (start=%s, end=%s, interval=1d)" %
              (symbol, from_date, to_date))

        tdf = ticker.history(start=from_date, end=to_date,
                             interval="1d", back_adjust=True, rounding=True)

        # Drop extra columns.
        tdf = tdf[["Open", "High", "Low", "Close", "Volume"]]

        #
        # Clean the tick data.
        # 1. Remove duplicate ticks.
        # 2. Sort in ascending order.
        #
        if tdf.index.has_duplicates:
            tdf = tdf[~tdf.index.duplicated(keep='first')]
            assert(not tdf.index.has_duplicates)
        if not tdf.index.is_monotonic_increasing:
            tdf = tdf.sort_index(ascending=True)
            assert(tdf.index.is_monotonic_increasing)

        # Yahoo finance volume is in 1/1000 th.
        tdf['Volume'] = tdf['Volume'].apply(lambda x: x*1000)

        #
        # Fixups.
        # 2016, 2018 and 2019 do not have data for Jan 01.
        # Copy data from Jan 02, so that we can correctly perform the market
        # open check.
        #
        # XXX This is not complete. Infact the data is very unclean, lot of
        #     missing candles, so we cannot use NIFTY50 data to find data
        #     on whether market was open on a specific day or not.
        #
        if symbol == "^NSEI":
                if year == 2016:
                        if '2016-01-01 00:00:00+05:30' not in tdf.index:
                                s = tdf.loc['2016-01-04 00:00:00+05:30']
                                r = pd.DataFrame({'Open': s.Open,
                                                  'High': s.High,
                                                  'Low': s.Low,
                                                  'Close': s.Close,
                                                  'Volume': s.Volume},
                                                 index=[pd.Timestamp('2016-01-01 00:00:00+05:30')])
                                tdf = pd.concat([r, tdf])
                elif year == 2018:
                        if '2018-01-01 00:00:00+05:30' not in tdf.index:
                                s = tdf.loc['2018-01-02 00:00:00+05:30']
                                r = pd.DataFrame({'Open': s.Open,
                                                  'High': s.High,
                                                  'Low': s.Low,
                                                  'Close': s.Close,
                                                  'Volume': s.Volume},
                                                 index=[pd.Timestamp('2018-01-01 00:00:00+05:30')])
                                tdf = pd.concat([r, tdf])
                elif year == 2019:
                        if '2019-01-01 00:00:00+05:30' not in tdf.index:
                                s = tdf.loc['2019-01-02 00:00:00+05:30']
                                r = pd.DataFrame({'Open': s.Open,
                                                  'High': s.High,
                                                  'Low': s.Low,
                                                  'Close': s.Close,
                                                  'Volume': s.Volume},
                                                 index=[pd.Timestamp('2019-01-01 00:00:00+05:30')])
                                tdf = pd.concat([r, tdf])


        # Timestamp returned by yfinance is 00:00:00, we use 09:15:00.
        tdf.index = (i + pd.Timedelta("9H15Min") for i in tdf.index)

        #
        # Range query must return the df and not dump it to filename.
        #
        if range_query:
            return tdf

        #
        # For some holidays yfinance wrongly returns Volume=0, skip those.
        #
        # XXX
        # There are many of these which are on valid days, so if we remove them
        # it affects our use of NIFTY50 historical data for correctly finding
        # trading days. Let them be with the risk that volume calculation will
        # be affected. Currently we don't use volume from NIFTY50 historical data.
        # XXX
        #
        #tdf = tdf[tdf["Volume"] != 0]
        tdf.to_csv(filename, float_format='%.2f')

    def load_daily_historical_data_from_yahoo(self, symbol, year, filename):
        assert(self.is_index(symbol))

        if symbol == "YF.NIFTY50":
                return self.load_daily_data_NIFTY50(year, filename)
        if symbol == "YF.NIFTY100":
                return self.load_daily_data_NIFTY100(year, filename)
        if symbol == "YF.NIFTYNEXT50":
                return self.load_daily_data_NIFTYNEXT50(year, filename)
        elif symbol == "YF.NIFTYBANK":
                return self.load_daily_data_NIFTYBANK(year, filename)
        #elif symbol == "YF.RELIANCE":
        #        return self.load_daily_data_RELIANCE(year,  filename)
        else:
            assert False, "Unsupported index symbol %s" % symbol


    def load_daily_data_NIFTY50(self, year, filename):
        """ Load NIFTY50 daily data.
        """
        # NIFTY50 ticker as per https://finance.yahoo.com/lookup is "^NSEI".
        return self.load_daily_data_from_yahoo("^NSEI", year, filename)

    def load_daily_data_NIFTY100(self, year, filename):
        """ Load NIFTY100 daily data.
        """
        # NIFTY100 ticker as per https://finance.yahoo.com/lookup is "^CNX100".
        return self.load_daily_data_from_yahoo("^CNX100", year, filename)

    def load_daily_data_NIFTYNEXT50(self, year, filename):
        """ Load NIFTYNEXT50 daily data.
        """
        # NIFTYNEXT50 ticker as per https://finance.yahoo.com/lookup is "^NSMIDCP".
        return self.load_daily_data_from_yahoo("^NSMIDCP", year, filename)

    def load_daily_data_NIFTYBANK(self, year, filename):
        """ Load NIFTYBANK daily data.
        """
        # NIFTYBANK ticker as per https://finance.yahoo.com/lookup is "^NSEBANK".
        return self.load_daily_data_from_yahoo("^NSEBANK", year, filename)

    def load_daily_data_RELIANCE(self, year, filename):
        """ Load RELIANCE daily data from yahoo.
        """
        # RELIANCE ticker as per https://finance.yahoo.com/lookup is "RELIANCE.NS".
        return self.load_daily_data_from_yahoo("RELIANCE.NS", year, filename)

