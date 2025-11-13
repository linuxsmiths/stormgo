import numpy as np
import pandas as pd
from pathlib import Path
import os
import datetime

#
# This file is cppbacktester/src/pyhistorical/helpers.py
# srcdir is cppbacktester/src.
#
srcdir = str(Path(__file__).resolve().parent.parent)
pyhistorical_dir = os.path.join(srcdir, "pyhistorical")

def read_ohlcv(csvfile, skip_first_n=None):
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
        'Date': str,
        'Open': np.float64,
        'High': np.float64,
        'Low': np.float64,
        'Close': np.float64,
        'Volume': np.float64
    }
    #
    # We assume columns are in the following order.
    # Some csvs have extra columns which we ignore as of now.
    #
    # TODO: Some csvs already contain technical data like various moving
    #       averages etc, if so, make use of that.
    #
    columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    try:
        df = pd.read_csv(
            csvfile,
            skiprows=range(1, skip_first_n) if skip_first_n is not None else None,
            names=columns,
            usecols=[i for i in range(len(columns))],
            index_col=0, parse_dates=True, dtype=dtypes)
    except (ValueError, TypeError):
        print(
            "pd.read_csv() threw ValueError/TypeError while parsing "
            "csvfile {}, most probably the first row is the header, "
            "re-trying with header=0. Consider removing the header to "
            "avoid double parsing in future!".format(csvfile))
        df = pd.read_csv(
            csvfile,
            names=columns,
            usecols=[i for i in range(len(columns))],
            index_col=0, parse_dates=True, dtype=dtypes, header=0)
    #
    # PERF:
    # Remove timezone info.
    # This is unnecessary and hurts performance.
    # This should match stockcache.read_csv().
    #
    df.index = df.index.tz_localize(None)

    # Must have 5 columns.
    assert(df.shape[1] == 5)
    # At least one row.
    assert(df.shape[0] != 0)
    # Index must be of type pd.Timestamp.
    #assert(df.index.is_all_dates == True)
    assert(df.index.inferred_type == 'datetime64')
    # Each other column must be of type float.
    assert(type(df['Open'].iloc[0]) == np.float64)
    assert(type(df['High'].iloc[0]) == np.float64)
    assert(type(df['Low'].iloc[0]) == np.float64)
    assert(type(df['Close'].iloc[0]) == np.float64)
    assert(type(df['Volume'].iloc[0]) == np.float64 or
           type(df['Volume'].iloc[0]) == np.int64)

    return df

def is_market_open(dt = None):
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
