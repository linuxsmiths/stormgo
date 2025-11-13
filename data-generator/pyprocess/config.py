import json
import logging
import os
from pathlib import Path

#
# This file is cppbacktester/src/pyprocess/config.py
# srcdir is cppbacktester/src.
#
srcdir = str(Path(__file__).resolve().parent.parent)
pyprocessdir = os.path.join(srcdir, "pyprocess")
pylivedir = os.path.join(srcdir, "pylive")
enginedir = os.path.join(srcdir, 'engine')

#
# Engine's config file.
# pyprocess works closely with the engine, so they share the config file to
# avoid duplication and for consistent config. Anything specific to pyprocess is
# added to the pyprocess specific config file which can define new keys and/or
# override existing keys.
#
ECFG_FILE = os.path.join(enginedir, 'backtester.json')
config = {}

#
# This is the pyprocess config. It can define new keys which are not present in
# engine config, if it contains keys already present in engine config those
# will be overriden.
#
# oconfig => override config.
#
CFG_FILE = os.path.join(pyprocessdir, 'backtester.json')
oconfig = {}

def load_config():
        global oconfig
        global config

        with open(CFG_FILE) as f:
                oconfig = json.load(f)
        with open(ECFG_FILE) as f:
                config = json.load(f)

        # Override config keys.
        config.update(oconfig)

# Load the json oconfig.
load_config()

#
# Now sanitize various config and set easy-access variable names for each
# config setting.
#

#
# Top level data directory containing historical stock data and other useful
# stuff. It has the following structure:
# NSE/
# NSE/{NIFTY_50.csv,NIFTY_AUTO.csv,...}
# NSE/historical/
# NSE/historical/{HDFC,INFY,...}/
# NSE/historical/HDFC/{HDFC_2017.csv,HDFC_2018.csv,...}
#
# NIFTY_50.csv, NIFTY_AUTO.csv is a list of corresponding stocks. The
# historical stock prices are contained in the corresponding directory inside
# NSE/historical/.
#
tld = config['tld']
assert(tld != "")
assert(os.path.isdir(tld))

# logdir must be present in, and only in, engine/backtester.json.
assert 'logdir' not in oconfig.keys(), ("logdir must not be defined in %s" % CFG_FILE)
assert 'logdir' in config.keys(), ("logdir not defined in %s" % ECFG_FILE)
logdir = config['logdir']
assert(os.path.exists(logdir))

#
# logfile is mandatory, even if user doesn't want to log they must specify
# "null" as the logfile.
#
assert 'logfile' in oconfig.keys(), ("logfile not defined in %s" % CFG_FILE)
logfile_name = oconfig['logfile']

# Get the absolute logfile path.
if logfile_name == "null":
        logfile = "/dev/null"
elif logfile_name == "tty":
        logfile = "/dev/tty"
else:
        logfile = os.path.join(logdir, logfile_name)

# Logging can be disabled by explicitly setting logfile to /dev/null.
skip_logging = (logfile == '/dev/null')

# Log to console if logfile is /dev/tty.
log_to_console = (logfile == '/dev/tty')

# PYPDebug logs will be logged if verbose logging is configured.
verbose = (config['verbose'] == "True")

# All stocks will be processed irrespective of whether they already have
# uptodate final.csv files.
force = (config['force'] == "True")

#
# Are we processing live data?
# XXX This is not used now, instead --live option is used to convey live mode.
#
process_live_data = (config['process_live_data'] == "True")

#
# If there's a missing candle, like following,
# 2017-01-06 12:44:00+05:30,3000.4,3001.0,3000.4,3001.0,53
# 2017-01-06 12:46:00+05:30,3002.0,3002.0,3002.0,3002.0,50
# then when we aggregate this into 5Min candles, the epoch assigned to the
# 12:45 candle is the one that corresponds to 12:46 since the epoch column
# is assigned before the aggregation and hence the 12:46 row got the Epoch
# corresponding to 12:46. This is what the aggregated 12:45 candle gets.
# StockInfo::candle_from_epoch() doesn't like it and the following assert
# fails:
# assert(epoch < epoch_at_idx);
# To fix this, we can assign Epoch after the aggregation so that 12:45 candles
# does indeed get the Epoch corresponding to 12:45. This masks this inaccuracy
# but lets backtester run on the not-so-clean historical data.
# AngelOne historical data has lot of such uncleanliness, which zerodha
# historical data is seen to be clean.
# Set this config when processing AngelOne data.
#
calculate_epoch_after_aggregation = (config['calculate_epoch_after_aggregation'] == "True")

#
# If omit_partial_days is set, pyprocess will skip days which do not have all
# 375x1Min candles. If there are too many such days with say just one/few candle
# missing we are rather better off not omitting those days but instead letting
# the aggregator use correct Epoch time for the aggregated candles by setting
# 'calculate_epoch_after_aggregation' to True. Since only few candles are
# missing the aggregate will not be very inaccurate.
# If there are multiple/many candles missing in a day, then not skipping those
# days might mask the uncleanliness and result in inaccurate data.
#
# In those cases it's better to skip those days and then omit those stocks which
# have too many missing days due to this, using the cfg.excludelist config option
# of the engine.
# If only few candles are missing but for many days then it's better to not
# skip the entire days but rather live with little inaccuracy but more stocks
# to process for backtesting.
#
omit_partial_days = (config['omit_partial_days'] == "True")

#
# NOTE: basicConfig() should be called before any call to logging.info() etc,
#       else the logger gets default initialized and doesn't use the arguments
#       passed to basicConfig(), that's why to be safe we call basicConfig()
#       here.
# NOTE: pyprocess is invoked from pylive/broker everytime it wants to process
#       newly generated live tick, so we open the logfile in append mode else
#       every invocation will clear the previous logfile which is not
#       desirable.
#
logging.basicConfig(format='[pyprocess] %(asctime)s: %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename=logfile,
                    #filemode='w',   # don't append every run to the file.
                    filemode='a',   # append (not truncate) as we may be called from pylive.
                    level=logging.INFO)

# CSV file containing list of stocks to process.
stocklist = tld + "/NSE/" + config['stocklist']
assert(os.path.isfile(stocklist))

#
# List of candles for which aggregated data is to be generated.
# In live mode we ignore the candles config and force the list of intraday
# candles.
# If you make any changes here, also update main.py so that -l/--live option
# is correctly handled.
#
if process_live_data:
        candles = ["1Min", "5Min", "10Min", "15Min"]
        print("[Live] Forcing intraday candles (%s), ignoring config.candles!" % candles)
else:
        candles = config['candles']
assert(len(candles) > 0)
