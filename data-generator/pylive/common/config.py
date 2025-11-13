import json
import logging
import os
import pandas as pd
from pathlib import Path

#
# This file is cppbacktester/src/pylive/common/config.py
# srcdir is cppbacktester/src.
#
srcdir = str(Path(__file__).resolve().parent.parent.parent)
pylivedir = os.path.join(srcdir, "pylive")
commondir = os.path.join(pylivedir, 'common')
crondir = os.path.join(pylivedir, "cron")
enginedir = os.path.join(srcdir, 'engine')
pyhistorical_dir = os.path.join(srcdir, "pyhistorical")

#
# Engine's config file.
# pylive works very closely with the engine, so they share the config file to
# avoid duplication and for consistent config. Anything specific to pylive is
# added to the pylive specific config file which can define new keys and/or
# override existing keys.
#
ECFG_FILE = os.path.join(enginedir, 'backtester.json')
config = {}

#
# This is the pylive config. It can define new keys which are not present in
# engine config, if it contains keys already present in engine config those
# will be overriden.
#
# oconfig => override config.
#
CFG_FILE = os.path.join(pylivedir, 'backtester.json')
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

# Should we load angelone_instruments.json or use the ref file.
skip_load_instruments_json = (config['skip_load_instruments_json'] == "True")

#
# These device log files are not accessible for cron jobs, since
# pylive/cron/main.py runs as a cron we force these to None, since the intend
# behind these logfile names is to log on stdout. Even with logfile set to
# None we will log to stdout.
#
if logfile == "/dev/tty":
        logfile = None

if logfile == "/dev/stdout":
        logfile = None

# If logfile is not set, we will log to console.
log_to_console = (logfile is None)

# PYPDebug logs will be logged if verbose logging is configured.
verbose = (config['verbose'] == "True")

#
# NOTE: basicConfig() should be called before any call to logging.info() etc,
#       else the logger gets default initialized and doesn't use the arguments
#       passed to basicConfig(), that's why to be safe we call basicConfig()
#       here.
#
# NOTE: We don't really want to open the logfile in append mode but to be
#       consistent with other modules we open this also in append mode, so
#       that we have all corresponding logs.
#
logging.basicConfig(format='[pylive] %(asctime)s: %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename=logfile,
                    #filemode='w',   # don't append every run to the file.
                    filemode='a',   # append (not truncate) to not clear old logs.
                    level=logging.INFO)

#
# Now sanitize various oconfig and set easy-access variable names for each
# oconfig setting.
#

#
# Top level data directory containing historical stock data and other useful
# stuff. It has the following structure:
# NSE/
# NSE/{NIFTY_50.csv,NIFTY_AUTO.csv,...}
# NSE/historical/
# NSE/historical/{HDFC,INFY,...}/
# NSE/historical/HDFC/{HDFC_2017.csv,HDFC_2018.csv,...}
# NSE/historical/HDFC/{HDFC.final.5Min.csv,HDFC.final.15Min.csv,...}
#
# NIFTY_50.csv, NIFTY_AUTO.csv is a list of corresponding stocks. The
# historical stock prices are contained in the corresponding directory inside
# NSE/historical/.
#
tld = config['tld']
assert(tld != "")
assert(os.path.isdir(tld))

# $tld/NSE/historical
historicaldir = os.path.join(tld, "NSE/historical")

stocklist = config['stocklist']
assert(stocklist != "")

#
# Sometimes we incorrectly leave stocklist as one.csv in engine backtester.json.
# This is not what we want as it causes data to not be refreshed as one would
# expect, force it to fail.
#
assert(stocklist in ["NIFTY_50.csv", "NIFTY_100.csv", "NIFTY_200.csv"])

# Broker information.
broker = config['broker']

# Till we support other brokers assert for angelone.
assert(broker['selection'] == 'angelone')
