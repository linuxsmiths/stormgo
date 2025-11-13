#!/usr/bin/env python3

#
# Note: Angelone APIs return close price which is midway between the actual
#       close price and adjusted close price. i.e. it is adjusted for splits
#       but not for dividends. To correctly use the close price for training
#       ML models we need to adjust it for dividends.
#

import os
import json
import argparse
from angelone import AngelOne
import helpers as ch

def main():
    """
    Main method to get/update historical data based on configurations provided
    in historical.json file.
    """
    f = open(os.path.join(ch.pyhistorical_dir, 'historical.json'))
    cfg = json.load(f)

    # Initialize parser
    parser = argparse.ArgumentParser()

    #
    # Add optional argument for loading historical data corresponding to live
    # data. This sounds oxymoronish but don't worry, it just means fetch
    # historical data just to top up recent candles (today's candles since
    # start of trading) for the purpose of live trading.
    #
    # This is useful when pylive crashes during the day and we need to quickly,
    # top up historical data till the last 1Min candle available for the day,
    #
    parser.add_argument("--live", help="Top up live candle data for today", action="store_true")

    #
    # Add optional argument for specifying the csv files containing list of
    # stocks for which historical data must be downloaded.
    #
    # Note: one.csv can be used if you want to just refresh the index data.
    #
    parser.add_argument("--stocklistcsv",
                        type=str,
                        choices=["NIFTY_50.csv", "NIFTY_100.csv", "NIFTY_200.csv", "one.csv"],
                        help="csv file containing stocks to process")

    # Read arguments from command line
    args = parser.parse_args()

    #
    # -l/--live commandline is same as setting process_live_data in
    # backtester.json. It's better to not set process_live_data in the config
    # and use the -l/--live command line option as that allows us to use both
    # modes without making config changes.
    #
    if args.live:
        cfg['topup_live_historical'] = "True"
        print("[pyhistorical] Topping up live candle data for today!")

    #
    # Allow user to override the stocklist_csv config using the --stocklistcsv
    # commandline option. Mainly for use by pycron.
    #
    if args.stocklistcsv:
        cfg['stocklist_csv'] = args.stocklistcsv
        print("[pyhistorical] Forcing stocklistcsv=%s" % cfg['stocklist_csv'])

    if cfg['broker']['selection'] == "angelone":
        broker = AngelOne()
    else:
        assert False, "[broker.selection] Only supported value is angelone"

    broker.download_instruments(cfg)
    broker.populate_list_of_stock_symbols(cfg)
    broker.populate_symbol_to_instrument_token_for_stocklist()
    if cfg['topup_live_historical'] == "True":
        broker.topup_live_candles_till_the_last_minute(cfg)
    else:
        broker.load_historical(cfg)

    f.close()

if __name__ == "__main__":
    main()
