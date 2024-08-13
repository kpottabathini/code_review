import logging
import os
import threading
import pandas as pd
import time

from tradebroker.FyersLabel import FyersTickLabels, FyersMarketDepthLabels
from tradebroker.FyersBroker import FyersBroker
from utils.Utils import Utils, IndexNames



class TickLogger:
    _lock = threading.Lock()
    _depth_lock = threading.Lock()
    df_list = {}
    df_market_depth_list = {}
    dest_folder_symbol = "D:\\MarketData\\TickerData\\"
    dest_folder_market_depth = "D:\\MarketData\\market_depth_data\\"


    @staticmethod
    def get_symbol_list():
        return ["NSE:NIFTYBANK-INDEX", "NSE:BANKNIFTY24AUGFUT",
                "NSE:HDFCBANK-EQ", "NSE:ICICIBANK-EQ", "NSE:AXISBANK-EQ", "NSE:SBIN-EQ",
                "NSE:KOTAKBANK-EQ", "NSE:INDUSINDBK-EQ"] + TickLogger.prepare_symbols()
    @staticmethod
    def log(quote=None, depth_data=None):
        with TickLogger._lock:
            logging.debug("TickLogger::logSymbol entering...")
            if quote is None and depth_data is None:
                logging.error("TickLogger::logSymbol input quote object is None ...")
                return

            if quote is not None and quote.type == "if":
                TickLogger.log_indice(quote)
            if quote is not None and quote.type == "sf":
                TickLogger.log_symbol(quote)
            if depth_data is not None:
                TickLogger.logMarketDepth(depth_data)

            logging.debug("TickLogger::logSymbol leaving")

            return

    @staticmethod
    def log_symbol(quote):
        logging.debug("TickLogger::logSymbol entering")
        symbol = quote.symbol
        symbol = symbol.split(":")[1]
        if symbol not in TickLogger.df_list.keys():
            TickLogger.df_list[symbol] = pd.DataFrame(columns=TickLogger.get_symbol_update_header())

        new_row = {}
        new_row[FyersTickLabels.ltp] = quote.ltp
        new_row[FyersTickLabels.vol_traded_today] = quote.vol_traded_today
        new_row[FyersTickLabels.exch_feed_time] = quote.exch_feed_time
        new_row[FyersTickLabels.bid_size] = quote.bid_size
        new_row[FyersTickLabels.ask_size] = quote.ask_size
        new_row[FyersTickLabels.bid_price] = quote.bid_price
        new_row[FyersTickLabels.ask_price] = quote.ask_price
        new_row[FyersTickLabels.last_traded_qty] = quote.last_traded_qty
        new_row[FyersTickLabels.tot_buy_qty] = quote.tot_buy_qty
        new_row[FyersTickLabels.tot_sell_qty] = quote.tot_sell_qty
        new_row[FyersTickLabels.avg_trade_price] = quote.avg_trade_price
        new_row[FyersTickLabels.low_price] = quote.low_price
        new_row[FyersTickLabels.high_price] = quote.high_price
        new_row[FyersTickLabels.open_price] = quote.open_price
        new_row[FyersTickLabels.prev_close_price] = quote.prev_close_price
        new_row[FyersTickLabels.type] = quote.type
        new_row[FyersTickLabels.symbol] = quote.symbol
        new_row[FyersTickLabels.ch] = quote.ch
        new_row[FyersTickLabels.chp] = quote.chp
        new_row[FyersTickLabels.last_traded_time] = quote.last_traded_time
        new_row_df = pd.DataFrame([new_row])
        TickLogger.df_list[symbol] = pd.concat([TickLogger.df_list[symbol], new_row_df], ignore_index=True)

        logging.debug("TickLogger::logSymbol leaving")

    @staticmethod
    def log_indice(quote):
        logging.debug("TickLogger::log_indice entering")
        symbol = quote.symbol
        symbol = symbol.split(":")[1]
        if symbol not in TickLogger.df_list.keys():
            TickLogger.df_list[symbol] = pd.DataFrame(columns=TickLogger.get_indice_upate_header())

        new_row = {}
        new_row[FyersTickLabels.exch_feed_time] = quote.exch_feed_time
        new_row[FyersTickLabels.symbol] = quote.symbol
        new_row[FyersTickLabels.type] = quote.type
        new_row[FyersTickLabels.ltp] = quote.ltp
        new_row[FyersTickLabels.open_price] = quote.open_price
        new_row[FyersTickLabels.high_price] = quote.high_price
        new_row[FyersTickLabels.low_price] = quote.low_price
        new_row[FyersTickLabels.prev_close_price] = quote.prev_close_price
        new_row[FyersTickLabels.ch] = quote.ch
        new_row[FyersTickLabels.chp] = quote.chp
        new_row_df = pd.DataFrame([new_row])
        TickLogger.df_list[symbol] = pd.concat([TickLogger.df_list[symbol], new_row_df], ignore_index=True)
        logging.debug("TickLogger::log_indice leaving")


    @staticmethod
    def logMarketDepth(market_depth):
        logging.debug("TickLogger::logMarketDepth entering")
        if market_depth is None:
            logging.error("TickLogger::logMarketDepth input object is None, returning")
            return
        data_type = market_depth[FyersMarketDepthLabels.type]
        if data_type != "dp":
            logging.error("TickLogger::logMarketDepth input data is not of tp type, returning")
            return


        symbol = market_depth[FyersMarketDepthLabels.symbol]
        symbol = symbol.split(":")[1]
        if symbol not in TickLogger.df_market_depth_list:
            TickLogger.df_market_depth_list[symbol] = pd.DataFrame(columns=TickLogger.get_market_depth_header())

        new_row = {}
        new_row[FyersMarketDepthLabels.type] = market_depth[FyersMarketDepthLabels.type]
        new_row[FyersMarketDepthLabels.timestamp] = time.time()
        new_row[FyersMarketDepthLabels.symbol] = market_depth[FyersMarketDepthLabels.symbol]
        new_row[FyersMarketDepthLabels.bid_price1] = market_depth[FyersMarketDepthLabels.bid_price1]
        new_row[FyersMarketDepthLabels.bid_price2] = market_depth[FyersMarketDepthLabels.bid_price2]
        new_row[FyersMarketDepthLabels.bid_price3] = market_depth[FyersMarketDepthLabels.bid_price3]
        new_row[FyersMarketDepthLabels.bid_price4] = market_depth[FyersMarketDepthLabels.bid_price4]
        new_row[FyersMarketDepthLabels.bid_price5] = market_depth[FyersMarketDepthLabels.bid_price5]
        new_row[FyersMarketDepthLabels.ask_price1] = market_depth[FyersMarketDepthLabels.ask_price1]
        new_row[FyersMarketDepthLabels.ask_price2] = market_depth[FyersMarketDepthLabels.ask_price2]
        new_row[FyersMarketDepthLabels.ask_price3] = market_depth[FyersMarketDepthLabels.ask_price3]
        new_row[FyersMarketDepthLabels.ask_price4] = market_depth[FyersMarketDepthLabels.ask_price4]
        new_row[FyersMarketDepthLabels.ask_price5] = market_depth[FyersMarketDepthLabels.ask_price5]
        new_row[FyersMarketDepthLabels.bid_size1] = market_depth[FyersMarketDepthLabels.bid_size1]
        new_row[FyersMarketDepthLabels.bid_size2] = market_depth[FyersMarketDepthLabels.bid_size2]
        new_row[FyersMarketDepthLabels.bid_size3] = market_depth[FyersMarketDepthLabels.bid_size3]
        new_row[FyersMarketDepthLabels.bid_size4] = market_depth[FyersMarketDepthLabels.bid_size4]
        new_row[FyersMarketDepthLabels.bid_size5] = market_depth[FyersMarketDepthLabels.bid_size5]
        new_row[FyersMarketDepthLabels.ask_size1] = market_depth[FyersMarketDepthLabels.ask_size1]
        new_row[FyersMarketDepthLabels.ask_size2] = market_depth[FyersMarketDepthLabels.ask_size2]
        new_row[FyersMarketDepthLabels.ask_size3] = market_depth[FyersMarketDepthLabels.ask_size3]
        new_row[FyersMarketDepthLabels.ask_size4] = market_depth[FyersMarketDepthLabels.ask_size4]
        new_row[FyersMarketDepthLabels.ask_size5] = market_depth[FyersMarketDepthLabels.ask_size5]
        new_row[FyersMarketDepthLabels.bid_order1] = market_depth[FyersMarketDepthLabels.bid_order1]
        new_row[FyersMarketDepthLabels.bid_order2] = market_depth[FyersMarketDepthLabels.bid_order2]
        new_row[FyersMarketDepthLabels.bid_order3] = market_depth[FyersMarketDepthLabels.bid_order3]
        new_row[FyersMarketDepthLabels.bid_order4] = market_depth[FyersMarketDepthLabels.bid_order4]
        new_row[FyersMarketDepthLabels.bid_order5] = market_depth[FyersMarketDepthLabels.bid_order5]
        new_row[FyersMarketDepthLabels.ask_order1] = market_depth[FyersMarketDepthLabels.ask_order1]
        new_row[FyersMarketDepthLabels.ask_order2] = market_depth[FyersMarketDepthLabels.ask_order2]
        new_row[FyersMarketDepthLabels.ask_order3] = market_depth[FyersMarketDepthLabels.ask_order3]
        new_row[FyersMarketDepthLabels.ask_order4] = market_depth[FyersMarketDepthLabels.ask_order4]
        new_row[FyersMarketDepthLabels.ask_order5] = market_depth[FyersMarketDepthLabels.ask_order5]


        new_row_df = pd.DataFrame([new_row])
        TickLogger.df_market_depth_list[symbol] = pd.concat([TickLogger.df_market_depth_list[symbol], new_row_df], ignore_index=True)
        logging.debug("TickLogger::logMarketDepth leaving")
        return


    @staticmethod
    def get_symbol_update_header():
        return [
            FyersTickLabels.exch_feed_time,
            FyersTickLabels.symbol,
            FyersTickLabels.type,
            FyersTickLabels.ltp,
            FyersTickLabels.open_price,
            FyersTickLabels.high_price,
            FyersTickLabels.low_price,
            FyersTickLabels.prev_close_price,
            FyersTickLabels.avg_trade_price,
            FyersTickLabels.vol_traded_today,
            FyersTickLabels.last_traded_qty,
            FyersTickLabels.tot_buy_qty,
            FyersTickLabels.tot_sell_qty,
            FyersTickLabels.bid_price,
            FyersTickLabels.ask_price,
            FyersTickLabels.bid_size,
            FyersTickLabels.ask_size,
            FyersTickLabels.ch,
            FyersTickLabels.chp
        ]

    @staticmethod
    def get_indice_upate_header():
        return [
            FyersTickLabels.exch_feed_time,
            FyersTickLabels.symbol,
            FyersTickLabels.type,
            FyersTickLabels.ltp,
            FyersTickLabels.open_price,
            FyersTickLabels.high_price,
            FyersTickLabels.low_price,
            FyersTickLabels.prev_close_price,
            FyersTickLabels.ch,
            FyersTickLabels.chp
        ]

    @staticmethod
    def get_market_depth_header():
        return [
            FyersMarketDepthLabels.timestamp,
            FyersMarketDepthLabels.type,
            FyersMarketDepthLabels.symbol,
            FyersMarketDepthLabels.bid_price1,
            FyersMarketDepthLabels.bid_price2,
            FyersMarketDepthLabels.bid_price3,
            FyersMarketDepthLabels.bid_price4,
            FyersMarketDepthLabels.bid_price5,
            FyersMarketDepthLabels.ask_price1,
            FyersMarketDepthLabels.ask_price2,
            FyersMarketDepthLabels.ask_price3,
            FyersMarketDepthLabels.ask_price4,
            FyersMarketDepthLabels.ask_price5,
            FyersMarketDepthLabels.bid_size1,
            FyersMarketDepthLabels.bid_size2,
            FyersMarketDepthLabels.bid_size3,
            FyersMarketDepthLabels.bid_size4,
            FyersMarketDepthLabels.bid_size5,
            FyersMarketDepthLabels.ask_size1,
            FyersMarketDepthLabels.ask_size2,
            FyersMarketDepthLabels.ask_size3,
            FyersMarketDepthLabels.ask_size4,
            FyersMarketDepthLabels.ask_size5,
            FyersMarketDepthLabels.bid_order1,
            FyersMarketDepthLabels.bid_order2,
            FyersMarketDepthLabels.bid_order3,
            FyersMarketDepthLabels.bid_order4,
            FyersMarketDepthLabels.bid_order5,
            FyersMarketDepthLabels.ask_order1,
            FyersMarketDepthLabels.ask_order2,
            FyersMarketDepthLabels.ask_order3,
            FyersMarketDepthLabels.ask_order4,
            FyersMarketDepthLabels.ask_order5
        ]

    @staticmethod
    def save_tickdata():
        logging.debug("TickLogger::save_tickdata entering...")
        with TickLogger._lock:
            for symbol in TickLogger.df_list:
                df = TickLogger.df_list[symbol]
                file_path = TickLogger.dest_folder_symbol + symbol + ".csv"
                # Check if the file exists
                if not os.path.isfile(file_path):
                    # If the file does not exist, write the DataFrame to a new file
                    df.to_csv(file_path, index=False)
                else:
                    df.to_csv(file_path, mode='a', header=False, index=False)
                # Clear all rows
                # df = df.drop(df.index)
                TickLogger.df_list[symbol] = TickLogger.df_list[symbol].drop(TickLogger.df_list[symbol].index)

            for symbol in TickLogger.df_market_depth_list:
                df = TickLogger.df_market_depth_list[symbol]
                file_path = TickLogger.dest_folder_market_depth + symbol + ".csv"
                # Check if the file exists
                if not os.path.isfile(file_path):
                    # If the file does not exist, write the DataFrame to a new file
                    df.to_csv(file_path, index=False)
                else:
                    df.to_csv(file_path, mode='a', header=False, index=False)
                # Clear all rows
                # df = df.drop(df.index)
                TickLogger.df_market_depth_list[symbol] = TickLogger.df_market_depth_list[symbol].drop(TickLogger.df_market_depth_list[symbol].index)
            x = 0

        logging.debug("TickLogger::save_tickdata leaving...")

    @staticmethod
    def prepare_symbols():
        # get BankNifty Symbol
        instance = FyersBroker.getInstance()
        data = instance.getQotes(["NSE:NIFTYBANK-INDEX"])  # Get BankNifty LTP
        logging.info(f"TickLogger::prepare_symbols data received for NSE:NIFTYBANK-INDEX")
        logging.info("=======================================================================")
        logging.info(data)
        logging.info("=======================================================================")
        symbol_list = []
        if (data is not None) and data['code'] == 200:

            # ltp = json.loads(data) # ['d']['v']['lp']
            ltp = data['d'][0]['v']['lp']
            strike_price = Utils.getNearestStrikePrice(ltp, nearestMultiple=100)
            start_strike = strike_price - 1000
            end_strike = strike_price + 1000
            current_symbol = ""
            for strike_price in range(start_strike, end_strike + 100, 100):
                for j in range(0, 2):
                    if j % 2 == 0:
                        current_symbol = Utils.prepareIndexOptionSymbol(exchange="NSE",
                                                                        inputSymbol=IndexNames.BANKNIFTY,
                                                                        strike_price=strike_price,
                                                                        option_type="CE")
                    else:
                        current_symbol = Utils.prepareIndexOptionSymbol(exchange="NSE",
                                                                                    inputSymbol=IndexNames.BANKNIFTY,
                                                                                    strike_price=strike_price,
                                                                                    option_type="PE")
                    if current_symbol not in symbol_list:
                        symbol_list.append(current_symbol)
        logging.info(f"TickLogger::prepare_symbols - final symbol list - {symbol_list}")
        return symbol_list







