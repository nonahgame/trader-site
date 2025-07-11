from binance.client import Client
from binance import ThreadedWebsocketManager
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as dt
import asyncio
import os
import logging

# Set up logging for debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Binance client (replace with your API key and secret)
api_key = os.getenv("BINANCE_API_KEY", "api_key")  # Use environment variables for security
secret_key = os.getenv("BINANCE_SECRET_KEY", "secret_key")
client = Client(api_key=api_key, api_secret=secret_key, tld="us", testnet=False) #True)

class LongOnlyTrader:
    def __init__(self, symbol, bar_length, return_thresh, volume_thresh, units, position=0):
        self.symbol = symbol
        self.bar_length = bar_length
        self.available_intervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
        self.units = units
        self.position = position
        self.trades = 0
        self.trade_values = []
        self.return_thresh = return_thresh
        self.volume_thresh = volume_thresh
        self.twm = None
        self.data = None
        self.prepared_data = None

    async def start_trading(self, historical_days):
        try:
            logger.info("Starting Binance WebSocket Manager")
            self.twm = ThreadedWebsocketManager()
            self.twm.start()

            if self.bar_length in self.available_intervals:
                logger.info(f"Fetching historical data for {self.symbol} with interval {self.bar_length}")
                self.get_most_recent(symbol=self.symbol, interval=self.bar_length, days=historical_days)
                logger.info(f"Starting kline socket for {self.symbol}")
                self.twm.start_kline_socket(
                    callback=self.stream_candles,
                    symbol=self.symbol,
                    interval=self.bar_length
                )
                # Keep the event loop running
                while True:
                    if datetime.now(dt.UTC) >= datetime(2025, 7, 11, 15, 52):
                        logger.info("Trading session ended due to time limit")
                        break
                    await asyncio.sleep(1)  # Prevent tight loop
            else:
                logger.error(f"Invalid bar_length: {self.bar_length}. Available intervals: {self.available_intervals}")
        except Exception as e:
            logger.error(f"Error in start_trading: {str(e)}")
            if "Service unavailable from a restricted location" in str(e):
                logger.error("Binance API blocked due to restricted location. Consider using a non-US server or proxy.")
        finally:
            if self.twm:
                self.twm.stop()
                logger.info("WebSocket stopped")

    def get_most_recent(self, symbol, interval, days):
        try:
            now = datetime.now(dt.UTC)
            past = str(now - timedelta(days=days))
            bars = client.get_historical_klines(
                symbol=symbol, interval=interval, start_str=past, end_str=None, limit=1000
            )
            df = pd.DataFrame(bars)
            df["Date"] = pd.to_datetime(df.iloc[:, 0], unit="ms")
            df.columns = [
                "Open Time", "Open", "High", "Low", "Close", "Volume",
                "Close Time", "Quote Asset Volume", "Number of Trades",
                "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore", "Date"
            ]
            df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
            df.set_index("Date", inplace=True)
            for column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")
            df["Complete"] = [True for row in range(len(df)-1)] + [False]
            self.data = df
            logger.info("Historical data fetched successfully")
        except Exception as e:
            logger.error(f"Error fetching historical data: {str(e)}")
            raise

    def stream_candles(self, msg):
        try:
            event_time = pd.to_datetime(msg["E"], unit="ms")
            start_time = pd.to_datetime(msg["k"]["t"], unit="ms")
            first = float(msg["k"]["o"])
            high = float(msg["k"]["h"])
            low = float

(msg["k"]["l"])
            close = float(msg["k"]["c"])
            volume = float(msg["k"]["v"])
            complete = msg["k"]["x"]

            if event_time >= datetime(2025, 7, 11, 15, 52):
                if self.twm:
                    self.twm.stop()
                    logger.info("WebSocket stopped due to time limit")
                if self.position != 0:
                    order = client.create_order(
                        symbol=self.symbol, side="SELL", type="MARKET", quantity=self.units
                    )
                    self.report_trade(order, "GOING NEUTRAL AND STOP")
                    self.position = 0
                else:
                    logger.info("STOP")
            else:
                print(".", end="", flush=True)
                self.data.loc[start_time] = [first, high, low, close, volume, complete]
                if complete:
                    self.define_strategy()
                    self.execute_trades()
        except Exception as e:
            logger.error(f"Error in stream_candles: {str(e)}")

    def define_strategy(self):
        try:
            df = self.data.copy()
            df = df[["Close", "Volume"]].copy()
            df["returns"] = np.log(df.Close / df.Close.shift())
            df["vol_ch"] = np.log(df.Volume.div(df.Volume.shift(1)))
            df.loc[df.vol_ch > 3, "vol_ch"] = np.nan
            df.loc[df.vol_ch < -3, "vol_ch"] = np.nan
            cond1 = df.returns >= self.return_thresh
            cond2 = df.vol_ch.between(self.volume_thresh[0], self.volume_thresh[1])
            df["position"] = 1
            df.loc[cond1 & cond2, "position"] = 0
            self.prepared_data = df.copy()
        except Exception as e:
            logger.error(f"Error in define_strategy: {str(e)}")

    def execute_trades(self):
        try:
            if self.prepared_data["position"].iloc[-1] == 1:
                if self.position == 0:
                    order = client.create_order(
                        symbol=self.symbol, side="BUY", type="MARKET", quantity=self.units
                    )
                    self.report_trade(order, "GOING LONG")
                self.position = 1
            elif self.prepared_data["position"].iloc[-1] == 0:
                if self.position == 1:
                    order = client.create_order(
                        symbol=self.symbol, side="SELL", type="MARKET", quantity=self.units
                    )
                    self.report_trade(order, "GOING NEUTRAL")
                self.position = 0
        except Exception as e:
            logger.error(f"Error in execute_trades: {str(e)}")

    def report_trade(self, order, going):
        try:
            side = order["side"]
            time = pd.to_datetime(order["transactTime"], unit="ms")
            base_units = float(order["executedQty"])
            quote_units = float(order["cummulativeQuoteQty"])
            price = round(quote_units / base_units, 5)
            self.trades += 1
            if side == "BUY":
                self.trade_values.append(-quote_units)
            elif side == "SELL":
                self.trade_values.append(quote_units)
            if self.trades % 2 == 0:
                real_profit = round(np.sum(self.trade_values[-2:]), 3)
                self.cum_profits = round(np.sum(self.trade_values), 3)
            else:
                real_profit = 0
                self.cum_profits = round(np.sum(self.trade_values[:-1]), 3)
            print(2 * "\n" + 100 * "-")
            print(f"{time} | {going}")
            print(f"{time} | Base_Units = {base_units} | Quote_Units = {quote_units} | Price = {price}")
            print(f"{time} | Profit = {real_profit} | CumProfits = {self.cum_profits}")
            print(100 * "-" + "\n")
            logger.info(f"Trade reported: {going}, Profit: {real_profit}, Cumulative Profits: {self.cum_profits}")
        except Exception as e:
            logger.error(f"Error in report_trade: {str(e)}")

# Parameters
symbol = "BTCUSDT"
bar_length = "1m"
return_thresh = 0
volume_thresh = [-3, 3]
units = 0.00011
position = 0

# Instantiate and start trading
async def main():
    trader = LongOnlyTrader(
        symbol=symbol,
        bar_length=bar_length,
        return_thresh=return_thresh,
        volume_thresh=volume_thresh,
        units=units,
        position=position
    )
    await trader.start_trading(historical_days=1/7)

if __name__ == "__main__":
    asyncio.run(main())
