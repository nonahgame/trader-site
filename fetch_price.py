# 1st update 
# fetch_price.py
from flask_setup import SYMBOL, TIMEFRAME, exchange, logger, last_valid_price
import pandas as pd
import numpy as np
import pandas_ta as ta
import time

def get_simulated_price(symbol=SYMBOL, exchange=exchange, timeframe=TIMEFRAME, retries=3, delay=5):
    global last_valid_price
    for attempt in range(retries):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=5)
            if not ohlcv:
                logger.warning(f"No data returned for {symbol}. Retrying...")
                time.sleep(delay)
                continue
            data = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EU_TZ)
            data['diff'] = data['Close'] - data['Open']
            non_zero_diff = data[abs(data['diff']) > 0]
            selected_data = non_zero_diff.iloc[-1] if not non_zero_diff.empty else data.iloc[-1]
            if abs(selected_data['diff']) < 0.00:
                logger.warning(f"Open and Close similar for {symbol} (diff={selected_data['diff']}). Accepting data.")
            last_valid_price = selected_data
            logger.debug(f"Fetched price data: {selected_data.to_dict()}")
            return selected_data
        except Exception as e:
            logger.error(f"Error fetching price (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    logger.error(f"Failed to fetch price for {symbol} after {retries} attempts.")
    if last_valid_price is not None:
        logger.info("Using last valid price data as fallback.")
        return last_valid_price
    return pd.Series({'Open': np.nan, 'Close': np.nan, 'High': np.nan, 'Low': np.nan, 'Volume': np.nan, 'diff': np.nan})

def technical_indicators(df):
    start_time = time.time()
    try:
        df = df.copy()
        df['Close'] = df['Close'].ffill()
        df['High'] = df['High'].ffill()
        df['Low'] = df['Low'].ffill()
        df['Volume'] = df['Volume'].ffill()

        df['ema1'] = ta.ema(df['Close'], length=12)
        df['ema2'] = ta.ema(df['Close'], length=26)
        df['rsi'] = ta.rsi(df['Close'], length=14)
        kdj = ta.kdj(df['High'], df['Low'], df['Close'], length=9, signal=3)
        df['k'] = kdj['K_9_3']
        df['d'] = kdj['D_9_3']
        df['j'] = kdj['J_9_3']
        macd = ta.macd(df['Close'], fast=12, slow=26, signal=9)
        df['macd'] = macd['MACD_12_26_9']
        df['macd_signal'] = macd['MACDs_12_26_9']
        df['macd_hist'] = macd['MACDh_12_26_9']
        df['diff'] = df['Close'] - df['Open']
        df['lst_diff'] = df['ema1'].shift(1) - df['ema1']

        st_length = 10
        st_multiplier = 3.0
        high_low = df['High'] - df['Low']
        high_close_prev = (df['High'] - df['Close'].shift()).abs()
        low_close_prev = (df['Low'] - df['Close'].shift()).abs()
        tr = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
        atr = tr.rolling(st_length, min_periods=1).mean()
        hl2 = (df['High'] + df['Low']) / 2
        basic_upperband = hl2 + (st_multiplier * atr)
        basic_lowerband = hl2 - (st_multiplier * atr)
        final_upperband = basic_upperband.copy()
        final_lowerband = basic_lowerband.copy()

        for i in range(1, len(df)):
            if (basic_upperband.iloc[i] < final_upperband.iloc[i-1]) or (df['Close'].iloc[i-1] > final_upperband.iloc[i-1]):
                final_upperband.iloc[i] = basic_upperband.iloc[i]
            else:
                final_upperband.iloc[i] = final_upperband.iloc[i-1]
            if (basic_lowerband.iloc[i] > final_lowerband.iloc[i-1]) or (df['Close'].iloc[i-1] < final_lowerband.iloc[i-1]):
                final_lowerband.iloc[i] = basic_lowerband.iloc[i]
            else:
                final_lowerband.iloc[i] = final_lowerband.iloc[i-1]

        supertrend = final_upperband.where(df['Close'] <= final_upperband, final_lowerband)
        supertrend_trend = df['Close'] > final_upperband.shift()
        supertrend_trend = supertrend_trend.fillna(True)
        df['supertrend'] = supertrend
        df['supertrend_trend'] = supertrend_trend.astype(int)
        df['supertrend_signal'] = np.where(
            supertrend_trend & ~supertrend_trend.shift().fillna(True), 'buy',
            np.where(~supertrend_trend & supertrend_trend.shift().fillna(True), 'sell', None)
        )

        stoch_rsi_len = 14
        stoch_k_len = 3
        stoch_d_len = 3
        rsi = df['rsi'].ffill()
        rsi_min = rsi.rolling(stoch_rsi_len, min_periods=1).min()
        rsi_max = rsi.rolling(stoch_rsi_len, min_periods=1).max()
        stochrsi = (rsi - rsi_min) / (rsi_max - rsi_min + 1e-12)
        df['stoch_rsi'] = stochrsi
        df['stoch_k'] = stochrsi.rolling(stoch_k_len, min_periods=1).mean() * 100
        df['stoch_d'] = df['stoch_k'].rolling(stoch_d_len, min_periods=1).mean()

        close_diff = df['Close'].diff().fillna(0)
        direction = np.sign(close_diff)
        df['obv'] = (direction * df['Volume']).fillna(0).cumsum()

        elapsed = time.time() - start_time
        logger.debug(f"Technical indicators calculated in {elapsed:.3f}s: {df.iloc[-1][['ema1', 'ema2', 'rsi', 'k', 'd', 'j', 'macd', 'macd_signal', 'macd_hist', 'diff', 'lst_diff', 'supertrend', 'supertrend_trend', 'supertrend_signal', 'stoch_k', 'stoch_d', 'obv']].to_dict()}")
        return df
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error calculating indicators after {elapsed:.3f}s: {e}")
        return df
