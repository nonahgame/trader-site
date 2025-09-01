# 1st update 
# database_utils.py
from flask_setup import (
    conn, db_lock, db_path, logger, EU_TZ, SYMBOL, TIMEFRAME, STOP_LOSS_PERCENT,
    TAKE_PROFIT_PERCENT, exchange, AMOUNTS, tracking_enabled, last_sell_profit,
    tracking_has_buy, tracking_buy_price, total_return_profit, upload_to_github
)
import pandas as pd
import numpy as np

def create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg, order_id, strategy):
    def safe_float(val, default=0.0):
        return float(val) if val is not None and not pd.isna(val) else default

    def safe_int(val, default=0):
        return int(val) if val is not None and not pd.isna(val) else default

    latest = df.iloc[-1] if not df.empty else pd.Series()

    return {
        'time': datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S") if 'EU_TZ' in globals() else datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        'action': action,
        'symbol': SYMBOL,
        'price': float(current_price),
        'open_price': safe_float(latest_data.get('Open')),
        'close_price': safe_float(latest_data.get('Close')),
        'volume': safe_float(latest_data.get('Volume')),
        'percent_change': float(((current_price - df['Close'].iloc[-2]) / df['Close'].iloc[-2] * 100)
                                if len(df) >= 2 and df['Close'].iloc[-2] != 0 else 0.0),
        'stop_loss': None,
        'take_profit': None,
        'profit': safe_float(profit),
        'total_profit': safe_float(total_profit),
        'return_profit': safe_float(return_profit),
        'total_return_profit': safe_float(total_return_profit),
        'ema1': safe_float(latest.get('ema1')),
        'ema2': safe_float(latest.get('ema2')),
        'rsi': safe_float(latest.get('rsi')),
        'k': safe_float(latest.get('k')),
        'd': safe_float(latest.get('d')),
        'j': safe_float(latest.get('j')),
        'diff': safe_float(latest.get('diff')),
        'macd': safe_float(latest.get('macd')),
        'macd_signal': safe_float(latest.get('macd_signal')),
        'macd_hist': safe_float(latest.get('macd_hist')),
        'lst_diff': safe_float(latest.get('lst_diff')),
        'supertrend': safe_float(latest.get('supertrend')),
        'supertrend_trend': safe_int(latest.get('supertrend_trend')),
        'stoch_rsi': safe_float(latest.get('stoch_rsi')),
        'stoch_k': safe_float(latest.get('stoch_k')),
        'stoch_d': safe_float(latest.get('stoch_d')),
        'obv': safe_float(latest.get('obv')),
        'message': msg,
        'timeframe': TIMEFRAME,
        'order_id': order_id,
        'strategy': strategy
    }

def store_signal(signal):
    global conn
    start_time = time.time()
    with db_lock:
        try:
            if conn is None:
                logger.warning("Database connection is None. Attempting to reinitialize.")
                from flask_setup import setup_database
                if not setup_database():
                    logger.error("Failed to reinitialize database for signal storage")
                    return
            c = conn.cursor()
            c.execute('''
                INSERT INTO trades (
                    time, action, symbol, price, open_price, close_price, volume,
                    percent_change, stop_loss, take_profit, profit, total_profit,
                    return_profit, total_return_profit, ema1, ema2, rsi, k, d, j, diff,
                    macd, macd_signal, macd_hist, lst_diff, supertrend, supertrend_trend,
                    stoch_rsi, stoch_k, stoch_d, obv, message, timeframe, order_id, strategy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                signal['time'], signal['action'], signal['symbol'], signal['price'],
                signal['open_price'], signal['close_price'], signal['volume'],
                signal['percent_change'], signal['stop_loss'], signal['take_profit'],
                signal['profit'], signal['total_profit'],
                signal['return_profit'], signal['total_return_profit'],
                signal['ema1'], signal['ema2'], signal['rsi'],
                signal['k'], signal['d'], signal['j'], signal['diff'],
                signal['macd'], signal['macd_signal'], signal['macd_hist'], signal['lst_diff'],
                signal['supertrend'], signal['supertrend_trend'],
                signal['stoch_rsi'], signal['stoch_k'], signal['stoch_d'], signal['obv'],
                signal['message'], signal['timeframe'], signal['order_id'], signal['strategy']
            ))
            conn.commit()
            elapsed = time.time() - start_time
            logger.debug(f"Signal stored successfully: action={signal['action']}, strategy={signal['strategy']}, time={signal['time']}, order_id={signal['order_id']}, db_write_time={elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error storing signal after {elapsed:.3f}s: {e}")
            conn = None

def ai_decision(df, stop_loss_percent=STOP_LOSS_PERCENT, take_profit_percent=TAKE_PROFIT_PERCENT, position=None, buy_price=None):
    if df.empty or len(df) < 1:
        logger.warning("DataFrame is empty or too small for decision.")
        return "hold", None, None, None

    latest = df.iloc[-1]
    close_price = latest['Close']
    open_price = latest['Open']
    kdj_k = latest['k'] if not pd.isna(latest['k']) else 0.0
    kdj_d = latest['d'] if not pd.isna(latest['d']) else 0.0
    kdj_j = latest['j'] if not pd.isna(latest['j']) else 0.0
    ema1 = latest['ema1'] if not pd.isna(latest['ema1']) else 0.0
    ema2 = latest['ema2'] if not pd.isna(latest['ema2']) else 0.0
    macd = latest['macd'] if not pd.isna(latest['macd']) else 0.0
    macd_signal = latest['macd_signal'] if not pd.isna(latest['macd_signal']) else 0.0
    rsi = latest['rsi'] if not pd.isna(latest['rsi']) else 0.0
    lst_diff = latest['lst_diff'] if not pd.isna(latest['lst_diff']) else 0.0
    stop_loss = None
    take_profit = None
    action = "hold"
    order_id = None

    usdt_amount = AMOUNTS
    try:
        quantity = usdt_amount / close_price
        market = exchange.load_markets()[SYMBOL]
        quantity_precision = market['precision']['amount']
        quantity = exchange.amount_to_precision(SYMBOL, quantity)
        logger.debug(f"Calculated quantity: {quantity} for {usdt_amount} USDT at price {close_price:.2f}")
    except Exception as e:
        logger.error(f"Error calculating quantity: {e}")
        return "hold", None, None, None

    if position == "long" and buy_price is not None:
        stop_loss = buy_price * (1 + stop_loss_percent / 100)
        take_profit = buy_price * (1 + take_profit_percent / 100)
        if close_price <= stop_loss:
            logger.info("Stop-loss triggered.")
            action = "sell"
        elif close_price >= take_profit:
            logger.info("Take-profit triggered.")
            action = "sell"
        elif (macd < macd_signal and lst_diff < -0.56):
            logger.info("Sell-logic triggered.")
            action = "sell"
        elif (lst_diff < -7.00 and kdj_j > 75.00):
            logger.info("Sell-logic triggered.")
            action = "sell"

    if action == "hold" and position is None:
        if (kdj_j < -46.00 and ema1 < ema2 or kdj_j < kdj_d and macd < macd_signal and rsi < 19.00):
            logger.info(f"Buy condition met: kdj_j={kdj_j:.2f}, kdj_d={kdj_d:.2f}, close={close_price:.2f}, open={open_price:.2f}, ema1={ema1:.2f}, ema2={ema2:.2f}")
            action = "buy"
        elif (lst_diff > 7.00 and kdj_j < 10.00 and rsi < 27.00):
            logger.info("Buy-logic triggered.")
            action = "buy"
        elif (macd > macd_signal and ema1 > ema2 and lst_diff > 7.00):
            logger.info("Buy-logic triggered.")
            action = "buy"

    if action == "buy" and position is not None:
        logger.debug("Prevented consecutive buy order.")
        action = "hold"
    if action == "sell" and position is None:
        logger.debug("Prevented sell order without open position.")
        action = "hold"

    if action in ["buy", "sell"] and bot_active:
        try:
            if action == "buy":
                order = exchange.create_market_buy_order(SYMBOL, quantity)
                order_id = str(order['id'])
                logger.info(f"Placed market buy order: {order_id}, quantity={quantity}, price={close_price:.2f}")
            elif action == "sell":
                balance = exchange.fetch_balance()
                asset_symbol = SYMBOL.split("/")[0]
                available_amount = balance[asset_symbol]['free']
                quantity = exchange.amount_to_precision(SYMBOL, available_amount)
                if float(quantity) <= 0:
                    logger.warning("No asset balance available to sell.")
                    return "hold", None, None, None
                order = exchange.create_market_sell_order(SYMBOL, quantity)
                order_id = str(order['id'])
                logger.info(f"Placed market sell order: {order_id}, quantity={quantity}, price={close_price:.2f}")
        except Exception as e:
            logger.error(f"Error placing market order: {e}")
            action = "hold"
            order_id = None

    logger.debug(f"AI decision: action={action}, stop_loss={stop_loss}, take_profit={take_profit}, order_id={order_id}")
    return action, stop_loss, take_profit, order_id

def handle_second_strategy(action, current_price, primary_profit):
    global tracking_enabled, last_sell_profit, tracking_has_buy, tracking_buy_price, total_return_profit
    return_profit = 0
    msg = ""
    if action == "buy":
        if last_sell_profit > 0:
            tracking_has_buy = True
            tracking_buy_price = current_price
            msg = ""
        else:
            msg = " (Paused Buy2)"
    elif action == "sell" and tracking_has_buy:
        last_sell_profit = primary_profit
        if last_sell_profit > 0:
            tracking_enabled = True
        else:
            tracking_enabled = False
        return_profit = current_price - tracking_buy_price
        total_return_profit += return_profit
        tracking_has_buy = False
        msg = f", Return Profit: {return_profit:.2f}"
    elif action == "sell" and not tracking_has_buy:
        last_sell_profit = primary_profit
        if last_sell_profit > 0:
            tracking_enabled = True
        else:
            tracking_enabled = False
        msg = " (Paused Sell2)"
    return return_profit, msg
