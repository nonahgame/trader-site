# 2nd update 
# trade_bot.py
from flask_setup import (
    bot_active, position, buy_price, total_profit, pause_duration, pause_start, conn,
    stop_time, BOT_TOKEN, CHAT_ID, SYMBOL, TIMEFRAME, TIMEFRAMES, AMOUNTS, EU_TZ,
    bot_lock, exchange, logger, upload_to_github
)
from telegram import Bot
import telegram
from fetch_price import get_simulated_price, technical_indicators  # Updated import
from database_utils import ai_decision, create_signal, store_signal, handle_second_strategy
from telegram_message import send_telegram_message
import pandas as pd
import time
from datetime import datetime, timedelta
import threading

def get_next_timeframe_boundary(current_time, timeframe_seconds):
    current_seconds = (current_time.hour * 3600 + current_time.minute * 60 + current_time.second)
    intervals_passed = current_seconds // timeframe_seconds
    next_boundary = (intervals_passed + 1) * timeframe_seconds
    seconds_until_boundary = next_boundary - current_seconds
    return seconds_until_boundary

def trading_bot():
    global bot_active, position, buy_price, total_profit, pause_duration, pause_start, conn, stop_time
    bot = None
    try:
        bot = Bot(token=BOT_TOKEN)
        logger.info("Telegram bot initialized successfully")
        test_signal = {
            'time': datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            'action': 'test',
            'symbol': SYMBOL,
            'price': 0.0,
            'open_price': 0.0,
            'close_price': 0.0,
            'volume': 0.0,
            'percent_change': 0.0,
            'stop_loss': None,
            'take_profit': None,
            'profit': 0.0,
            'total_profit': 0.0,
            'return_profit': 0.0,
            'total_return_profit': 0.0,
            'ema1': 0.0,
            'ema2': 0.0,
            'rsi': 0.0,
            'k': 0.0,
            'd': 0.0,
            'j': 0.0,
            'diff': 0.0,
            'macd': 0.0,
            'macd_signal': 0.0,
            'macd_hist': 0.0,
            'lst_diff': 0.0,
            'supertrend': 0.0,
            'supertrend_trend': 0,
            'stoch_rsi': 0.0,
            'stoch_k': 0.0,
            'stoch_d': 0.0,
            'obv': 0.0,
            'message': f"Test message for {SYMBOL} bot startup",
            'timeframe': TIMEFRAME,
            'order_id': None,
            'strategy': 'test'
        }
        send_telegram_message(test_signal, BOT_TOKEN, CHAT_ID)
        store_signal(test_signal)
    except telegram.error.InvalidToken:
        logger.warning("Invalid Telegram bot token. Telegram functionality disabled.")
        bot = None
    except telegram.error.ChatNotFound:
        logger.warning(f"Chat not found for chat_id: {CHAT_ID}. Telegram functionality disabled.")
        bot = None
    except Exception as e:
        logger.error(f"Error initializing Telegram bot: {e}")
        bot = None

    last_update_id = 0
    df = None

    initial_signal = {
        'time': datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        'action': 'hold',
        'symbol': SYMBOL,
        'price': 0.0,
        'open_price': 0.0,
        'close_price': 0.0,
        'volume': 0.0,
        'percent_change': 0.0,
        'stop_loss': None,
        'take_profit': None,
        'profit': 0.0,
        'total_profit': 0.0,
        'return_profit': 0.0,
        'total_return_profit': 0.0,
        'ema1': 0.0,
        'ema2': 0.0,
        'rsi': 0.0,
        'k': 0.0,
        'd': 0.0,
        'j': 0.0,
        'diff': 0.0,
        'macd': 0.0,
        'macd_signal': 0.0,
        'macd_hist': 0.0,
        'lst_diff': 0.0,
        'supertrend': 0.0,
        'supertrend_trend': 0,
        'stoch_rsi': 0.0,
        'stoch_k': 0.0,
        'stoch_d': 0.0,
        'obv': 0.0,
        'message': f"Initializing bot for {SYMBOL}",
        'timeframe': TIMEFRAME,
        'order_id': None,
        'strategy': 'initial'
    }
    store_signal(initial_signal)
    upload_to_github(db_path, 'rr_bot.bd')
    logger.info("Initial hold signal generated")

    for attempt in range(3):
        try:
            ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=100)
            if not ohlcv:
                logger.warning(f"No historical data for {SYMBOL}. Retrying...")
                time.sleep(5)
                continue
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EU_TZ)
            df.set_index('timestamp', inplace=True)
            df['High'] = df['High'].fillna(df['Close'])
            df['Low'] = df['Low'].fillna(df['Close'])
            df = technical_indicators(df)  # Updated function call
            logger.info(f"Initial df shape: {df.shape}")
            break
        except Exception as e:
            logger.error(f"Error fetching historical data (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                logger.error(f"Failed to fetch historical data for {SYMBOL}")
                return

    timeframe_seconds = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '1d': 86400}.get(TIMEFRAME, TIMEFRAMES)

    current_time = datetime.now(EU_TZ)
    seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
    logger.info(f"Waiting {seconds_to_wait:.2f} seconds to align with next {TIMEFRAME} boundary")
    time.sleep(seconds_to_wait)

    while True:
        loop_start_time = datetime.now(EU_TZ)
        with bot_lock:
            if STOP_AFTER_SECONDS > 0 and datetime.now(EU_TZ) >= stop_time:
                bot_active = False
                if position == "long":
                    latest_data = get_simulated_price()
                    if not pd.isna(latest_data['Close']):
                        profit = latest_data['Close'] - buy_price
                        total_profit += profit
                        return_profit, msg = handle_second_strategy("sell", latest_data['Close'], profit)
                        usdt_amount = AMOUNTS
                        quantity = exchange.amount_to_precision(SYMBOL, usdt_amount / latest_data['Close'])
                        order_id = None
                        try:
                            order = exchange.create_market_sell_order(SYMBOL, quantity)
                            order_id = str(order['id'])
                            logger.info(f"Placed market sell order on stop: {order_id}, quantity={quantity}, price={latest_data['Close']:.2f}")
                        except Exception as e:
                            logger.error(f"Error placing market sell order on stop: {e}")
                        signal = create_signal("sell", latest_data['Close'], latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot stopped due to time limit{msg}", order_id, "primary")
                        store_signal(signal)
                        if bot:
                            send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                    position = None
                logger.info("Bot stopped due to time limit")
                upload_to_github(db_path, 'rr_bot.bd')
                break

            if not bot_active:
                logger.info("Bot is stopped. Attempting to restart.")
                bot_active = True
                position = None
                pause_start = None
                pause_duration = 0
                if STOP_AFTER_SECONDS > 0:
                    stop_time = datetime.now(EU_TZ) + timedelta(seconds=STOP_AFTER_SECONDS)
                if bot:
                    bot.send_message(chat_id=CHAT_ID, text="Bot restarted automatically.")
                continue

        try:
            if pause_start and pause_duration > 0:
                elapsed = (datetime.now(EU_TZ) - pause_start).total_seconds()
                if elapsed < pause_duration:
                    logger.info(f"Bot paused, resuming in {int(pause_duration - elapsed)} seconds")
                    time.sleep(min(pause_duration - elapsed, 60))
                    continue
                else:
                    pause_start = None
                    pause_duration = 0
                    position = None
                    logger.info("Bot resumed after pause")
                    if bot:
                        bot.send_message(chat_id=CHAT_ID, text="Bot resumed after pause.")

            latest_data = get_simulated_price()
            if pd.isna(latest_data['Close']):
                logger.warning("Skipping cycle due to missing price data.")
                current_time = datetime.now(EU_TZ)
                seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
                time.sleep(seconds_to_wait)
                continue
            current_price = latest_data['Close']
            current_time = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")

            if bot:
                try:
                    updates = bot.get_updates(offset=last_update_id, timeout=10)
                    for update in updates:
                        if update.message and update.message.text:
                            text = update.message.text.strip()
                            command_chat_id = update.message.chat.id
                            if text == '/help':
                                bot.send_message(chat_id=command_chat_id, text="Commands: /help, /stop, /stopN, /start, /status, /performance, /count")
                            elif text == '/stop':
                                with bot_lock:
                                    if bot_active and position == "long":
                                        profit = current_price - buy_price
                                        total_profit += profit
                                        return_profit, msg = handle_second_strategy("sell", current_price, profit)
                                        usdt_amount = AMOUNTS
                                        quantity = exchange.amount_to_precision(SYMBOL, usdt_amount / current_price)
                                        order_id = None
                                        try:
                                            order = exchange.create_market_sell_order(SYMBOL, quantity)
                                            order_id = str(order['id'])
                                            logger.info(f"Placed market sell order on /stop: {order_id}, quantity={quantity}, price={current_price:.2f}")
                                        except Exception as e:
                                            logger.error(f"Error placing market sell order on /stop: {e}")
                                        signal = create_signal("sell", current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot stopped via Telegram{msg}", order_id, "primary")
                                        store_signal(signal)
                                        if bot:
                                            send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                        position = None
                                    bot_active = False
                                bot.send_message(chat_id=command_chat_id, text="Bot stopped.")
                                upload_to_github(db_path, 'rr_bot.bd')
                            elif text.startswith('/stop') and text[5:].isdigit():
                                multiplier = int(text[5:])
                                with bot_lock:
                                    pause_duration = multiplier * timeframe_seconds
                                    pause_start = datetime.now(EU_TZ)
                                    if position == "long":
                                        profit = current_price - buy_price
                                        total_profit += profit
                                        return_profit, msg = handle_second_strategy("sell", current_price, profit)
                                        usdt_amount = AMOUNTS
                                        quantity = exchange.amount_to_precision(SYMBOL, usdt_amount / current_price)
                                        order_id = None
                                        try:
                                            order = exchange.create_market_sell_order(SYMBOL, quantity)
                                            order_id = str(order['id'])
                                            logger.info(f"Placed market sell order on /stopN: {order_id}, quantity={quantity}, price={current_price:.2f}")
                                        except Exception as e:
                                            logger.error(f"Error placing market sell order on /stopN: {e}")
                                        signal = create_signal("sell", current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot paused via Telegram{msg}", order_id, "primary")
                                        store_signal(signal)
                                        if bot:
                                            send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                        position = None
                                    bot_active = False
                                bot.send_message(chat_id=command_chat_id, text=f"Bot paused for {pause_duration/60} minutes.")
                                upload_to_github(db_path, 'rr_bot.bd')
                            elif text == '/start':
                                with bot_lock:
                                    if not bot_active:
                                        bot_active = True
                                        position = None
                                        pause_start = None
                                        pause_duration = 0
                                        if STOP_AFTER_SECONDS > 0:
                                            stop_time = datetime.now(EU_TZ) + timedelta(seconds=STOP_AFTER_SECONDS)
                                        bot.send_message(chat_id=command_chat_id, text="Bot started.")
                            elif text == '/status':
                                status = "active" if bot_active else f"paused for {int(pause_duration - (datetime.now(EU_TZ) - pause_start).total_seconds())} seconds" if pause_start else "stopped"
                                bot.send_message(chat_id=command_chat_id, text=status)
                            elif text == '/performance':
                                bot.send_message(chat_id=command_chat_id, text=get_performance())
                            elif text == '/count':
                                bot.send_message(chat_id=command_chat_id, text=get_trade_counts())
                        last_update_id = update.update_id + 1
                except telegram.error.InvalidToken:
                    logger.warning("Invalid Telegram bot token. Skipping Telegram updates.")
                    bot = None
                except telegram.error.ChatNotFound:
                    logger.warning(f"Chat not found for chat_id: {CHAT_ID}. Skipping Telegram updates.")
                    bot = None
                except Exception as e:
                    logger.error(f"Error processing Telegram updates: {e}")

            new_row = pd.DataFrame({
                'Open': [latest_data['Open']],
                'Close': [latest_data['Close']],
                'High': [latest_data['High']],
                'Low': [latest_data['Low']],
                'Volume': [latest_data['Volume']],
                'diff': [latest_data['diff']]
            }, index=[pd.Timestamp.now(tz=EU_TZ)])
            df = pd.concat([df, new_row]).tail(100)
            df = technical_indicators(df)  # Updated function call

            prev_close = df['Close'].iloc[-2] if len(df) >= 2 else df['Close'].iloc[-1]
            percent_change = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0.0
            action, stop_loss, take_profit, order_id = ai_decision(df, position=position, buy_price=buy_price)

            with bot_lock:
                profit = 0
                return_profit = 0
                msg = f"HOLD {SYMBOL} at {current_price:.2f}"
                if bot_active and action == "buy" and position is None:
                    position = "long"
                    buy_price = current_price
                    return_profit, msg_suffix = handle_second_strategy("buy", current_price, 0)
                    msg = f"BUY {SYMBOL} at {current_price:.2f}, Order ID: {order_id}{msg_suffix}"
                elif bot_active and action == "sell" and position == "long":
                    profit = current_price - buy_price
                    total_profit += profit
                    return_profit, msg_suffix = handle_second_strategy("sell", current_price, profit)
                    msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}, Order ID: {order_id}{msg_suffix}"
                    if stop_loss and current_price <= stop_loss:
                        msg += " (Stop-Loss)"
                    elif take_profit and current_price >= take_profit:
                        msg += " (Take-Profit)"
                    position = None

                signal = create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg, order_id, "primary")
                store_signal(signal)
                logger.debug(f"Generated signal: action={signal['action']}, time={signal['time']}, price={signal['price']:.2f}, order_id={signal['order_id']}")

                if bot_active and action != "hold" and bot:
                    threading.Thread(target=send_telegram_message, args=(signal, BOT_TOKEN, CHAT_ID), daemon=True).start()

            if bot_active and action != "hold":
                upload_to_github(db_path, 'rr_bot.bd')

            loop_end_time = datetime.now(EU_TZ)
            processing_time = (loop_end_time - loop_start_time).total_seconds()
            seconds_to_wait = get_next_timeframe_boundary(loop_end_time, timeframe_seconds)
            adjusted_sleep = seconds_to_wait - processing_time
            if adjusted_sleep < 0:
                logger.warning(f"Processing time ({processing_time:.2f}s) exceeded timeframe interval ({timeframe_seconds}s), skipping sleep")
                adjusted_sleep = 0
            logger.debug(f"Sleeping for {adjusted_sleep:.2f} seconds to align with next {TIMEFRAME} boundary")
            time.sleep(adjusted_sleep)
        except Exception as e:
            logger.error(f"Error in trading loop: {e}")
            current_time = datetime.now(EU_TZ)
            seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
            time.sleep(seconds_to_wait)
