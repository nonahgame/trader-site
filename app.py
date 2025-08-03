']}" if 
except ': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '1d': 86400}.get(TIMEFRAME, TIMEFRAMES)
    
    
            if datetime.now(EU_TZ) >= stop_time:
                bot_active = False
                if 

            if not bot_active:
                time.sleep(10)
                continue

        try:
            if pause_start and pause_duration > 0:
                elapsed = (datetime.now(EU_TZ) - pause_start).total_seconds()
                if elapsed < pause_duration:
                    logger.info(f"Bot paused, resuming in {int(pause_duration - elapsed)} seconds")
                    time.sleep(min(pause_duration - elapsed, 60))
                    continue
                else:
                    datetime.now(EU_TZ)
                                                        live_position = None
                                        live_buy_price = None
                                        save_state()
                                    bot_active = False
                                bot.send_message(chat_id=command_chat_id, text=f"Bot paused for {pause_duration/60} minutes.")
                                upload_to_github(db_path, 're_bot.db')  # Updated to re_bot.db
                            elif text == '/start':
                                with bot_lock:
                                    if not bot_active:
                                        bot_active = True
                                        position = None
                                        live_position = None
                                        live_buy_price = None
                                        pause_start = None
                                        pause_duration = 0
                                        save_state()
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
                except telegram.error.ChatNotFound:
                    logger.error(f"Chat not found for chat_id: {CHAT_ID}. Skipping Telegram updates.")
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
            df = add_technical_indicators(df)

            prev_close = df['Close'].iloc[-2] if len(df) >= 2 else df['Close'].iloc[-1]
            percent_change = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0.0
            recommended_action = ai_decision(df, position=position)
            live_action, order_id = third_strategy(df, position=live_position, buy_price=live_buy_price)

            with bot_lock:
                action = "hold"
                profit = 0
                return_profit = 0
                msg = f"HOLD {SYMBOL} at {current_price:.2f}"
                if bot_active and recommended_action == "buy" and position is None:
                    position = "long"
                    buy_price = current_price
                    action = "buy"
                    return_profit, msg_suffix = handle_second_strategy("buy", current_price, 0)
                    msg = f"BUY {SYMBOL} at {current_price:.2f}{msg_suffix}"
                elif bot_active and recommended_action == "sell" and position == "long":
                    profit = current_price - buy_price
                    total_profit += profit
                    return_profit, msg_suffix = handle_second_strategy("sell", current_price, profit)
                    position = None
                    action = "sell"
                    msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}{msg_suffix}"

                signal = create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg, None, "primary")
                store_signal(signal)
                logger.debug(f"Generated primary signal: action={signal['action']}, time={signal['time']}, price={signal['price']:.2f}")

                live_msg = f"LIVE HOLD {SYMBOL} at {current_price:.2f}"
                live_profit = 0
                if bot_active and live_action == "buy" and live_position is None:
                    live_msg = f"LIVE BUY {SYMBOL} at {current_price:.2f}, Order ID: {order_id}"
                elif bot_active and live_action == "sell" and live_position == "long":
                    live_profit = current_price - live_buy_price
                    live_total_profit += live_profit
                    live_msg = f"LIVE SELL {SYMBOL} at {current_price:.2f}, Profit: {live_profit:.2f}, Order ID: {order_id}"

                if live_action != "hold":
                    live_signal = create_signal(live_action, current_price, latest_data, df, live_profit, live_total_profit, 0, 0, live_msg, order_id, "third")
                    store_signal(live_signal)
                    logger.debug(f"Generated live signal: action={live_signal['action']}, time={live_signal['time']}, price={live_signal['price']:.2f}, order_id={order_id}")

                if bot_active and (action != "hold" or live_action != "hold") and bot:
                    if action != "hold":
                        threading.Thread(target=send_telegram_message, args=(signal, BOT_TOKEN, CHAT_ID), daemon=True).start()
                    if live_action != "hold":
                        threading.Thread(target=send_telegram_message, args=(live_signal, BOT_TOKEN, CHAT_ID), daemon=True).start()

            if bot_active and (action != "hold" or live_action != "hold"):
                upload_to_github(db_path, 're_bot.db')  # Updated to re_bot.db

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

# Helper functions
def create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg, order_id, strategy):
    latest = df.iloc[-1]
    return {
        'time': datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        'action': action,
        'symbol': SYMBOL,
        'price': float(current_price),
        'open_price': float(latest_data['Open']) if not pd.isna(latest_data['Open']) else 0.0,
        'close_price': float(latest_data['Close']) if not pd.isna(latest_data['Close']) else 0.0,
        'volume': float(latest_data['Volume']) if not pd.isna(latest_data['Volume']) else 0.0,
        'percent_change': float(((current_price - df['Close'].iloc[-2]) / df['Close'].iloc[-2] * 100) if len(df) >= 2 and df['Close'].iloc[-2] != 0 else 0.0),
        'stop_loss': None,
        'take_profit': None,
        'profit': float(profit),
        'total_profit': float(total_profit),
        'return_profit': float(return_profit),
        'total_return_profit': float(total_return_profit),
        'ema1': float(latest['ema1']) if not pd.isna(latest['ema1']) else 0.0,
        'ema2': float(latest['ema2']) if not pd.isna(latest['ema2']) else 0.0,
        'rsi': float(latest['rsi']) if not pd.isna(latest['rsi']) else 0.0,
        'k': float(latest['k']) if not pd.isna(latest['k']) else 0.0,
        'd': float(latest['d']) if not pd.isna(latest['d']) else 0.0,
        'j': float(latest['j']) if not pd.isna(latest['j']) else 0.0,
        'diff': float(latest['diff']) if not pd.isna(latest['diff']) else 0.0,
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
                if not setup_database():
                    logger.error("Failed to reinitialize database for signal storage")
                    return
            c = conn.cursor()
            c.execute('''
                INSERT INTO trades (
                    time, action, symbol, price, open_price, close_price, volume,
                    percent_change, stop_loss, take_profit, profit, total_profit,
                    return_profit, total_return_profit, ema1, ema2, rsi, k, d, j, diff, message, timeframe, order_id, strategy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                signal['time'], signal['action'], signal['symbol'], signal['price'],
                signal['open_price'], signal['close_price'], signal['volume'],
                signal['percent_change'], signal['stop_loss'], signal['take_profit'],
                signal['profit'], signal['total_profit'],
                signal['return_profit'], signal['total_return_profit'],
                signal['ema1'], signal['ema2'], signal['rsi'],
                signal['k'], signal['d'], signal['j'], signal['diff'],
                signal['message'], signal['timeframe'], signal['order_id'], signal['strategy']
            ))
            conn.commit()
            elapsed = time.time() - start_time
            logger.debug(f"Signal stored successfully: action={signal['action']}, strategy={signal['strategy']}, time={signal['time']}, order_id={signal['order_id']}, db_write_time={elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error storing signal after {elapsed:.3f}s: {e}")
            conn = None

def get_performance():
    global conn
    start_time = time.time()
    with db_lock:
        try:
            if conn is None:
                logger.warning("Database connection is None. Attempting to reinitialize.")
                if not setup_database():
                    logger.error("Failed to reinitialize database for performance")
                    return "Database not initialized. Please try again later."
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM trades")
            trade_count = c.fetchone()[0]
            if trade_count == 0:
                return "No trades available for performance analysis."
            c.execute("SELECT DISTINCT timeframe FROM trades")
            timeframes = [row[0] for row in c.fetchall()]
            message = "Performance Statistics by Timeframe:\n"
            for tf in timeframes:
                c.execute("SELECT MIN(time), MAX(time), SUM(profit), SUM(return_profit), COUNT(*) FROM trades WHERE action='sell' AND profit IS NOT NULL AND timeframe=?", (tf,))
                result = c.fetchone()
                min_time, max_time, total_profit_db, total_return_profit_db, win_trades = result if result else (None, None, None, None, 0)
                c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND profit < 0 AND timeframe=?", (tf,))
                loss_trades = c.fetchone()[0]
                duration = (datetime.strptime(max_time, "%Y-%m-%d %H:%M:%S") - datetime.strptime(min_time, "%Y-%m-%d %H:%M:%S")).total_seconds() / 3600 if min_time and max_time else "N/A"
                total_profit_db = total_profit_db if total_profit_db is not None else 0
                total_return_profit_db = total_return_profit_db if total_return_profit_db is not None else 0
                message += f"""
Timeframe: {tf}
Duration (hours): {duration if duration != "N/A" else duration}
Win Trades: {win_trades}
Loss Trades: {loss_trades}
Total Profit: {total_profit_db:.2f}
Total Return Profit: {total_return_profit_db:.2f}
"""
            elapsed = time.time() - start_time
            logger.debug(f"Performance data fetched in {elapsed:.3f}s")
            return message
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error fetching performance after {elapsed:.3f}s: {e}")
            conn = None
            return f"Error fetching performance data: {str(e)}"

def get_trade_counts():
    global conn
    start_time = time.time()
    with db_lock:
        try:
            if conn is None:
                logger.warning("Database connection is None. Attempting to reinitialize.")
                if not setup_database():
                    logger.error("Failed to reinitialize database for trade counts")
                    return "Database not initialized. Please try again later."
            c = conn.cursor()
            c.execute("SELECT DISTINCT timeframe FROM trades")
            timeframes = [row[0] for row in c.fetchall()]
            message = "Trade Counts by Timeframe:\n"
            for tf in timeframes:,))
                loss_trades = c.fetchone()[0]
                total_profit_db = total_profit_db if total_profit_db is not None else 0
                
