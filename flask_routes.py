# 1st update 
# flask_routes.py
from flask import render_template, jsonify
from flask_setup import app, conn, bot_active, stop_time, EU_TZ, db_lock, TIMEFRAME, logger, setup_database
import time
from datetime import datetime

def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

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
            for tf in timeframes:
                c.execute("SELECT COUNT(*), SUM(profit), SUM(return_profit) FROM trades WHERE timeframe=?", (tf,))
                total_trades, total_profit_db, total_return_profit_db = c.fetchone()
                c.execute("SELECT COUNT(*) FROM trades WHERE action='buy' AND timeframe=?", (tf,))
                buy_trades = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND timeframe=?", (tf,))
                sell_trades = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND profit > 0 AND timeframe=?", (tf,))
                win_trades = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND profit < 0 AND timeframe=?", (tf,))
                loss_trades = c.fetchone()[0]
                total_profit_db = total_profit_db if total_profit_db is not None else 0
                total_return_profit_db = total_return_profit_db if total_return_profit_db is not None else 0
                message += f"""
Timeframe: {tf}
Total Trades: {total_trades}
Buy Trades: {buy_trades}
Sell Trades: {sell_trades}
Win Trades: {win_trades}
Loss Trades: {loss_trades}
Total Profit: {total_profit_db:.2f}
Total Return Profit: {total_return_profit_db:.2f}
"""
            elapsed = time.time() - start_time
            logger.debug(f"Trade counts fetched in {elapsed:.3f}s")
            return message
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error fetching trade counts after {elapsed:.3f}s: {e}")
            conn = None
            return f"Error fetching trade counts: {str(e)}"

@app.route('/')
def index():
    global conn, stop_time
    status = "active" if bot_active else "stopped"
    start_time = time.time()
    with db_lock:
        try:
            if conn is None:
                logger.warning("Database connection is None in index route. Attempting to reinitialize.")
                if not setup_database():
                    logger.error("Failed to reinitialize database for index route")
                    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S") if stop_time else "N/A"
                    current_time = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    return render_template(
                        'index.html',
                        signal=None,
                        status=status,
                        timeframe=TIMEFRAME,
                        trades=[],
                        stop_time=stop_time_str,
                        current_time=current_time
                    )

            c = conn.cursor()
            c.execute("SELECT * FROM trades ORDER BY time DESC LIMIT 16")
            rows = c.fetchall()
            columns = [col[0] for col in c.description]
            trades = [dict(zip(columns, row)) for row in rows]

            numeric_fields = [
                'price', 'open_price', 'close_price', 'volume', 'percent_change', 'stop_loss',
                'take_profit', 'profit', 'total_profit', 'return_profit', 'total_return_profit',
                'ema1', 'ema2', 'rsi', 'k', 'd', 'j', 'diff', 'macd', 'macd_signal', 'macd_hist',
                'lst_diff', 'supertrend', 'stoch_rsi', 'stoch_k', 'stoch_d', 'obv'
            ]

            for trade in trades:
                for field in numeric_fields:
                    trade[field] = safe_float(trade.get(field))

            signal = trades[0] if trades else None
            stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S") if stop_time else "N/A"
            current_time = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")

            elapsed = time.time() - start_time
            logger.info(
                f"Rendering index.html: status={status}, timeframe={TIMEFRAME}, trades={len(trades)}, "
                f"signal_exists={signal is not None}, signal_time={signal['time'] if signal else 'None'}, "
                f"query_time={elapsed:.3f}s"
            )

            return render_template(
                'index.html',
                signal=signal,
                status=status,
                timeframe=TIMEFRAME,
                trades=trades,
                stop_time=stop_time_str,
                current_time=current_time
            )

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error rendering index.html after {elapsed:.3f}s: {e}")
            conn = None
            return "<h1>Error</h1><p>Failed to load page. Please try again later.</p>", 500

@app.route('/status')
def status():
    status = "active" if bot_active else "stopped"
    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S") if stop_time else "N/A"
    return jsonify({"status": status, "timeframe": TIMEFRAME, "stop_time": stop_time_str})

@app.route('/performance')
def performance():
    return jsonify({"performance": get_performance()})

@app.route('/trades')
def trades():
    global conn
    start_time = time.time()
    with db_lock:
        try:
            if conn is None:
                logger.warning("Database connection is None in trades route. Attempting to reinitialize.")
                if not setup_database():
                    logger.error("Failed to reinitialize database for trades route")
                    return jsonify({"error": "Database not initialized. Please try again later."}), 503
            c = conn.cursor()
            c.execute("SELECT * FROM trades ORDER BY time DESC LIMIT 16")
            trades = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
            elapsed = time.time() - start_time
            logger.debug(f"Fetched {len(trades)} trades for /trades endpoint in {elapsed:.3f}s")
            return jsonify(trades)
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error fetching trades after {elapsed:.3f}s: {e}")
            conn = None
            return jsonify({"error": f"Failed to fetch trades: {str(e)}"}), 500
