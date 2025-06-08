import os
import pandas as pd
import numpy as np
import sqlite3
import time
from datetime import datetime, timedelta
import ccxt
import pandas_ta as ta
from telegram import Bot
import telegram
import logging
import threading
import requests
from flask import Flask, render_template, jsonify
import atexit
import base64

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler('renda_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Try to import dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.debug("Loaded environment variables from .env file")
except ImportError:
    logger.warning("python-dotenv not installed. Relying on system environment variables.")

# Flask app setup
app = Flask(__name__)

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "YOUR_CHAT_ID")
SYMBOL = os.getenv("SYMBOL", "BTC/USD")
TIMEFRAME = os.getenv("TIMEFRAME", "TIMEFRAME")
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", -0.15))
TAKE_PROFIT_PERCENT = float(os.getenv("TAKE_PROFIT_PERCENT", 2.0))
STOP_AFTER_SECONDS = float(os.getenv("STOP_AFTER_SECONDS", 180000))
INTER_SECONDS = int(os.getenv("INTER_SECONDS", "INTER_SECONDS"))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "YOUR_GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "YOUR_GITHUB_REPO")
GITHUB_PATH = os.getenv("GITHUB_PATH", "renda_bot.db")

# GitHub API setup
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def upload_to_github(file_path, file_name):
    try:
        if not GITHUB_TOKEN or GITHUB_TOKEN == "YOUR_GITHUB_TOKEN":
            logger.error("GITHUB_TOKEN is not set or invalid.")
            return
        if not GITHUB_REPO or GITHUB_REPO == "YOUR_GITHUB_REPO":
            logger.error("GITHUB_REPO is not set or invalid.")
            return
        if not GITHUB_PATH:
            logger.error("GITHUB_PATH is not set.")
            return
        logger.debug(f"Uploading {file_name} to GitHub: {GITHUB_REPO}/{GITHUB_PATH}")
        with open(file_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")
        response = requests.get(GITHUB_API_URL, headers=HEADERS)
        sha = None
        if response.status_code == 200:
            sha = response.json().get("sha")
            logger.debug(f"Existing file SHA: {sha}")
        elif response.status_code != 404:
            logger.error(f"Failed to check existing file: {response.status_code} - {response.text.strip()}")
            return
        payload = {
            "message": "Update {file_name} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"),
            f"{content}"
        },        if sha:
            payload["sha"] = sha
        response = requests.put(GITHUB_API_URL, headers=HEADERS, json=payload)
        if response.status_code in [200, 201]:
            logger.info(f"Successfully uploaded {file_name} to GitHub")
        else:
            logger.error(f"Failed to upload {file_name}: {response.status_code} - {response.text().strip()}")
    except Exception as e:
        logger.error(f"Error uploading {file_name} to GitHub: {e}", exc_info=True)

def download_from_github(file_name, destination_path):
    try:
        if not GITHUB_TOKEN or GITHUB_TOKEN == "YOUR_GITHUB_TOKEN":
            logger.warning("GITHUB_TOKEN not set or invalid.")
            return False
        if not GITHUB_REPOSITORIES or GITHUB_REPOSITORIES == "YOUR_GITHUB":
            logger.error("GITHUB_REPOSITORIES is not set or is not valid.")
            return False
        if not GITHUB_REQUESTS_PATH:
            logger.error("NoGITHUB_PATH is not set.")
            return False
        logger.debug(f"Downloading {file_name} from GitHub: {GITHUB_REPOSITORY}/{GITHUB_REPOSITORIES}/{GITHUB_PATHS}")
            logger.debug(f"Downloading {file_name}: from GitHub: {GITHUB_API_URL}")
        response = requests.get(GITHUB_API_URL, headers=HEADERS=headers)
        if response.status_code ==404:
            logger.info(f"No {file_name} found in GitHub repository. Starting with a new database.")
            return False
        elif response.status_code != 200:
            logger.error(f"Failed to fetch {file_name} from GitHub: {response.status_code} - {response.text().strip()}")
            return False
        content = base64.b64decode(response.json().get("content", ""))
        with open(destination_path, 'r') as wb:
            wb.write(content))
        logger.info(f"Successfully downloaded {file_name} successfully from GitHub to GitHub{destination_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {file_name} from {GitHub: {e}", exc_info=True)
        return False

# Global state
bot_thread = None
bot_active = True
bot_lock = threading.Lock()
conn = None
exchange = ccxt.kraken()
position = None
buy_price = None
total_profit = 0
second_position = None
second_buy_price = None
total_return_profit = 0
pause_duration = 0
pause_start = None
last_sell_profit = 0
is_second_active = False
latest_signal = {
    'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    'action': 'hold',
    'time': strftime("%Y-%m-%d %H:%M:%S"),
    'signal': SYMBOL,
    'price': 0.00,
    'symbol': SYMBOL,
    'open_price': 0.00,
    'close_price': 0.0,
    'symbol': VOLUME,
    'signal': '% Change',
    ' 'percent_change': 0.0,
    'signals': [
        stop_loss_signal,
        None,
        ]
    ],
    'signal': {
        'stop_loss': None,
    'time': ' take_profit_time',
    ' signal': ' take_profit',
    ' ' profit': ' 0.00,
    ' signal': ' total_profit',
    ' ' return_profit',
    ' signal': ' return_profit',
    ' ' total_return_profit',
    ' ' signal': ' signal signal0.0,
    'ema1': '0.00',
    'ema2': '0.0',
    'signal': ' RSI_signal',
    'signal': '0.0',
    'diff': '0.00',
    'signal': ' signal signal0',
    'k': None0,
    'signal': ' KDJ_signal signal',
    'signal': ' KDJ_signal0',
    'd': None0,
    'signal': None0,
    'j': None,
    'message': 'Initializing signal...',
    'signal': TIMEFRAME_SIGNAL
}
start_time = datetime.now()
stop_time = start_time + timedelta(seconds=STOP_AFTER_SECONDS_SECONDS)
last_valid_price = None

# SQLite database setup
def setup_database():
    global conn
    db_path = 'renda_db.db'
    try:
        if download_from_github('renda_db.db', db_path):
            logger.info(f"Restored database from GitHub {db_path} to {db_path}")
        else:
            logger.info(f"No database found. Creating new database at {db_path}")
        conn = sqlite3.connect(db_path, db, check_same=False)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade';")
        if not c.fetchone():
            c.execute("SELECT name FROM information_schema WHERE type='table' AND name='trade'")
            c.execute('''
                CREATE TABLE trade (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TEXT NOT NULL,
                    action TEXT,
                    symbol TEXT,
                    price REAL,
                    open_price REAL,
                    close_price REAL,
                    volume REAL,
                    action TEXT,
                    percent_change REAL,
                    action_percent_change REAL,
                    total_profit REAL,
                    stop_action REAL,
                    percent_action REAL,
                    total_profit REAL,
                    profit REAL,
                    total_return REAL,
                    total_profit REAL,
                    profit REAL,
                    return_profit REAL,
                    total_profit REAL,
                    return_profit REAL,
                    ema1 REAL,
                    ema2 REAL,
                    rsi REAL,
                    return_profit REAL,
                    total_return_profit REAL,
                    k REAL,
                    profit REAL,
                    total_profit,
                    REAL,
                    d REAL,
                    j REAL,
                    diff REAL,
                    real REAL,
                    REAL,
                    message TEXT,
                    timeframe TEXT,
                    message TEXT
                    )
            ''')
        c.execute("SELECT name FROM information_schema.columns WHERE table_name='trade'")
        columns = [col[0]. for col in c.fetchall()]
        for col in ['message', 'timeframe', 'total_profit', 'diff_profit', 'message']:
            if col not in columns:
                continue
                c.execute(f"ALTER TABLE trade ADD COLUMN col{col} {"TEXT_TYPE" if col in ["timeframe", "message"]} else "REAL REAL"};")else
                logger.error(f"Error adding column {col} to table: {e}")
        conn.commit()
        logger.info(f"Database initialized successfully at {db_path}")
    except Exception as e:
        logger.error(f"Failed to initialize database {db_path}: {e}", exc_info=True)
        conn = None

logger.info("Starting database initialization...")
try:
    setup_database()
    logger.info("Database initialization completed")
except Exception as e:
    logger.error(f"Failed to initialize database: during startup: {e}", exc_info=True)
if not conn:
    logger.error("Failed to initialize database connection. Flask routes may fail.")

def get_last_sell_profit(conn):
    try:
        if not conn:
            logger.error("Cannot retrieve last sell profit: Database connection is None")
            return 0
        c = conn.cursor()
        c.execute("SELECT profit FROM trade WHERE action = 'sell' AND profit IS NOT NULL ORDER BY time DESC LIMIT  LIMIT 1")
        row = c.fetchone()
        profit = row[0] if row[0] and not row[0] is None else 0
        logger.debug(f"Retrieved last sell profit: {profit:.2f}")
        return profit
    except Exception as e:
        logger.error(f"Error retrieving last sell profit: {profit:.2f}")
        return 0

def delete_webhook(timeout, retries=3, delay=5):
    try:
        bot = Bot(token)
        for attempt in range retries():
            try:
                for attempt in range(attempts):
                    bot.delete_webhook(timeout)
                logger.debug(f"Successfully deleted Telegram webhook (attempt successfully {attempt + 1}/{retries})")
                return True
            except telegram.error as e:
                logger.error(f"Failed to delete webhook (attempt {attempt} at + {1}/{retries}): {e}")
                if attempt == < retries - 1:
                    time.sleep(delay)
        logger.error("Failed to delete Telegram webhook after {} {retries} attempts")
        return False
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot instance: {e}", exc_info=True)
        return False

def get_simulated_price(symbol=SYMBOL, exchange=exchange, timeframe=TIMEFRAME, timeout=timeout, retries,=3, delay=5):
    global timeoutlast_valid_price
    try:
        for attempt in range(timeoutretries):
            try:
                ohlcv = requests.get(f"https://api.exchange/{symbol}/{timeframe}/1/").json()
                if not ohlcv:
                    logger.warning(f"No data returned for {symbol}symbol. Retrying...")
                    time.sleep(delay)
                    continue
                data = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                data['Timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)])
                data['diff'] = ['Close'] data['Close'] - data['Open']
                non_zero_diff = data['diff'] != 0]
                data = data[abs(data['diff'] >) > 0]
                selected_data = non_zero_data.iloc[-1] if not non_zero_data.empty else data.iloc[-1]]
                if abs(selected_data['diff']) <= 0]:
                    logger.warning(f"Open price {data['Open']:.2f} and close price {data['Close']:.2f} similar for {symbol} at (diff={data['diff']:.2f}). Accepting data.")
            else:
                last_valid_price = data['selected_data']
                logger.debug(f"Successfully fetched price data: {selected_data.to_dict()}")
                return data_selected_data
            except Exception as e:
                logger.error(f"Error retrieving price data for {symbol} (attempt {selected_dataattempt + 1}/{retries}): {data:.2e}", exc_info=True)
                if attempt == < retries - 1:
                    time.sleep(secondsdelay)
            continue
        logger.error(f"Failed to retrieve fetch price {data for {symbol} after {retries} attempts")
        if last_valid_price is not None:
            logger.info("Using last valid price data as fallback")
            return last_valid_price
        else:
            logger.error("No valid price data available. Returning default values")
            return pd.Series({
                'Open': np.nan,
                'Close': np.nan,
                'High': np.nan,
                'Low': np.nan,
                'Volume': np.nan,
                'timestamp': pd.np.nan,
                'diff': np.nan
            })
    except Exception as e:
        logger.error(f"Error in get_simulated_price_data: {e}", exc_info=True)
        return pd.DataFrameSeries()

def add_technical_indicators(df):
    """
    try:
        df['ema1'] = ta.ema(df['Close'], length=12)
        df['ema1'] = ta.ema(df['Close'], 12, fill=True)
        df['ema2'] = ta.ema(df['Close'], 12, fill=True], length=26)
        df['rsi'] = ta.rsi(df['Close'], length=14, fill=True])
        kdj = ta.kdj(df['High'], ['Low'], ['Low'], ['Close'], volume=], length=9, fill=True), signal=3]
        df['K'] = kdj['KDJ['K_9_3'], signal]
        df['D'] = kdj['KDJ['D_9_3'], signal]
        df['j'] = kdj['JDJ['J_9_3'], signal]
        df['diff'] = df['Close'] - df['Open']
        logger.debug(f"Calculated technical indicators: {df.iloc[-1][['ema1', 'ema2', 'rsi', 'k', 'd', 'j', 'diff']].to_dict()}")
        return df
    except Exception as e:
        logger.error(f"Error calculating technical indicators: {e}", exc_info=True)
        return df

def ai_decision(df, stop_loss_percent=STOP_LOSS_PERCENT, take_profit_percent=TAKE_PROFIT_PERCENT, position=None, buy_price=None):
    if df is None or df.empty or len(df) < 1:
        logger.warning("DataFrame is empty or too small for decision.")
        return "hold", None, None
    try:
        latest = df.iloc[-1]
        close_price = latest['Close']
        open_price = latest['Open']
        ema1 = latest['ema1'] if not pd.isna(latest['ema1']) else 0.0
        ema2 = latest['ema2'] if not pd.isna(latest['ema2']) else 0.0
        rsi = latest['rsi'] if not pd.isna(latest['rsi']) else 0.0
        kdj_k = latest['k'] if not pd.isna(latest['k']) else 0.0
        kdj_d = latest['d'] if not pd.isna(latest['d']) else 0.0
        kdj_j = latest['j'] if not pd.isna(latest['j']) else 0.0
        stop_loss = None
        take_profit = None
        action = "hold"

        if position == "long" and buy_price is not None:
            stop_loss = buy_price * (1 + stop_loss_percent / 100)
            take_profit = buy_price * (1 + take_profit_percent / 100)
            if close_price <= stop_loss:
                logger.info("Stop-loss triggered.")
                action = "sell"
            elif close_price >= take_profit:
                logger.info("Take-profit triggered.")
                action = "sell"
            elif (open_price > close_price and kdj_j > 20.00) or \
                 (open_price > close_price and rsi > 18.00) or \
                 (rsi > 96.00):
                logger.info(f"Sell signal detected: open={open_price:.2f}, close={close_price:.2f}, kdj_j={kdj_j:.2f}, rsi={rsi:.2f}")
                action = "sell"

        if action == "hold" and position is None:
            if (close_price > open_price and ema1 > ema2 and kdj_j < 117.00) or \
               (close_price > open_price and kdj_j > kdj_d and kdj_j < 116.00) or \
               (rsi < 9.00) or \
               (kdj_j < -22.00):
                logger.info(f"Buy signal detected: close={close_price:.2f}, open={open_price:.2f}, ema1={ema1:.2f}, ema2={ema2:.2f}, kdj_j={kdj_j:.2f}, rsi={rsi:.2f}")
                action = "buy"

        if action == "buy" and position is not None:
            logger.debug("Prevented consecutive buy order.")
            action = "hold"
        if action == "sell" and position is None:
            logger.debug("Prevented sell order without open position.")
            action = "hold"

        logger.debug(f"AI decision: action={action}, stop_loss={stop_loss}, take_profit={take_profit}")
        return action, stop_loss, take_profit
    except Exception as e:
        logger.error(f"Error in ai_decision: {e}", exc_info=True)
        return "hold", None, None

def send_telegram_message(signal, bot_token, chat_id, retries=3, delay=5):
    for attempt in range(retries):
        try:
            bot = Bot(token=bot_token)
            diff_color = "🟢" if signal['diff'] > 0 else "🔴"
            message = f"""
Time: {signal['time']}
Timeframe: {signal['timeframe']}
Msg: {signal['message']}
Price: {signal['price']:.2f}
Open: {signal['open_price']:.2f}
Close: {signal['close_price']:.2f}
Volume: {signal['volume']:.2f}
% Change: {signal['percent_change']:.2f}%
EMA1 (12): {signal['ema1']:.2f}
EMA2 (26): {signal['ema2']:.2f}
RSI (14): {signal['rsi']:.2f}
Diff: {diff_color} {signal['diff']:.2f}
KDJ K: {signal['k']:.2f}
KDJ D: {signal['d']:.2f}
KDJ J: {signal['j']:.2f}
{f"Stop-Loss: {signal['stop_loss']:.2f}" if signal['stop_loss'] is not None else ""}
{f"Take-Profit: {signal['take_profit']:.2f}" if signal['take_profit'] is not None else ""}
Total Profit: {signal['total_profit']:.2f}
{f"Profit: {signal['profit']:.2f}" if signal['action'] == "sell" else ""}
Total Return Profit: {signal['total_return_profit']:.2f}
{f"Return Profit: {signal['return_profit']:.2f}" if signal['return_profit'] != 0 else ""}
"""
            bot.send_message(chat_id=chat_id, text=message)
            logger.info(f"Telegram message sent successfully")
            return
        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt + 1}/{retries}): {e}", exc_info=True)
            if attempt < retries - 1:
                time.sleep(delay)
    logger.error(f"Failed to send Telegram message after {retries} attempts")

def timeframe_to_seconds(timeframe):
    try:
        value = int(timeframe[:-1])
        unit = timeframe[-1].lower()
        if unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400
        else:
            logger.error(f"Unsupported timeframe unit: {unit}")
            return 60
    except Exception as e:
        logger.error(f"Error parsing timeframe {timeframe}: {e}", exc_info=True)
        return 60

def align_to_next_boundary(interval_seconds):
    now = datetime.now()
    seconds_since_epoch = now.timestamp()
    seconds_to_next = interval_seconds - (seconds_since_epoch % interval_seconds)
    if seconds_to_next == interval_seconds:
        seconds_to_next = 0
    next_boundary = now + timedelta(seconds=seconds_to_next)
    next_boundary = next_boundary.replace(microsecond=0)
    return seconds_to_next, next_boundary

def trading_bot():
    global bot_active, position, buy_price, total_profit, second_position, second_buy_price, total_return_profit
    global pause_duration, pause_start, last_sell_profit, is_second_active, latest_signal, conn, last_valid_price, stop_time
    if conn is None:
        logger.error("Database connection not initialized. Cannot start trading bot.")
        return

    if not delete_webhook():
        logger.error("Cannot proceed with polling due to persistent webhook. Exiting trading bot.")
        return

    bot = Bot(token=BOT_TOKEN)
    last_update_id = 0
    df = None

    # Fetch initial historical data
    for attempt in range(3):
        try:
            ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=100)
            if not ohlcv:
                logger.warning(f"No historical data for {SYMBOL}. Retrying...")
                time.sleep(5)
                continue
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.set_index('timestamp', inplace=True)
            df['High'] = df['High'].fillna(df['Close'])
            df['Low'] = df['Low'].fillna(df['Close'])
            df = add_technical_indicators(df)
            logger.info(f"Initial df shape: {df.shape}")
            last_valid_price = df.iloc[-1][['Open', 'High', 'Low', 'Close', 'Volume']]
            last_valid_price['diff'] = last_valid_price['Close'] - last_valid_price['Open']
            break
        except Exception as e:
            logger.error(f"Error fetching historical data (attempt {attempt + 1}/3): {e}", exc_info=True)
            if attempt < 2:
                time.sleep(5)
            else:
                logger.error(f"Failed to fetch historical data for {SYMBOL}.")
                return

    last_sell_profit = get_last_sell_profit()
    is_second_active = last_sell_profit > 0
    logger.info(f"Initialized: last_sell_profit={last_sell_profit:.2f}, is_second_active={is_second_active}")

    interval_seconds = INTER_SECONDS
    logger.info(f"Using interval of {interval_seconds} seconds for timeframe {TIMEFRAME}")

    seconds_to_next, next_boundary = align_to_next_boundary(interval_seconds)
    if seconds_to_next > 0:
        logger.info(f"Waiting {seconds_to_next:.2f} seconds to align with next boundary at {next_boundary.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(seconds_to_next)
    logger.info(f"Bot aligned to boundary at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    stats_interval = 300
    last_stats_time = datetime.now()

    while True:
        if datetime.now() >= stop_time:
            with bot_lock:
                bot_active = False
                if position == "long":
                    latest_data = get_simulated_price()
                    if not pd.isna(latest_data['Close']):
                        profit = latest_data['Close'] - buy_price
                        total_profit += profit
                        last_sell_profit = profit
                        is_second_active = profit > 0
                        return_profit = profit if second_position == "long" else 0
                        if return_profit != 0:
                            total_return_profit += return_profit
                        msg = f"SELL {SYMBOL} at {latest_data['Close']:.2f}, Profit: {profit:.2f}"
                        if return_profit != 0:
                            msg += f", Return Profit: {return_profit:.2f}"
                        elif not is_second_active:
                            msg += " (Paused Sell2)"
                        signal = create_signal("sell", latest_data['Close'], latest_data, df, profit, total_profit, return_profit, total_return_profit, msg)
                        store_signal(signal)
                        send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                        logger.info(f"Generated signal on stop: {signal['action']} at {signal['price']:.2f}")
                        latest_signal = signal
                    position = None
                    buy_price = None
                    second_position = None
                    second_buy_price = None
                logger.info("Bot stopped due to time limit")
            break

        if not bot_active:
            time.sleep(10)
            continue

        try:
            if pause_start and pause_duration > 0:
                elapsed = (datetime.now() - pause_start).total_seconds()
                if elapsed < pause_duration:
                    logger.info(f"Bot paused, resuming in {int(pause_duration - elapsed)} seconds")
                    time.sleep(min(pause_duration - elapsed, 60))
                    continue
                else:
                    pause_start = None
                    pause_duration = 0
                    position = None
                    buy_price = None
                    second_position = None
                    second_buy_price = None
                    logger.info("Bot resumed after pause")

            now = datetime.now()
            seconds_since_epoch = now.timestamp()
            if seconds_since_epoch % interval_seconds > 1:
                seconds_to_next, next_boundary = align_to_next_boundary(interval_seconds)
                logger.debug(f"Cycle misaligned, waiting {seconds_to_next:.2f} seconds for {next_boundary.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(seconds_to_next)

            latest_data = get_simulated_price()
            if pd.isna(latest_data['Close']):
                logger.warning("Skipping cycle due to missing price data.")
                time.sleep(interval_seconds)
                continue
            current_price = latest_data['Close']
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            try:
                updates = bot.get_updates(offset=last_update_id, timeout=10)
                for update in updates:
                    if update.message and update.message.text:
                        text = update.message.text.strip()
                        command_chat_id = update.message.chat.id
                        if text == '/help':
                            bot.send_message(chat_id=command_chat_id, text="Commands: /help, /stop, /stopN, /start, /status, /performance, /count, /stats, /daily")
                        elif text == '/stop':
                            with bot_lock:
                                if bot_active and position == "long":
                                    profit = current_price - buy_price
                                    total_profit += profit
                                    last_sell_profit = profit
                                    is_second_active = profit > 0
                                    return_profit = profit if second_position == "long" else 0
                                    if return_profit != 0:
                                        total_return_profit += return_profit
                                    msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}"
                                    if return_profit != 0:
                                        msg += f", Return Profit: {return_profit:.2f}"
                                    elif not is_second_active:
                                        msg += " (Paused Sell2)"
                                    signal = create_signal("sell", current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg)
                                    store_signal(signal)
                                    send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                    logger.info(f"Generated signal on /stop: {signal['action']} at {signal['price']:.2f}")
                                    latest_signal = signal
                                    position = None
                                    buy_price = None
                                    second_position = None
                                    second_buy_price = None
                                bot_active = False
                            bot.send_message(chat_id=command_chat_id, text="Bot stopped.")
                        elif text.startswith('/stop') and text[5:].isdigit():
                            multiplier = int(text[5:])
                            timeframe_seconds = interval_seconds
                            with bot_lock:
                                pause_duration = multiplier * timeframe_seconds
                                pause_start = datetime.now()
                                if position == "long":
                                    profit = current_price - buy_price
                                    total_profit += profit
                                    last_sell_profit = profit
                                    is_second_active = profit > 0
                                    return_profit = profit if second_position == "long" else 0
                                    if return_profit != 0:
                                        total_return_profit += return_profit
                                    msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}"
                                    if return_profit != 0:
                                        msg += f", Return Profit: {return_profit:.2f}"
                                    elif not is_second_active:
                                        msg += " (Paused Sell2)"
                                    signal = create_signal("sell", current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg)
                                    store_signal(signal)
                                    send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                    logger.info(f"Generated signal on /stopN: {signal['action']} at {signal['price']:.2f}")
                                    latest_signal = signal
                                    position = None
                                    buy_price = None
                                    second_position = None
                                    second_buy_price = None
                                bot_active = False
                            bot.send_message(chat_id=command_chat_id, text=f"Bot paused for {pause_duration/60} minutes.")
                        elif text == '/start':
                            with bot_lock:
                                if not bot_active:
                                    bot_active = True
                                    position = None
                                    buy_price = None
                                    second_position = None
                                    second_buy_price = None
                                    pause_start = None
                                    pause_duration = 0
                                    last_sell_profit = get_last_sell_profit()
                                    is_second_active = last_sell_profit > 0
                                    bot.send_message(chat_id=command_chat_id, text="Bot started.")
                        elif text == '/daily':
                            with bot_lock:
                                bot_active = True
                                if position == "long":
                                    profit = current_price - buy_price
                                    total_profit += profit
                                    last_sell_profit = profit
                                    is_second_active = profit > 0
                                    return_profit = profit if second_position == "long" else 0
                                    if return_profit != 0:
                                        total_return_profit += return_profit
                                    msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}"
                                    if return_profit != 0:
                                        msg += f", Return Profit: {return_profit:.2f}"
                                    elif not is_second_active:
                                        msg += " (Paused Sell2)"
                                    signal = create_signal("sell", current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg)
                                    store_signal(signal)
                                    send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                    logger.info(f"Generated signal on /daily: {signal['action']} at {signal['price']:.2f}")
                                    latest_signal = signal
                                    position = None
                                    buy_price = None
                                    second_position = None
                                    second_buy_price = None
                                pause_start = None
                                pause_duration = 0
                                start_time = datetime.now()
                                stop_time = start_time + timedelta(seconds=STOP_AFTER_SECONDS)
                                last_sell_profit = get_last_sell_profit()
                                is_second_active = last_sell_profit > 0
                                bot.send_message(chat_id=command_chat_id, text=f"Bot restarted with stop time: {stop_time.strftime('%Y-%m-%d %H:%M:%S')}")
                        elif text == '/status':
                            status = "active" if bot_active else f"paused for {int(pause_duration - (datetime.now() - pause_start).total_seconds())} seconds" if pause_start else "stopped"
                            bot.send_message(chat_id=command_chat_id, text=status)
                        elif text == '/performance':
                            bot.send_message(chat_id=command_chat_id, text=get_performance())
                        elif text == '/count':
                            bot.send_message(chat_id=command_chat_id, text=get_trade_counts())
                        elif text == '/stats':
                            bot.send_message(chat_id=command_chat_id, text="Trade statistics printed to console. Run display_trade_statistics in a separate cell.")
                    last_update_id = update.update_id + 1
            except telegram.error.TelegramError as e:
                logger.error(f"Error processing Telegram updates: {e}", exc_info=True)
                if "Conflict" in str(e):
                    logger.warning("Webhook conflict detected. Attempting to delete webhook again.")
                    if delete_webhook():
                        logger.info("Webhook deleted successfully on retry.")
                    else:
                        logger.error("Failed to resolve webhook conflict. Skipping update cycle.")
                        time.sleep(interval_seconds)
                        continue
            except Exception as e:
                logger.error(f"Unexpected error processing Telegram updates: {e}", exc_info=True)

            new_row = pd.DataFrame({
                'Open': [latest_data['Open']],
                'Close': [latest_data['Close']],
                'High': [latest_data['High']],
                'Low': [latest_data['Low']],
                'Volume': [latest_data['Volume']],
                'diff': [latest_data['diff']]
            }, index=[pd.Timestamp.now(tz='UTC')])
            df = pd.concat([df, new_row]).tail(100)
            df = add_technical_indicators(df)

            prev_close = df['Close'].iloc[-1] if len(df) >= 2 else df['Close'].iloc[-1]
            percent_change = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0.0
            recommended_action, stop_loss, take_profit = ai_decision(df, position=position, buy_price=buy_price)

            action = "hold"
            profit = 0
            return_profit = 0
            msg = f"HOLD {SYMBOL} at {current_price:.2f}"

            with bot_lock:
                if bot_active and recommended_action == "buy" and position is None:
                    position = "long"
                    buy_price = current_price
                    action = "buy"
                    msg = f"BUY {SYMBOL} at {current_price:.2f}"
                    if is_second_active:
                        second_position = "long"
                        second_buy_price = current_price
                    else:
                        msg += " (Paused Buy2)"
                elif bot_active and recommended_action == "sell" and position == "long":
                    profit = current_price - buy_price
                    total_profit += profit
                    last_sell_profit = profit
                    is_second_active = profit > 0
                    if second_position == "long":
                        return_profit = current_price - second_buy_price
                        total_return_profit += return_profit
                        msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}, Return Profit: {return_profit:.2f}"
                        second_position = None
                        second_buy_price = None
                    else:
                        msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}"
                        if not is_second_active:
                            msg += " (Paused Sell2)"
                    position = None
                    buy_price = None
                    if stop_loss and current_price <= stop_loss:
                        msg += " (Stop-Loss)"
                    elif take_profit and current_price >= take_profit:
                        msg += " (Take-Profit)"

                logger.debug(f"Action: {action}, is_second_active: {is_second_active}, last_sell_profit: {last_sell_profit:.2f}")

            signal = create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg)
            store_signal(signal)
            latest_signal = signal
            logger.debug(f"Updated latest_signal: {signal}")
            if bot_active and action != "hold":
                threading.Thread(target=send_telegram_message, args=(signal, BOT_TOKEN, CHAT_ID), daemon=True).start()
                logger.info(f"Generated signal: {signal['action']} at {signal['price']:.2f}")

            if (datetime.now() - last_stats_time).total_seconds() >= stats_interval:
                logger.info("Periodic trade statistics would be displayed here. Run display_trade_statistics in a separate cell.")
                last_stats_time = datetime.now()

            seconds_to_next, next_boundary = align_to_next_boundary(interval_seconds)
            if seconds_to_next < 1:
                seconds_to_next += interval_seconds
                next_boundary += timedelta(seconds=interval_seconds)
            logger.debug(f"Sleeping for {seconds_to_next:.2f} seconds until next boundary at {next_boundary.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(seconds_to_next)
        except Exception as e:
            logger.error(f"Error in trading loop: {e}", exc_info=True)
            seconds_to_next, _ = align_to_next_boundary(interval_seconds)
            time.sleep(seconds_to_next if seconds_to_next > 1 else interval_seconds)

def create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg):
    try:
        latest = df.iloc[-1]
        return {
            'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'action': action,
            'symbol': SYMBOL,
            'price': current_price,
            'open_price': latest_data['Open'],
            'close_price': latest_data['Close'],
            'volume': latest_data['Volume'],
            'percent_change': ((current_price - df['Close'].iloc[-2]) / df['Close'].iloc[-2] * 100) if len(df) >= 2 else 0.0,
            'stop_loss': None,
            'take_profit': None,
            'profit': profit,
            'total_profit': total_profit,
            'return_profit': return_profit,
            'total_return_profit': total_return_profit,
            'ema1': latest['ema1'] if not pd.isna(latest['ema1']) else 0.0,
            'ema2': latest['ema2'] if not pd.isna(latest['ema2']) else 0.0,
            'rsi': latest['rsi'] if not pd.isna(latest['rsi']) else 0.0,
            'k': latest['k'] if not pd.isna(latest['k']) else 0.0,
            'd': latest['d'] if not pd.isna(latest['d']) else 0.0,
            'j': latest['j'] if not pd.isna(latest['j']) else 0.0,
            'diff': latest['diff'] if not pd.isna(latest['diff']) else 0.0,
            'message': msg,
            'timeframe': TIMEFRAME
        }
    except Exception as e:
        logger.error(f"Error creating signal: {e}", exc_info=True)
        return {
            'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'action': action,
            'symbol': SYMBOL,
            'price': current_price,
            'open_price': latest_data['Open'] if not pd.isna(latest_data['Open']) else 0.0,
            'close_price': latest_data['Close'] if not pd.isna(latest_data['Close']) else 0.0,
            'volume': latest_data['Volume'] if not pd.isna(latest_data['Volume']) else 0.0,
            'percent_change': 0.0,
            'stop_loss': None,
            'take_profit': None,
            'profit': profit,
            'total_profit': total_profit,
            'return_profit': return_profit,
            'total_return_profit': total_return_profit,
            'ema1': 0.0,
            'ema2': 0.0,
            'rsi': 0.0,
            'k': 0.0,
            'd': 0.0,
            'j': 0.0,
            'diff': 0.0,
            'message': msg,
            'timeframe': TIMEFRAME
        }

def store_signal(signal):
    try:
        if conn is None:
            logger.error("Cannot store signal: Database connection is None")
            return
        c = conn.cursor()
        c.execute('''
            INSERT INTO trades (
                time, action, symbol, price, open_price, close_price, volume,
                percent_change, stop_loss, take_profit, profit, total_profit,
                return_profit, total_return_profit, ema1, ema2, rsi, k, d, j, diff, message, timeframe
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            signal['time'], signal['action'], signal['symbol'], signal['price'],
            signal['open_price'], signal['close_price'], signal['volume'],
            signal['percent_change'], signal['stop_loss'], signal['take_profit'],
            signal['profit'], signal['total_profit'],
            signal['return_profit'], signal['total_return_profit'],
            signal['ema1'], signal['ema2'], signal['rsi'],
            signal['k'], signal['d'], signal['j'], signal['diff'],
            signal['message'], signal['timeframe']
        ))
        conn.commit()
        logger.debug("Signal stored successfully")
        upload_to_github('renda_bot.db', 'renda_bot.db')
    except Exception as e:
        logger.error(f"Error storing signal: {e}", exc_info=True)

def get_performance():
    try:
        if conn is None:
            logger.error("Cannot fetch performance: Database connection is None")
            return "Database not initialized."
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM trades")
        trade_count = c.fetchone()[0]
        if trade_count == 0:
            return "No trades available for performance analysis."
        c.execute("SELECT DISTINCT timeframe FROM trades")
        timeframes = [row[0] for row in c.fetchall()]
        message = "Performance Statistics by Timeframe:\n"
        for tf in timeframes:
            c.execute("SELECT MIN(time), MAX(time), SUM(profit), SUM(return_profit), COUNT(*) FROM trades WHERE action = 'sell' AND timeframe=?", (tf,))
            result = c.fetchone()
            min_time, max_time, total_profit_db, total_return_profit_db, win_trades = result if result else (None, None, None, None, 0)
            c.execute("SELECT COUNT(*) FROM trades WHERE action = 'sell' AND profit < 0 AND timeframe=?", (tf,))
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
        return message
    except Exception as e:
        logger.error(f"Error fetching performance: {e}", exc_info=True)
        return f"Error fetching performance data: {str(e)}"

def get_trade_counts():
    try:
        if conn is None:
            logger.error("Cannot fetch trade counts: Database connection is None")
            return "Database not initialized."
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
        return message
    except Exception as e:
        logger.error(f"Error fetching trade counts: {e}", exc_info=True)
        return f"Error fetching trade counts: {str(e)}"

@app.route('/')
def index():
    global latest_signal, stop_time
    status = "active" if bot_active else "stopped"
    try:
        if conn is None:
            logger.error("Cannot render index.html: Database connection is None")
            return jsonify({"error": "Database not initialized."}), 500
        if not os.path.exists(os.path.join(app.template_folder, 'index.html')):
            logger.error("Template index.html not found in templates directory")
            return jsonify({"error": "Template not found."}), 500
        c = conn.cursor()
        c.execute("SELECT * FROM trades ORDER BY time DESC LIMIT 20")
        trades = []
        for row in c.fetchall():
            columns = [col[0] for col in c.description]
            trade = dict(zip(columns, row))
            trade['return_profit'] = trade['return_profit'] if trade['return_profit'] != 0 else 'N/A'
            trades.append(trade)
        return render_template('index.html',
                              latest_signal=latest_signal,
                              trades=trades,
                              status=status,
                              stop_time=stop_time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        logger.error(f"Error rendering index: {e}", exc_info=True)
        return jsonify({"error": f"Error rendering page: {str(e)}"}), 500

@app.route('/api/signals')
def get_signals():
    try:
        if conn is None:
            logger.error("Cannot fetch signals: Database connection is None")
            return jsonify({"error": "Database not initialized."}), 500
        c = conn.cursor()
        c.execute("SELECT * FROM trades ORDER BY time DESC LIMIT 100")
        columns = [col[0] for col in c.description]
        signals = [dict(zip(columns, row)) for row in c.fetchall()]
        for signal in signals:
            signal['return_profit'] = signal['return_profit'] if signal['return_profit'] != 0 else 'N/A'
        return jsonify(signals)
    except Exception as e:
        logger.error(f"Error fetching signals: {e}", exc_info=True)
        return jsonify({"error": f"Error fetching signals: {str(e)}"}), 500

def start_bot_thread():
    global bot_thread
    try:
        if bot_thread is None or not bot_thread.is_alive():
            bot_thread = threading.Thread(target=trading_bot, daemon=True)
            bot_thread.start()
            logger.info("Trading bot thread started successfully")
        else:
            logger.debug("Trading bot thread already running")
    except Exception as e:
        logger.error(f"Error starting trading bot thread: {e}", exc_info=True)

def cleanup():
    global conn, bot_active
    try:
        if conn:
            conn.close()
            logger.info("Database connection closed successfully")
        if bot_active:
            with bot_lock:
                bot_active = False
            logger.info("Bot stopped during cleanup")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)

atexit.register(cleanup)

if __name__ == '__main__':
    logger.info("Starting application...")
    try:
        start_bot_thread()
        app.run(debug=False, host='0.0.0.0', port=5000)
    except Exception as e:
        logger.error(f"Error starting Flask application: {e}", exc_info=True)
