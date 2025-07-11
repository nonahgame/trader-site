import os
import pandas as pd
import numpy as np
import sqlite3
import time
from datetime import datetime, timedelta
import pytz
import ccxt.async_support as ccxt
import pandas_ta as ta
from telegram import Bot
import telegram
import threading
import requests
import base64
import logging
from flask import Flask, render_template, jsonify
import atexit
import asyncio

# Custom formatter for US/Eastern timezone (retained for consistency)
class EasternFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, tz=pytz.timezone('US/Eastern')):
        super().__init__(fmt, datefmt)
        self.tz = tz

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler('td_sto.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Customize werkzeug logger
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_handler = logging.StreamHandler()
werkzeug_handler.setFormatter(EasternFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
werkzeug_logger.handlers = [werkzeug_handler, logging.FileHandler('td_sto.log')]
werkzeug_logger.setLevel(logging.DEBUG)

# Flask app setup
app = Flask(__name__)

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN", "BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "CHAT_ID")
SYMBOL = os.getenv("SYMBOL", "BTC/USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "TIMEFRAME")
TIMEFRAMES = int(os.getenv("INTER_SECONDS", "INTER_SECONDS"))
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", -2.0))
TAKE_PROFIT_PERCENT = float(os.getenv("TAKE_PROFIT_PERCENT", 8.0))
STOP_AFTER_SECONDS = float(os.getenv("STOP_AFTER_SECONDS", 270000))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "GITHUB_REPO")
GITHUB_PATH = os.getenv("GITHUB_PATH", "GITHUB_PATH")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "BINANCE_SECRET_KEY")
TRADE_VOLUME = float(os.getenv("TRADE_VOLUME", 0.001))
PROXY_HTTP = os.getenv("PROXY_HTTP", "http://102.214.216.233:80")  # Optional proxy
PROXY_HTTPS = os.getenv("PROXY_HTTPS", "http://169.239.223.79:9999")  # Optional proxy

# GitHub API setup
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Database path
db_path = 'r_bot.db'

# Timezone setup
EASTERN_TZ = pytz.timezone('US/Eastern')

# Global state
bot_thread = None
bot_active = True
bot_lock = threading.Lock()
db_lock = threading.Lock()
conn = None
exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET_KEY,
    'enableRateLimit': True,
    'proxies': {'http': PROXY_HTTP, 'https': PROXY_HTTPS} if PROXY_HTTP and PROXY_HTTPS else {}
})
position = None
buy_price = None
total_profit = 0
pause_duration = 0
pause_start = None
tracking_enabled = True
last_sell_profit = 0
tracking_has_buy = False
tracking_buy_price = None
total_return_profit = 0
start_time = datetime.now(EASTERN_TZ)
stop_time = start_time + timedelta(seconds=STOP_AFTER_SECONDS)
last_valid_price = None

# GitHub database functions
def upload_to_github(file_path, file_name):
    try:
        if not GITHUB_TOKEN or GITHUB_TOKEN == "GITHUB_TOKEN":
            logger.error("GITHUB_TOKEN is not set or invalid.")
            return
        if not GITHUB_REPO or GITHUB_REPO == "GITHUB_REPO":
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
            logger.error(f"Failed to check existing file on GitHub: {response.status_code} - {response.text}")
            return
        payload = {
            "message": f"Update {file_name} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "content": content
        }
        if sha:
            payload["sha"] = sha
        response = requests.put(GITHUB_API_URL, headers=HEADERS, json=payload)
        if response.status_code in [200, 201]:
            logger.info(f"Successfully uploaded {file_name} to GitHub")
        else:
            logger.error(f"Failed to upload {file_name} to GitHub: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Error uploading {file_name} to GitHub: {e}", exc_info=True)

def download_from_github(file_name, destination_path):
    try:
        if not GITHUB_TOKEN or GITHUB_TOKEN == "GITHUB_TOKEN":
            logger.error("GITHUB_TOKEN is not set or invalid.")
            return False
        if not GITHUB_REPO or GITHUB_REPO == "GITHUB_REPO":
            logger.error("GITHUB_REPO is not set or invalid.")
            return False
        if not GITHUB_PATH:
            logger.error("GITHUB_PATH is not set.")
            return False
        logger.debug(f"Downloading {file_name} from GitHub: {GITHUB_REPO}/{GITHUB_PATH}")
        response = requests.get(GITHUB_API_URL, headers=HEADERS)
        if response.status_code == 404:
            logger.info(f"No {file_name} found in GitHub repository. Starting with a new database.")
            return False
        elif response.status_code != 200:
            logger.error(f"Failed to fetch {file_name} from GitHub: {response.status_code} - {response.text}")
            return False
        content = base64.b64decode(response.json()["content"])
        with open(destination_path, "wb") as f:
            f.write(content)
        logger.info(f"Downloaded {file_name} from GitHub to {destination_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {file_name} from GitHub: {e}", exc_info=True)
        return False

# Keep-alive mechanism
def keep_alive():
    while True:
        try:
            requests.get('https://www.google.com')
            logger.debug("Keep-alive ping sent")
            time.sleep(300)
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")
            time.sleep(60)

# SQLite database setup
def setup_database():
    global conn
    with db_lock:
        for attempt in range(3):
            try:
                logger.info(f"Database setup attempt {attempt + 1}/3")
                if os.path.exists(db_path):
                    try:
                        test_conn = sqlite3.connect(db_path, check_same_thread=False)
                        c = test_conn.cursor()
                        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        logger.info(f"Existing database found at {db_path}, tables: {c.fetchall()}")
                        test_conn.close()
                    except sqlite3.DatabaseError as e:
                        logger.error(f"Existing database at {db_path} is corrupted: {e}")
                        os.remove(db_path)
                        logger.info(f"Removed corrupted database file at {db_path}")

                logger.info(f"Attempting to download database from GitHub: {GITHUB_API_URL}")
                if download_from_github('r_bot.db', db_path):
                    logger.info(f"Downloaded database from GitHub to {db_path}")
                    try:
                        test_conn = sqlite3.connect(db_path, check_same_thread=False)
                        c = test_conn.cursor()
                        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tables = c.fetchall()
                        logger.info(f"Downloaded database is valid, tables: {tables}")
                        test_conn.close()
                    except sqlite3.DatabaseError as e:
                        logger.error(f"Downloaded database is corrupted: {e}")
                        os.remove(db_path)
                        logger.info(f"Removed invalid downloaded database file at {db_path}")

                logger.info(f"Connecting to database at {db_path}")
                conn = sqlite3.connect(db_path, check_same_thread=False)
                c = conn.cursor()
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades';")
                if not c.fetchone():
                    c.execute('''
                        CREATE TABLE trades (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            time TEXT,
                            action TEXT,
                            symbol TEXT,
                            price REAL,
                            open_price REAL,
                            close_price REAL,
                            volume REAL,
                            percent_change REAL,
                            stop_loss REAL,
                            take_profit REAL,
                            profit REAL,
                            total_profit REAL,
                            return_profit REAL,
                            total_return_profit REAL,
                            ema1 REAL,
                            ema2 REAL,
                            rsi REAL,
                            k REAL,
                            d REAL,
                            j REAL,
                            diff REAL,
                            message TEXT,
                            timeframe TEXT
                        )
                    ''')
                    logger.info("Created new trades table")
                c.execute("PRAGMA table_info(trades);")
                columns = [col[1] for col in c.fetchall()]
                for col in ['return_profit', 'total_return_profit', 'diff', 'message', 'timeframe']:
                    if col not in columns:
                        c.execute(f"ALTER TABLE trades ADD COLUMN {col} {'REAL' if col in ['return_profit', 'total_return_profit', 'diff'] else 'TEXT'};")
                        logger.info(f"Added column {col} to trades table")
                conn.commit()
                logger.info(f"Database initialized successfully at {db_path}, size: {os.path.getsize(db_path)} bytes")
                upload_to_github(db_path, 'r_bot.db')
                return True
            except sqlite3.Error as e:
                logger.error(f"SQLite error during database setup (attempt {attempt + 1}/3): {e}", exc_info=True)
                conn = None
                time.sleep(2)
            except Exception as e:
                logger.error(f"Unexpected error during database setup (attempt {attempt + 1}/3): {e}", exc_info=True)
                conn = None
                time.sleep(2)
        logger.error("Failed to initialize database after 3 attempts")
        conn = None
        return False

# Initialize database in main thread
logger.info("Initializing database in main thread")
if not setup_database():
    logger.error("Failed to initialize database in main thread. Flask routes may fail.")

# Check account balance (async)
async def check_balance():
    try:
        balance = await exchange.fetch_balance()
        usdt_free = balance['USDT']['free'] if 'USDT' in balance else 0.0
        btc_free = balance['BTC']['free'] if 'BTC' in balance else 0.0
        logger.debug(f"Account balance: {usdt_free:.2f} USDT, {btc_free:.6f} BTC")
        return usdt_free, btc_free
    except Exception as e:
        logger.error(f"Error checking balance: {e}")
        return 0.0, 0.0

# Fetch price data (async)
async def get_simulated_price(symbol=SYMBOL, exchange=exchange, timeframe=TIMEFRAME, retries=3, delay=5):
    global last_valid_price
    for attempt in range(retries):
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=5)
            if not ohlcv:
                logger.warning(f"No data returned for {symbol}. Retrying...")
                await asyncio.sleep(delay)
                continue
            data = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EASTERN_TZ)
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
                await asyncio.sleep(delay)
    logger.error(f"Failed to fetch price for {symbol} after {retries} attempts.")
    if last_valid_price is not None:
        logger.info("Using last valid price data as fallback.")
        return last_valid_price
    return pd.Series({'Open': np.nan, 'Close': np.nan, 'High': np.nan, 'Low': np.nan, 'Volume': np.nan, 'diff': np.nan})

# Calculate technical indicators
def add_technical_indicators(df):
    try:
        df['ema1'] = ta.ema(df['Close'], length=12)
        df['ema2'] = ta.ema(df['Close'], length=26)
        df['rsi'] = ta.rsi(df['Close'], length=14)
        kdj = ta.kdj(df['High'], df['Low'], df['Close'], length=9, signal=3)
        df['k'] = kdj['K_9_3']
        df['d'] = kdj['D_9_3']
        df['j'] = kdj['J_9_3']
        df['diff'] = df['Close'] - df['Open']
        logger.debug(f"Technical indicators calculated: {df.iloc[-1][['ema1', 'ema2', 'rsi', 'k', 'd', 'j', 'diff']].to_dict()}")
        return df
    except Exception as e:
        logger.error(f"Error calculating indicators: {e}")
        return df

# AI decision logic (simplified for testing)
def ai_decision(df, stop_loss_percent=STOP_LOSS_PERCENT, take_profit_percent=TAKE_PROFIT_PERCENT, position=None, buy_price=None):
    if df.empty or len(df) < 1:
        logger.warning("DataFrame is empty or too small for decision.")
        return "hold", None, None
    
    latest = df.iloc[-1]
    close_price = latest['Close']
    open_price = latest['Open']
    kdj_k = latest['k'] if not pd.isna(latest['k']) else 0.0
    kdj_d = latest['d'] if not pd.isna(latest['d']) else 0.0
    kdj_j = latest['j'] if not pd.isna(latest['j']) else 0.0
    ema1 = latest['ema1'] if not pd.isna(latest['ema1']) else 0.0
    ema2 = latest['ema2'] if not pd.isna(latest['ema2']) else 0.0
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
        elif close_price < open_price:
            logger.info(f"Downward price movement detected: open={open_price:.2f}, close={close_price:.2f}")
            action = "sell"
        elif kdj_j > 80.0:
            logger.info(f"Overbought KDJ J detected: kdj_j={kdj_j:.2f}")
            action = "sell"

    if action == "hold" and position is None:
        if kdj_j < 20.0 or (close_price > open_price and ema1 > ema2):
            logger.info(f"Buy condition met: kdj_j={kdj_j:.2f}, kdj_d={kdj_d:.2f}, close={close_price:.2f}, open={open_price:.2f}, ema1={ema1:.2f}, ema2={ema2:.2f}")
            action = "buy"

    if action == "buy" and position is not None:
        logger.debug("Prevented consecutive buy order.")
        action = "hold"
    if action == "sell" and position is None:
        logger.debug("Prevented sell order without open position.")
        action = "hold"

    logger.debug(f"AI decision: action={action}, stop_loss={stop_loss}, take_profit={take_profit}, kdj_j={kdj_j:.2f}")
    return action, stop_loss, take_profit

# Second strategy logic
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

# Telegram message sending
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
{f"Total Profit: {signal['total_profit']:.2f}" if signal['action'] in ["buy", "sell"] else ""}
{f"Profit: {signal['profit']:.2f}" if signal['action'] == "sell" else ""}
"""
            bot.send_message(chat_id=chat_id, text=message)
            logger.info(f"Telegram message sent successfully")
            return
        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    logger.error(f"Failed to send Telegram message after {retries} attempts")

# Calculate sleep time to align with timeframe boundaries
def get_next_timeframe_boundary(current_time, timeframe_seconds):
    current_seconds = (current_time.hour * 3600 + current_time.minute * 60 + current_time.second)
    intervals_passed = current_seconds // timeframe_seconds
    next_boundary = (intervals_passed + 1) * timeframe_seconds
    seconds_until_boundary = next_boundary - current_seconds
    return max(seconds_until_boundary, 1)

# Trading bot with live orders
async def trading_bot():
    global bot_active, position, buy_price, total_profit, pause_duration, pause_start, conn, exchange
    bot = None
    try:
        bot = Bot(token=BOT_TOKEN)
        logger.info("Telegram bot initialized successfully")
    except telegram.error.InvalidToken:
        logger.warning("Invalid Telegram bot token. Telegram functionality disabled.")
    except Exception as e:
        logger.error(f"Error initializing Telegram bot: {e}")

    last_update_id = 0
    df = None

    initial_signal = {
        'time': datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
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
        'message': f"Initializing bot for {SYMBOL}",
        'timeframe': TIMEFRAME
    }
    store_signal(initial_signal)
    upload_to_github(db_path, 'r_bot.db')
    logger.info("Initial hold signal generated")

    for attempt in range(3):
        try:
            await exchange.load_markets()
            ohlcv = await exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=100)
            if not ohlcv:
                logger.warning(f"No historical data for {SYMBOL}. Retrying...")
                await asyncio.sleep(5)
                continue
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EASTERN_TZ)
            df.set_index('timestamp', inplace=True)
            df['High'] = df['High'].fillna(df['Close'])
            df['Low'] = df['Low'].fillna(df['Close'])
            df = add_technical_indicators(df)
            logger.info(f"Initial df shape: {df.shape}")
            break
        except Exception as e:
            logger.error(f"Error fetching historical data (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(5)
            else:
                logger.error(f"Failed to fetch historical data for {SYMBOL}")
                return

    timeframe_seconds = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '1d': 86400}.get(TIMEFRAME, TIMEFRAMES)
    
    # Align start to next timeframe boundary
    current_time = datetime.now(EASTERN_TZ)
    seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
    logger.info(f"Waiting {seconds_to_wait:.2f} seconds to align with next {TIMEFRAME} boundary")
    await asyncio.sleep(seconds_to_wait)

    while True:
        loop_start_time = datetime.now(EASTERN_TZ)
        logger.debug(f"Starting trading loop iteration at {loop_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        with bot_lock:
            if datetime.now(EASTERN_TZ) >= stop_time:
                bot_active = False
                if position == "long":
                    latest_data = await get_simulated_price()
                    if not pd.isna(latest_data['Close']):
                        try:
                            order = await exchange.create_market_sell_order(SYMBOL, TRADE_VOLUME)
                            profit = latest_data['Close'] - buy_price
                            total_profit += profit
                            return_profit, msg = handle_second_strategy("sell", latest_data['Close'], profit)
                            signal = create_signal("sell", latest_data['Close'], latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot stopped due to time limit{msg}")
                            store_signal(signal)
                            if bot:
                                send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                            logger.info(f"Placed sell order on stop: {order}")
                        except Exception as e:
                            logger.error(f"Error placing sell order on stop: {e}")
                    position = None
                logger.info("Bot stopped due to time limit")
                upload_to_github(db_path, 'r_bot.db')
                break

            if not bot_active:
                logger.info("Bot is not active, checking again in 10 seconds")
                await asyncio.sleep(10)
                continue

        try:
            if pause_start and pause_duration > 0:
                elapsed = (datetime.now(EASTERN_TZ) - pause_start).total_seconds()
                if elapsed < pause_duration:
                    logger.info(f"Bot paused, resuming in {int(pause_duration - elapsed)} seconds")
                    await asyncio.sleep(min(pause_duration - elapsed, 60))
                    continue
                else:
                    pause_start = None
                    pause_duration = 0
                    position = None
                    logger.info("Bot resumed after pause")

            # Check balance
            usdt_free, btc_free = await check_balance()
            logger.debug(f"Balance check: {usdt_free:.2f} USDT, {btc_free:.6f} BTC")

            latest_data = await get_simulated_price()
            if pd.isna(latest_data['Close']):
                logger.warning("Skipping cycle due to missing price data.")
                current_time = datetime.now(EASTERN_TZ)
                seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
                await asyncio.sleep(seconds_to_wait)
                continue
            current_price = latest_data['Close']
            current_time = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S")
            logger.debug(f"Current price: {current_price:.2f}, time: {current_time}")

            if bot:
                try:
                    updates = bot.get_updates(offset=last_update_id, timeout=10)
                    for update in updates:
                        if update.message and update.message.text:
                            text = update.message.text.strip()
                            command_chat_id = update.message.chat.id
                            logger.debug(f"Received Telegram command: {text}")
                            if text == '/help':
                                bot.send_message(chat_id=command_chat_id, text="Commands: /help, /stop, /stopN, /start, /status, /performance, /count")
                            elif text == '/stop':
                                with bot_lock:
                                    if bot_active and position == "long":
                                        try:
                                            usdt_free, btc_free = await check_balance()
                                            if btc_free >= TRADE_VOLUME:
                                                order = await exchange.create_market_sell_order(SYMBOL, TRADE_VOLUME)
                                                profit = current_price - buy_price
                                                total_profit += profit
                                                return_profit, msg = handle_second_strategy("sell", current_price, profit)
                                                signal = create_signal("sell", current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot stopped via Telegram{msg}")
                                                store_signal(signal)
                                                if bot:
                                                    send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                                logger.info(f"Placed sell order on Telegram stop: {order}")
                                                position = None
                                            else:
                                                logger.error(f"Insufficient BTC balance ({btc_free:.6f}) for sell order of {TRADE_VOLUME:.6f}")
                                        except Exception as e:
                                            logger.error(f"Error placing sell order on Telegram stop: {e}")
                                    bot_active = False
                                bot.send_message(chat_id=command_chat_id, text="Bot stopped.")
                                upload_to_github(db_path, 'r_bot.db')
                            elif text.startswith('/stop') and text[5:].isdigit():
                                multiplier = int(text[5:])
                                with bot_lock:
                                    pause_duration = multiplier * timeframe_seconds
                                    pause_start = datetime.now(EASTERN_TZ)
                                    if position == "long":
                                        try:
                                            usdt_free, btc_free = await check_balance()
                                            if btc_free >= TRADE_VOLUME:
                                                order = await exchange.create_market_sell_order(SYMBOL, TRADE_VOLUME)
                                                profit = current_price - buy_price
                                                total_profit += profit
                                                return_profit, msg = handle_second_strategy("sell", current_price, profit)
                                                signal = create_signal("sell", latest_data['Close'], latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot paused via Telegram{msg}")
                                                store_signal(signal)
                                                if bot:
                                                    send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                                logger.info(f"Placed sell order on Telegram pause: {order}")
                                                position = None
                                            else:
                                                logger.error(f"Insufficient BTC balance ({btc_free:.6f}) for sell order of {TRADE_VOLUME:.6f}")
                                        except Exception as e:
                                            logger.error(f"Error placing sell order on Telegram pause: {e}")
                                    bot_active = False
                                bot.send_message(chat_id=command_chat_id, text=f"Bot paused for {pause_duration/60} minutes.")
                                upload_to_github(db_path, 'r_bot.db')
                            elif text == '/start':
                                with bot_lock:
                                    if not bot_active:
                                        bot_active = True
                                        position = None
                                        pause_start = None
                                        pause_duration = 0
                                        bot.send_message(chat_id=command_chat_id, text="Bot started.")
                                        logger.info("Bot restarted via Telegram")
                            elif text == '/status':
                                status = "active" if bot_active else f"paused for {int(pause_duration - (datetime.now(EASTERN_TZ) - pause_start).total_seconds())} seconds" if pause_start else "stopped"
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
            }, index=[pd.Timestamp.now(tz=EASTERN_TZ)])
            df = pd.concat([df, new_row]).tail(100)
            df = add_technical_indicators(df)

            prev_close = df['Close'].iloc[-2] if len(df) >= 2 else df['Close'].iloc[-1]
            percent_change = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0.0
            recommended_action, stop_loss, take_profit = ai_decision(df, position=position, buy_price=buy_price)
            logger.debug(f"AI decision: {recommended_action}, position: {position}, stop_loss: {stop_loss}, take_profit: {take_profit}")

            with bot_lock:
                action = "hold"
                profit = 0
                return_profit = 0
                msg = f"HOLD {SYMBOL} at {current_price:.2f}"
                if bot_active and recommended_action == "buy" and position is None:
                    try:
                        usdt_free, btc_free = await check_balance()
                        min_notional = 10  # Binance minimum order ~$10
                        if usdt_free >= current_price * TRADE_VOLUME * 1.001:
                            order = await exchange.create_market_buy_order(SYMBOL, TRADE_VOLUME)
                            position = "long"
                            buy_price = current_price
                            action = "buy"
                            return_profit, msg_suffix = handle_second_strategy("buy", current_price, 0)
                            msg = f"BUY {SYMBOL} at {current_price:.2f}{msg_suffix}"
                            logger.info(f"Placed buy order: {order}")
                        else:
                            logger.error(f"Insufficient USDT balance ({usdt_free:.2f}) for buy order of {TRADE_VOLUME:.6f} BTC at {current_price:.2f}")
                            msg = f"Failed to place BUY order for {SYMBOL} at {current_price:.2f}: Insufficient balance"
                    except Exception as e:
                        logger.error(f"Error placing buy order: {e}")
                        action = "hold"
                        msg = f"Failed to place BUY order for {SYMBOL} at {current_price:.2f}: {str(e)}"
                elif bot_active and recommended_action == "sell" and position == "long":
                    try:
                        usdt_free, btc_free = await check_balance()
                        if btc_free >= TRADE_VOLUME:
                            order = await exchange.create_market_sell_order(SYMBOL, TRADE_VOLUME)
                            profit = current_price - buy_price
                            total_profit += profit
                            return_profit, msg_suffix = handle_second_strategy("sell", current_price, profit)
                            position = None
                            action = "sell"
                            msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}{msg_suffix}"
                            if stop_loss and current_price <= stop_loss:
                                msg += " (Stop-Loss)"
                            elif take_profit and current_price >= take_profit:
                                msg += " (Take-Profit)"
                            logger.info(f"Placed sell order: {order}")
                        else:
                            logger.error(f"Insufficient BTC balance ({btc_free:.6f}) for sell order of {TRADE_VOLUME:.6f}")
                            msg = f"Failed to place SELL order for {SYMBOL} at {current_price:.2f}: Insufficient balance"
                    except Exception as e:
                        logger.error(f"Error placing sell order: {e}")
                        action = "hold"
                        msg = f"Failed to place SELL order for {SYMBOL} at {current_price:.2f}: {str(e)}"

                signal = create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg)
                store_signal(signal)
                logger.debug(f"Generated and stored signal: action={signal['action']}, time={signal['time']}, price={signal['price']:.2f}")
                if bot_active and action != "hold" and bot:
                    threading.Thread(target=send_telegram_message, args=(signal, BOT_TOKEN, CHAT_ID), daemon=True).start()

            # Store all signals to ensure database updates every minute
            upload_to_github(db_path, 'r_bot.db')
            
            # Sleep until the next timeframe boundary
            loop_end_time = datetime.now(EASTERN_TZ)
            processing_time = (loop_end_time - loop_start_time).total_seconds()
            seconds_to_wait = get_next_timeframe_boundary(loop_end_time, timeframe_seconds)
            adjusted_sleep = max(seconds_to_wait - processing_time, 1)
            logger.debug(f"Sleeping for {adjusted_sleep:.2f} seconds to align with next {TIMEFRAME} boundary, processing_time={processing_time:.2f}s")
            await asyncio.sleep(adjusted_sleep)
        except Exception as e:
            logger.error(f"Error in trading loop: {e}", exc_info=True)
            current_time = datetime.now(EASTERN_TZ)
            seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
            logger.debug(f"Recovering from error, sleeping for {seconds_to_wait:.2f} seconds")
            await asyncio.sleep(seconds_to_wait)

# Helper functions
def create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg):
    latest = df.iloc[-1]
    signal = {
        'time': datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
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
        'timeframe': TIMEFRAME
    }
    logger.debug(f"Created signal: {signal}")
    return signal

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
            elapsed = time.time() - start_time
            logger.info(f"Signal stored successfully: action={signal['action']}, time={signal['time']}, db_write_time={elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error storing signal after {elapsed:.3f}s: {e}", exc_info=True)
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
                    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S")
                    current_time = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    return render_template('index.html', signal=None, status=status, timeframe=TIMEFRAME,
                                         trades=[], stop_time=stop_time_str, current_time=current_time)
            c = conn.cursor()
            c.execute("SELECT * FROM trades ORDER BY time DESC LIMIT 16")
            rows = c.fetchall()
            columns = [col[0] for col in c.description]
            trades = [dict(zip(columns, row)) for row in rows]
            signal = trades[0] if trades else None
            stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S")
            current_time = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S")
            elapsed = time.time() - start_time
            logger.info(f"Rendering index.html: status={status}, timeframe={TIMEFRAME}, trades={len(trades)}, signal_exists={signal is not None}, signal_time={signal['time'] if signal else 'None'}, query_time={elapsed:.3f}s")
            return render_template('index.html', signal=signal, status=status, timeframe=TIMEFRAME,
                                 trades=trades, stop_time=stop_time_str, current_time=current_time)
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error rendering index.html after {elapsed:.3f}s: {e}")
            conn = None
            return render_template('index.html', signal=None, status=status, timeframe=TIMEFRAME,
                                  trades=[], stop_time=stop_time.strftime("%Y-%m-%d %H:%M:%S"),
                                  current_time=datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S"))

@app.route('/status')
def status():
    status = "active" if bot_active else "stopped"
    return jsonify({"status": status, "timeframe": TIMEFRAME, "stop_time": stop_time.strftime("%Y-%m-%d %H:%M:%S")})

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

# Cleanup
def cleanup():
    global conn, exchange
    if conn:
        conn.close()
        logger.info("Database connection closed")
        upload_to_github(db_path, 'r_bot.db')
        logger.info("Final database backup to GitHub completed")
    asyncio.run(exchange.close())
    logger.info("Exchange connection closed")

atexit.register(cleanup)

# Start trading bot in async context
async def main():
    global bot_thread
    if bot_thread is None or not bot_thread.is_alive():
        bot_thread = threading.Thread(target=lambda: asyncio.run(trading_bot()), daemon=True)
        bot_thread.start()
        logger.info("Trading bot started automatically")

    # Start keep-alive thread
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    logger.info("Keep-alive thread started")

# Start Flask server
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    logger.info(f"Starting Flask server on port {port}")
    asyncio.run(main())
    app.run(host='0.0.0.0', port=port, debug=False)
