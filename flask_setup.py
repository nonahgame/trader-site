# 1st update 
# flask_setup.py
import os
import pandas as pd
import numpy as np
import sqlite3
import time
from datetime import datetime, timedelta
import pytz
import ccxt
import pandas_ta as ta
from telegram import Bot
import telegram
import logging
import threading
import requests
import base64
from flask import Flask
import atexit
import asyncio

pd.set_option('future.no_silent_downcasting', True)

# Custom formatter for EU timezone (UTC)
class EUFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, tz=pytz.utc):
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

# Try to import dotenv, with fallback if not installed
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.debug("Loaded environment variables from .env file")
except ImportError:
    logger.warning("python-dotenv not installed. Relying on system environment variables.")

werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_handler = logging.StreamHandler()
werkzeug_handler.setFormatter(EUFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
werkzeug_logger.handlers = [werkzeug_handler, logging.FileHandler('td_sto.log')]
werkzeug_logger.setLevel(logging.DEBUG)

# Flask app setup
app = Flask(__name__)

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN", "BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "CHAT_ID")
SYMBOL = os.getenv("SYMBOL", "SYMBOL")
TIMEFRAME = os.getenv("TIMEFRAME", "TIMEFRAME")
TIMEFRAMES = int(os.getenv("INTER_SECONDS", "INTER_SECONDS"))
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", "STOP_LOSS_PERCENT"))
TAKE_PROFIT_PERCENT = float(os.getenv("TAKE_PROFIT_PERCENT", "TAKE_PROFIT_PERCENT"))
STOP_AFTER_SECONDS = float(os.getenv("STOP_AFTER_SECONDS", 0))  # Set to 0 to disable auto-stop
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "GITHUB_REPO")
GITHUB_PATH = os.getenv("GITHUB_PATH", "GITHUB_PATH")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "BINANCE_API_SECRET")
AMOUNTS = float(os.getenv("AMOUNTS", "AMOUNTS"))

# GitHub API setup
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Database path
db_path = 'rr_bot.bd'

# Timezone setup
EU_TZ = pytz.utc

# Global state
bot_thread = None
bot_active = True
bot_lock = threading.Lock()
db_lock = threading.Lock()
conn = None
exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_API_SECRET,
    'enableRateLimit': True,
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
start_time = datetime.now(EU_TZ)
stop_time = None  # Disable stop time if STOP_AFTER_SECONDS is 0
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

# Database setup
def setup_database():
    global conn
    with db_lock:
        for attempt in range(3):
            try:
                logger.info(f"Database setup attempt {attempt + 1}/3")
                if not os.path.exists(db_path):
                    logger.info(f"Database file {db_path} does not exist. Creating new database.")
                    conn = sqlite3.connect(db_path, check_same_thread=False)
                    logger.info(f"Created new database file at {db_path}")
                else:
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
                        conn = sqlite3.connect(db_path, check_same_thread=False)
                        logger.info(f"Created new database file at {db_path} after corruption")

                logger.info(f"Attempting to download database from GitHub: {GITHUB_API_URL}")
                if download_from_github('rr_bot.bd', db_path):
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
                        conn = sqlite3.connect(db_path, check_same_thread=False)
                        logger.info(f"Created new database file at {db_path} after failed download")

                if conn is None:
                    conn = sqlite3.connect(db_path, check_same_thread=False)
                logger.info(f"Connected to database at {db_path}")

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
                            macd REAL,
                            macd_signal REAL,
                            macd_hist REAL,
                            lst_diff REAL,
                            supertrend REAL,
                            supertrend_trend INTEGER,
                            stoch_rsi REAL,
                            stoch_k REAL,
                            stoch_d REAL,
                            obv REAL,
                            message TEXT,
                            timeframe TEXT,
                            order_id TEXT,
                            strategy TEXT
                        )
                    ''')
                    logger.info("Created new trades table")
                c.execute("PRAGMA table_info(trades);")
                columns = [col[1] for col in c.fetchall()]
                for col in ['return_profit', 'total_return_profit', 'diff', 'macd', 'macd_signal', 'macd_hist', 'lst_diff', 'message', 'timeframe', 'order_id', 'strategy']:
                    if col not in columns:
                        c.execute(f"ALTER TABLE trades ADD COLUMN {col} {'REAL' if col in ['return_profit', 'total_return_profit', 'diff', 'macd', 'macd_signal', 'macd_hist', 'lst_diff'] else 'TEXT'};")
                        logger.info(f"Added column {col} to trades table")
                conn.commit()
                logger.info(f"Database initialized successfully at {db_path}, size: {os.path.getsize(db_path)} bytes")
                upload_to_github(db_path, 'rr_bot.bd')
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
