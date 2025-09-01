# 4th updates 
# flask_setup.py
import os
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
import ccxt
from datetime import datetime, timedelta
import pytz
import pandas as pd
from dotenv import load_dotenv
import requests
import time
import threading
import base64
from flask import Flask  # Added Flask import

# Initialize Flask app
app = Flask(__name__)

# Load environment variables
load_dotenv()

# Initialize global variables
bot_active = True
position = None
buy_price = None
total_profit = 0.0
pause_duration = 0
pause_start = None
conn = None
db_lock = threading.Lock()
total_return_profit = 0.0
last_sell_profit = 0.0
tracking_enabled = False
tracking_has_buy = False
tracking_buy_price = None
STOP_AFTER_SECONDS = int(os.getenv("STOP_AFTER_SECONDS", 0))
stop_time = datetime.now(pytz.UTC) + timedelta(seconds=STOP_AFTER_SECONDS) if STOP_AFTER_SECONDS > 0 else None
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
SYMBOL = os.getenv("SYMBOL", "BTC/USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
TIMEFRAMES = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '1d': 86400}
INTER_SECONDS = int(os.getenv("INTER_SECONDS", 60))
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", 2.0))
TAKE_PROFIT_PERCENT = float(os.getenv("TAKE_PROFIT_PERCENT", 5.0))
AMOUNTS = float(os.getenv("AMOUNTS", 100.0))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_PATH = os.getenv("GITHUB_PATH")
EU_TZ = pytz.timezone("Europe/Amsterdam")
db_path = '/opt/pcn/trader-site/rr_bot.bd'

# Initialize logger
logger = logging.getLogger('flask_setup')
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler('/opt/pcn/trader-site/td_sto.log', maxBytes=1000000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize exchange
try:
    exchange = ccxt.binance({
        'apiKey': os.getenv("BINANCE_API_KEY"),
        'secret': os.getenv("BINANCE_API_SECRET"),
        'enableRateLimit': True,
    })
    logger.info("Binance exchange initialized successfully")
except Exception as e:
    logger.error(f"Error initializing Binance exchange: {e}")
    exchange = None

# Bot lock for thread safety
bot_lock = threading.Lock()

def check_database_integrity(db_path):
    """Check if the database is valid and accessible."""
    try:
        with sqlite3.connect(db_path) as temp_conn:
            cursor = temp_conn.cursor()
            # Test query to check database integrity
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades';")
            if not cursor.fetchone():
                logger.warning("Trades table does not exist in the database.")
                return False
            # Try a simple write operation to test database integrity
            cursor.execute("CREATE TABLE IF NOT EXISTS test_integrity (id INTEGER PRIMARY KEY)")
            cursor.execute("INSERT INTO test_integrity (id) VALUES (1)")
            temp_conn.commit()
            cursor.execute("DROP TABLE test_integrity")
            temp_conn.commit()
            return True
    except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
        logger.error(f"Database integrity check failed: {e}")
        return False

def get_database_last_modified(db_path):
    """Get the last modified time of the database file."""
    try:
        mtime = os.path.getmtime(db_path)
        return datetime.fromtimestamp(mtime, tz=pytz.UTC)
    except FileNotFoundError:
        logger.warning(f"Database file {db_path} does not exist.")
        return None
    except Exception as e:
        logger.error(f"Error checking database last modified time: {e}")
        return None

def setup_database():
    """Set up the SQLite database with forced recreation if missing, corrupt, or outdated."""
    global conn, db_path
    max_age_seconds = 300  # 5 minutes

    # Check if database exists and its last update time
    database_exists = os.path.exists(db_path)
    last_modified = get_database_last_modified(db_path)
    current_time = datetime.now(pytz.UTC)
    is_outdated = last_modified and (current_time - last_modified).total_seconds() > max_age_seconds

    # Force recreate if database doesn't exist, is corrupt, or is outdated
    if not database_exists or is_outdated or not check_database_integrity(db_path):
        logger.info(f"Forcing database recreation: exists={database_exists}, outdated={is_outdated}, integrity={check_database_integrity(db_path)}")
        try:
            # Remove existing database file if it exists
            if database_exists:
                os.remove(db_path)
                logger.info(f"Removed existing database file: {db_path}")

            # Create new database
            with db_lock:
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                c.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
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
                conn.commit()
                logger.info(f"Database created successfully at {db_path}")
                return True
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            conn = None
            return False
    else:
        # Database exists and is valid; connect to it
        try:
            with db_lock:
                conn = sqlite3.connect(db_path)
                logger.info(f"Connected to existing database at {db_path}")
                return True
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            conn = None
            return False

def upload_to_github(db_path, file_name):
    """Upload database file to GitHub."""
    try:
        with open(db_path, 'rb') as f:
            content = f.read()
        encoded_content = base64.b64encode(content).decode('utf-8')
        headers = {
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }
        url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}/{file_name}'
        
        # Check if file exists on GitHub
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            sha = response.json()['sha']
            data = {
                'message': f'Update {file_name}',
                'content': encoded_content,
                'sha': sha,
                'branch': 'main'
            }
        else:
            data = {
                'message': f'Create {file_name}',
                'content': encoded_content,
                'branch': 'main'
            }
        
        response = requests.put(url, headers=headers, json=data)
        if response.status_code in [200, 201]:
            logger.info(f"Successfully uploaded {file_name} to GitHub")
            return True
        else:
            logger.error(f"Failed to upload {file_name} to GitHub: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error uploading to GitHub: {e}")
        return False

def download_from_github(file_name):
    """Download database file from GitHub."""
    try:
        headers = {
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }
        url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}/{file_name}'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            content = base64.b64decode(response.json()['content'])
            with open(db_path, 'wb') as f:
                f.write(content)
            logger.info(f"Successfully downloaded {file_name} from GitHub")
            return True
        else:
            logger.error(f"Failed to download {file_name} from GitHub: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error downloading from GitHub: {e}")
        return False
