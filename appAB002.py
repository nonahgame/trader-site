# 3rd update 
# app.py
from flask_setup import (
    app, conn, STOP_AFTER_SECONDS, EU_TZ, db_path, logger, setup_database,
    upload_to_github
)
from flask_routes import get_performance, get_trade_counts
from trade_bot import trading_bot
import threading
import requests
import time
import atexit
import os

def keep_alive():
    """Send periodic keep-alive pings to prevent server timeout."""
    while True:
        try:
            requests.get('https://www.google.com')
            logger.debug("Keep-alive ping sent")
            time.sleep(300)  # Ping every 5 minutes
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")
            time.sleep(60)  # Retry after 1 minute on error

def cleanup():
    """Close database connection and upload to GitHub on exit."""
    global conn
    if conn:
        conn.close()
        logger.info("Database connection closed")
        upload_to_github(db_path, 'rr_bot.bd')
        logger.info("Final database backup to GitHub completed")

atexit.register(cleanup)

if __name__ == "__main__":
    logger.info("Initializing database in main thread")
    if not setup_database():
        logger.error("Failed to initialize database in main thread. Exiting.")
        exit(1)

    bot_thread = threading.Thread(target=trading_bot, daemon=True)
    bot_thread.start()
    logger.info("Trading bot thread started")

    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    logger.info("Keep-alive thread started")

    port = int(os.getenv("PORT", 4040))
    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
