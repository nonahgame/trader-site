# 1st update 
# app.py
from flask_setup import (
    app, bot_thread, conn, STOP_AFTER_SECONDS, EU_TZ, db_path, logger, setup_database,
    upload_to_github
)
from flask_routes import get_performance, get_trade_counts
from trade_bot import trading_bot
import threading
import requests
import time
import asyncio
import atexit
import os

def keep_alive():
    while True:
        try:
            requests.get('https://www.google.com')
            logger.debug("Keep-alive ping sent")
            time.sleep(300)
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")
            time.sleep(60)

def cleanup():
    global conn
    if conn:
        conn.close()
        logger.info("Database connection closed")
        upload_to_github(db_path, 'rr_bot.bd')
        logger.info("Final database backup to GitHub completed")

atexit.register(cleanup)

async def main():
    pass

if __name__ == "__main__":
    logger.info("Initializing database in main thread")
    if not setup_database():
        logger.error("Failed to initialize database in main thread. Flask routes may fail.")

    if bot_thread is None or not bot_thread.is_alive():
        bot_thread = threading.Thread(target=trading_bot, daemon=True)
        bot_thread.start()
        logger.info("Trading bot started automatically")

    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    logger.info("Keep-alive thread started")

    port = int(os.getenv("PORT", 4040))
    logger.info(f"Starting Flask server on port {port}")
    asyncio.run(main())
    app.run(host='0.0.0.0', port=port, debug=False)
