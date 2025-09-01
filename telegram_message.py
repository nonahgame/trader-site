# 1st update 
# telegram_message.py
from flask_setup import logger
from telegram import Bot
import telegram

def send_telegram_message(signal, bot_token, chat_id, retries=3, delay=5):
    for attempt in range(retries):
        try:
            bot = Bot(token=bot_token)
            diff_color = "🟢" if signal['diff'] > 0 else "🔴"
            message = f"""
Time: {signal['time']}
Timeframe: {signal['timeframe']}
Strategy: {signal['strategy']}
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
MACD (DIF): {signal['macd']:.2f}
MACD Signal (DEA): {signal['macd_signal']:.2f}
MACD Hist: {signal['macd_hist']:.2f}
Lst Diff: {signal['lst_diff']:.2f}
Supertrend: {signal['supertrend']:.2f}
Supertrend Trend: {'Up' if signal['supertrend_trend'] else 'Down'}
Stoch RSI: {signal['stoch_rsi']:.2f}
Stoch K: {signal['stoch_k']:.2f}
Stoch D: {signal['stoch_d']:.2f}
OBV: {signal['obv']:.2f}
{f"Stop-Loss: {signal['stop_loss']:.2f}" if signal['stop_loss'] is not None else ""}
{f"Take-Profit: {signal['take_profit']:.2f}" if signal['take_profit'] is not None else ""}
{f"Total Profit: {signal['total_profit']:.2f}" if signal['action'] in ["buy", "sell"] else ""}
{f"Profit: {signal['profit']:.2f}" if signal['action'] == "sell" else ""}
{f"Order ID: {signal['order_id']}" if signal['order_id'] else ""}
"""
            bot.send_message(chat_id=chat_id, text=message)
            logger.info(f"Telegram message sent successfully: {signal['action']}, order_id={signal['order_id']}")
            return
        except telegram.error.InvalidToken:
            logger.error(f"Invalid Telegram bot token: {bot_token}")
            return
        except telegram.error.ChatNotFound:
            logger.error(f"Chat not found for chat_id: {chat_id}")
            return
        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    logger.error(f"Failed to send Telegram message after {retries} attempts")
