import os
import time
import threading
import telebot
import ccxt
import pandas as pd
import pandas_ta as ta
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')

# --- هێنانی زانیارییەکان لە ژینگەی کارکردن ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# ئاڵوگۆڕی Bitget بۆ بازاڕی فیووچەرز
exchange = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

TIMEFRAME = '15m'
CANDLE_LIMIT = 100
last_signal_time = {}

def calculate_leverage(entry_price, stop_loss):
    risk_pct = abs(entry_price - stop_loss) / entry_price * 100
    if risk_pct <= 1.2:
        return "10x - 12x"
    elif risk_pct <= 2.5:
        return "5x - 7x"
    else:
        return "3x - 5x"

def get_all_futures_symbols():
    try:
        markets = exchange.load_markets()
        return [
            s for s, m in markets.items()
            if m.get('quote') == 'USDT' and m.get('active', True) and m.get('swap', True)
        ]
    except Exception as e:
        print(f"Error loading markets: {e}")
        return []

def plot_and_save_chart(df, symbol, entry, sl, tp, signal_type):
    filename = f"chart_{symbol.replace('/', '_').replace(':', '_')}_{int(time.time())}.png"
    plot_df = df.tail(60).copy()
    plot_df['timestamp'] = pd.to_datetime(plot_df['timestamp'], unit='ms')
    plot_df.set_index('timestamp', inplace=True)
    
    apds = [
        mpf.make_addplot(plot_df['EMA_50'], color='cyan', width=1.2),
        mpf.make_addplot(plot_df['EMA_200'], color='orange', width=1.5),
    ]

    h_lines = dict(hlines=[entry, sl, tp], colors=['blue', 'red', 'green'], linestyle='--', widths=1.2)

    custom_style = mpf.make_mpf_style(base_mpf_style='nightclouds', rc={'font.size': 8})

    mpf.plot(
        plot_df,
        type='candle',
        volume=True,
        addplot=apds,
        hlines=h_lines,
        style=custom_style,
        title=f"\n{symbol} ({TIMEFRAME}) - {signal_type}\nGreen: TP | Blue: Entry | Red: SL",
        savefig=filename
    )
    return filename

def analyze_symbol(symbol):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=CANDLE_LIMIT)
        if not ohlcv or len(ohlcv) < 80:
            return

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['EMA_50'] = ta.ema(df['close'], length=50)
        df['EMA_200'] = ta.ema(df['close'], length=200)
        df['RSI'] = ta.rsi(df['close'], length=14)
        df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        df['VOL_MA'] = ta.sma(df['volume'], length=20)

        last = df.iloc[-2]
        prev = df.iloc[-3]

        close = float(last['close'])
        ema_50 = float(last['EMA_50'])
        ema_200 = float(last['EMA_200'])
        rsi = float(last['RSI'])
        atr = float(last['ATR'])
        vol = float(last['volume'])
        vol_ma = float(last['VOL_MA'])

        display_name = symbol.split(':')[0]
        current_time = time.time()

        # مەرجی LONG
        if close > ema_50 > ema_200 and rsi < 42 and last['RSI'] > prev['RSI'] and vol > vol_ma:
            sl = round(close - (atr * 1.5), 5)
            tp1 = round(close + (atr * 2.5), 5)
            tp2 = round(close + (atr * 4.0), 5)
            leverage = calculate_leverage(close, sl)

            chart_path = plot_and_save_chart(df, display_name, close, sl, tp1, "LONG")
            caption = (
                f"🟢 *سیگناڵی کڕین (LONG)*\n\n"
                f"🪙 *دراو:* `{display_name}` (Bitget Futures)\n"
                f"⏱ *تایم‌فرەیم:* `15m`\n"
                f"📍 *نرخی چوونەژوور:* `{close}`\n"
                f"🎯 *تارگێت ١:* `{tp1}`\n"
                f"🎯 *تارگێت ٢:* `{tp2}`\n"
                f"🛑 *ستۆپ لۆس:* `{sl}`\n"
                f"⚡️ *لیڤەرەیج:* `{leverage}`\n\n"
                f"📊 *شیکاری:*\n"
                f"• ترێند: `Bullish Trend`\n"
                f"• دۆخی RSI: `{round(rsi, 1)}`"
            )
            with open(chart_path, 'rb') as photo:
                bot.send_photo(TELEGRAM_CHAT_ID, photo, caption=caption, parse_mode="Markdown")
            os.remove(chart_path)
            last_signal_time[symbol] = current_time

        # مەرجی SHORT
        elif close < ema_50 < ema_200 and rsi > 58 and last['RSI'] < prev['RSI'] and vol > vol_ma:
            sl = round(close + (atr * 1.5), 5)
            tp1 = round(close - (atr * 2.5), 5)
            tp2 = round(close - (atr * 4.0), 5)
            leverage = calculate_leverage(close, sl)

            chart_path = plot_and_save_chart(df, display_name, close, sl, tp1, "SHORT")
            caption = (
                f"🔴 *سیگناڵی فرۆشتن (SHORT)*\n\n"
                f"🪙 *دراو:* `{display_name}` (Bitget Futures)\n"
                f"⏱ *تایم‌فرەیم:* `15m`\n"
                f"📍 *نرخی چوونەژوور:* `{close}`\n"
                f"🎯 *تارگێت ١:* `{tp1}`\n"
                f"🎯 *تارگێت ٢:* `{tp2}`\n"
                f"🛑 *ستۆپ لۆس:* `{sl}`\n"
                f"⚡️ *لیڤەرەیج:* `{leverage}`\n\n"
                f"📊 *شیکاری:*\n"
                f"• ترێند: `Bearish Trend`\n"
                f"• دۆخی RSI: `{round(rsi, 1)}`"
            )
            with open(chart_path, 'rb') as photo:
                bot.send_photo(TELEGRAM_CHAT_ID, photo, caption=caption, parse_mode="Markdown")
            os.remove(chart_path)
            last_signal_time[symbol] = current_time

    except Exception:
        pass

def scanner_loop():
    """لووپی سکانکردنی بازاڕ لە پاشبنەما (Background)"""
    while True:
        try:
            symbols = get_all_futures_symbols()
            for symbol in symbols:
                if symbol in last_signal_time and (time.time() - last_signal_time[symbol]) < 3600:
                    continue
                analyze_symbol(symbol)
                time.sleep(0.15)
            time.sleep(120)
        except Exception as e:
            print(f"Scanner error: {e}")
            time.sleep(30)

# --- فەرمانەکانی تەلەگرام ---

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    reply = (
        "🚀 *بۆتی سیگناڵی کریپتۆ (Bitget Futures) بەسەرکەوتوویی کاردەکات!*\n\n"
        "📊 *تایم‌فرەیم:* `15m`\n"
        "🔍 بۆتەکە بە بەردەوامی بازاڕ دەپشکنێت و کاتێک مەرجەکانی ستراتیجی پڕبوونەوە سیگناڵەکە بە وێنەی چارتەوە دەنێرێت."
    )
    bot.reply_to(message, reply, parse_mode="Markdown")

@bot.message_handler(commands=['scan', 'status'])
def send_status(message):
    bot.reply_to(message, "⚡️ سکانەرەکە لە پاشبنەما بەردەوامە لە فەحسکردنی هەموو مارکێتی فیووچەرزی Bitget...")

if __name__ == "__main__":
    # چالاککردنی سکانەرەکە لە Threadێکی جیاواز بۆ ئەوەی ڕێگری لە وەڵامدانەوەی نامەکان نەکات
    scanner_thread = threading.Thread(target=scanner_loop, daemon=True)
    scanner_thread.start()

    print("بۆتەکە ئامادەیە و بەردەوامە لە گوێگرتن لە نامەکان...")
    bot.infinity_polling(timeout=10, long_polling_timeout=5)
