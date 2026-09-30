import os
import time
import threading
import telebot
import ccxt
import pandas as pd
import pandas_ta as ta
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf

# --- زانیارییەکان لە Railway Variables وەردەگیرێن ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# بەستنەوە بە بازاڕی فیووچەرزی Bitget (USDT-M Perpetual)
exchange = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

TIMEFRAME = '15m'
CANDLE_LIMIT = 100
last_signal_time = {}

def calculate_leverage(entry_price, stop_loss):
    risk_pct = abs(entry_price - stop_loss) / entry_price * 100
    if risk_pct <= 1.5:
        return "10x - 15x"
    elif risk_pct <= 3.0:
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
        print(f"Market error: {e}")
        return []

def plot_and_save_chart(df, symbol, entry, sl, tp, signal_type):
    filename = f"chart_{int(time.time()*1000)}.png"
    plot_df = df.tail(45).copy()
    plot_df['timestamp'] = pd.to_datetime(plot_df['timestamp'], unit='ms')
    plot_df.set_index('timestamp', inplace=True)
    
    apds = [
        mpf.make_addplot(plot_df['EMA_50'], color='cyan', width=1.0),
        mpf.make_addplot(plot_df['EMA_200'], color='orange', width=1.2),
    ]

    # چاککردنی هەڵەکە: بەکارهێنانی linewidths لەبری widths
    h_lines = dict(
        hlines=[entry, sl, tp], 
        colors=['#2196F3', '#F44336', '#4CAF50'], 
        linestyle='--', 
        linewidths=1.2
    )
    custom_style = mpf.make_mpf_style(base_mpf_style='nightclouds', rc={'font.size': 8})

    mpf.plot(
        plot_df,
        type='candle',
        volume=False,
        addplot=apds,
        hlines=h_lines,
        style=custom_style,
        title=f"\n{symbol} ({TIMEFRAME}) - {signal_type}\nGreen: TP | Blue: Entry | Red: SL",
        savefig=dict(fname=filename, dpi=100, bbox_inches='tight')
    )
    return filename

def check_signal(symbol, force_send=False, chat_id=None):
    target_chat = chat_id if chat_id else TELEGRAM_CHAT_ID
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=CANDLE_LIMIT)
        if not ohlcv or len(ohlcv) < 60:
            if force_send:
                bot.send_message(target_chat, "⚠️ نەتوانرا داتای پێویست لە Bitget وەربگیرێت.")
            return

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['EMA_50'] = ta.ema(df['close'], length=50)
        df['EMA_200'] = ta.ema(df['close'], length=200)
        df['RSI'] = ta.rsi(df['close'], length=14)
        df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)

        last = df.iloc[-2]
        prev = df.iloc[-3]

        close = float(last['close'])
        ema_200 = float(last['EMA_200']) if not pd.isna(last['EMA_200']) else close
        rsi = float(last['RSI'])
        atr = float(last['ATR']) if not pd.isna(last['ATR']) else (close * 0.01)

        display_name = symbol.split(':')[0]

        is_long = (close > ema_200) and (rsi < 48) and (last['RSI'] > prev['RSI'])
        is_short = (close < ema_200) and (rsi > 52) and (last['RSI'] < prev['RSI'])

        if force_send:
            is_long = True

        if is_long:
            sl = round(close - (atr * 1.5), 4)
            tp = round(close + (atr * 2.5), 4)
            leverage = calculate_leverage(close, sl)

            chart_path = plot_and_save_chart(df, display_name, close, sl, tp, "LONG")
            caption = (
                f"🟢 *سیگناڵی کڕین (LONG)*\n\n"
                f"🪙 *دراو:* `{display_name}` (Bitget Futures)\n"
                f"⏱ *تایم‌فرەیم:* `15m`\n"
                f"📍 *نرخی چوونەژوور:* `{close}`\n"
                f"🎯 *تارگێت:* `{tp}`\n"
                f"🛑 *ستۆپ لۆس:* `{sl}`\n"
                f"⚡️ *لیڤەرەیج:* `{leverage}`\n\n"
                f"📊 *شیکاری:*\n"
                f"• ترێند: `Bullish Trend`\n"
                f"• خاڵی RSI: `{round(rsi, 1)}`"
            )
            with open(chart_path, 'rb') as photo:
                bot.send_photo(target_chat, photo, caption=caption, parse_mode="Markdown")
            if os.path.exists(chart_path):
                os.remove(chart_path)
            last_signal_time[symbol] = time.time()

        elif is_short:
            sl = round(close + (atr * 1.5), 4)
            tp = round(close - (atr * 2.5), 4)
            leverage = calculate_leverage(close, sl)

            chart_path = plot_and_save_chart(df, display_name, close, sl, tp, "SHORT")
            caption = (
                f"🔴 *سیگناڵی فرۆشتن (SHORT)*\n\n"
                f"🪙 *دراو:* `{display_name}` (Bitget Futures)\n"
                f"⏱ *تایم‌فرەیم:* `15m`\n"
                f"📍 *نرخی چوونەژوور:* `{close}`\n"
                f"🎯 *تارگێت:* `{tp}`\n"
                f"🛑 *ستۆپ لۆس:* `{sl}`\n"
                f"⚡️ *لیڤەرەیج:* `{leverage}`\n\n"
                f"📊 *شیکاری:*\n"
                f"• ترێند: `Bearish Trend`\n"
                f"• خاڵی RSI: `{round(rsi, 1)}`"
            )
            with open(chart_path, 'rb') as photo:
                bot.send_photo(target_chat, photo, caption=caption, parse_mode="Markdown")
            if os.path.exists(chart_path):
                os.remove(chart_path)
            last_signal_time[symbol] = time.time()

    except Exception as e:
        if force_send:
            bot.send_message(target_chat, f"❌ کێشەی تەکنیکی ڕوویدا:\n`{str(e)}`", parse_mode="Markdown")

def scanner_loop():
    while True:
        try:
            symbols = get_all_futures_symbols()
            for symbol in symbols:
                if symbol in last_signal_time and (time.time() - last_signal_time[symbol]) < 2400:
                    continue
                check_signal(symbol)
                time.sleep(0.15)
            time.sleep(60)
        except Exception as e:
            time.sleep(20)

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "🚀 *بۆتی سیگناڵ چالاکە!*\nبۆ وەرگرتنی وێنەی چارت و تێست فەرمانی `/test` بنێرە.", parse_mode="Markdown")

@bot.message_handler(commands=['test'])
def test_signal(message):
    threading.Thread(target=check_signal, args=('BTC/USDT:USDT', True, message.chat.id)).start()

if __name__ == "__main__":
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()
    bot.infinity_polling(timeout=10, long_polling_timeout=5)
