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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

exchange = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

TIMEFRAME = '15m'
CANDLE_LIMIT = 120
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
        print(f"Error loading markets: {e}")
        return []

def plot_and_save_chart(df, symbol, entry, sl, tp, signal_type):
    filename = f"chart_{int(time.time()*1000)}.png"
    plot_df = df.tail(50).copy()
    plot_df['timestamp'] = pd.to_datetime(plot_df['timestamp'], unit='ms')
    plot_df.set_index('timestamp', inplace=True)
    
    apds = [
        mpf.make_addplot(plot_df['EMA_50'], color='cyan', width=1.0),
        mpf.make_addplot(plot_df['EMA_200'], color='orange', width=1.2),
    ]

    h_lines = dict(hlines=[entry, sl, tp], colors=['#2196F3', '#F44336', '#4CAF50'], linestyle='--', widths=1.2)
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

def check_signal(symbol, force_send=False):
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

        is_long = (close > ema_200) and (rsi < 48) and (last['RSI'] > prev['RSI']) and (vol >= vol_ma * 0.8)
        is_short = (close < ema_200) and (rsi > 52) and (last['RSI'] < prev['RSI']) and (vol >= vol_ma * 0.8)

        # ئەگەر تێست بوو، ڕاستەوخۆ بەپێی نزیکترین مەرج سیگناڵ دەنێرێت
        if force_send:
            is_long = True

        if is_long:
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
                f"• ترێند: `Bullish Pullback`\n"
                f"• ئاستی RSI: `{round(rsi, 1)}`"
            )
            with open(chart_path, 'rb') as photo:
                bot.send_photo(TELEGRAM_CHAT_ID, photo, caption=caption, parse_mode="Markdown")
            if os.path.exists(chart_path):
                os.remove(chart_path)
            last_signal_time[symbol] = current_time
            print(f"Signal sent: {display_name} LONG")

        elif is_short:
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
                f"• ترێند: `Bearish Pullback`\n"
                f"• ئاستی RSI: `{round(rsi, 1)}`"
            )
            with open(chart_path, 'rb') as photo:
                bot.send_photo(TELEGRAM_CHAT_ID, photo, caption=caption, parse_mode="Markdown")
            if os.path.exists(chart_path):
                os.remove(chart_path)
            last_signal_time[symbol] = current_time
            print(f"Signal sent: {display_name} SHORT")

    except Exception as e:
        print(f"Error analyzing {symbol}: {e}")

def scanner_loop():
    while True:
        try:
            symbols = get_all_futures_symbols()
            for symbol in symbols:
                if symbol in last_signal_time and (time.time() - last_signal_time[symbol]) < 2400:
                    continue
                check_signal(symbol)
                time.sleep(0.12)
            time.sleep(60)
        except Exception as e:
            print(f"Loop error: {e}")
            time.sleep(20)

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "🚀 *بۆتی سیگناڵ چالاکە!*\nبۆ پشکنینی وێنە فەرمانی `/test` لێبدە.", parse_mode="Markdown")

@bot.message_handler(commands=['test'])
def test_signal(message):
    bot.reply_to(message, "⏳ خەریکی کێشانی وێنەی چارت و تێستکردنی سیگناڵم...")
    check_signal('BTC/USDT:USDT', force_send=True)

if __name__ == "__main__":
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()
    bot.infinity_polling(timeout=10, long_polling_timeout=5)
