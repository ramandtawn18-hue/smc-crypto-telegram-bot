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
import matplotlib.pyplot as plt

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

exchange = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

TIMEFRAME = '15m'
CANDLE_LIMIT = 260
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
    """دروستکردنی چارت بە ستایلی تەواو ڕەسەنی TradingView Light"""
    filename = f"tv_chart_{int(time.time()*1000)}.png"
    
    plot_df = df.tail(65).copy()
    plot_df['timestamp'] = pd.to_datetime(plot_df['timestamp'], unit='ms')
    plot_df.set_index('timestamp', inplace=True)
    
    # ڕەنگەکانی TradingView (سەوزی نەعنایی و سووری کز)
    marketcolors = mpf.make_marketcolors(
        up='#089981',
        down='#F23645',
        edge={'up': '#089981', 'down': '#F23645'},
        wick={'up': '#089981', 'down': '#F23645'}
    )
    
    tv_light_style = mpf.make_mpf_style(
        marketcolors=marketcolors,
        facecolor='#FFFFFF',      # پاشبنەمای سپی خاوێن
        edgecolor='#E0E3EB',
        figcolor='#FFFFFF',
        gridcolor='#F0F3FA',      # هێڵی تۆڕی زۆر کاڵ
        gridstyle='-',
        gridaxis='both',
        rc={
            'text.color': '#131722',
            'axes.labelcolor': '#787B86',
            'xtick.color': '#787B86',
            'ytick.color': '#787B86',
            'font.family': 'sans-serif',
            'font.size': 9
        }
    )

    clean_symbol = symbol.replace('/', '').replace(':USDT', '') + 'PERP'
    last_price = plot_df['close'].iloc[-1]
    
    title_text = f"{clean_symbol} PERPETUAL CONTRACT · 15 · Bitget  {last_price}"

    # هێڵەکانی TP و Entry و SL بە ڕەنگی نەرم
    h_lines = dict(
        hlines=[float(tp), float(entry), float(sl)],
        colors=['#089981', '#2962FF', '#F23645'],
        linestyle='--',
        linewidths=1.2
    )

    fig, axlist = mpf.plot(
        plot_df,
        type='candle',
        volume=False,
        hlines=h_lines,
        style=tv_light_style,
        title=f"\n{title_text}\n(Green: TP | Blue: Entry | Red: SL)",
        returnfig=True,
        figsize=(10, 5.5),
        savefig=dict(fname=filename, dpi=160, bbox_inches='tight')
    )
    plt.close(fig)
    return filename

def check_signal(symbol, force_send=False, chat_id=None):
    target_chat = chat_id if chat_id else TELEGRAM_CHAT_ID
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=CANDLE_LIMIT)
        if not ohlcv or len(ohlcv) < 220:
            if force_send:
                bot.send_message(target_chat, "⚠️ داتای پێویست لە Bitget وەرنەگیرا.")
            return

        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)

        df['EMA_50'] = ta.ema(df['close'], length=50)
        df['EMA_200'] = ta.ema(df['close'], length=200)
        df['RSI'] = ta.rsi(df['close'], length=14)
        df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)

        df.dropna(inplace=True)
        if len(df) < 50:
            return

        last = df.iloc[-2]
        prev = df.iloc[-3]

        close = float(last['close'])
        ema_200 = float(last['EMA_200'])
        rsi = float(last['RSI'])
        atr = float(last['ATR'])

        display_name = symbol.split(':')[0]
        coin_base = display_name.replace('/', '').replace('USDT', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol=BITGET%3A{coin_base}USDT.P"

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
                f"• خاڵی RSI: `{round(rsi, 1)}`\n\n"
                f"📈 [بینینی تەواوی چارت لە TradingView]({tv_link})"
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
                f"• خاڵی RSI: `{round(rsi, 1)}`\n\n"
                f"📈 [بینینی تەواوی چارت لە TradingView]({tv_link})"
            )
            with open(chart_path, 'rb') as photo:
                bot.send_photo(target_chat, photo, caption=caption, parse_mode="Markdown")
            if os.path.exists(chart_path):
                os.remove(chart_path)
            last_signal_time[symbol] = time.time()

    except Exception as e:
        if force_send:
            bot.send_message(target_chat, f"❌ هەڵە: `{str(e)}`", parse_mode="Markdown")

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
    bot.reply_to(message, "🚀 *بۆتی سیگناڵ ئامادەیە!*\nبۆ وەرگرتنی وێنەی چارت فەرمانی `/test` بنێرە.", parse_mode="Markdown")

@bot.message_handler(commands=['test'])
def test_signal(message):
    threading.Thread(target=check_signal, args=('BTC/USDT:USDT', True, message.chat.id)).start()

if __name__ == "__main__":
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()
    bot.infinity_polling(timeout=10, long_polling_timeout=5)
