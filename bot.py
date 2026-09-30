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

# کۆگای دۆخی سکانەر و سیگناڵەکان
active_signals = {}
last_signal_time = {}
scanner_stats = {
    "last_scan_time": "هێشتا دەستی پێنەکردووە",
    "scanned_count": 0,
    "total_symbols": 0,
    "is_running": True
}

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

def plot_and_save_chart(df, symbol, entry, sl, tp1, tp2, tp3, signal_type):
    filename = f"tv_chart_{int(time.time()*1000)}.png"
    
    plot_df = df.tail(60).copy()
    plot_df['timestamp'] = pd.to_datetime(plot_df['timestamp'], unit='ms')
    plot_df.set_index('timestamp', inplace=True)
    
    marketcolors = mpf.make_marketcolors(
        up='#089981',
        down='#F23645',
        edge={'up': '#089981', 'down': '#F23645'},
        wick={'up': '#089981', 'down': '#F23645'}
    )
    
    tv_style = mpf.make_mpf_style(
        marketcolors=marketcolors,
        facecolor='#FFFFFF',
        edgecolor='#E0E3EB',
        figcolor='#FFFFFF',
        gridcolor='#F0F3FA',
        gridstyle='--',
        rc={
            'text.color': '#131722',
            'axes.labelcolor': '#787B86',
            'xtick.color': '#787B86',
            'ytick.color': '#787B86',
            'font.family': 'sans-serif',
            'font.size': 10
        }
    )

    clean_symbol = symbol.replace('/', '').replace(':USDT', '') + 'PERP'
    last_price = plot_df['close'].iloc[-1]
    title_text = f"{clean_symbol} · 15m · Bitget  ({last_price})"

    h_lines = dict(
        hlines=[tp3, tp2, tp1, entry, sl],
        colors=['#056656', '#089981', '#26a69a', '#2962FF', '#F23645'],
        linestyle='--',
        linewidths=1.3
    )

    fig, axlist = mpf.plot(
        plot_df,
        type='candle',
        volume=False,
        hlines=h_lines,
        style=tv_style,
        title=f"\n{title_text}\nSignal: {signal_type}",
        returnfig=True,
        figsize=(16, 9),
        savefig=dict(fname=filename, dpi=140, bbox_inches='tight')
    )

    ax = axlist[0]
    xmin, xmax = ax.get_xlim()
    text_x = xmin + (xmax - xmin) * 0.015

    levels = [
        (tp3, f"TP3: {tp3}", '#056656'),
        (tp2, f"TP2: {tp2}", '#089981'),
        (tp1, f"TP1: {tp1}", '#26a69a'),
        (entry, f"Entry: {entry}", '#2962FF'),
        (sl, f"SL: {sl}", '#F23645')
    ]

    for price_val, label, col in levels:
        ax.text(text_x, price_val, f"  {label}  ", color='white',
                fontsize=9, weight='bold', verticalalignment='center',
                bbox=dict(boxstyle='round,pad=0.25', facecolor=col, edgecolor='none', alpha=0.9))

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
            tp1 = round(close + (atr * 1.5), 4)
            tp2 = round(close + (atr * 2.5), 4)
            tp3 = round(close + (atr * 4.0), 4)
            leverage = calculate_leverage(close, sl)

            chart_path = plot_and_save_chart(df, display_name, close, sl, tp1, tp2, tp3, "LONG")
            caption = (
                f"🟢 *سیگناڵی کڕین (LONG)*\n\n"
                f"🪙 *دراو:* `{display_name}` (Bitget Futures)\n"
                f"⏱ *تایم‌فرەیم:* `15m`\n"
                f"📍 *نرخی چوونەژوور:* `{close}`\n"
                f"🎯 *تارگێتی یەکەم (TP1):* `{tp1}`\n"
                f"🎯 *تارگێتی دووەم (TP2):* `{tp2}`\n"
                f"🎯 *تارگێتی سێیەم (TP3):* `{tp3}`\n"
                f"🛑 *ستۆپ لۆس (SL):* `{sl}`\n"
                f"⚡️ *لیڤەرەیج:* `{leverage}`\n\n"
                f"📊 *شیکاری:*\n"
                f"• ترێند: `Bullish Trend`\n"
                f"• خاڵی RSI: `{round(rsi, 1)}`\n\n"
                f"📈 [بینینی تەواوی چارت لە TradingView]({tv_link})"
            )
            with open(chart_path, 'rb') as photo:
                msg = bot.send_photo(target_chat, photo, caption=caption, parse_mode="Markdown")
            
            if os.path.exists(chart_path):
                os.remove(chart_path)
            
            active_signals[symbol] = {
                'type': 'LONG',
                'tp1': tp1,
                'message_id': msg.message_id,
                'chat_id': target_chat,
                'name': display_name
            }
            last_signal_time[symbol] = time.time()

        elif is_short:
            sl = round(close + (atr * 1.5), 4)
            tp1 = round(close - (atr * 1.5), 4)
            tp2 = round(close - (atr * 2.5), 4)
            tp3 = round(close - (atr * 4.0), 4)
            leverage = calculate_leverage(close, sl)

            chart_path = plot_and_save_chart(df, display_name, close, sl, tp1, tp2, tp3, "SHORT")
            caption = (
                f"🔴 *سیگناڵی فرۆشتن (SHORT)*\n\n"
                f"🪙 *دراو:* `{display_name}` (Bitget Futures)\n"
                f"⏱ *تایم‌فرەیم:* `15m`\n"
                f"📍 *نرخی چوونەژوور:* `{close}`\n"
                f"🎯 *تارگێتی یەکەم (TP1):* `{tp1}`\n"
                f"🎯 *تارگێتی دووەم (TP2):* `{tp2}`\n"
                f"🎯 *تارگێتی سێیەم (TP3):* `{tp3}`\n"
                f"🛑 *ستۆپ لۆس (SL):* `{sl}`\n"
                f"⚡️ *لیڤەرەیج:* `{leverage}`\n\n"
                f"📊 *شیکاری:*\n"
                f"• ترێند: `Bearish Trend`\n"
                f"• خاڵی RSI: `{round(rsi, 1)}`\n\n"
                f"📈 [بینینی تەواوی چارت لە TradingView]({tv_link})"
            )
            with open(chart_path, 'rb') as photo:
                msg = bot.send_photo(target_chat, photo, caption=caption, parse_mode="Markdown")
            
            if os.path.exists(chart_path):
                os.remove(chart_path)

            active_signals[symbol] = {
                'type': 'SHORT',
                'tp1': tp1,
                'message_id': msg.message_id,
                'chat_id': target_chat,
                'name': display_name
            }
            last_signal_time[symbol] = time.time()

    except Exception as e:
        if force_send:
            bot.send_message(target_chat, f"❌ هەڵە: `{str(e)}`", parse_mode="Markdown")

def tp_monitoring_loop():
    while True:
        try:
            if active_signals:
                for symbol, data in list(active_signals.items()):
                    ticker = exchange.fetch_ticker(symbol)
                    current_price = float(ticker['last'])

                    hit = False
                    if data['type'] == 'LONG' and current_price >= data['tp1']:
                        hit = True
                    elif data['type'] == 'SHORT' and current_price <= data['tp1']:
                        hit = True

                    if hit:
                        hit_msg = (
                            f"🎯 *TP1 HIT! ✅*\n\n"
                            f"🪙 دراو: `{data['name']}`\n"
                            f"💵 نرخی پێکراو: `{data['tp1']}`\n"
                            f"✨ قازانجی تارگێتی یەکەم بە سەرکەوتوویی مسۆگەر کرا!"
                        )
                        bot.send_message(
                            data['chat_id'],
                            hit_msg,
                            reply_to_message_id=data['message_id'],
                            parse_mode="Markdown"
                        )
                        del active_signals[symbol]

                    time.sleep(0.5)

            time.sleep(10)
        except Exception:
            time.sleep(10)

def scanner_loop():
    global scanner_stats
    while True:
        try:
            symbols = get_all_futures_symbols()
            scanner_stats["total_symbols"] = len(symbols)
            scanned = 0
            
            for symbol in symbols:
                if symbol in last_signal_time and (time.time() - last_signal_time[symbol]) < 2400:
                    continue
                check_signal(symbol)
                scanned += 1
                scanner_stats["scanned_count"] = scanned
                scanner_stats["last_scan_time"] = time.strftime('%H:%M:%S')
                time.sleep(0.15)
                
            time.sleep(60)
        except Exception as e:
            time.sleep(20)

# --- فەرمانەکانی تەلەگرام ---

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(
        message, 
        "🚀 *بۆتی سیگناڵی Bitget بە تەواوی ئامادەیە!*\n\n"
        "• بۆ بینینی دۆخی پشکنین فەرمانی `/status` یان `/scan` بنێرە.\n"
        "• بۆ تاقیکردنەوەی چارت بە ڕێژەی 16:9 فەرمانی `/test` بنێرە.", 
        parse_mode="Markdown"
    )

@bot.message_handler(commands=['scan', 'status'])
def send_status(message):
    status_text = (
        "📊 *ڕاپۆرتی دۆخی بۆت (Live Status)*\n\n"
        f"🟢 دۆخی سێرڤەر: `Online & Active`\n"
        f"⏱ تایم‌فرەیم: `15m`\n"
        f"🪙 کۆی گشتی دراوەکانی Bitget: `{scanner_stats['total_symbols']}`\n"
        f"🔍 دراوە پشکنراوەکانی ئەم خولە: `{scanner_stats['scanned_count']}`\n"
        f"🕒 دوایین پشکنین: `{scanner_stats['last_scan_time']}`\n"
        f"🎯 سیگناڵە چالاکەکان بۆ چاودێری TP1: `{len(active_signals)}`\n\n"
        "⚡️ _سکانەر لە پاشبنەما بەردەوامە و هەرکات مەرجەکان پڕبوونەوە سیگناڵ دەنێرێت._"
    )
    bot.reply_to(message, status_text, parse_mode="Markdown")

@bot.message_handler(commands=['test'])
def test_signal(message):
    bot.reply_to(message, "⏳ خەریکی ئامادەکردنی وێنەی چارت بە دیزاینی 16:9م...")
    threading.Thread(target=check_signal, args=('BTC/USDT:USDT', True, message.chat.id)).start()

if __name__ == "__main__":
    t_scan = threading.Thread(target=scanner_loop, daemon=True)
    t_scan.start()

    t_tp = threading.Thread(target=tp_monitoring_loop, daemon=True)
    t_tp.start()

    bot.infinity_polling(timeout=10, long_polling_timeout=5)


