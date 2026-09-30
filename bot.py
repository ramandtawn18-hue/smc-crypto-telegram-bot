import os
import time
import requests
import ccxt
import pandas as pd
import pandas_ta as ta
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')  # بۆ کارکردن لەسەر سێرڤەر بەبێ GUI

# --- ڕێکخستنەکانی تەلەگرام ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

# پەیوەندی بە بازاڕی فیووچەرزی Bitget (USDT-M Futures)
exchange = ccxt.bitget({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap'
    }
})

TIMEFRAME = '15m'
CANDLE_LIMIT = 100
last_signal_time = {}

def send_telegram_photo(photo_path, caption):
    """ناردنی وێنەی چارت لەگەڵ دەقی شیکارییەکە"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, 'rb') as photo:
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "caption": caption,
                "parse_mode": "Markdown"
            }
            files = {"photo": photo}
            response = requests.post(url, data=payload, files=files, timeout=20)
            return response.json()
    except Exception as e:
        print(f"Error sending photo to Telegram: {e}")
    finally:
        # سڕینەوەی وێنەکە لە سێرڤەر بۆ ئەوەی جێگا نەگرێت
        if os.path.exists(photo_path):
            os.remove(photo_path)

def calculate_leverage(entry_price, stop_loss):
    """هەژمارکردنی لیڤەرەیجی گونجاو بەپێی مەودای ستۆپ لۆس"""
    risk_pct = abs(entry_price - stop_loss) / entry_price * 100
    if risk_pct <= 1.2:
        return "10x - 12x"
    elif risk_pct <= 2.5:
        return "5x - 7x"
    else:
        return "3x - 5x"

def get_all_futures_symbols():
    """وەرگرتنی تەواوی دراوەکانی بازاڕی فیووچەرزی Bitget"""
    try:
        markets = exchange.load_markets()
        symbols = [
            s for s, m in markets.items()
            if m.get('quote') == 'USDT' and m.get('active', True) and m.get('swap', True)
        ]
        return symbols
    except Exception as e:
        print(f"Error loading Bitget markets: {e}")
        return []

def plot_and_save_chart(df, symbol, entry, sl, tp, signal_type):
    """کێشانی مۆمەکان و ئاستەکانی TP / SL لەسەر وێنە"""
    filename = f"chart_{symbol.replace('/', '_').replace(':', '_')}_{int(time.time())}.png"
    
    # ئامادەکردنی داتاکان بۆ mplfinance
    plot_df = df.tail(60).copy()
    plot_df['timestamp'] = pd.to_datetime(plot_df['timestamp'], unit='ms')
    plot_df.set_index('timestamp', inplace=True)
    
    # هێڵەکان بۆ ئیندیکەیتەرەکان و ئاستەکان
    apds = [
        mpf.make_addplot(plot_df['EMA_50'], color='cyan', width=1.2),
        mpf.make_addplot(plot_df['EMA_200'], color='orange', width=1.5),
    ]

    h_lines = dict(hlines=[entry, sl, tp], colors=['blue', 'red', 'green'], linestyle='--', widths=1.2)

    custom_style = mpf.make_mpf_style(
        base_mpf_style='nightclouds',
        rc={'font.size': 8}
    )

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
        
        # ئیندیکەیتەرەکان
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

        # ----------------- مەرجی کڕین (LONG) -----------------
        # 1. ترێندی بەرزبوونەوە: EMA 50 لەسەروو EMA 200 و نرخ لەسەرووی هەردووکیان
        # 2. RSI گەڕانەوە لە کاتی Pullback (لەژێر 42 بێت و بەرەو سەرەوە وەرگەڕێتەوە)
        # 3. قەبارەی بازرگانی لە ئاستی ئاسایی زیاتر بێت
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
                f"📍 *نرخی چوونەژوور (Entry):* `{close}`\n"
                f"🎯 *تارگێتی یەکەم (TP1):* `{tp1}`\n"
                f"🎯 *تارگێتی دووەم (TP2):* `{tp2}`\n"
                f"🛑 *ستۆپ لۆس (SL):* `{sl}`\n"
                f"⚡️ *لیڤەرەیجی پێشنیارکراو:* `{leverage}`\n\n"
                f"📊 *شیکاری تەکنیکی:*\n"
                f"• ترێندی سەروو: `EMA50 > EMA200`\n"
                f"• خاڵی هەڵگەڕانەوەی RSI: `{round(rsi, 1)}`\n"
                f"• پشکنینی نەختینە: `Volume > Average`"
            )
            send_telegram_photo(chart_path, caption)
            last_signal_time[symbol] = current_time
            print(f"Signal Alert: {display_name} LONG")
            time.sleep(2)

        # ----------------- مەرجی فرۆشتن (SHORT) -----------------
        # 1. ترێندی دابەزین: EMA 50 لەژێر EMA 200 و نرخ لەژێر هەردووکیان
        # 2. RSI گەڕانەوە لە کاتی بەرزبوونەوەی کاتی (لەسەرووی 58 بێت و داببەزێت)
        # 3. قەبارەی بازرگانی بەرز بێت
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
                f"📍 *نرخی چوونەژوور (Entry):* `{close}`\n"
                f"🎯 *تارگێتی یەکەم (TP1):* `{tp1}`\n"
                f"🎯 *تارگێتی دووەم (TP2):* `{tp2}`\n"
                f"🛑 *ستۆپ لۆس (SL):* `{sl}`\n"
                f"⚡️ *لیڤەرەیجی پێشنیارکراو:* `{leverage}`\n\n"
                f"📊 *شیکاری تەکنیکی:*\n"
                f"• ترێندی خواروو: `EMA50 < EMA200`\n"
                f"• خاڵی هەڵگەڕانەوەی RSI: `{round(rsi, 1)}`\n"
                f"• پشکنینی نەختینە: `Volume > Average`"
            )
            send_telegram_photo(chart_path, caption)
            last_signal_time[symbol] = current_time
            print(f"Signal Alert: {display_name} SHORT")
            time.sleep(2)

    except Exception as e:
        # هەڵەی کاتی فەچکردنی هەندێک جووتە دراو
        pass

def main():
    print("بۆت دەستی بە کارکردن کرد...")
    while True:
        try:
            symbols = get_all_futures_symbols()
            print(f"[{time.strftime('%H:%M:%S')}] پشکنینی {len(symbols)} دراوی بازاڕی فیووچەرز...")
            
            for symbol in symbols:
                # ڕێگری لەوەی هەمان دراو لە ماوەی کەمتر لە کاتژمێرێکدا دووبارە ببێتەوە
                if symbol in last_signal_time and (time.time() - last_signal_time[symbol]) < 3600:
                    continue

                analyze_symbol(symbol)
                time.sleep(0.15)  # پاراستنی داواکارییەکان لە لیمیت بوون

            # دوای تەواوبوونی گشت بازاڕەکە، 3 خولەک چاوەڕێ دەکات
            time.sleep(180)
        except Exception as e:
            print(f"Global Loop Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
