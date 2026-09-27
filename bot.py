import os
import requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

BYBIT_URL = "https://api.bybit.com/v5/market/kline"


# =========================
# Telegram
# =========================

def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

    requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text
        },
        timeout=15
    )


# =========================
# Get BTC Market Data
# =========================

def get_btc_data():

    params = {
        "category": "linear",
        "symbol": "BTCUSDT",
        "interval": "15",
        "limit": "100"
    }

    response = requests.get(
        BYBIT_URL,
        params=params,
        timeout=15
    )

    response.raise_for_status()

    data = response.json()

    if data.get("retCode") != 0:
        return []

    candles = data.get("result", {}).get("list", [])

    if not candles:
        return []

    # Bybit candles بە پێچەوانەی کات دێن
    # بۆیە دەیانگۆڕین بۆ کۆن → نوێ
    candles.reverse()

    return candles


# =========================
# SMC Analysis
# =========================

def analyze_smc():

    candles = get_btc_data()

    if not isinstance(candles, list) or len(candles) < 50:

        return (
            "❌ نەتوانرا داتای BTC وەربگیرێت.\n\n"
            "تکایە دووبارە هەوڵ بدە."
        )

    # Close
    closes = [
        float(c[4])
        for c in candles
    ]

    # High
    highs = [
        float(c[2])
        for c in candles
    ]

    # Low
    lows = [
        float(c[3])
        for c in candles
    ]

    current_price = closes[-1]

    # =========================
    # Recent Structure
    # =========================

    recent_high = max(
        highs[-21:-1]
    )

    recent_low = min(
        lows[-21:-1]
    )

    previous_high = max(
        highs[-41:-21]
    )

    previous_low = min(
        lows[-41:-21]
    )

    signal = "WAIT"
    structure = "RANGE"

    # =========================
    # BOS
    # =========================

    if current_price > recent_high:

        signal = "BUY"
        structure = "BULLISH BOS"

    elif current_price < recent_low:

        signal = "SELL"
        structure = "BEARISH BOS"

    # =========================
    # CHoCH
    # =========================

    elif (
        recent_high > previous_high
        and recent_low > previous_low
    ):

        structure = "BULLISH CHoCH"
        signal = "BUY"

    elif (
        recent_high < previous_high
        and recent_low < previous_low
    ):

        structure = "BEARISH CHoCH"
        signal = "SELL"


    # =========================
    # BUY
    # =========================

    if signal == "BUY":

        entry = current_price

        sl = recent_low

        risk = entry - sl

        if risk <= 0:

            signal = "WAIT"

        else:

            tp1 = entry + (risk * 1.5)

            tp2 = entry + (risk * 2.5)


    # =========================
    # SELL
    # =========================

    elif signal == "SELL":

        entry = current_price

        sl = recent_high

        risk = sl - entry

        if risk <= 0:

            signal = "WAIT"

        else:

            tp1 = entry - (risk * 1.5)

            tp2 = entry - (risk * 2.5)


    # =========================
    # WAIT
    # =========================

    if signal == "WAIT":

        return (

            "📊 BTC/USDT — SMC ANALYSIS\n\n"

            f"💰 Price: ${current_price:,.2f}\n"

            f"🧠 Structure: {structure}\n\n"

            "⚪ Signal: WAIT\n"

            "⏳ No clear setup yet.\n\n"

            "⏱ Timeframe: 15m"

        )


    # =========================
    # SIGNAL
    # =========================

    return (

        "🚨 BTC/USDT — SMC SIGNAL\n\n"

        f"📍 Signal: {signal}\n"

        f"💰 Entry: ${entry:,.2f}\n"

        f"🛑 Stop Loss: ${sl:,.2f}\n"

        f"🎯 TP1: ${tp1:,.2f}\n"

        f"🎯 TP2: ${tp2:,.2f}\n\n"
