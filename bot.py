import os
import requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

BYBIT_URL = "https://api.bybit.com/v5/market/kline"


# =========================
# SEND TELEGRAM MESSAGE
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
# GET BTC DATA FROM BYBIT
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
        raise Exception(
            f"Bybit error: {data.get('retMsg', 'Unknown error')}"
        )

    candles = data.get(
        "result",
        {}
    ).get(
        "list",
        []
    )

    if not candles:
        raise Exception(
            "Bybit returned empty candle data"
        )

    # Bybit data comes newest first.
    # Reverse it to oldest -> newest.
    candles.reverse()

    return candles


# =========================
# SMC ANALYSIS
# =========================

def analyze_smc():

    candles = get_btc_data()

    if len(candles) < 50:
        raise Exception(
            f"Not enough BTC candles: {len(candles)}"
        )

    closes = [
        float(candle[4])
        for candle in candles
    ]

    highs = [
        float(candle[2])
        for candle in candles
    ]

    lows = [
        float(candle[3])
        for candle in candles
    ]

    current_price = closes[-1]

    # =========================
    # MARKET STRUCTURE
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
    # BULLISH BOS
    # =========================

    if current_price > recent_high:

        signal = "BUY"
        structure = "BULLISH BOS"

    # =========================
    # BEARISH BOS
    # =========================

    elif current_price < recent_low:

        signal = "SELL"
        structure = "BEARISH BOS"

    # =========================
    # BULLISH CHoCH
    # =========================

    elif (
        recent_high > previous_high
        and recent_low > previous_low
    ):

        signal = "BUY"
        structure = "BULLISH CHoCH"

    # =========================
    # BEARISH CHoCH
    # =========================

    elif (
        recent_high < previous_high
        and recent_low < previous_low
    ):

        signal = "SELL"
        structure = "BEARISH CHoCH"


    # =========================
    # BUY CALCULATION
    # =========================

    if signal == "BUY":

        entry = current_price

        sl = recent_low

        risk = entry - sl

        if risk <= 0:

            signal = "WAIT"

        else:

            tp1 = entry + (
                risk * 1.5
            )

            tp2 = entry + (
                risk * 2.5
            )


    # =========================
    # SELL CALCULATION
    # =========================

    elif signal == "SELL":

        entry = current_price

        sl = recent_high

        risk = sl - entry

        if risk <= 0:

            signal = "WAIT"

        else:

            tp1 = entry - (
                risk * 1.5
            )

            tp2 = entry - (
                risk * 2.5
            )


    # =========================
    # WAIT MESSAGE
    # =========================

    if signal == "WAIT":

        return (
            "📊 BTC/USDT — SMC ANALYSIS\n\n"
            f"💰 Price: ${current_price:,.2f}\n"
            f"🧠 Structure: {structure}\n\n"
            "⚪
