import os
import time
import requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

KRAKEN_API = "https://api.kraken.com/0/public"

TIMEFRAME = 15
CANDLE_LIMIT = 100
MAX_PAIRS = 80
REQUEST_DELAY = 0.25


def send_message(chat_id, text):
    url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage"

    requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text
        },
        timeout=20
    )


def kraken_get(endpoint, params=None):
    response = requests.get(
        KRAKEN_API + endpoint,
        params=params or {},
        timeout=20
    )

    response.raise_for_status()

    data = response.json()

    if data.get("error"):
        raise Exception(str(data["error"]))

    return data.get("result", {})


def get_usd_pairs():
    result = kraken_get("/AssetPairs")

    pairs = []

    for key, info in result.items():
        altname = info.get("altname", key)
        quote = str(info.get("quote", "")).upper()
        status = str(info.get("status", "online")).lower()

        if status != "online":
            continue

        if quote not in ("ZUSD", "USD"):
            continue

        if ".d" in altname.lower():
            continue

        pairs.append(altname)

    pairs = list(dict.fromkeys(pairs))

    return pairs[:MAX_PAIRS]


def get_candles(pair):
    result = kraken_get(
        "/OHLC",
        {
            "pair": pair,
            "interval": TIMEFRAME
        }
    )

    candles = None

    for key in result:
        if key != "last":
            candles = result[key]
            break

    if not candles:
        raise Exception("No candle data for " + pair)

    return candles[-CANDLE_LIMIT:]


def analyze_pair(pair):
    candles = get_candles(pair)

    if len(candles) < 50:
        return None

    closes = []
    highs = []
    lows = []

    for candle in candles:
        closes.append(float(candle[4]))
        highs.append(float(candle[2]))
        lows.append(float(candle[3]))

    current_price = closes[-1]

    recent_high = max(highs[-21:-1])
    recent_low = min(lows[-21:-1])

    previous_high = max(highs[-41:-21])
    previous_low = min(lows[-41:-21])

    signal = "WAIT"
    structure = "RANGE"

    if current_price > recent_high:
        signal = "BUY"
        structure = "BULLISH BOS"

    elif current_price < recent_low:
        signal = "SELL"
        structure = "BEARISH BOS"

    elif recent_high > previous_high and recent_low > previous_low:
        signal = "BUY"
        structure = "BULLISH CHoCH"

    elif recent_high < previous_high and recent_low < previous_low:
        signal = "SELL"
        structure = "BEARISH CHoCH"

    if signal == "BUY":
        entry = current_price
        sl = recent_low
        risk = entry - sl

        if risk <= 0:
            return None

        tp1 = entry + (risk * 1.5)
        tp2 = entry + (risk * 2.5)

    elif signal == "SELL":
        entry = current_price
        sl = recent_high
        risk = sl - entry

        if risk <= 0:
            return None

        tp1 = entry - (risk * 1.5)
        tp2 = entry - (risk * 2.5)

    else:
        return None

    return {
        "pair": pair,
        "signal": signal,
        "entry": entry,
        "sl
