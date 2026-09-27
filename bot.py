import os
import time
import requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

API = "https://api.kraken.com/0/public"

TIMEFRAME = 15
LIMIT = 100
MAX_PAIRS = 50


def send(chat_id, text):
    requests.post(
        "https://api.telegram.org/bot" + TOKEN + "/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text
        },
        timeout=20
    )


def kraken(path, params=None):
    r = requests.get(
        API + path,
        params=params or {},
        timeout=20
    )

    r.raise_for_status()

    data = r.json()

    if data.get("error"):
        raise Exception(str(data["error"]))

    return data.get("result", {})


def get_pairs():
    result = kraken("/AssetPairs")

    out = []

    for key, info in result.items():

        name = info.get(
            "altname",
            key
        )

        quote = str(
            info.get("quote", "")
        ).upper()

        status = str(
            info.get("status", "online")
        ).lower()

        if status == "online":

            if quote in ("ZUSD", "USD"):

                if ".d" not in name.lower():

                    out.append(name)

    return list(
        dict.fromkeys(out)
    )[:MAX_PAIRS]


def get_candles(pair):

    result = kraken(
        "/OHLC",
        {
            "pair": pair,
            "interval": TIMEFRAME
        }
    )

    for key, value in result.items():

        if key != "last" and value:

            return value[-LIMIT:]

    raise Exception("No candle data")


def analyze(pair):

    c = get_candles(pair)

    if len(c) < 50:
        return None

    close = []
    high = []
    low = []

    for candle in c:

        close.append(float(candle[4]))
        high.append(float(candle[2]))
        low.append(float(candle[3]))

    current = close[-1]

    recent_high = max(
        high[-21:-1]
    )

    recent_low = min(
        low[-21:-1]
    )

    previous_high = max(
        high[-41:-21]
    )

    previous_low = min(
        low[-41:-21]
    )

    signal = "WAIT"
    structure = "RANGE"

    # LONG - Break of Structure
    if current > recent_high:

        signal = "LONG"
        structure = "BULLISH BOS"

    # SHORT - Break of Structure
    elif current < recent_low:

        signal = "SHORT"
        structure = "BEARISH BOS"

    # LONG - Change of Character
    elif (
        recent_high > previous_high
        and recent_low > previous_low
    ):

        signal = "LONG"
        structure = "BULLISH CHoCH"

    # SHORT - Change of Character
    elif (
        recent_high < previous_high
        and recent_low < previous_low
    ):

        signal = "SHORT"
        structure = "BEARISH CHoCH"

    if signal == "WAIT":
        return None

    # LONG setup
    if signal == "LONG":

        entry = current

        sl = recent_low

        risk = entry - sl

        if risk <= 0:
            return None

        tp1 = entry + risk * 1.5

        tp2 = entry + risk * 2.5

    # SHORT setup
