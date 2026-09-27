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
        raise Exception(
            str(data["error"])
        )

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

    raise Exception(
        "No candle data"
    )


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

    if current > recent_high:

        signal = "BUY"
        structure = "BULLISH BOS"

    elif current < recent_low:

        signal = "SELL"
        structure = "BEARISH BOS"

    elif (
        recent_high > previous_high
        and recent_low > previous_low
    ):

        signal = "BUY"
        structure = "BULLISH CHoCH"

    elif (
        recent_high < previous_high
        and recent_low < previous_low
    ):

        signal = "SELL"
        structure = "BEARISH CHoCH"

    if signal == "WAIT":
        return None

    if signal == "BUY":

        entry = current
        sl = recent_low
        risk = entry - sl

        if risk <= 0:
            return None

        tp1 = entry + risk * 1.5
        tp2 = entry + risk * 2.5

    else:

        entry = current
        sl = recent_high
        risk = sl - entry

        if risk <= 0:
            return None

        tp1 = entry - risk * 1.5
        tp2 = entry - risk * 2.5

    return (
        pair,
        signal,
        entry,
        sl,
        tp1,
        tp2,
        structure
    )


def format_price(value):

    if value >= 1000:
        return format(
            value,
            ",.2f"
        )

    if value >= 1:
        return format(
            value,
            ".4f"
        )

    return format(
        value,
        ".8f"
    )


def scan(chat_id):

    send(
        chat_id,
        "SMC scan started...\n"
        "Market: Kraken USD pairs\n"
        "Timeframe: 15m"
    )

    try:

        all_pairs = get_pairs()
        signals = []

        for i, pair in enumerate(all_pairs):

            try:

                result = analyze(pair)

                if result:
                    signals.append(result)

            except Exception:
                pass

            if i < len(all_pairs) - 1:
                time.sleep(0.3)

        if not signals:

            send(
                chat_id,
                "SMC SCAN\n\n"
                "Pairs checked: "
                + str(len(all_pairs))
                + "\n"
                "No clear BUY/SELL setup right now."
            )

            return

        text = "SMC SIGNALS\n\n"

        for s in signals:

            block = (
                "PAIR: "
                + s[0]
                + "\n"
                + "Signal: "
                + s[1]
                + "\n"
                + "Entry: $"
                + format_price(s[2])
                + "\n"
                + "Stop Loss: $"
                + format_price(s[3])
                + "\n"
                + "TP1: $"
                + format_price(s[4])
                + "\n"
                + "TP2: $"
                + format_price(s[5])
                + "\n"
                + "Structure: "
                + s[6]
                + "\n"
                + "Timeframe: 15m\n\n"
            )

            if len(text) + len(block) > 3500:

                send(
                    chat_id,
                    text
                )

                text = (
                    "SMC SIGNALS - continued\n\n"
                )

            text += block

        send(
            chat_id,
            text
        )

    except Exception as e:

        send(
            chat_id,
            "Scanner error: "
            + type(e).__name__
            + ": "
            + str(e)
        )


def main():

    if not TOKEN:
        return

    r = requests.get(
        "https://api.telegram.org/bot"
        + TOKEN
        + "/getUpdates",
        timeout=20
    )

    r.raise_for_status()

    data = r.json()

    for update in data.get(
        "result",
        []
    ):

        message = update.get(
            "message",
            {}
        )

        chat = message.get(
            "chat",
            {}
        )

        text = message.get(
            "text",
            ""
        )

        if not chat:
            continue

        chat_id = chat["id"]

        if text == "/start":

            send(
                chat_id,
                "SMC Crypto Signals\n\n"
                "/scan - scan USD crypto pairs\n"
                "/status - bot status"
            )

        elif text == "/status":

            send(
                chat_id,
                "Bot Status: ONLINE\n"
                "Multi-Coin Scanner: ACTIVE\n"
                "Timeframe: 15m"
            )

        elif text == "/scan":

            scan(chat_id)


if __name__ == "__main__":
    main()
