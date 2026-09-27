import os
import requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

KRAKEN_URL = "https://api.kraken.com/0/public/OHLC"


def send_message(chat_id, text):
    url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage"

    requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text
        },
        timeout=15
    )


def get_btc_data():

    params = {
        "pair": "XBTUSD",
        "interval": "15"
    }

    response = requests.get(
        KRAKEN_URL,
        params=params,
        timeout=15
    )

    response.raise_for_status()

    data = response.json()

    if data.get("error"):
        raise Exception(
            str(data.get("error"))
        )

    result = data.get("result", {})

    candles = None

    for key in result:
        if key != "last":
            candles = result[key]
            break

    if not candles:
        raise Exception(
            "Kraken returned empty BTC data"
        )

    return candles


def analyze_smc():

    candles = get_btc_data()

    if len(candles) < 50:
        raise Exception(
            "Not enough BTC candle data"
        )

    closes = []
    highs = []
    lows = []

    for candle in candles:

        closes.append(
            float(candle[4])
        )

        highs.append(
            float(candle[2])
        )

        lows.append(
            float(candle[3])
        )

    current_price = closes[-1]

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

    if current_price > recent_high:

        signal = "BUY"
        structure = "BULLISH BOS"

    elif current_price < recent_low:

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

    if signal == "BUY":

        entry = current_price
        sl = recent_low
        risk = entry - sl

        if risk <= 0:
            signal = "WAIT"

        else:
            tp1 = entry + (risk * 1.5)
            tp2 = entry + (risk * 2.5)

    elif signal == "SELL":

        entry = current_price
        sl = recent_high
        risk = sl - entry

        if risk <= 0:
            signal = "WAIT"

        else:
            tp1 = entry - (risk * 1.5)
            tp2 = entry - (risk * 2.5)

    if signal == "WAIT":

        return (
            "BTC/USD - SMC ANALYSIS\n\n"
            + "Price: $"
            + format(current_price, ",.2f")
            + "\n"
            + "Structure: "
            + structure
            + "\n\n"
            + "Signal: WAIT\n"
            + "No clear setup yet.\n\n"
            + "Timeframe: 15m"
        )

    return (
        "BTC/USD - SMC SIGNAL\n\n"
        + "Signal: "
        + signal
        + "\n"
        + "Entry: $"
        + format(entry, ",.2f")
        + "\n"
        + "Stop Loss: $"
        + format(sl, ",.2f")
        + "\n"
        + "TP1: $"
        + format(tp1, ",.2f")
        + "\n"
        + "TP2: $"
        + format(tp2, ",.2f")
        + "\n\n"
        + "Structure: "
        + structure
        + "\n"
        + "Liquidity: Recent swing levels\n"
        + "Timeframe: 15m\n\n"
        + "Educational signal - not financial advice."
    )


def main():

    url = (
        "https://api.telegram.org/bot"
        + TOKEN
        + "/getUpdates"
    )

    response = requests.get(
        url,
        timeout=15
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("ok"):
        return

    updates = data.get(
        "result",
        []
    )

    for update in updates:

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

            send_message(
                chat_id,
                "SMC Crypto Bot\n\n"
                "بەخێربێیت!\n"
                "بۆتەکە ئامادەیە.\n\n"
                "/btc - شیکردنەوەی BTC\n"
                "/signal - SMC Signal\n"
                "/status - بارودۆخی بۆت"
            )

        elif text == "/status":

            send_message(
                chat_id,
                "Bot Status: ONLINE\n"
                "Market Analysis: ACTIVE\n"
                "SMC Engine: ACTIVE\n"
                "BTC Analysis: ACTIVE"
            )

        elif text == "/btc" or text == "/signal":

            try:

                result = analyze_smc()

                send_message(
                    chat_id,
                    result
                )

            except Exception as error:

                error_message = (
                    "BTC data error\n\n"
                    + type(error).__name__
                    + ": "
                    + str(error)
                )

                send_message(
                    chat_id,
                    error_message
                )


if __name__ == "__main__":
    main()
