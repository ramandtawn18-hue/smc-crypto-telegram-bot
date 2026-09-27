import os
import requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

BYBIT_URL = "https://api.bybit.com/v5/market/kline"


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

    candles.reverse()

    return candles


def analyze_smc():
    candles = get_btc_data()

    if not isinstance(candles, list) or len(candles) < 50:
        return (
            "❌ نەتوانرا داتای BTC وەربگیرێت.\n\n"
            "تکایە دووبارە هەوڵ بدە."
        )

    closes = [float(c[4]) for c in candles]
    highs = [float(c[2]) for c in candles]
    lows = [float(c[3]) for c in candles]

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
            tp1 = entry + risk * 1.5
            tp2 = entry + risk * 2.5

    elif signal == "SELL":
        entry = current_price
        sl = recent_high
        risk = sl - entry

        if risk <= 0:
            signal = "WAIT"
        else:
            tp1 = entry - risk * 1.5
            tp2 = entry - risk * 2.5

    if signal == "WAIT":
        return (
            "📊 BTC/USDT — SMC ANALYSIS\n\n"
            f"💰 Price: ${current_price:,.2f}\n"
            f"🧠 Structure: {structure}\n\n"
            "⚪ Signal: WAIT\n"
            "⏳ No clear setup yet.\n\n"
            "⏱ Timeframe: 15m"
        )

    return (
        "🚨 BTC/USDT — SMC SIGNAL\n\n"
        f"📍 Signal: {signal}\n"
        f"💰 Entry: ${entry:,.2f}\n"
        f"🛑 Stop Loss: ${sl:,.2f}\n"
        f"🎯 TP1: ${tp1:,.2f}\n"
        f"🎯 TP2: ${tp2:,.2f}\n\n"
        f"🧠 Structure: {structure}\n"
        "💧 Liquidity: Recent swing levels\n"
        "⏱ Timeframe: 15m\n\n"
        "⚠️ Educational signal — not financial advice."
    )


def main():
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"

    response = requests.get(
        url,
        timeout=15
    )

    data = response.json()

    if not data.get("ok"):
        return

    updates = data.get("result", [])

    for update in updates:
        message = update.get("message", {})
        chat = message.get("chat", {})
        text = message.get("text", "")

        if not chat:
            continue

        chat_id = chat["id"]

        if text == "/start":
            send_message(
                chat_id,
                "🤖 SMC Crypto Bot\n\n"
                "بەخێربێیت!\n"
                "بۆتەکە ئامادەیە. ✅\n\n"
                "📊 /btc — شیکردنەوەی BTC\n"
                "📈 /signal — SMC Signal\n"
                "🟢 /status — بارودۆخی بۆت"
            )

        elif text == "/status":
            send_message(
                chat_id,
                "🟢 Bot Status: ONLINE\n"
                "📊 Market Analysis: ACTIVE\n"
                "🧠 SMC Engine: ACTIVE\n"
                "₿ BTC Analysis: ACTIVE"
            )

        elif text in ["/btc", "/signal"]:
            try:
                result = analyze_smc()
                send_message(
                    chat_id,
                    result
                )

            except Exception:
                send_message(
                    chat_id,
                    "❌ کێشەیەک ڕوویدا لە "
                    "وەرگرتنی داتای BTC.\n\n"
                    "تکایە دووبارە هەوڵ بدە."
                )


if __name__ == "__main__":
    main()
