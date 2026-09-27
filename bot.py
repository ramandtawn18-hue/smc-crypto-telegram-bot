import os
import time
import threading
import requests
from flask import Flask, jsonify


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

API = "https://api.kraken.com/0/public"

TIMEFRAME = 15
LIMIT = 100
MAX_PAIRS = 50

# Market scan every 5 minutes
SCAN_INTERVAL = 300

# Send maximum one signal every minute
SEND_INTERVAL = 60


app = Flask(__name__)


stop_event = threading.Event()
force_scan_event = threading.Event()

state_lock = threading.Lock()

scanner_thread = None
scanner_running = False

active_chat_id = None

pending_signals = []

seen_signals = set()
seen_order = []

next_send_at = 0


# =========================================================
# WEB SERVER
# =========================================================

@app.get("/")
def home():

    return "SMC Crypto Bot is running."


@app.get("/health")
def health():

    return jsonify(
        {
            "status": "ok",
            "scanner_running": scanner_running
        }
    )


# =========================================================
# TELEGRAM
# =========================================================

def send(chat_id, text):

    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing"
        )

    r = requests.post(

        "https://api.telegram.org/bot"
        + TOKEN
        + "/sendMessage",

        json={
            "chat_id": chat_id,
            "text": text
        },

        timeout=20
    )

    r.raise_for_status()


# =========================================================
# KRAKEN
# =========================================================

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

    return data.get(
        "result",
        {}
    )


# =========================================================
# GET USD PAIRS
# =========================================================

def get_pairs():

    result = kraken(
        "/AssetPairs"
    )

    out = []

    for key, info in result.items():

        name = info.get(
            "altname",
            key
        )

        quote = str(
            info.get(
                "quote",
                ""
            )
        ).upper()

        status = str(
            info.get(
                "status",
                "online"
            )
        ).lower()

        if status == "online":

            if quote in (
                "ZUSD",
                "USD"
            ):

                if ".d" not in name.lower():

                    out.append(
                        name
                    )

    return list(
        dict.fromkeys(out)
    )[:MAX_PAIRS]


# =========================================================
# GET CANDLES
# =========================================================

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


# =========================================================
# ANALYZE
# =========================================================

def analyze(pair):

    candles = get_candles(
        pair
    )

    if len(candles) < 50:

        return None

    close = []
    high = []
    low = []

    for candle in candles:

        close.append(
            float(candle[4])
        )

        high.append(
            float(candle[2])
        )

        low.append(
            float(candle[3])
        )

    current = close[-1]

    candle_time = int(
        candles[-1][0]
    )

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


    # =========================
    # BOS
    # =========================

    if current > recent_high:

        signal = "LONG"

        structure = "BULLISH BOS"


    elif current < recent_low:

        signal = "SHORT"

        structure = "BEARISH BOS"


    # =========================
    # CHoCH
    # =========================

    elif (
        recent_high > previous_high
        and
        recent_low > previous_low
    ):

        signal = "LONG"

        structure = "BULLISH CHoCH"


    elif (
        recent_high < previous_high
        and
        recent_low < previous_low
    ):

        signal = "SHORT"

        structure = "BEARISH CHoCH"


    if signal == "WAIT":

        return None


    # =====================================================
    # LONG
    # =====================================================

    if signal == "LONG":

        entry = current

        sl = recent_low

        risk = entry - sl

        if risk <= 0:

            return None

        tp1 = entry + (
            risk * 1.5
        )

        tp2 = entry + (
            risk * 2.5
        )


    # =====================================================
    # SHORT
    # =====================================================

    else:

        entry = current

        sl = recent_high

        risk = sl - entry

        if risk <= 0:

            return None

        tp1 = entry - (
            risk * 1.5
        )

        tp2 = entry - (
            risk * 2.5
        )


    return (

        pair,

        signal,

        entry,

        sl,

        tp1,

        tp2,

        structure,

        candle_time

    )


# =========================================================
# PRICE FORMAT
# =========================================================

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


# =========================================================
# DUPLICATE FILTER
# =========================================================

def remember_signal(key):

    if key in seen_signals:

        return False

    seen_signals.add(
        key
    )

    seen_order.append(
        key
    )

    if len(seen_order) > 5000:

        old = seen_order.pop(
            0
        )

        seen_signals.discard(
            old
        )

    return True


# =========================================================
# SCAN ONCE
# =========================================================

def scan_once():

    pairs = get_pairs()

    new_signals = []

    for i, pair in enumerate(pairs):

        try:

            result = analyze(
                pair
            )

            if result:

                # One signal per pair per candle
                key = (
                    result[0],
                    result[7]
                )

                if remember_signal(
                    key
                ):

                    new_signals.append(
                        result
                    )

        except Exception:

            pass


        if i < len(pairs) - 1:

            time.sleep(
                0.3
            )


    return (
        new_signals,
        len(pairs)
    )


# =========================================================
# SIGNAL MESSAGE
# =========================================================

def signal_message(s):

    if s[1] == "LONG":

        emoji = "🟢"

    else:

        emoji = "🔴"


    return (

        emoji
        + " "
        + s[1]
        + "\n\n"

        + "PAIR: "
        + s[0]
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

        + "Timeframe: 15m"

    )


# =========================================================
# SCANNER LOOP
# =========================================================

def scanner_loop(chat_id):

    global scanner_running
    global next_send_at


    next_scan_at = 0


    try:

        while not stop_event.is_set():

            now = time.time()


            # =============================================
            # SCAN
            # =============================================

            if (
                force_scan_event.is_set()
                or
                now >= next_scan_at
            ):

                force_scan_event.clear()


                try:

                    new_signals, checked = scan_once()


                    with state_lock:

                        pending_signals.extend(
                            new_signals
                        )


                    if not new_signals:

                        send(

                            chat_id,

                            "SCAN COMPLETE\n\n"

                            "Pairs checked: "
                            + str(checked)
                            + "\n"

                            "No new LONG/SHORT setup."

                        )


                except Exception as e:

                    send(

                        chat_id,

                        "SCANNER ERROR\n"

                        + type(e).__name__
                        + ": "
                        + str(e)

                    )


                next_scan_at = (
                    time.time()
                    + SCAN_INTERVAL
                )


            # =============================================
            # SEND ONE SIGNAL
            # =============================================

            now = time.time()


            with state_lock:

                has_pending = bool(
                    pending_signals
                )


            if (
                has_pending
                and
                now >= next_send_at
            ):


                with state_lock:

                    if pending_signals:

                        signal = (
                            pending_signals.pop(
                                0
                            )
                        )

                    else:

                        signal = None


                if signal:

                    try:

                        send(

                            chat_id,

                            signal_message(
                                signal
                            )

                        )

                        next_send_at = (
                            time.time()
                            + SEND_INTERVAL
                        )


                    except Exception as e:

                        with state_lock:

                            pending_signals.insert(
                                0,
                                signal
                            )


                        try:

                            send(

                                chat_id,

                                "SEND ERROR\n"
                                + type(e).__name__
                                + ": "
                                + str(e)

                            )

                        except Exception:

                            pass


                        next_send_at = (
                            time.time()
                            + 30
                        )


            stop_event.wait(
                1
            )


    finally:

        scanner_running = False


# =========================================================
# START SCANNER
# =========================================================

def start_scanner(chat_id):

    global scanner_thread
    global scanner_running
    global active_chat_id
    global next_send_at


    with state_lock:

        active_chat_id = chat_id


    if (
        scanner_thread
        and
        scanner_thread.is_alive()
    ):

        # Already running:
        # request an immediate new scan

        force_scan_event.set()

        return False


    stop_event.clear()

    force_scan_event.set()

    next_send_at = 0


    scanner_thread = threading.Thread(

        target=scanner_loop,

        args=(chat_id,),

        daemon=True

    )


    scanner_running = True

    scanner_thread.start()


    return True


# =========================================================
# STOP SCANNER
# =========================================================

def stop_scanner():

    global active_chat_id


    stop_event.set()

    force_scan_event.clear()


    with state_lock:

        pending_signals.clear()

        active_chat_id = None


# =========================================================
# STATUS
# =========================================================

def status_text():

    with state_lock:

        pending = len(
            pending_signals
        )


    return (

        "BOT STATUS: ONLINE\n"

        "Scanner: "
        + (
            "ACTIVE"
            if scanner_running
            else
            "STOPPED"
        )
        + "\n"

        "Market: Kraken USD\n"

        "Timeframe: 15m\n"

        "Pending signals: "
        + str(pending)
        + "\n"

        "Signal delivery: 1 per minute"

    )


# =========================================================
# TELEGRAM LONG POLLING
# =========================================================

def telegram_loop():

    global offset
    global active_chat_id


    offset = None


    # Make sure webhook mode is disabled

    try:

        requests.get(

            "https://api.telegram.org/bot"
            + TOKEN
            + "/deleteWebhook",

            params={
                "drop_pending_updates": "false"
            },

            timeout=15

        )

    except Exception:

        pass


    while True:

        try:

            params = {

                "timeout": 25,

                "limit": 100,

                "allowed_updates": [
                    "message"
                ]

            }


            if offset is not None:

                params[
                    "offset"
                ] = offset


            r = requests.get(

                "https://api.telegram.org/bot"
                + TOKEN
                + "/getUpdates",

                params=params,

                timeout=35

            )


            r.raise_for_status()


            data = r.json()


            for update in data.get(
                "result",
                []
            ):


                # Confirm this update

                offset = (
                    update.get(
                        "update_id",
                        0
                    )
                    + 1
                )


                message = update.get(
                    "message",
                    {}
                )


                chat = message.get(
                    "chat",
                    {}
                )


                if not chat:

                    continue


                chat_id = chat["id"]


                text = message.get(
                    "text",
                    ""
                ).strip()


                # =========================================
                # /start
                # =========================================

                if text == "/start":

                    send(

                        chat_id,

                        "SMC Crypto Signals\n\n"

                        "/scan - Start scanner now\n"

                        "/stop - Stop scanner\n"

                        "/status - Bot status"

                    )


                # =========================================
                # /scan
                # =========================================

                elif text == "/scan":


                    if (
                        active_chat_id is not None
                        and
                        active_chat_id != chat_id
                    ):

                        send(

                            chat_id,

                            "Scanner is already active "
                            "in another chat."

                        )

                        continue


                    started = start_scanner(
                        chat_id
                    )


                    if started:

                        send(

                            chat_id,

                            "SMC SCAN STARTED\n"

                            "First scan is starting now.\n\n"

                            "Each signal will be sent "
                            "separately.\n"

                            "Maximum delivery: "
                            "1 signal per minute."

                        )

                    else:

                        send(

                            chat_id,

                            "SCANNER ALREADY ACTIVE\n"

                            "Immediate rescan requested."

                        )


                # =========================================
                # /stop
                # =========================================

                elif text == "/stop":


                    if active_chat_id == chat_id:

                        stop_scanner()


                        send(

                            chat_id,

                            "SCANNER STOPPED"

                        )

                    else:

                        send(

                            chat_id,

                            "No active scanner "
                            "for this chat."

                        )


                # =========================================
                # /status
                # =========================================

                elif text == "/status":

                    send(

                        chat_id,

                        status_text()

                    )


        except Exception:

            time.sleep(
                5
            )


# =========================================================
# START TELEGRAM THREAD
# =========================================================

def start_background_bot():

    if not TOKEN:

        print(
            "TELEGRAM_BOT_TOKEN is missing."
        )

        return


    thread = threading.Thread(

        target=telegram_loop,

        daemon=True

    )


    thread.start()


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    start_background_bot()


    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )


    app.run(

        host="0.0.0.0",

        port=port

    )
