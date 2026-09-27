import os
import time
import threading

import requests

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

from flask import Flask, jsonify


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

KRAKEN_API = "https://api.kraken.com/0/public"
TELEGRAM_API = "https://api.telegram.org/bot"

TIMEFRAME = 15
LIMIT = 100
MAX_PAIRS = 50

SCAN_INTERVAL = 300
SEND_INTERVAL = 60
CHART_CANDLES = 60
HTTP_TIMEOUT = 20

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
offset = None


# =========================================================
# WEB SERVER
# =========================================================

@app.get("/")
def home():
    return "SMC Crypto Bot is running."


@app.get("/health")
def health():
    with state_lock:
        running = scanner_running
        pending = len(pending_signals)
    return jsonify({
        "status": "ok",
        "scanner_running": running,
        "pending_signals": pending,
    })


# =========================================================
# TELEGRAM
# =========================================================

def telegram_url(method):
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    return TELEGRAM_API + TOKEN + "/" + method


def send(chat_id, text):
    r = requests.post(
        telegram_url("sendMessage"),
        json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()


def send_photo(chat_id, photo_path, caption, reply_markup=None):
    with open(photo_path, "rb") as photo:
        data = {"chat_id": chat_id, "caption": caption}
        if reply_markup is not None:
            data["reply_markup"] = reply_markup

        r = requests.post(
            telegram_url("sendPhoto"),
            data=data,
            files={"photo": photo},
            timeout=HTTP_TIMEOUT,
        )
    r.raise_for_status()


# =========================================================
# KRAKEN
# =========================================================

def kraken(path, params=None):
    r = requests.get(
        KRAKEN_API + path,
        params=params or {},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(str(data["error"]))
    return data.get("result", {})


def get_pairs():
    result = kraken("/AssetPairs")
    out = []

    for key, info in result.items():
        if not isinstance(info, dict):
            continue

        status = str(info.get("status", "online")).lower()
        if status != "online":
            continue

        altname = str(info.get("altname", key))
        wsname = str(info.get("wsname", ""))
        names = (altname.upper(), wsname.upper(), str(key).upper())

        # USDT ONLY.
        if not any(name.endswith("USDT") or name.endswith("/USDT") for name in names):
            continue

        if ".D" in altname.upper() or ".D" in wsname.upper():
            continue

        out.append(altname)

    return list(dict.fromkeys(out))[:MAX_PAIRS]


def get_candles(pair):
    result = kraken(
        "/OHLC",
        {"pair": pair, "interval": TIMEFRAME},
    )

    for key, value in result.items():
        if key != "last" and value:
            return value[-LIMIT:]

    raise RuntimeError("No candle data")


# =========================================================
# SMC ANALYSIS
# =========================================================

def analyze(pair):
    candles = get_candles(pair)
    if len(candles) < 50:
        return None

    close = [float(c[4]) for c in candles]
    high = [float(c[2]) for c in candles]
    low = [float(c[3]) for c in candles]
    volume = [float(c[6]) for c in candles]

    current = close[-1]
    candle_time = int(candles[-1][0])

    recent_high = max(high[-21:-1])
    recent_low = min(low[-21:-1])
    previous_high = max(high[-41:-21])
    previous_low = min(low[-41:-21])

    signal = "WAIT"
    structure = "RANGE"

    if current > recent_high:
        signal = "LONG"
        structure = "BULLISH BOS"
    elif current < recent_low:
        signal = "SHORT"
        structure = "BEARISH BOS"
    elif recent_high > previous_high and recent_low > previous_low:
        signal = "LONG"
        structure = "BULLISH CHoCH"
    elif recent_high < previous_high and recent_low < previous_low:
        signal = "SHORT"
        structure = "BEARISH CHoCH"

    if signal == "WAIT":
        return None

    if signal == "LONG":
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

    lookback_24h = min(96, len(close) - 1)
    old_price = close[-1 - lookback_24h]
    change_24h = ((current - old_price) / old_price * 100) if old_price else 0.0

    volume_base = volume[-21:-1]
    avg_volume = sum(volume_base) / len(volume_base) if volume_base else 0
    volume_spike = volume[-1] / avg_volume if avg_volume > 0 else 0

    # Contextual liquidity sweep checks.
    swept_sell_side = low[-1] < recent_low and close[-1] > recent_low
    swept_buy_side = high[-1] > recent_high and close[-1] < recent_high

    return {
        "pair": pair,
        "signal": signal,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "structure": structure,
        "candle_time": candle_time,
        "candles": candles,
        "change_24h": change_24h,
        "volume": volume[-1],
        "volume_spike": volume_spike,
        "swept_sell_side": swept_sell_side,
        "swept_buy_side": swept_buy_side,
        "recent_high": recent_high,
        "recent_low": recent_low,
    }


def refresh_signal(signal):
    try:
        latest = analyze(signal["pair"])
        if not latest:
            return None

        # It must still be the same 15m candle and same setup.
        if latest["candle_time"] != signal["candle_time"]:
            return None
        if latest["signal"] != signal["signal"]:
            return None
        if latest["structure"] != signal["structure"]:
            return None

        # Return fresh Entry / SL / TP / chart data.
        return latest
    except Exception:
        return None


# =========================================================
# FORMATTING
# =========================================================

def format_price(value):
    value = float(value)
    if value >= 1000:
        return format(value, ",.2f")
    if value >= 1:
        return format(value, ".4f")
    if value >= 0.01:
        return format(value, ".6f")
    return format(value, ".8f")


def display_pair(pair):
    pair = str(pair).upper()
    if pair.endswith("USDT") and "/" not in pair:
        return pair[:-4] + "/USDT"
    return pair


def signal_caption(s):
    emoji = "🟢" if s["signal"] == "LONG" else "🔴"
    volume_text = (
        f"{s['volume_spike']:.1f}x volume spike"
        if s["volume_spike"] >= 2
        else f"{s['volume_spike']:.1f}x volume"
    )

    if s["swept_sell_side"]:
        liquidity = "Sell-side liquidity swept"
    elif s["swept_buy_side"]:
        liquidity = "Buy-side liquidity swept"
    else:
        liquidity = "No clear liquidity sweep"

    return (
        f"{emoji} {s['signal']} — {display_pair(s['pair'])}\n\n"
        f"💰 Entry: ${format_price(s['entry'])}\n"
        f"🛑 Stop Loss: ${format_price(s['sl'])}\n"
        f"🎯 TP1: ${format_price(s['tp1'])}\n"
        f"🎯 TP2: ${format_price(s['tp2'])}\n\n"
        f"📊 Structure: {s['structure']}\n"
        f"⏱ Timeframe: 15m\n"
        f"📈 24h Change: {s['change_24h']:+.2f}%\n"
        f"💧 Volume: {volume_text}\n"
        f"🔎 Liquidity: {liquidity}\n"
        f"✅ Fresh confirmation"
    )


# =========================================================
# CHART
# =========================================================

def make_chart(signal):
    candles = signal["candles"][-CHART_CANDLES:]
    fig, ax = plt.subplots(figsize=(12, 7), dpi=140)

    fig.patch.set_facecolor("#0b1220")
    ax.set_facecolor("#0b1220")

    for i, candle in enumerate(candles):
        open_price = float(candle[1])
        high_price = float(candle[2])
        low_price = float(candle[3])
        close_price = float(candle[4])

        up = close_price >= open_price
        body_low = open_price if up else close_price
        body_height = abs(close_price - open_price)
        body_color = "#19c37d" if up else "#ef4444"

        ax.vlines(i, low_price, high_price, linewidth=1, color="#cbd5e1", zorder=2)
        height = max(body_height, (high_price - low_price) * 0.003)
        ax.add_patch(Rectangle(
            (i - 0.32, body_low), 0.64, height,
            facecolor=body_color, edgecolor=body_color, zorder=3,
        ))

    entry = signal["entry"]
    sl = signal["sl"]
    tp1 = signal["tp1"]
    tp2 = signal["tp2"]

    ax.axhline(entry, linestyle="--", linewidth=1.7, color="#38bdf8", label=f"Entry {format_price(entry)}")
    ax.axhline(sl, linestyle="--", linewidth=1.7, color="#ef4444", label=f"SL {format_price(sl)}")
    ax.axhline(tp1, linestyle="--", linewidth=1.7, color="#22c55e", label=f"TP1 {format_price(tp1)}")
    ax.axhline(tp2, linestyle="--", linewidth=1.7, color="#16a34a", label=f"TP2 {format_price(tp2)}")

    ax.axhline(signal["recent_high"], linestyle=":", linewidth=1, color="#a78bfa", alpha=0.8)
    ax.axhline(signal["recent_low"], linestyle=":", linewidth=1, color="#f59e0b", alpha=0.8)

    x_last = len(candles) - 1
    ax.annotate(
        signal["structure"],
        xy=(x_last, entry),
        xytext=(max(2, x_last - 16), entry),
        color="white",
        fontsize=10,
        arrowprops={"arrowstyle": "->", "color": "#38bdf8"},
    )

    direction = "LONG 🟢" if signal["signal"] == "LONG" else "SHORT 🔴"
    ax.set_title(
        f"{display_pair(signal['pair'])} • 15m • SMC {direction}",
        color="white", fontsize=16, fontweight="bold", pad=14,
    )

    ax.text(
        0.01, 0.98,
        f"Entry {format_price(entry)}    SL {format_price(sl)}    TP1 {format_price(tp1)}    TP2 {format_price(tp2)}",
        transform=ax.transAxes, va="top", color="white", fontsize=9,
        bbox={"boxstyle": "round,pad=0.45", "facecolor": "#111827", "edgecolor": "#334155"},
    )

    ax.grid(True, alpha=0.12, linewidth=0.7)
    ax.tick_params(colors="#cbd5e1", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#334155")

    ax.set_xlim(-1, len(candles))
    ax.legend(
        loc="upper left", bbox_to_anchor=(0.01, 0.90),
        frameon=False, fontsize=8, labelcolor="white",
    )

    fig.tight_layout()
    filename = f"/tmp/smc_{str(signal['pair']).replace('/', '_')}_{signal['candle_time']}.png"
    fig.savefig(filename, dpi=140, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return filename


# =========================================================
# DUPLICATE FILTER
# =========================================================

def remember_signal(key):
    if key in seen_signals:
        return False

    seen_signals.add(key)
    seen_order.append(key)

    if len(seen_order) > 5000:
        old = seen_order.pop(0)
        seen_signals.discard(old)

    return True


# =========================================================
# SCAN ONCE
# =========================================================

def scan_once():
    pairs = get_pairs()
    new_signals = []

    for i, pair in enumerate(pairs):
        try:
            result = analyze(pair)
            if result:
                key = (
                    result["pair"],
                    result["candle_time"],
                    result["signal"],
                    result["structure"],
                )
                if remember_signal(key):
                    new_signals.append(result)
        except Exception:
            pass

        if i < len(pairs) - 1:
            time.sleep(0.3)

    return new_signals, len(pairs)


# =========================================================
# SEND ONE FRESH SIGNAL
# =========================================================

def send_one_fresh_signal(chat_id):
    global next_send_at

    while True:
        with state_lock:
            if not pending_signals:
                return False
            signal = pending_signals.pop(0)

        fresh_signal = refresh_signal(signal)

        # Old/changed setup: discard and check the next one.
        if fresh_signal is None:
            continue

        try:
            chart_path = make_chart(fresh_signal)
            caption = signal_caption(fresh_signal)

            reply_markup = {
                "inline_keyboard": [[
                    {
                        "text": "📊 TradingView",
                        "url": (
                            "https://www.tradingview.com/chart/?symbol=KRAKEN:"
                            + str(fresh_signal["pair"]).upper()
                        ),
                    }
                ]]
            }

            send_photo(chat_id, chart_path, caption, reply_markup)

            next_send_at = time.time() + SEND_INTERVAL

            try:
                os.remove(chart_path)
            except Exception:
                pass

            return True

        except Exception as e:
            with state_lock:
                pending_signals.insert(0, fresh_signal)

            try:
                send(chat_id, "SEND ERROR\n" + type(e).__name__ + ": " + str(e))
            except Exception:
                pass

            next_send_at = time.time() + 30
            return False


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

            if force_scan_event.is_set() or now >= next_scan_at:
                force_scan_event.clear()

                try:
                    new_signals, checked = scan_once()
                    with state_lock:
                        pending_signals.extend(new_signals)

                    if not new_signals:
                        send(
                            chat_id,
                            "SCAN COMPLETE\n\n"
                            f"Pairs checked: {checked}\n"
                            "Market: USDT pairs\n"
                            "No new LONG/SHORT setup.",
                        )
                except Exception as e:
                    try:
                        send(chat_id, "SCANNER ERROR\n" + type(e).__name__ + ": " + str(e))
                    except Exception:
                        pass

                next_scan_at = time.time() + SCAN_INTERVAL

            now = time.time()
            with state_lock:
                has_pending = bool(pending_signals)

            if has_pending and now >= next_send_at:
                send_one_fresh_signal(chat_id)

            stop_event.wait(1)

    finally:
        scanner_running = False


# =========================================================
# START / STOP / STATUS
# =========================================================

def start_scanner(chat_id):
    global scanner_thread
    global scanner_running
    global active_chat_id
    global next_send_at

    with state_lock:
        active_chat_id = chat_id

    if scanner_thread and scanner_thread.is_alive():
        force_scan_event.set()
        return False

    stop_event.clear()
    force_scan_event.set()
    next_send_at = 0

    scanner_thread = threading.Thread(
        target=scanner_loop,
        args=(chat_id,),
        daemon=True,
    )

    scanner_running = True
    scanner_thread.start()
    return True


def stop_scanner():
    global active_chat_id
    stop_event.set()
    force_scan_event.clear()

    with state_lock:
        pending_signals.clear()
        active_chat_id = None


def status_text():
    with state_lock:
        pending = len(pending_signals)
        running = scanner_running

    return (
        "BOT STATUS: ONLINE\n"
        f"Scanner: {'ACTIVE' if running else 'STOPPED'}\n"
        "Market: Kraken USDT\n"
        "Timeframe: 15m\n"
        f"Pending signals: {pending}\n"
        "Signal delivery: 1 per minute\n"
        "Chart: ENABLED\n"
        "Fresh validation: ENABLED"
    )


# =========================================================
# TELEGRAM LONG POLLING
# =========================================================

def telegram_loop():
    global offset
    global active_chat_id

    offset = None

    try:
        requests.get(
            telegram_url("deleteWebhook"),
            params={"drop_pending_updates": "false"},
            timeout=15,
        )
    except Exception:
        pass

    while True:
        try:
            params = {
                "timeout": 25,
                "limit": 100,
                "allowed_updates": ["message"],
            }
            if offset is not None:
                params["offset"] = offset

            r = requests.get(
                telegram_url("getUpdates"),
                params=params,
                timeout=35,
            )
            r.raise_for_status()
            data = r.json()

            for update in data.get("result", []):
                offset = update.get("update_id", 0) + 1
                message = update.get("message", {})
                chat = message.get("chat", {})

                if not chat:
                    continue

                chat_id = chat["id"]
                text = message.get("text", "").strip()

                if text == "/start":
                    send(
                        chat_id,
                        "🚀 SMC Crypto Signals\n\n"
                        "/scan - Start scanner now\n"
                        "/stop - Stop scanner\n"
                        "/status - Bot status\n\n"
                        "Market: USDT pairs\n"
                        "Timeframe: 15m\n"
                        "Charts: ON\n"
                        "Fresh validation: ON",
                    )

                elif text == "/scan":
                    if active_chat_id is not None and active_chat_id != chat_id:
                        send(chat_id, "Scanner is already active in another chat.")
                        continue

                    started = start_scanner(chat_id)
                    if started:
                        send(
                            chat_id,
                            "🚀 SMC SCAN STARTED\n\n"
                            "USDT pairs only.\n"
                            "Fresh validation enabled.\n"
                            "Each valid signal gets its own chart.\n"
                            "Maximum delivery: 1 signal per minute.",
                        )
                    else:
                        send(
                            chat_id,
                            "SCANNER ALREADY ACTIVE\n"
                            "Immediate rescan requested.",
                        )

                elif text == "/stop":
                    if active_chat_id == chat_id:
                        stop_scanner()
                        send(chat_id, "🛑 SCANNER STOPPED\nPending signals cleared.")
                    else:
                        send(chat_id, "No active scanner for this chat.")

                elif text == "/status":
                    send(chat_id, status_text())

        except Exception:
            time.sleep(5)


# =========================================================
# MAIN
# =========================================================

def start_background_bot():
    if not TOKEN:
        print("TELEGRAM_BOT_TOKEN is missing.")
        return

    thread = threading.Thread(target=telegram_loop, daemon=True)
    thread.start()


if __name__ == "__main__":
    start_background_bot()
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
