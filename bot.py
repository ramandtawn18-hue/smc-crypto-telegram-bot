import os
import time
import math
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify

# ============================================================
# SAIWAN — 15m Trendline Breakout (Bitget USDT Perpetual)
# Mechanical strategy: Trendline + CLOSED candle breakout only.
# No AI / RSI / MACD / SMC / FVG / OB / Volume filters.
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BITGET_BASE = "https://api.bitget.com"
PRODUCT_TYPE = "USDT-FUTURES"
INTERVAL = "15m"
GRANULARITY = "15m"

SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "25"))
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "180"))
PIVOT_WINDOW = int(os.getenv("PIVOT_WINDOW", "3"))
MIN_LINE_TOUCHES = int(os.getenv("MIN_LINE_TOUCHES", "2"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "0"))  # 0 = all eligible contracts

# Optional: 1 = send chart image when matplotlib is available.
SEND_CHART = os.getenv("SEND_CHART", "1") == "1"

app = Flask(__name__)

state = {
    "running": False,
    "last_scan": None,
    "last_error": None,
    "signals_sent": 0,
    "symbols": 0,
}

# One signal per exact breakout candle + direction + line.
sent_breakouts = set()
processed_candle = {}

session = requests.Session()
session.headers.update({"User-Agent": "SAIWAN-Trendline-Bot/1.0"})


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def get_json(path, params=None, timeout=12):
    r = session.get(BITGET_BASE + path, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != "00000":
        raise RuntimeError(f"Bitget API: {data.get('code')} {data.get('msg')}")
    return data.get("data")


def get_symbols():
    rows = get_json("/api/v2/mix/market/contracts", {
        "productType": PRODUCT_TYPE,
    })
    out = []
    for x in rows or []:
        symbol = str(x.get("symbol", ""))
        status = str(x.get("symbolStatus", x.get("status", ""))).lower()
        quote = str(x.get("quoteCoin", "")).upper()
        base = str(x.get("baseCoin", "")).upper()
        contract_type = str(x.get("symbolType", x.get("type", ""))).lower()
        if not symbol.endswith("USDT"):
            continue
        if quote and quote != "USDT":
            continue
        if status and status not in {"normal", "online", "listed"}:
            continue
        if contract_type and contract_type not in {"perpetual", "perpetual_contract", ""}:
            # Current Bitget contract endpoint normally exposes perpetual USDT contracts.
            continue
        out.append(symbol)
    out = sorted(set(out))
    return out[:MAX_SYMBOLS] if MAX_SYMBOLS > 0 else out


def get_candles(symbol):
    rows = get_json("/api/v2/mix/market/candles", {
        "symbol": symbol,
        "productType": PRODUCT_TYPE,
        "granularity": GRANULARITY,
        "limit": min(max(CANDLE_LIMIT, 80), 200),
    })
    # Bitget returns newest first. Convert to oldest first.
    candles = []
    for r in rows or []:
        if len(r) < 6:
            continue
        candles.append({
            "ts": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]),
        })
    candles.sort(key=lambda x: x["ts"])
    # Never analyze the currently-forming candle.
    if len(candles) >= 2:
        candles = candles[:-1]
    return candles


def is_pivot_high(c, i, n):
    if i < n or i + n >= len(c):
        return False
    h = c[i]["high"]
    return all(h > c[j]["high"] for j in range(i-n, i)) and all(h > c[j]["high"] for j in range(i+1, i+n+1))


def is_pivot_low(c, i, n):
    if i < n or i + n >= len(c):
        return False
    lo = c[i]["low"]
    return all(lo < c[j]["low"] for j in range(i-n, i)) and all(lo < c[j]["low"] for j in range(i+1, i+n+1))


def pivots(c):
    highs, lows = [], []
    n = PIVOT_WINDOW
    for i in range(n, len(c)-n):
        if is_pivot_high(c, i, n):
            highs.append(i)
        if is_pivot_low(c, i, n):
            lows.append(i)
    return highs, lows


def line_value(line, idx):
    i1, p1, i2, p2 = line
    return p1 + (p2 - p1) * (idx - i1) / (i2 - i1)


def price_tol(price):
    return max(abs(price) * 0.0008, 1e-12)


def valid_down_line(c, i1, i2, highs):
    # Descending resistance line through two pivot highs.
    if not (i1 < i2 and c[i1]["high"] > c[i2]["high"]):
        return None
    p1, p2 = c[i1]["high"], c[i2]["high"]
    line = (i1, p1, i2, p2)
    touches = 0
    for j in highs:
        if j <= i2:
            lv = line_value(line, j)
            if abs(c[j]["high"] - lv) <= price_tol(c[j]["high"]):
                touches += 1
    # Do not accept a line that is clearly broken by a later closed candle before the signal.
    for j in range(i2 + 1, len(c)-1):
        if c[j]["close"] > line_value(line, j) + price_tol(c[j]["close"]):
            return None
    if touches < MIN_LINE_TOUCHES:
        return None
    return line, touches


def valid_up_line(c, i1, i2, lows):
    # Ascending support line through two pivot lows.
    if not (i1 < i2 and c[i1]["low"] < c[i2]["low"]):
        return None
    p1, p2 = c[i1]["low"], c[i2]["low"]
    line = (i1, p1, i2, p2)
    touches = 0
    for j in lows:
        if j <= i2:
            lv = line_value(line, j)
            if abs(c[j]["low"] - lv) <= price_tol(c[j]["low"]):
                touches += 1
    for j in range(i2 + 1, len(c)-1):
        if c[j]["close"] < line_value(line, j) - price_tol(c[j]["close"]):
            return None
    if touches < MIN_LINE_TOUCHES:
        return None
    return line, touches


def choose_lines(c, highs, lows, signal_idx):
    # Search recent pairs. Prefer the most recent second pivot and then the
    # line with the most touches. The line is fixed once a breakout is found.
    down = []
    up = []
    recent_highs = [i for i in highs if i < signal_idx]
    recent_lows = [i for i in lows if i < signal_idx]

    for a_pos in range(max(0, len(recent_highs)-10), len(recent_highs)):
        i1 = recent_highs[a_pos]
        for b_pos in range(a_pos+1, len(recent_highs)):
            i2 = recent_highs[b_pos]
            x = valid_down_line(c, i1, i2, recent_highs)
            if x:
                down.append((x[0], x[1]))

    for a_pos in range(max(0, len(recent_lows)-10), len(recent_lows)):
        i1 = recent_lows[a_pos]
        for b_pos in range(a_pos+1, len(recent_lows)):
            i2 = recent_lows[b_pos]
            x = valid_up_line(c, i1, i2, recent_lows)
            if x:
                up.append((x[0], x[1]))

    down.sort(key=lambda z: (z[0][2], z[1]), reverse=True)
    up.sort(key=lambda z: (z[0][2], z[1]), reverse=True)
    return down[0] if down else None, up[0] if up else None


def find_breakout(symbol, c):
    if len(c) < max(60, PIVOT_WINDOW*2+10):
        return None
    signal_idx = len(c)-1
    highs, lows = pivots(c)
    down, up = choose_lines(c, highs, lows, signal_idx)
    candle = c[signal_idx]

    # LONG: closed 15m candle closes above a descending trendline.
    if down:
        line, touches = down
        lv = line_value(line, signal_idx)
        prev_lv = line_value(line, signal_idx-1)
        if c[signal_idx-1]["close"] <= prev_lv + price_tol(c[signal_idx-1]["close"]) and candle["close"] > lv + price_tol(candle["close"]):
            # Structural SL: latest pivot low before breakout, preferably after line anchor.
            candidate_lows = [i for i in lows if line[2] <= i < signal_idx]
            if not candidate_lows:
                candidate_lows = [i for i in lows if i < signal_idx]
            if candidate_lows:
                sl_idx = candidate_lows[-1]
                entry = candle["close"]
                sl = c[sl_idx]["low"]
                risk = entry - sl
                if risk > 0:
                    key = (symbol, candle["ts"], "LONG", line[0], line[2])
                    return {
                        "symbol": symbol, "side": "LONG", "entry": entry, "sl": sl,
                        "tp1": entry + risk, "tp2": entry + 2*risk, "tp3": entry + 3*risk,
                        "candle_ts": candle["ts"], "line": line, "line_price": lv,
                        "touches": touches, "sl_idx": sl_idx, "key": key,
                    }

    # SHORT: closed 15m candle closes below an ascending trendline.
    if up:
        line, touches = up
        lv = line_value(line, signal_idx)
        prev_lv = line_value(line, signal_idx-1)
        if c[signal_idx-1]["close"] >= prev_lv - price_tol(c[signal_idx-1]["close"]) and candle["close"] < lv - price_tol(candle["close"]):
            candidate_highs = [i for i in highs if line[2] <= i < signal_idx]
            if not candidate_highs:
                candidate_highs = [i for i in highs if i < signal_idx]
            if candidate_highs:
                sl_idx = candidate_highs[-1]
                entry = candle["close"]
                sl = c[sl_idx]["high"]
                risk = sl - entry
                if risk > 0:
                    key = (symbol, candle["ts"], "SHORT", line[0], line[2])
                    return {
                        "symbol": symbol, "side": "SHORT", "entry": entry, "sl": sl,
                        "tp1": entry - risk, "tp2": entry - 2*risk, "tp3": entry - 3*risk,
                        "candle_ts": candle["ts"], "line": line, "line_price": lv,
                        "touches": touches, "sl_idx": sl_idx, "key": key,
                    }
    return None


def fmt_price(x):
    if x == 0:
        return "0"
    ax = abs(x)
    if ax >= 1000:
        return f"{x:.2f}"
    if ax >= 1:
        return f"{x:.4f}"
    if ax >= 0.01:
        return f"{x:.6f}"
    if ax >= 0.0001:
        return f"{x:.8f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def telegram_send(text):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = session.post(url, json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}, timeout=15)
    r.raise_for_status()


def chart_bytes(symbol, candles, signal):
    if not SEND_CHART:
        return None
    try:
        import io
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        n = min(80, len(candles))
        data = candles[-n:]
        xs = list(range(n))
        fig, ax = plt.subplots(figsize=(12, 6), dpi=140)
        for i, k in enumerate(data):
            up = k["close"] >= k["open"]
            # neutral professional chart; no strategy colors are required.
            ax.plot([i, i], [k["low"], k["high"]], linewidth=0.8)
            ax.plot([i, i], [k["open"], k["close"]], linewidth=4)
        line = signal["line"]
        # Map original candle index to local x coordinate.
        start_global = len(candles)-n
        x1 = line[0] - start_global
        x2 = n-1
        if x2 >= 0:
            y1 = line_value(line, line[0])
            y2 = line_value(line, len(candles)-1)
            ax.plot([x1, x2], [y1, y2], linewidth=1.6, linestyle="--")
        sx = n-1
        ax.axhline(signal["entry"], linewidth=1.0, linestyle="-")
        ax.axhline(signal["sl"], linewidth=1.0, linestyle=":")
        ax.axhline(signal["tp1"], linewidth=0.9, linestyle=":")
        ax.axhline(signal["tp2"], linewidth=0.9, linestyle=":")
        ax.axhline(signal["tp3"], linewidth=0.9, linestyle=":")
        ax.set_title(f"SAIWAN • {symbol} • 15m Trendline Breakout • {signal['side']}")
        ax.set_xlabel("Closed 15m candles")
        ax.set_ylabel("Price")
        ax.grid(alpha=0.18)
        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception:
        return None


def telegram_send_signal(signal, candles):
    side_icon = "🟢" if signal["side"] == "LONG" else "🔴"
    dt = datetime.fromtimestamp(signal["candle_ts"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    text = (
        f"🚀 SAIWAN TRENDLINE BREAKOUT\n\n"
        f"{side_icon} {signal['side']} • {signal['symbol']}\n"
        f"⏱ Timeframe: 15m\n"
        f"📌 Breakout: CLOSED candle\n"
        f"🕒 Candle close: {dt}\n\n"
        f"ENTRY: {fmt_price(signal['entry'])}\n"
        f"SL: {fmt_price(signal['sl'])}\n"
        f"TP1: {fmt_price(signal['tp1'])}  (1R)\n"
        f"TP2: {fmt_price(signal['tp2'])}  (2R)\n"
        f"TP3: {fmt_price(signal['tp3'])}  (3R)\n\n"
        f"📐 Trendline touches: {signal['touches']}\n"
        f"📊 Method: Trendline + Breakout only\n"
        f"🏦 Bitget USDT Perpetual\n\n"
        f"⚠️ Signal only — no automatic order."
    )
    telegram_send(text)

    chart = chart_bytes(signal["symbol"], candles, signal)
    if chart is not None and BOT_TOKEN and CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
            r = session.post(url, data={"chat_id": CHAT_ID, "caption": f"{signal['symbol']} 15m {signal['side']} — Trendline Breakout"}, files={"photo": ("chart.png", chart, "image/png")}, timeout=20)
            r.raise_for_status()
        except Exception:
            pass


def scan_once():
    symbols = get_symbols()
    state["symbols"] = len(symbols)
    for symbol in symbols:
        try:
            candles = get_candles(symbol)
            if not candles:
                continue
            closed_ts = candles[-1]["ts"]
            if processed_candle.get(symbol) == closed_ts:
                continue
            processed_candle[symbol] = closed_ts
            signal = find_breakout(symbol, candles)
            if not signal:
                continue
            if signal["key"] in sent_breakouts:
                continue
            sent_breakouts.add(signal["key"])
            telegram_send_signal(signal, candles)
            state["signals_sent"] += 1
        except Exception as e:
            # Continue scanning other contracts.
            state["last_error"] = f"{symbol}: {e}"


def scanner_loop():
    state["running"] = True
    while True:
        try:
            scan_once()
            state["last_scan"] = now_utc()
            state["last_error"] = None
        except Exception as e:
            state["last_error"] = str(e)
        time.sleep(max(10, SCAN_SECONDS))


def telegram_loop():
    if not BOT_TOKEN or not CHAT_ID:
        return
    offset = None
    while True:
        try:
            r = session.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates", params={"timeout": 25, "offset": offset}, timeout=35)
            data = r.json()
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                msg = u.get("message") or {}
                text = (msg.get("text") or "").strip().lower()
                if str(msg.get("chat", {}).get("id")) != str(CHAT_ID):
                    continue
                if text == "/start":
                    telegram_send("🤖 SAIWAN is online.\n\nStrategy: 15m Trendline Breakout only.\nUse /status or /scan")
                elif text == "/scan":
                    telegram_send("🔎 SAIWAN: scanning Bitget USDT Perpetual 15m closed candles...")
                    try:
                        scan_once()
                        telegram_send("✅ Scan completed.")
                    except Exception as e:
                        telegram_send(f"❌ Scan error: {e}")
                elif text == "/status":
                    telegram_send(
                        "🤖 SAIWAN STATUS\n\n"
                        f"Bot: ONLINE\n"
                        f"Scanner: {'RUNNING' if state['running'] else 'STARTING'}\n"
                        f"Market: Bitget USDT Perpetual\n"
                        f"Timeframe: 15m only\n"
                        f"Strategy: Trendline + CLOSED Breakout\n"
                        f"Symbols: {state['symbols']}\n"
                        f"Signals sent: {state['signals_sent']}\n"
                        f"Last scan: {state['last_scan'] or 'not yet'}\n"
                        f"Last error: {state['last_error'] or 'none'}"
                    )
        except Exception as e:
            state["last_error"] = f"Telegram: {e}"
            time.sleep(5)


@app.get("/")
def home():
    return jsonify({"bot": "SAIWAN", "strategy": "15m Trendline Breakout", "status": "online"})


@app.get("/status")
def status():
    return jsonify(state)


if __name__ == "__main__":
    threading.Thread(target=scanner_loop, daemon=True).start()
    threading.Thread(target=telegram_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
else:
    threading.Thread(target=scanner_loop, daemon=True).start()
    threading.Thread(target=telegram_loop, daemon=True).start()
