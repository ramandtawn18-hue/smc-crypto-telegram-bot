import os
import time
import json
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from flask import Flask, jsonify

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OKX_API = "https://www.okx.com"
TELEGRAM_API = "https://api.telegram.org/bot"

TF_15M = "15m"
TIMEFRAME = TF_15M
CANDLE_LIMIT = 220
MAX_PAIRS = 120
SCAN_WORKERS = 6
SCAN_INTERVAL = 60
SEND_INTERVAL = 60
CHART_CANDLES = 70
HTTP_TIMEOUT = 15
MIN_SCORE = 7

TP1_R = 1.5
TP2_R = 2.5
TP3_R = 4.0

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

session = requests.Session()
session.headers.update({"User-Agent": "Trend-RSI-Volatility-Telegram-Bot/3.0", "Accept": "application/json"})


def fmt_price(x):
    x = float(x)
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def okx_get(path, params=None, retries=2):
    last = None
    for attempt in range(retries + 1):
        try:
            r = session.get(OKX_API + path, params=params or {}, timeout=HTTP_TIMEOUT)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else min(2.0 * (attempt + 1), 6.0)
                if attempt < retries:
                    time.sleep(delay)
                    continue
            if r.status_code in (418, 500, 502, 503, 504) and attempt < retries:
                time.sleep(min(1.5 * (attempt + 1), 5.0))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last = e
            if attempt < retries:
                time.sleep(min(0.8 * (attempt + 1), 3.0))
                continue
            break
    raise last or RuntimeError("OKX API request failed")


def get_contracts():
    payload = okx_get("/api/v5/public/instruments", {"instType": "SWAP"})
    if not isinstance(payload, dict) or payload.get("code") != "0":
        raise RuntimeError(f"OKX API error {payload.get("code") if isinstance(payload, dict) else "unknown"}: {payload.get("msg") if isinstance(payload, dict) else "invalid response"}")
    return [
        x for x in payload.get("data", [])
        if x.get("instType") == "SWAP"
        and x.get("settleCcy") == "USDT"
        and x.get("state") == "live"
        and x.get("instId", "").endswith("-USDT-SWAP")
    ]


def get_tickers():
    payload = okx_get("/api/v5/market/tickers", {"instType": "SWAP"})
    if not isinstance(payload, dict) or payload.get("code") != "0":
        raise RuntimeError(f"OKX API error {payload.get("code") if isinstance(payload, dict) else "unknown"}: {payload.get("msg") if isinstance(payload, dict) else "invalid response"}")
    return payload.get("data", [])


def get_klines(inst_id, interval=TIMEFRAME, limit=CANDLE_LIMIT):
    payload = okx_get("/api/v5/market/candles", {"instId": inst_id, "bar": interval, "limit": min(limit, 300)})
    if not isinstance(payload, dict) or payload.get("code") != "0":
        raise RuntimeError(f"OKX API error {payload.get("code") if isinstance(payload, dict) else "unknown"}: {payload.get("msg") if isinstance(payload, dict) else "invalid response"}")
    rows = []
    for v in payload.get("data", []):
        try:
            if len(v) < 9 or str(v[8]) != "1":
                continue
            rows.append({
                "time": int(v[0]) // 1000,
                "open": float(v[1]),
                "high": float(v[2]),
                "low": float(v[3]),
                "close": float(v[4]),
                "vol": float(v[5]),
            })
        except (TypeError, ValueError, IndexError):
            continue
    rows.sort(key=lambda x: x["time"])
    return rows[-limit:]


def ema(values, period):
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    out = [values[0]]
    for x in values[1:]:
        out.append(x * k + out[-1] * (1 - k))
    return out


def atr(rows, period=14):
    if len(rows) < period + 1:
        return None
    trs = []
    for i in range(1, len(rows)):
        r, p = rows[i], rows[i - 1]
        trs.append(max(r["high"] - r["low"], abs(r["high"] - p["close"]), abs(r["low"] - p["close"])))
    return sum(trs[-period:]) / period


def swing_points(rows, left=2, right=2):
    highs, lows = [], []
    for i in range(left, len(rows) - right):
        h = rows[i]["high"]
        l = rows[i]["low"]
        if all(h > rows[j]["high"] for j in range(i-left, i)) and all(h >= rows[j]["high"] for j in range(i+1, i+right+1)):
            highs.append((i, h))
        if all(l < rows[j]["low"] for j in range(i-left, i)) and all(l <= rows[j]["low"] for j in range(i+1, i+right+1)):
            lows.append((i, l))
    return highs, lows


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def trend_context(rows):
    closes = [r["close"] for r in rows]
    if len(closes) < 60:
        return "NEUTRAL", None, None, None
    e20, e50, e200 = ema(closes, 20), ema(closes, 50), ema(closes, 200)
    slope20 = e20[-1] - e20[-6]
    slope50 = e50[-1] - e50[-6]
    if e20[-1] > e50[-1] and slope20 > 0 and slope50 >= 0 and closes[-1] > e20[-1]:
        return "BULLISH", e20[-1], e50[-1], e200[-1]
    if e20[-1] < e50[-1] and slope20 < 0 and slope50 <= 0 and closes[-1] < e20[-1]:
        return "BEARISH", e20[-1], e50[-1], e200[-1]
    if e20[-1] > e50[-1]:
        return "BULLISH", e20[-1], e50[-1], e200[-1]
    if e20[-1] < e50[-1]:
        return "BEARISH", e20[-1], e50[-1], e200[-1]
    return "NEUTRAL", e20[-1], e50[-1], e200[-1]


def line_value(p1, p2, x):
    i1, y1 = p1
    i2, y2 = p2
    if i2 == i1:
        return y2
    return y1 + (y2 - y1) * ((x - i1) / (i2 - i1))


def trendline_signal(rows, direction):
    highs, lows = swing_points(rows[-90:], left=2, right=2)
    if direction == "LONG" and len(highs) >= 2:
        p1, p2 = highs[-2], highs[-1]
        if p2[1] < p1[1]:
            now = len(rows[-90:]) - 1
            line_now = line_value(p1, p2, now)
            prev_i = now - 1
            line_prev = line_value(p1, p2, prev_i)
            return rows[-2]["close"] <= line_prev and rows[-1]["close"] > line_now, line_now
    if direction == "SHORT" and len(lows) >= 2:
        p1, p2 = lows[-2], lows[-1]
        if p2[1] > p1[1]:
            now = len(rows[-90:]) - 1
            line_now = line_value(p1, p2, now)
            prev_i = now - 1
            line_prev = line_value(p1, p2, prev_i)
            return rows[-2]["close"] >= line_prev and rows[-1]["close"] < line_now, line_now
    return False, None


def structure_bias(rows, direction):
    highs, lows = swing_points(rows[-80:], left=2, right=2)
    if len(highs) < 2 or len(lows) < 2:
        return False
    if direction == "LONG":
        return highs[-1][1] >= highs[-2][1] and lows[-1][1] > lows[-2][1]
    return highs[-1][1] < highs[-2][1] and lows[-1][1] <= lows[-2][1]


def volatility_score(rows):
    a = atr(rows, 14)
    if not a or rows[-1]["close"] <= 0:
        return 0.0, "LOW"
    atr_values = []
    for i in range(30, len(rows)):
        x = atr(rows[:i+1], 14)
        if x:
            atr_values.append(x)
    if not atr_values:
        return 0.0, "LOW"
    med = sorted(atr_values)[len(atr_values)//2]
    ratio = a / med if med else 0
    if 0.85 <= ratio <= 1.80:
        return min(10.0, 7.0 + (ratio - 0.85) * 2.0), "HEALTHY"
    if 0.65 <= ratio < 0.85:
        return 5.5, "LOW"
    if 1.80 < ratio <= 2.60:
        return 6.0, "HIGH"
    if ratio > 2.60:
        return 3.0, "EXTREME"
    return 4.0, "LOW"


def analyze(symbol, rows15):
    if len(rows15) < 120:
        return None
    r15 = rows15[:-1]
    if len(r15) < 100:
        return None
    cur = r15[-1]
    prev = r15[-2]
    closes = [r["close"] for r in r15]
    a = atr(r15, 14)
    rv = rsi(closes, 14)
    if not a or not rv or a <= 0:
        return None

    trend, ema20_now, ema50_now, ema200_now = trend_context(r15)
    vol_score, volatility = volatility_score(r15)

    # Candidate direction comes from trend + the latest impulse, not SMC/Fibonacci.
    body = abs(cur["close"] - cur["open"])
    rng = max(cur["high"] - cur["low"], 1e-12)
    close_pos = (cur["close"] - cur["low"]) / rng
    momentum_long = cur["close"] > cur["open"] and body >= 0.55 * a and close_pos >= 0.62
    momentum_short = cur["close"] < cur["open"] and body >= 0.55 * a and close_pos <= 0.38

    long_rsi = 52 <= rv <= 68
    short_rsi = 32 <= rv <= 48
    long_trendline, long_line = trendline_signal(r15, "LONG")
    short_trendline, short_line = trendline_signal(r15, "SHORT")
    long_structure = structure_bias(r15, "LONG")
    short_structure = structure_bias(r15, "SHORT")

    avg_vol = sum(x["vol"] for x in r15[-21:-1]) / 20.0
    vol_ratio = cur["vol"] / avg_vol if avg_vol > 0 else 0.0
    volume_ok = vol_ratio >= 1.05

    # Breakout/rejection context gives the strategy a trigger without SMC/Fibonacci.
    hh = max(x["high"] for x in r15[-20:-1])
    ll = min(x["low"] for x in r15[-20:-1])
    long_break = cur["close"] > hh and prev["close"] <= hh
    short_break = cur["close"] < ll and prev["close"] >= ll

    long_checks = {
        "Trend bullish": trend == "BULLISH",
        "RSI healthy": long_rsi,
        "Trendline breakout": long_trendline,
        "Higher-low structure": long_structure,
        "Momentum candle": momentum_long,
        "Volume expansion": volume_ok,
        "Volatility healthy": 4.5 <= vol_score,
        "20-bar breakout": long_break,
    }
    short_checks = {
        "Trend bearish": trend == "BEARISH",
        "RSI healthy": short_rsi,
        "Trendline breakdown": short_trendline,
        "Lower-high structure": short_structure,
        "Momentum candle": momentum_short,
        "Volume expansion": volume_ok,
        "Volatility healthy": 4.5 <= vol_score,
        "20-bar breakdown": short_break,
    }

    long_score = sum(long_checks.values())
    short_score = sum(short_checks.values())
    if long_score >= short_score:
        direction, checks, score = "LONG", long_checks, long_score
    else:
        direction, checks, score = "SHORT", short_checks, short_score

    # Require a directional trend plus at least two independent confirmations.
    trend_ok = checks["Trend bullish"] if direction == "LONG" else checks["Trend bearish"]
    rsi_ok = checks["RSI healthy"]
    momentum_ok = checks["Momentum candle"]
    breakout_ok = checks["20-bar breakout"]
    trendline_ok = checks["Trendline breakout"] if direction == "LONG" else checks["Trendline breakdown"]
    if not trend_ok or not rsi_ok:
        return None
    if score < MIN_SCORE:
        return None
    if not (momentum_ok or breakout_ok or trendline_ok):
        return None

    entry = cur["close"]
    recent_lows = [x["low"] for x in r15[-12:]]
    recent_highs = [x["high"] for x in r15[-12:]]
    if direction == "LONG":
        sl = min(min(recent_lows), entry - 1.15*a)
        risk = entry - sl
        if risk <= 0 or risk > 3.5*a:
            return None
        tp1, tp2, tp3 = entry + 1.5*risk, entry + 2.5*risk, entry + 4.0*risk
    else:
        sl = max(max(recent_highs), entry + 1.15*a)
        risk = sl - entry
        if risk <= 0 or risk > 3.5*a:
            return None
        tp1, tp2, tp3 = entry - 1.5*risk, entry - 2.5*risk, entry - 4.0*risk

    confidence = min(95, max(55, int(48 + score * 5 + min(vol_score, 8) * 1.5)))
    structure = "Trendline breakout" if direction == "LONG" else "Trendline breakdown"
    if breakout_ok:
        structure = "20-bar breakout" if direction == "LONG" else "20-bar breakdown"

    return {
        "symbol": symbol, "direction": direction, "structure": structure,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "score": score, "max_score": len(checks), "trend15": trend,
        "rsi": rv, "volatility": volatility, "volatility_score": vol_score,
        "volume_ratio": vol_ratio, "confidence": confidence,
        "trendline": long_line if direction == "LONG" else short_line,
        "ema20": ema20_now, "ema50": ema50_now, "ema200": ema200_now,
        "time": cur["time"], "rows": r15[-CHART_CANDLES:], "checks": checks,
    }

def make_chart(sig):
    rows = sig["rows"]
    fig, ax = plt.subplots(figsize=(12, 7), dpi=140)
    width = 0.62
    for i, r in enumerate(rows):
        up = r["close"] >= r["open"]
        ax.vlines(i, r["low"], r["high"], linewidth=1)
        body_low = min(r["open"], r["close"])
        body_h = max(abs(r["close"] - r["open"]), 1e-12)
        rect = Rectangle((i-width/2, body_low), width, body_h, fill=True, alpha=0.78)
        ax.add_patch(rect)

    ax.axhline(sig["entry"], linestyle="--", linewidth=1.2, label="Entry")
    ax.axhline(sig["sl"], linestyle="--", linewidth=1.2, label="SL")
    ax.axhline(sig["tp1"], linestyle=":", linewidth=1.0, label="TP1")
    ax.axhline(sig["tp2"], linestyle=":", linewidth=1.0, label="TP2")
    ax.axhline(sig["tp3"], linestyle=":", linewidth=1.0, label="TP3")
    if sig.get("trendline") is not None:
        x0 = max(0, len(rows)-25)
        x1 = len(rows)-1
        # Visual trendline approximation ending at the trigger.
        y1 = sig["trendline"]
        slope = (rows[-1]["close"] - y1) / max(1, len(rows)-x0)
        y0 = y1 - slope * (x1-x0)
        ax.plot([x0, x1], [y0, y1], linewidth=1.5, label="Trendline")

    direction = sig["direction"]
    ax.set_title(f"OKX USDT-SWAP • {sig['symbol']} • 15m • {direction} • Trend + RSI + Volatility")
    ax.set_xlim(-1, len(rows))
    ax.grid(alpha=0.15)
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    path = f"/tmp/chart_{sig['symbol'].replace('/', '_')}_{sig['time']}.png"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path

def telegram_url(method):
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    return TELEGRAM_API + TOKEN + "/" + method


def send_message(chat_id, text, reply_markup=None):
    data = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    r = requests.post(telegram_url("sendMessage"), data=data, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram sendMessage {r.status_code}: {r.text[:500]}")


def send_photo(chat_id, photo_path, caption, reply_markup=None):
    data = {"chat_id": chat_id, "caption": caption}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    with open(photo_path, "rb") as f:
        r = requests.post(telegram_url("sendPhoto"), data=data, files={"photo": f}, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram sendPhoto {r.status_code}: {r.text[:1000]}")


def status_text():
    with state_lock:
        return (
            "BOT STATUS: ONLINE\n"
            f"Scanner: {'RUNNING' if scanner_running else 'STOPPED'}\n"
            "Market: OKX USDT-SWAP Futures\n"
            "Strategy: Trend + RSI + Volatility\n"
            "Data source: OKX Futures market data\n"
            "Scan: 15m only\n"
            f"Pending signals: {len(pending_signals)}\n"
            "TPs: 1.5R / 2.5R / 4R\n"
            "Signal gate: 7/8\n"
            "Chart: ENABLED\n"
            "TradingView: chart link only"
        )

def _error_bucket(exc):
    msg = str(exc).replace("\n", " ").strip()
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        code = getattr(resp, "status_code", None)
        if code:
            return f"HTTP {code}"
    if isinstance(exc, requests.Timeout):
        return "TIMEOUT"
    if isinstance(exc, requests.ConnectionError):
        return "CONNECTION"
    return type(exc).__name__


def scan_once():
    global pending_signals
    contracts = get_contracts()
    tickers = get_tickers()
    tv = {x.get("instId"): x for x in tickers}
    eligible = []
    for c in contracts:
        sym = c.get("instId", "")
        try:
            liquidity = float(tv.get(sym, {}).get("volCcy24h", 0))
        except (TypeError, ValueError):
            liquidity = 0.0
        if liquidity > 0:
            eligible.append((liquidity, sym))
    eligible.sort(reverse=True)
    pairs = [s for _, s in eligible[:MAX_PAIRS]]

    def check_symbol(symbol):
        try:
            rows = get_klines(symbol)
            if len(rows) < 80:
                return symbol, None, None
            return symbol, analyze(symbol, rows), None
        except Exception as e:
            return symbol, None, e

    found = []
    error_buckets = {}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = [pool.submit(check_symbol, symbol) for symbol in pairs]
        for fut in as_completed(futures):
            symbol, sig, err = fut.result()
            if err is not None:
                key = _error_bucket(err)
                error_buckets[key] = error_buckets.get(key, 0) + 1
                continue
            if sig:
                key = f"{symbol}:{sig['direction']}:{sig['time']}"
                if key not in seen_signals:
                    sig["key"] = key
                    found.append(sig)

    with state_lock:
        for sig in found:
            seen_signals.add(sig["key"])
            seen_order.append(sig["key"])
            pending_signals.append(sig)
        while len(seen_order) > 4000:
            seen_signals.discard(seen_order.pop(0))

    total_errors = sum(error_buckets.values())
    summary = ", ".join(f"{name}={count}" for name, count in sorted(error_buckets.items(), key=lambda kv: kv[1], reverse=True)[:4])
    print(f"OKX 15m scan: universe={len(eligible)}, scanned={len(pairs)}, confirmed={len(found)}, errors={total_errors}, workers={SCAN_WORKERS}")
    if summary:
        print(f"OKX error summary: {summary}")


def signal_caption(sig):
    d = "🟢 LONG" if sig["direction"] == "LONG" else "🔴 SHORT"
    checks_text = "\n".join(f"• {name}: {'YES' if ok else 'NO'}" for name, ok in sig["checks"].items())
    return (
        f"🚀 NEW CONFIRMED SIGNAL\n\n{d}\n"
        f"⭐ {sig['symbol']} (OKX Futures)\n"
        f"⏱ Timeframe: 15m\n"
        f"📊 Analysis: Trend + RSI + Volatility\n"
        f"🔗 Data: OKX Futures • TradingView chart\n\n"
        f"📈 ANALYSIS\n"
        f"• Trend: {sig['trend15']}\n"
        f"• Structure: {sig['structure']}\n"
        f"• RSI: {sig['rsi']:.1f}\n"
        f"• Volatility: {sig['volatility']}\n"
        f"• Volume: {sig['volume_ratio']:.2f}x\n"
        f"• Bias: {sig['direction']}\n"
        f"• Entry Zone: {fmt_price(sig['entry'] * 0.998)} – {fmt_price(sig['entry'] * 1.002)}\n"
        f"• Stop Loss: {fmt_price(sig['sl'])}\n"
        f"• Take Profit 1: {fmt_price(sig['tp1'])} (R:R 1:1.5)\n"
        f"• Take Profit 2: {fmt_price(sig['tp2'])} (R:R 1:2.5)\n"
        f"• Take Profit 3: {fmt_price(sig['tp3'])} (R:R 1:4)\n\n"
        f"📋 SCORE: {sig['score']}/{sig['max_score']}\n"
        f"🎯 CONFIDENCE: {sig['confidence']}%\n\n"
        f"Checks:\n{checks_text}\n\n"
        "⚠️ Signal only — no automatic trading."
    )

def scanner_loop():
    global scanner_running
    scanner_running=True
    while not stop_event.is_set():
        try: scan_once()
        except Exception as e: print(f"SCAN LOOP ERROR {type(e).__name__}: {e}")
        force_scan_event.clear()
        for _ in range(SCAN_INTERVAL):
            if stop_event.is_set() or force_scan_event.is_set(): break
            time.sleep(1)
    scanner_running=False


def sender_loop():
    global next_send_at
    while True:
        time.sleep(1)
        if not active_chat_id:
            continue
        now = time.time()
        if now < next_send_at:
            continue
        sig = None
        with state_lock:
            if pending_signals:
                sig = pending_signals.pop(0)
        if not sig:
            continue
        try:
            path = make_chart(sig)
            tv_symbol = sig["symbol"]
            markup = {"inline_keyboard": [[{"text": "📈 TradingView", "url": f"https://www.tradingview.com/chart/?symbol=OKX:{tv_symbol}"}]]}
            send_photo(active_chat_id, path, signal_caption(sig), markup)
            next_send_at = time.time() + SEND_INTERVAL
        except Exception as e:
            print(f"SEND ERROR {type(e).__name__}: {e}")


def start_scanner(chat_id):
    global scanner_thread, active_chat_id
    active_chat_id = chat_id
    with state_lock:
        running = scanner_running
    if not running:
        stop_event.clear()
        scanner_thread = threading.Thread(target=scanner_loop, daemon=True)
        scanner_thread.start()
    force_scan_event.set()


def stop_scanner():
    stop_event.set()


def poll_updates():
    global offset, active_chat_id
    while True:
        try:
            r = requests.get(telegram_url("getUpdates"), params={"timeout": 25, "offset": offset}, timeout=35)
            r.raise_for_status()
            data = r.json()
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                text = (msg.get("text") or "").strip()
                if not chat.get("id"):
                    continue
                active_chat_id = chat["id"]
                if text.startswith("/start"):
                    send_message(active_chat_id,
                        "🚀 OKX Confirmed Trend + RSI + Volatility\n\n"
                        "/scan - Start scanner\n"
                        "/stop - Stop scanner\n"
                        "/status - Bot status\n\n"
                        "Market: OKX USDT-SWAP Futures\n"
                        "Scan timeframe: 15m only\n"
                        "Confirmation: 15m + Trend + RSI + Volatility\n"
                        "TPs: 1.5R / 2.5R / 4R\n"
                        "Signal gate: 7/8")
                elif text.startswith("/scan"):
                    start_scanner(active_chat_id)
                    send_message(active_chat_id,
                        "🚀 OKX CONFIRMED SIGNAL SCANNER STARTED\n\n"
                        "Only 15m is scanned.\n"
                        "No early/radar-only alerts will be sent.\n"
                        "A signal is sent only after complete 15m + Trend + RSI + Volatility confirmation.\n"
                        "TP1 1.5R • TP2 2.5R • TP3 4R.")
                elif text.startswith("/stop"):
                    stop_scanner(); send_message(active_chat_id, "🛑 Scanner stopped.")
                elif text.startswith("/status"):
                    send_message(active_chat_id, status_text())
        except Exception as e:
            print(f"TELEGRAM ERROR {type(e).__name__}: {e}")
            time.sleep(3)


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    threading.Thread(target=poll_updates, daemon=True).start()
    threading.Thread(target=sender_loop, daemon=True).start()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
