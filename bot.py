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
MEXC_API = "https://api.mexc.com"


def fmt_price(x):
    x = float(x)
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")
TELEGRAM_API = "https://api.telegram.org/bot"

TF_15M = "Min15"
TIMEFRAME = TF_15M
CANDLE_LIMIT = 220
MAX_PAIRS = 120
SCAN_WORKERS = 5
SCAN_INTERVAL = 60
SEND_INTERVAL = 60
CHART_CANDLES = 70
HTTP_TIMEOUT = 20
MIN_SCORE = 8

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
session.headers.update({"User-Agent": "SMC-Fib-Telegram-Bot/1.0", "Accept": "application/json"})


def api_get(path, params=None, retries=2):
    last = None
    for attempt in range(retries + 1):
        try:
            r = session.get(MEXC_API + path, params=params or {}, timeout=HTTP_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(min(2.0 * (attempt + 1), 5.0))
                continue
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, dict) and payload.get("success") is False:
                raise RuntimeError(f"MEXC API error {payload.get('code')}: {payload.get('message')}")
            return payload
        except Exception as e:
            last = e
            if attempt < retries and isinstance(e, requests.RequestException):
                time.sleep(0.6 * (attempt + 1))
                continue
            break
    raise last or RuntimeError("MEXC API request failed")


def get_contracts():
    last_error = None
    for path in ("/api/v1/contract/detail", "/api/v1/contract/detail/country"):
        try:
            payload = api_get(path)
            data = payload.get("data", []) if isinstance(payload, dict) else []
            if isinstance(data, dict):
                data = [data]
            return [x for x in data if isinstance(x, dict)]
        except Exception as e:
            last_error = e
    raise last_error or RuntimeError("Cannot load MEXC contracts")


def get_tickers():
    payload = api_get("/api/v1/contract/ticker")
    data = payload.get("data", []) if isinstance(payload, dict) else []
    if isinstance(data, dict):
        data = [data]
    return [x for x in data if isinstance(x, dict)]


def get_klines(symbol, interval, limit=CANDLE_LIMIT):
    payload = api_get(f"/api/v1/contract/kline/{symbol}", {"interval": interval})
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        return []
    keys = ("time", "open", "high", "low", "close", "vol")
    if not all(k in data for k in keys):
        return []
    rows = []
    for vals in zip(*(data[k] for k in keys)):
        try:
            rows.append({
                "time": int(vals[0]), "open": float(vals[1]), "high": float(vals[2]),
                "low": float(vals[3]), "close": float(vals[4]), "vol": float(vals[5])
            })
        except Exception:
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


def htf_trend(rows):
    closes = [r["close"] for r in rows]
    if len(closes) < 55:
        return "NEUTRAL"
    e20, e50 = ema(closes, 20), ema(closes, 50)
    if e20[-1] > e50[-1] and closes[-1] > e20[-1]:
        return "BULLISH"
    if e20[-1] < e50[-1] and closes[-1] < e20[-1]:
        return "BEARISH"
    return "NEUTRAL"


def fvg_check(rows, direction):
    start = max(2, len(rows) - 12)
    for i in range(start, len(rows)):
        a, b, c = rows[i-2], rows[i-1], rows[i]
        if direction == "LONG" and c["low"] > a["high"]:
            return True, (a["high"], c["low"])
        if direction == "SHORT" and c["high"] < a["low"]:
            return True, (c["high"], a["low"])
    return False, None


def ob_check(rows, direction):
    # Find the most recent opposite candle followed by a displacement candle.
    a = atr(rows, 14) or 0
    if a <= 0:
        return False, None
    for i in range(len(rows)-2, max(3, len(rows)-16), -1):
        cur, nxt = rows[i], rows[i+1]
        body = abs(nxt["close"] - nxt["open"])
        if body < 1.15 * a:
            continue
        if direction == "LONG" and cur["close"] < cur["open"] and nxt["close"] > cur["high"]:
            return True, (cur["low"], cur["high"])
        if direction == "SHORT" and cur["close"] > cur["open"] and nxt["close"] < cur["low"]:
            return True, (cur["low"], cur["high"])
    return False, None


def fib_context(rows, direction):
    highs, lows = swing_points(rows[-100:])
    if not highs or not lows:
        return False, None, None, None
    hi_i, hi = highs[-1]
    lo_i, lo = lows[-1]
    # Use the most recent meaningful impulse.
    if direction == "LONG":
        if lo_i >= hi_i:
            return False, None, None, None
        low, high = lo, hi
        rng = high - low
        if rng <= 0:
            return False, None, None, None
        levels = {
            "0.500": high - rng*0.500,
            "0.618": high - rng*0.618,
            "0.705": high - rng*0.705,
            "0.786": high - rng*0.786,
        }
        price = rows[-1]["close"]
        near = min(levels.values(), key=lambda x: abs(price-x))
        confluence = min(abs(price-near)/rng, 1.0) <= 0.035
        return confluence, levels, low, high
    else:
        if hi_i >= lo_i:
            return False, None, None, None
        high, low = hi, lo
        rng = high - low
        if rng <= 0:
            return False, None, None, None
        levels = {
            "0.500": low + rng*0.500,
            "0.618": low + rng*0.618,
            "0.705": low + rng*0.705,
            "0.786": low + rng*0.786,
        }
        price = rows[-1]["close"]
        near = min(levels.values(), key=lambda x: abs(price-x))
        confluence = min(abs(price-near)/rng, 1.0) <= 0.035
        return confluence, levels, low, high


def analyze(symbol, rows15):
    if len(rows15) < 80:
        return None

    # Only completed 15m candles are used. The newest candle can still be forming.
    r15 = rows15[:-1]
    if len(r15) < 70:
        return None

    cur = r15[-1]
    prev = r15[-2]
    a = atr(r15, 14)
    if not a or a <= 0:
        return None

    # Reject unusually dead volatility regimes.
    recent_ranges = sorted(x["high"] - x["low"] for x in r15[-30:])
    median_range = recent_ranges[len(recent_ranges) // 2]
    if median_range <= 0 or a < 0.35 * median_range:
        return None

    highs, lows = swing_points(r15[-70:])
    if len(highs) < 2 or len(lows) < 2:
        return None
    recent_high = highs[-1][1]
    recent_low = lows[-1][1]
    prior_high = highs[-2][1]
    prior_low = lows[-2][1]

    long_bos = cur["close"] > recent_high and prev["close"] <= recent_high
    short_bos = cur["close"] < recent_low and prev["close"] >= recent_low

    # A signal starts with an actual structural break; trend alone is never enough.
    if long_bos:
        direction, structure = "LONG", "BULLISH BOS"
    elif short_bos:
        direction, structure = "SHORT", "BEARISH BOS"
    else:
        return None

    # Single-timeframe trend context: 15m EMA20/EMA50.
    trend15 = htf_trend(r15)
    trend_ok = (direction == "LONG" and trend15 == "BULLISH") or (direction == "SHORT" and trend15 == "BEARISH")

    # Liquidity sweep on the 15m structure.
    sweep = False
    if direction == "LONG":
        for _, lv in lows[-5:-1]:
            if cur["low"] < lv and cur["close"] > lv:
                sweep = True; break
    else:
        for _, hv in highs[-5:-1]:
            if cur["high"] > hv and cur["close"] < hv:
                sweep = True; break

    fvg, _ = fvg_check(r15, direction)
    ob, _ = ob_check(r15, direction)
    fib_ok, fib_levels, _, _ = fib_context(r15, direction)

    avg_vol = sum(r["vol"] for r in r15[-21:-1]) / 20.0
    volume_ok = cur["vol"] >= 1.20 * avg_vol if avg_vol > 0 else False
    body = abs(cur["close"] - cur["open"])
    displacement = body >= 1.20 * a
    candle_range = cur["high"] - cur["low"]
    close_confirm = (
        cur["close"] > cur["open"] and
        candle_range > 0 and
        cur["close"] >= cur["low"] + 0.65 * candle_range
    ) if direction == "LONG" else (
        cur["close"] < cur["open"] and
        candle_range > 0 and
        cur["close"] <= cur["low"] + 0.35 * candle_range
    )

    fib_zone_ok = False
    if fib_levels:
        loz, hiz = min(fib_levels.values()), max(fib_levels.values())
        fib_zone_ok = loz - 0.02*a <= cur["close"] <= hiz + 0.02*a

    checks = {
        "15m trend alignment": trend_ok,
        "Liquidity sweep": sweep,
        "Fibonacci 0.5/0.618/0.705/0.786": fib_ok,
        "Fibonacci pullback zone": fib_zone_ok,
        "Fair Value Gap": fvg,
        "Order Block": ob,
        "15m volume expansion": volume_ok,
        "15m displacement": displacement,
        "15m close confirmation": close_confirm,
    }
    score = sum(1 for v in checks.values() if v)

    # Strict final confirmation: all core SMC/Fib conditions plus 8/9 score.
    if not trend_ok or not fib_ok:
        return None
    if not (fvg and ob and volume_ok and displacement and close_confirm):
        return None
    if score < MIN_SCORE:
        return None

    entry = cur["close"]
    if direction == "LONG":
        swing_sl = min(lows[-4:], key=lambda x: x[1])[1]
        sl = min(swing_sl, entry - 0.8*a)
        risk = entry - sl
        if risk <= 0 or risk > 3.0*a:
            return None
        tp1, tp2, tp3 = entry + TP1_R*risk, entry + TP2_R*risk, entry + TP3_R*risk
    else:
        swing_sl = max(highs[-4:], key=lambda x: x[1])[1]
        sl = max(swing_sl, entry + 0.8*a)
        risk = sl - entry
        if risk <= 0 or risk > 3.0*a:
            return None
        tp1, tp2, tp3 = entry - TP1_R*risk, entry - TP2_R*risk, entry - TP3_R*risk

    return {
        "symbol": symbol, "direction": direction, "structure": structure,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "score": score, "max_score": len(checks), "trend15": trend15,
        "fib_levels": fib_levels or {}, "fvg": fvg, "ob": ob,
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
        body_h = max(abs(r["close"]-r["open"]), 1e-12)
        rect = Rectangle((i-width/2, body_low), width, body_h, fill=True, alpha=0.75)
        ax.add_patch(rect)
    ax.axhline(sig["entry"], linestyle="--", linewidth=1, label="Entry")
    ax.axhline(sig["sl"], linestyle="--", linewidth=1, label="SL")
    ax.axhline(sig["tp1"], linestyle=":", linewidth=1, label="TP1")
    ax.axhline(sig["tp2"], linestyle=":", linewidth=1, label="TP2")
    ax.axhline(sig["tp3"], linestyle=":", linewidth=1, label="TP3")
    for name, level in sig["fib_levels"].items():
        ax.axhline(level, linestyle="-.", linewidth=0.7, alpha=0.55)
        ax.text(len(rows)-1, level, f" Fib {name}", va="bottom", fontsize=7)
    ax.set_title(f"MEXC Futures • {sig['symbol']} • 15m • {sig['direction']} • SMC + Fibonacci")
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
            "Market: MEXC Futures USDT\n"
            "Strategy: Confirmed SMC + Fibonacci\n"
            "Scan: 15m only\n"
            f"Pending signals: {len(pending_signals)}\n"
            "TPs: 1.5R / 2.5R / 4R\n"
            "SMC/Fib gate: 8/9\n"
            "Chart: ENABLED"
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
    if "MEXC API error" in msg:
        return msg[:80]
    return type(exc).__name__


def scan_once():
    global pending_signals
    contracts = get_contracts()
    tickers = get_tickers()
    tv = {x.get("symbol"): x for x in tickers}
    eligible = []
    for c in contracts:
        sym = c.get("symbol", "")
        if (c.get("quoteCoin") != "USDT" or c.get("futureType") != 1
                or c.get("state") != 0 or not c.get("apiAllowed", True)):
            continue
        try:
            liq = float(tv.get(sym, {}).get("amount24", 0))
        except Exception:
            liq = 0.0
        eligible.append((liq, sym))
    eligible.sort(reverse=True)
    pairs = [s for _, s in eligible[:MAX_PAIRS]]

    def check_symbol(symbol):
        try:
            r15 = get_klines(symbol, TIMEFRAME, 240)
            if len(r15) < 80:
                return symbol, None, None
            return symbol, analyze(symbol, r15), None
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

    summary = ", ".join(f"{name}={count}" for name, count in sorted(error_buckets.items(), key=lambda kv: kv[1], reverse=True)[:4])
    print(f"MEXC 15m scan: universe={len(eligible)}, scanned={len(pairs)}, confirmed={len(found)}, errors={sum(error_buckets.values())}, workers={SCAN_WORKERS}")
    if summary:
        print(f"MEXC error summary: {summary}")

def signal_caption(sig):
    d="🟢 LONG" if sig["direction"]=="LONG" else "🔴 SHORT"
    fib=", ".join(f"{k}: {fmt_price(v)}" for k,v in sig["fib_levels"].items())
    return (f"🚀 MEXC CONFIRMED SMC + Fibonacci SIGNAL\n\n{d}\nPair: {sig['symbol']}\n"
            f"Entry: {fmt_price(sig['entry'])}\nSL: {fmt_price(sig['sl'])}\n"
            f"TP1: {fmt_price(sig['tp1'])} (1.5R)\nTP2: {fmt_price(sig['tp2'])} (2.5R)\nTP3: {fmt_price(sig['tp3'])} (4R)\n\n"
            f"Structure: {sig['structure']}\n15m trend: {sig['trend15']}\n"
            f"SMC/Fib Score: {sum(sig['checks'].values())}/9\n"
            f"Fibonacci: {fib}\n"
            f"FVG: {'YES' if sig['fvg'] else 'NO'} | OB: {'YES' if sig['ob'] else 'NO'}\n"
            "Confirmation: 15m + SMC + Fibonacci\n\n"
            "⚠️ Signal only — no automatic trading.")


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
            tv_symbol = sig["symbol"].replace("_", "_")
            markup = {"inline_keyboard": [[{"text": "📈 TradingView", "url": f"https://www.tradingview.com/chart/?symbol=MEXC:{tv_symbol}"}]]}
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
                        "🚀 MEXC Confirmed SMC + Fibonacci\n\n"
                        "/scan - Start scanner\n"
                        "/stop - Stop scanner\n"
                        "/status - Bot status\n\n"
                        "Market: MEXC Futures USDT\n"
                        "Scan timeframe: 15m only\n"
                        "Confirmation: 15m + SMC + Fibonacci\n"
                        "TPs: 1.5R / 2.5R / 4R\n"
                        "SMC/Fib gate: 8/9")
                elif text.startswith("/scan"):
                    start_scanner(active_chat_id)
                    send_message(active_chat_id,
                        "🚀 MEXC CONFIRMED SIGNAL SCANNER STARTED\n\n"
                        "Only 15m is scanned.\n"
                        "No early/radar-only alerts will be sent.\n"
                        "A signal is sent only after complete 15m + SMC + Fibonacci confirmation.\n"
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
