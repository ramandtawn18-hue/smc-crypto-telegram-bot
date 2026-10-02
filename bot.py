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

# ============================================================
# SAIWAN — 1H TRENDLINE BREAKOUT CRYPTO SIGNAL BOT
# ============================================================
# Logic:
#   LONG  = 1H candle CLOSES above a descending swing-high trendline.
#   SHORT = 1H candle CLOSES below an ascending swing-low trendline.
#   ENTRY = breakout candle close.
#   SL    = last structural swing low/high before breakout.
#   TP1/2/3 = 1R / 2R / 3R.
# Market: Bitget USDT perpetual crypto futures only.
# No AI, RSI, MACD, SMC, FVG, OB, volume, Fibonacci, ATR filters.
# ============================================================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
BITGET_API = "https://api.bitget.com"
BITGET_PRODUCT = "USDT-FUTURES"
TELEGRAM_API = "https://api.telegram.org/bot"

TIMEFRAME = "1H"
CANDLE_LIMIT = 240
CHART_CANDLES = 80
PIVOT_WINDOW = 3
TOUCH_TOLERANCE_PCT = 0.0015
SCAN_INTERVAL = 45
MONITOR_INTERVAL = 8
SCAN_WORKERS = 8
HTTP_TIMEOUT = 15
MAX_PAIRS = 0  # 0 = all eligible contracts

TP1_R = 1.0
TP2_R = 2.0
TP3_R = 3.0

app = Flask(__name__)
stop_event = threading.Event()
force_scan_event = threading.Event()
state_lock = threading.RLock()

scanner_running = False
scanner_thread = None
monitor_thread = None
telegram_thread = None
active_chat_id = None
offset = None

# key -> active signal. Only one active signal per symbol is allowed.
active_signals = {}
seen_breakouts = set()
seen_order = []
signal_history = []
MAX_HISTORY = 500

stats_lock = threading.Lock()
stats = {
    "last_scan": None,
    "last_duration": 0.0,
    "universe": 0,
    "scanned": 0,
    "signals": 0,
    "errors": 0,
    "error_summary": "",
}

session = requests.Session()
session.headers.update({
    "User-Agent": "SAIWAN/1.0",
    "Accept": "application/json",
})
rate_lock = threading.Lock()
last_request = 0.0
MIN_REQUEST_INTERVAL = 0.055


def fmt_price(x):
    x = float(x)
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    if x >= 0.0001:
        return f"{x:.8f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def bitget_get(path, params=None, retries=3):
    global last_request
    last = None
    for attempt in range(retries + 1):
        try:
            with rate_lock:
                wait = MIN_REQUEST_INTERVAL - (time.monotonic() - last_request)
                if wait > 0:
                    time.sleep(wait)
                last_request = time.monotonic()
            r = session.get(BITGET_API + path, params=params or {}, timeout=HTTP_TIMEOUT)
            if r.status_code == 429 or r.status_code in (403, 418, 500, 502, 503, 504):
                if attempt < retries:
                    time.sleep(min(1.0 * (attempt + 1), 5.0))
                    continue
            r.raise_for_status()
            data = r.json()
            if str(data.get("code")) != "00000":
                raise RuntimeError(f"Bitget {data.get('code')}: {data.get('msg', 'unknown')}")
            return data
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last = exc
            if attempt < retries:
                time.sleep(min(0.8 * (attempt + 1), 4.0))
    raise last or RuntimeError("Bitget request failed")


def get_contracts():
    payload = bitget_get(
        "/api/v2/mix/market/contracts",
        {"productType": BITGET_PRODUCT},
    )
    out = []
    for x in payload.get("data") or []:
        symbol = str(x.get("symbol", "")).upper()
        if (
            symbol.endswith("USDT")
            and x.get("quoteCoin") == "USDT"
            and str(x.get("symbolType", "")).lower() == "perpetual"
            and str(x.get("symbolStatus", "")).lower() == "normal"
            and str(x.get("isRwa", "NO")).upper() != "YES"
        ):
            out.append(x)
    return out


def get_tickers():
    payload = bitget_get(
        "/api/v2/mix/market/tickers",
        {"productType": BITGET_PRODUCT},
    )
    return payload.get("data") or []


def get_last_price(symbol):
    payload = bitget_get(
        "/api/v2/mix/market/ticker",
        {"productType": BITGET_PRODUCT, "symbol": symbol},
    )
    data = payload.get("data") or []
    if not data:
        return None
    try:
        return float(data[0]["lastPr"])
    except (KeyError, TypeError, ValueError):
        return None


def get_klines(symbol, limit=CANDLE_LIMIT):
    payload = bitget_get(
        "/api/v2/mix/market/candles",
        {
            "symbol": symbol,
            "productType": BITGET_PRODUCT,
            "granularity": TIMEFRAME,
            "limit": min(limit, 1000),
            "kLineType": "market",
        },
    )
    raw = payload.get("data") or []
    now_ms = int(time.time() * 1000)
    candle_ms = 60 * 60 * 1000
    rows = []
    for v in raw:
        try:
            if len(v) < 6:
                continue
            ts = int(v[0])
            # Never use the currently forming 1H candle.
            if ts + candle_ms > now_ms:
                continue
            rows.append({
                "time": ts // 1000,
                "open": float(v[1]),
                "high": float(v[2]),
                "low": float(v[3]),
                "close": float(v[4]),
                "vol": float(v[5]),
            })
        except (TypeError, ValueError, IndexError):
            continue
    rows.sort(key=lambda r: r["time"])
    return rows[-limit:]


def swing_points(rows, left=PIVOT_WINDOW, right=PIVOT_WINDOW):
    highs, lows = [], []
    for i in range(left, len(rows) - right):
        h = rows[i]["high"]
        l = rows[i]["low"]
        if all(h > rows[j]["high"] for j in range(i-left, i)) and all(h >= rows[j]["high"] for j in range(i+1, i+right+1)):
            highs.append((i, h))
        if all(l < rows[j]["low"] for j in range(i-left, i)) and all(l <= rows[j]["low"] for j in range(i+1, i+right+1)):
            lows.append((i, l))
    return highs, lows


def line_value(p1, p2, x):
    i1, y1 = p1
    i2, y2 = p2
    if i2 == i1:
        return y2
    return y1 + (y2 - y1) * ((x - i1) / (i2 - i1))


def _line_is_clean(rows, p1, p2, side):
    """Reject a trendline that was already closed-through before the break."""
    start = p1[0]
    end = min(p2[0], len(rows) - 1)
    tol = TOUCH_TOLERANCE_PCT
    for i in range(start + 1, end + 1):
        lv = line_value(p1, p2, i)
        c = rows[i]["close"]
        if side == "LONG" and c > lv * (1 + tol):
            return False
        if side == "SHORT" and c < lv * (1 - tol):
            return False
    return True


def _touch_count(rows, pivots, p1, p2):
    lo, hi = p1[0], p2[0]
    tol = TOUCH_TOLERANCE_PCT
    count = 0
    for idx, price in pivots:
        if lo <= idx <= hi:
            lv = line_value(p1, p2, idx)
            if abs(price - lv) / max(abs(lv), 1e-12) <= tol:
                count += 1
    return count


def best_downtrend_line(rows):
    highs, _ = swing_points(rows)
    if len(highs) < 2:
        return None
    candidates = []
    # Recent pivots are more useful, but we evaluate all pairs in the visible window.
    for a in range(max(0, len(highs) - 12), len(highs) - 1):
        for b in range(a + 1, len(highs)):
            p1, p2 = highs[a], highs[b]
            if p2[1] >= p1[1]:
                continue
            span = p2[0] - p1[0]
            if span < 5:
                continue
            if not _line_is_clean(rows, p1, p2, "LONG"):
                continue
            touches = _touch_count(rows, highs, p1, p2)
            if touches < 2:
                continue
            candidates.append((touches, p2[0], -span, p1, p2))
    if not candidates:
        return None
    _, _, _, p1, p2 = max(candidates)
    return {"p1": p1, "p2": p2, "touches": _touch_count(rows, highs, p1, p2), "side": "LONG"}


def best_uptrend_line(rows):
    _, lows = swing_points(rows)
    if len(lows) < 2:
        return None
    candidates = []
    for a in range(max(0, len(lows) - 12), len(lows) - 1):
        for b in range(a + 1, len(lows)):
            p1, p2 = lows[a], lows[b]
            if p2[1] <= p1[1]:
                continue
            span = p2[0] - p1[0]
            if span < 5:
                continue
            if not _line_is_clean(rows, p1, p2, "SHORT"):
                continue
            touches = _touch_count(rows, lows, p1, p2)
            if touches < 2:
                continue
            candidates.append((touches, p2[0], -span, p1, p2))
    if not candidates:
        return None
    _, _, _, p1, p2 = max(candidates)
    return {"p1": p1, "p2": p2, "touches": _touch_count(rows, lows, p1, p2), "side": "SHORT"}


def breakout_signal(rows, symbol):
    if len(rows) < 80:
        return None
    # The last row is the newest CLOSED candle.
    cur_i = len(rows) - 1
    prev_i = cur_i - 1

    down = best_downtrend_line(rows)
    if down:
        p1, p2 = down["p1"], down["p2"]
        if p2[0] < prev_i:
            prev_line = line_value(p1, p2, prev_i)
            cur_line = line_value(p1, p2, cur_i)
            if rows[prev_i]["close"] <= prev_line and rows[cur_i]["close"] > cur_line:
                lows, _ = swing_points(rows[:cur_i], PIVOT_WINDOW, PIVOT_WINDOW)
                # Last swing LOW before breakout, after the second trendline anchor.
                _, pivot_lows = swing_points(rows[:cur_i], PIVOT_WINDOW, PIVOT_WINDOW)
                valid = [(i, p) for i, p in pivot_lows if p2[0] <= i < cur_i]
                if valid:
                    sl_idx, sl = valid[-1]
                    entry = rows[cur_i]["close"]
                    risk = entry - sl
                    if risk > 0:
                        return make_signal(symbol, "LONG", rows, cur_i, down, entry, sl, sl_idx)

    up = best_uptrend_line(rows)
    if up:
        p1, p2 = up["p1"], up["p2"]
        if p2[0] < prev_i:
            prev_line = line_value(p1, p2, prev_i)
            cur_line = line_value(p1, p2, cur_i)
            if rows[prev_i]["close"] >= prev_line and rows[cur_i]["close"] < cur_line:
                _, pivot_lows = swing_points(rows[:cur_i], PIVOT_WINDOW, PIVOT_WINDOW)
                highs, _ = swing_points(rows[:cur_i], PIVOT_WINDOW, PIVOT_WINDOW)
                valid = [(i, p) for i, p in highs if p2[0] <= i < cur_i]
                if valid:
                    sl_idx, sl = valid[-1]
                    entry = rows[cur_i]["close"]
                    risk = sl - entry
                    if risk > 0:
                        return make_signal(symbol, "SHORT", rows, cur_i, up, entry, sl, sl_idx)
    return None


def make_signal(symbol, direction, rows, breakout_idx, line, entry, sl, sl_idx):
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    if direction == "LONG":
        tp1, tp2, tp3 = entry + risk * TP1_R, entry + risk * TP2_R, entry + risk * TP3_R
    else:
        tp1, tp2, tp3 = entry - risk * TP1_R, entry - risk * TP2_R, entry - risk * TP3_R
    return {
        "key": f"{symbol}:{rows[breakout_idx]['time']}:{direction}:{line['p1'][0]}:{line['p2'][0]}",
        "symbol": symbol,
        "direction": direction,
        "time": rows[breakout_idx]["time"],
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "risk": risk,
        "line": line,
        "sl_idx": sl_idx,
        "breakout_idx": breakout_idx,
        "rows": rows[-CHART_CANDLES:],
        "touches": line["touches"],
        "timeframe": TIMEFRAME,
    }


def scan_symbol(symbol):
    try:
        rows = get_klines(symbol)
        return symbol, breakout_signal(rows, symbol), None
    except Exception as exc:
        return symbol, None, f"{type(exc).__name__}: {exc}"


def scan_once():
    started = time.monotonic()
    errors = []
    try:
        contracts = get_contracts()
        symbols = [str(x["symbol"]).upper() for x in contracts]
        if MAX_PAIRS > 0:
            tickers = get_tickers()
            volumes = {}
            for t in tickers:
                try:
                    volumes[str(t.get("symbol", "")).upper()] = float(t.get("quoteVolume", 0))
                except Exception:
                    pass
            symbols.sort(key=lambda s: volumes.get(s, 0), reverse=True)
            symbols = symbols[:MAX_PAIRS]

        with stats_lock:
            stats["universe"] = len(symbols)

        found = []
        with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
            futures = [pool.submit(scan_symbol, s) for s in symbols]
            for fut in as_completed(futures):
                symbol, sig, err = fut.result()
                if err:
                    errors.append(f"{symbol}: {err}")
                elif sig:
                    found.append(sig)

        for sig in sorted(found, key=lambda x: x["time"]):
            handle_candidate(sig)

        with stats_lock:
            stats["scanned"] = len(symbols)
            stats["signals"] += len(found)
            stats["errors"] = len(errors)
            stats["error_summary"] = " | ".join(errors[:3])
            stats["last_scan"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            stats["last_duration"] = round(time.monotonic() - started, 2)
        print(f"SAIWAN 1H scan: universe={len(symbols)} found={len(found)} errors={len(errors)} duration={time.monotonic()-started:.1f}s")
    except Exception as exc:
        with stats_lock:
            stats["errors"] += 1
            stats["error_summary"] = f"{type(exc).__name__}: {exc}"
            stats["last_duration"] = round(time.monotonic() - started, 2)
        print(f"SCAN ERROR: {type(exc).__name__}: {exc}")


def handle_candidate(sig):
    with state_lock:
        # Never repeat the same breakout candle/trendline.
        if sig["key"] in seen_breakouts:
            return
        seen_breakouts.add(sig["key"])
        seen_order.append(sig["key"])
        if len(seen_order) > 3000:
            old = seen_order.pop(0)
            seen_breakouts.discard(old)

        # Avoid overlapping signals on the same symbol. Once TP3/SL closes it,
        # a later, new closed-candle breakout may create a fresh signal.
        if sig["symbol"] in {v["symbol"] for v in active_signals.values()}:
            return

    if not active_chat_id:
        print(f"SIGNAL READY but no Telegram chat yet: {sig['symbol']} {sig['direction']}")
        return

    try:
        path = make_chart(sig)
        tv = tradingview_url(sig["symbol"])
        markup = {"inline_keyboard": [[{"text": "📈 Open in TradingView", "url": tv}]]}
        message_id = send_photo(active_chat_id, path, signal_caption(sig), markup)
        if message_id:
            with state_lock:
                active_signals[sig["key"]] = {
                    "key": sig["key"],
                    "symbol": sig["symbol"],
                    "direction": sig["direction"],
                    "entry": sig["entry"],
                    "sl": sig["sl"],
                    "tp1": sig["tp1"],
                    "tp2": sig["tp2"],
                    "tp3": sig["tp3"],
                    "chat_id": active_chat_id,
                    "message_id": message_id,
                    "tp1_hit": False,
                    "tp2_hit": False,
                    "tp3_hit": False,
                    "sl_hit": False,
                    "created": time.time(),
                }
                signal_history.append(sig.copy())
                if len(signal_history) > MAX_HISTORY:
                    signal_history.pop(0)
        print(f"SENT {sig['symbol']} {sig['direction']} entry={fmt_price(sig['entry'])}")
    except Exception as exc:
        print(f"SEND ERROR {sig['symbol']}: {type(exc).__name__}: {exc}")


def signal_caption(sig):
    side = "🟢 LONG" if sig["direction"] == "LONG" else "🔴 SHORT"
    return (
        "🚀 SAIWAN CONFIRMED SIGNAL\n\n"
        f"{side}\n"
        f"⭐ {sig['symbol']} · Bitget Futures\n"
        f"⏱ Timeframe: {TIMEFRAME}\n"
        "📐 Setup: Trendline Breakout\n"
        f"🔗 Trendline touches: {sig['touches']}\n\n"
        f"ENTRY: {fmt_price(sig['entry'])}\n"
        f"SL: {fmt_price(sig['sl'])}\n"
        f"TP1: {fmt_price(sig['tp1'])}  (1R)\n"
        f"TP2: {fmt_price(sig['tp2'])}  (2R)\n"
        f"TP3: {fmt_price(sig['tp3'])}  (3R)\n\n"
        "✅ Confirmation: CLOSED 1H candle beyond trendline\n"
        "⚠️ Signal only — no automatic trading."
    )


def monitor_active_signals():
    while not stop_event.is_set():
        try:
            with state_lock:
                signals = [x.copy() for x in active_signals.values()]
            if signals:
                tickers = get_tickers()
                prices = {}
                for t in tickers:
                    try:
                        prices[str(t.get("symbol", "")).upper()] = float(t["lastPr"])
                    except Exception:
                        pass
                for sig in signals:
                    price = prices.get(sig["symbol"])
                    if price is not None:
                        check_signal_event(sig["key"], price)
        except Exception as exc:
            print(f"MONITOR ERROR: {type(exc).__name__}: {exc}")
        for _ in range(MONITOR_INTERVAL):
            if stop_event.is_set():
                break
            time.sleep(1)


def check_signal_event(key, price):
    with state_lock:
        sig = active_signals.get(key)
        if not sig:
            return
        direction = sig["direction"]
        hit = None
        close_after = False
        if direction == "LONG":
            if not sig["sl_hit"] and price <= sig["sl"]:
                sig["sl_hit"] = True
                hit = ("SL", sig["sl"])
                close_after = True
            elif not sig["tp1_hit"] and price >= sig["tp1"]:
                sig["tp1_hit"] = True
                hit = ("TP1", sig["tp1"])
            elif not sig["tp2_hit"] and price >= sig["tp2"]:
                sig["tp2_hit"] = True
                hit = ("TP2", sig["tp2"])
            elif not sig["tp3_hit"] and price >= sig["tp3"]:
                sig["tp3_hit"] = True
                hit = ("TP3", sig["tp3"])
                close_after = True
        else:
            if not sig["sl_hit"] and price >= sig["sl"]:
                sig["sl_hit"] = True
                hit = ("SL", sig["sl"])
                close_after = True
            elif not sig["tp1_hit"] and price <= sig["tp1"]:
                sig["tp1_hit"] = True
                hit = ("TP1", sig["tp1"])
            elif not sig["tp2_hit"] and price <= sig["tp2"]:
                sig["tp2_hit"] = True
                hit = ("TP2", sig["tp2"])
            elif not sig["tp3_hit"] and price <= sig["tp3"]:
                sig["tp3_hit"] = True
                hit = ("TP3", sig["tp3"])
                close_after = True
        chat_id = sig["chat_id"]
        message_id = sig["message_id"]
        snapshot = sig.copy()

    if hit:
        level, level_price = hit
        side = "🟢 LONG" if snapshot["direction"] == "LONG" else "🔴 SHORT"
        if level == "SL":
            text = (
                f"🛑 STOP LOSS HIT\n\n{side}\n⭐ {snapshot['symbol']}\n"
                f"Level: {fmt_price(level_price)}\nObserved: {fmt_price(price)}\n\n"
                "Signal closed — monitoring stopped."
            )
        else:
            text = (
                f"🎯 {level} HIT\n\n{side}\n⭐ {snapshot['symbol']}\n"
                f"Target: {fmt_price(level_price)}\nObserved: {fmt_price(price)}\n\n"
                + ("TP3 reached — signal closed." if level == "TP3" else "Signal remains active; next target is still monitored.")
            )
        try:
            send_message(chat_id, text, reply_to_message_id=message_id)
        except Exception as exc:
            print(f"TP/SL TELEGRAM ERROR: {type(exc).__name__}: {exc}")
        if close_after:
            with state_lock:
                active_signals.pop(key, None)


def tradingview_url(symbol):
    # Bitget symbol maps cleanly to TradingView's BITGET:<SYMBOL> futures chart.
    return f"https://www.tradingview.com/chart/?symbol=BITGET%3A{symbol}&interval=60"


def make_chart(sig):
    rows = sig["rows"]
    n = len(rows)
    fig, ax = plt.subplots(figsize=(16, 9), dpi=100, facecolor="#0b0e11")
    ax.set_facecolor("#0b0e11")

    up = "#26a69a"
    down = "#ef5350"
    grid = "#1d2329"
    text = "#e6edf3"
    muted = "#8b949e"
    line_color = "#f0b90b"
    entry_color = "#ffffff"
    tp_color = "#26a69a"
    sl_color = "#ef5350"

    width = 0.58
    for i, r in enumerate(rows):
        c = up if r["close"] >= r["open"] else down
        ax.vlines(i, r["low"], r["high"], color=c, linewidth=1.0, zorder=2)
        lo = min(r["open"], r["close"])
        body = max(abs(r["close"] - r["open"]), abs(r["close"]) * 1e-7)
        ax.add_patch(Rectangle((i - width / 2, lo), width, body,
                               facecolor=c, edgecolor=c, linewidth=0.5, zorder=3))

    # Trendline is the exact mathematical line used by the detector.
    line = sig["line"]
    offset = sig["breakout_idx"] - (len(rows) if len(rows) == CANDLE_LIMIT else sig["breakout_idx"])
    # Use index coordinates relative to visible chart window.
    full_visible_start = max(0, sig["breakout_idx"] - len(rows) + 1)
    x1 = line["p1"][0] - full_visible_start
    x2 = line["p2"][0] - full_visible_start
    xb = sig["breakout_idx"] - full_visible_start
    xend = n - 1
    if x2 < n:
        xs = [max(0, x1), xend]
        ys = [line_value(line["p1"], line["p2"], full_visible_start + xs[0]),
              line_value(line["p1"], line["p2"], full_visible_start + xs[1])]
        ax.plot(xs, ys, color=line_color, linewidth=2.2, zorder=5)
        ax.text(max(0, x1), ys[0],
                "DESCENDING RESISTANCE" if sig["direction"] == "LONG" else "ASCENDING SUPPORT",
                color=line_color, fontsize=9, fontweight="bold", va="bottom")

    # Entry / SL / TP levels.
    levels = [
        (sig["entry"], "ENTRY", entry_color, "--", 1.5),
        (sig["sl"], "SL", sl_color, "-", 1.5),
        (sig["tp1"], "TP1", tp_color, ":", 1.1),
        (sig["tp2"], "TP2", tp_color, ":", 1.1),
        (sig["tp3"], "TP3", tp_color, ":", 1.1),
    ]
    right = n + 10
    for y, label, color, style, lw in levels:
        ax.axhline(y, color=color, linestyle=style, linewidth=lw, alpha=0.95)
        ax.text(right + 0.2, y, f"{label}  {fmt_price(y)}", color=color,
                fontsize=9, fontweight="bold", va="center")

    ax.scatter([xb], [sig["entry"]], s=70,
               color=up if sig["direction"] == "LONG" else down,
               edgecolor="#ffffff", linewidth=0.9, zorder=8)
    ax.annotate("BREAKOUT", xy=(xb, sig["entry"]),
                xytext=(max(0, xb - 12), sig["entry"]),
                arrowprops=dict(arrowstyle="->", color=line_color, lw=1.6),
                color=line_color, fontsize=10, fontweight="bold")

    ax.text(0.01, 1.045,
            f"{sig['symbol']}  ·  SAIWAN  ·  {sig['direction']}  ·  1H TRENDLINE BREAKOUT",
            transform=ax.transAxes, color=text, fontsize=15, fontweight="bold")
    ax.text(0.01, 1.015,
            "CLOSED-CANDLE CONFIRMATION  •  ENTRY = BREAKOUT CLOSE  •  STRUCTURAL SL  •  1R / 2R / 3R",
            transform=ax.transAxes, color=muted, fontsize=9)
    ax.text(0.99, 1.045, "BITGET USDT PERPETUAL", transform=ax.transAxes,
            color=muted, fontsize=9, ha="right", fontweight="bold")

    ax.grid(axis="y", color=grid, linewidth=0.65)
    ax.grid(axis="x", color=grid, linewidth=0.35, alpha=0.5)
    ax.tick_params(axis="both", colors=muted, labelsize=8, length=0)
    for side in ["top", "left", "bottom", "right"]:
        ax.spines[side].set_visible(False)
    ax.yaxis.tick_right()

    step = max(1, n // 8)
    ticks = list(range(0, n, step))
    if ticks[-1] != n - 1:
        ticks.append(n - 1)
    ax.set_xticks(ticks)
    ax.set_xticklabels([
        datetime.fromtimestamp(rows[i]["time"], tz=timezone.utc).strftime("%d %b\\n%H:%M")
        for i in ticks
    ])

    lows = [r["low"] for r in rows] + [sig["sl"], sig["tp3"]]
    highs = [r["high"] for r in rows] + [sig["entry"], sig["tp3"]]
    ymin, ymax = min(lows), max(highs)
    span = max(ymax - ymin, abs(rows[-1]["close"]) * 0.01)
    ax.set_ylim(ymin - span * 0.08, ymax + span * 0.10)
    ax.set_xlim(-1, right + 4)
    fig.subplots_adjust(left=0.035, right=0.86, top=0.88, bottom=0.09)

    safe = "".join(c if c.isalnum() else "_" for c in sig["symbol"])
    path = f"/tmp/saiwan_{safe}_{sig['time']}.png"
    fig.savefig(path, dpi=100, facecolor="#0b0e11", edgecolor="none")
    plt.close(fig)
    return path


def telegram_url(method):
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    return TELEGRAM_API + TOKEN + "/" + method


def send_message(chat_id, text, reply_markup=None, reply_to_message_id=None):
    data = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    if reply_to_message_id is not None:
        data["reply_to_message_id"] = str(reply_to_message_id)
    r = requests.post(telegram_url("sendMessage"), data=data, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram sendMessage {r.status_code}: {r.text[:500]}")
    return (r.json().get("result") or {}).get("message_id")


def send_photo(chat_id, path, caption, reply_markup=None):
    data = {"chat_id": chat_id, "caption": caption}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    with open(path, "rb") as f:
        r = requests.post(telegram_url("sendPhoto"), data=data,
                          files={"photo": f}, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram sendPhoto {r.status_code}: {r.text[:1000]}")
    return (r.json().get("result") or {}).get("message_id")


def status_text():
    with state_lock:
        active = list(active_signals.values())
        chat = active_chat_id
    with stats_lock:
        s = stats.copy()
    lines = [
        "🚀 SAIWAN STATUS",
        "",
        f"Scanner: {'RUNNING' if scanner_running else 'STOPPED'}",
        f"Market: CRYPTO ONLY — Bitget USDT perpetual",
        f"Timeframe: {TIMEFRAME}",
        f"Universe: {s['universe']}",
        f"Last scan: {s['last_scan'] or '—'}",
        f"Last duration: {s['last_duration']}s",
        f"Last scan found: {s['signals']}",
        f"Active signals: {len(active)}",
        f"Telegram chat: {'connected' if chat else 'not detected'}",
        "",
        "Logic: trendline close breakout only",
        "Entry: breakout candle close",
        "SL: last structural swing",
        "TP: 1R / 2R / 3R",
        "TP/SL monitor: ENABLED",
    ]
    if s["error_summary"]:
        lines += ["", f"Last errors: {s['error_summary'][:500]}"]
    return "\n".join(lines)


def start_scanner(chat_id):
    global scanner_thread, active_chat_id, scanner_running
    active_chat_id = chat_id
    if not scanner_running:
        stop_event.clear()
        scanner_thread = threading.Thread(target=scanner_loop, name="saiwan-scanner", daemon=True)
        scanner_thread.start()
    force_scan_event.set()


def stop_scanner():
    stop_event.set()


def scanner_loop():
    global scanner_running
    scanner_running = True
    while not stop_event.is_set():
        try:
            scan_once()
        except Exception as exc:
            print(f"SCANNER LOOP ERROR: {type(exc).__name__}: {exc}")
        force_scan_event.clear()
        for _ in range(SCAN_INTERVAL):
            if stop_event.is_set() or force_scan_event.is_set():
                break
            time.sleep(1)
    scanner_running = False


def poll_updates():
    global offset, active_chat_id
    conflict_wait = 3
    while True:
        try:
            r = requests.get(
                telegram_url("getUpdates"),
                params={"timeout": 25, "offset": offset,
                        "allowed_updates": json.dumps(["message"])},
                timeout=35,
            )
            if r.status_code == 409:
                print("TELEGRAM 409: another poller is active; retrying")
                time.sleep(conflict_wait)
                conflict_wait = min(conflict_wait * 2, 30)
                continue
            r.raise_for_status()
            conflict_wait = 3
            for upd in r.json().get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                text = (msg.get("text") or "").strip()
                if not chat.get("id"):
                    continue
                active_chat_id = chat["id"]
                if text.startswith("/start"):
                    send_message(active_chat_id,
                        "🚀 SAIWAN — 1H Trendline Breakout\n\n"
                        "/scan — start scanner\n"
                        "/stop — stop scanner\n"
                        "/status — status\n\n"
                        "Market: CRYPTO ONLY\n"
                        "Exchange: Bitget USDT Perpetual Futures\n"
                        "Timeframe: 1H only\n"
                        "LONG: closed candle above descending trendline\n"
                        "SHORT: closed candle below ascending trendline\n"
                        "TP/SL monitoring: ENABLED")
                elif text.startswith("/scan"):
                    start_scanner(active_chat_id)
                    send_message(active_chat_id,
                        "🚀 SAIWAN scanner started.\n\n"
                        "CRYPTO ONLY • USDT perpetuals • 1H CLOSED candles\n"
                        "Trendline breakout only.\n"
                        "Entry = breakout close • SL = structural swing • TP1/2/3 = 1R/2R/3R.\n"
                        "TP/SL notifications are enabled.")
                elif text.startswith("/stop"):
                    stop_scanner()
                    send_message(active_chat_id, "🛑 SAIWAN scanner stopped.")
                elif text.startswith("/status"):
                    send_message(active_chat_id, status_text())
        except Exception as exc:
            print(f"TELEGRAM ERROR: {type(exc).__name__}: {exc}")
            time.sleep(3)


def start_background_services():
    global telegram_thread, monitor_thread
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    try:
        requests.post(telegram_url("deleteWebhook"), data={"drop_pending_updates": "false"}, timeout=10)
    except Exception as exc:
        print(f"Webhook cleanup warning: {type(exc).__name__}: {exc}")
    telegram_thread = threading.Thread(target=poll_updates, name="telegram-poller", daemon=True)
    monitor_thread = threading.Thread(target=monitor_active_signals, name="tp-sl-monitor", daemon=True)
    telegram_thread.start()
    monitor_thread.start()


@app.get("/")
def health():
    return jsonify({"ok": True, "bot": "SAIWAN", "market": "crypto-only", "timeframe": TIMEFRAME})


@app.get("/status")
def health_status():
    return jsonify({
        "ok": True,
        "scanner_running": scanner_running,
        "active_signals": len(active_signals),
        "stats": stats,
    })


_services_started = False
_services_lock = threading.Lock()


def ensure_services():
    global _services_started
    if _services_started:
        return
    with _services_lock:
        if _services_started:
            return
        start_background_services()
        _services_started = True


# Gunicorn imports bot:app, so initialize the background workers at import time.
ensure_services()
