import io
import os
import time
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify

# ============================================================
# SAIWAN — 15m Trendline Breakout
# Bitget USDT-M Futures | Telegram signal bot
# Strategy: trendline + CLOSED 15m candle breakout only.
# No AI / RSI / MACD / SMC / FVG / OB / volume filters.
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip() or None

BITGET_BASE = "https://api.bitget.com"
PRODUCT_TYPE = "USDT-FUTURES"
INTERVAL = "15m"
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "25"))
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "240"))
PIVOT_WINDOW = int(os.getenv("PIVOT_WINDOW", "3"))
MIN_TOUCHES = 2
SYMBOL_COOLDOWN_MIN = int(os.getenv("SYMBOL_COOLDOWN_MIN", "180"))
BREAKOUT_BUFFER_PCT = float(os.getenv("BREAKOUT_BUFFER_PCT", "0.0005"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "0"))  # 0 = all eligible

app = Flask(__name__)
s = requests.Session()
s.headers.update({"User-Agent": "SAIWAN/TrendlineBreakout"})

state = {
    "running": True,
    "last_scan": None,
    "last_error": None,
    "signals_sent": 0,
    "symbols": 0,
    "chat_id": CHAT_ID,
}

seen_keys = set()
last_signal_by_symbol = {}
last_candle_by_symbol = {}
telegram_offset = None


def bitget(path, params=None, timeout=15):
    r = s.get(BITGET_BASE + path, params=params, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if j.get("code") != "00000":
        raise RuntimeError(f"Bitget {j.get('code')}: {j.get('msg')}")
    return j.get("data", [])


def get_symbols():
    rows = bitget("/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    out = []
    for x in rows:
        sym = str(x.get("symbol", "")).upper()
        quote = str(x.get("quoteCoin", "")).upper()
        status = str(x.get("symbolStatus", x.get("status", ""))).lower()
        if sym.endswith("USDT") and quote == "USDT" and status in ("normal", "online", "", "listed"):
            out.append(sym)
    out = sorted(set(out))
    return out[:MAX_SYMBOLS] if MAX_SYMBOLS else out


def get_candles(symbol):
    rows = bitget("/api/v2/mix/market/candles", {
        "symbol": symbol,
        "productType": PRODUCT_TYPE,
        "granularity": INTERVAL,
        "limit": min(CANDLE_LIMIT, 1000),
    })
    rows = sorted(rows, key=lambda x: int(x[0]))
    now_ms = int(time.time() * 1000)
    out = []
    for r in rows:
        ts = int(r[0])
        out.append({
            "ts": ts,
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
        })
    # Bitget's current candle can still be changing. Use only fully closed candles.
    interval_ms = 15 * 60 * 1000
    out = [x for x in out if x["ts"] + interval_ms <= now_ms]
    return out


def pivot_high(c, i):
    n = PIVOT_WINDOW
    if i < n or i + n >= len(c):
        return False
    h = c[i]["high"]
    return all(h > c[j]["high"] for j in range(i - n, i)) and all(h > c[j]["high"] for j in range(i + 1, i + n + 1))


def pivot_low(c, i):
    n = PIVOT_WINDOW
    if i < n or i + n >= len(c):
        return False
    lo = c[i]["low"]
    return all(lo < c[j]["low"] for j in range(i - n, i)) and all(lo < c[j]["low"] for j in range(i + 1, i + n + 1))


def pivots(c):
    highs, lows = [], []
    for i in range(PIVOT_WINDOW, len(c) - PIVOT_WINDOW):
        if pivot_high(c, i): highs.append(i)
        if pivot_low(c, i): lows.append(i)
    return highs, lows


def line_value(line, i):
    i1, p1, i2, p2 = line
    return p1 + (p2 - p1) * (i - i1) / (i2 - i1)


def near(a, b):
    return abs(a - b) <= max(abs(a) * 0.001, 1e-12)


def build_down_line(c, highs, i1, i2, before):
    # Resistance: two descending pivot highs.
    if not (i1 < i2 < before and c[i1]["high"] > c[i2]["high"]):
        return None
    line = (i1, c[i1]["high"], i2, c[i2]["high"])
    touches = 0
    for j in highs:
        if j < before and near(c[j]["high"], line_value(line, j)):
            touches += 1
    if touches < MIN_TOUCHES:
        return None
    # Before the breakout, a closed candle must not already have closed above resistance.
    for j in range(i2 + 1, before):
        if c[j]["close"] > line_value(line, j) + max(abs(c[j]["close"]) * BREAKOUT_BUFFER_PCT, 1e-12):
            return None
    return line, touches


def build_up_line(c, lows, i1, i2, before):
    # Support: two ascending pivot lows.
    if not (i1 < i2 < before and c[i1]["low"] < c[i2]["low"]):
        return None
    line = (i1, c[i1]["low"], i2, c[i2]["low"])
    touches = 0
    for j in lows:
        if j < before and near(c[j]["low"], line_value(line, j)):
            touches += 1
    if touches < MIN_TOUCHES:
        return None
    for j in range(i2 + 1, before):
        if c[j]["close"] < line_value(line, j) - max(abs(c[j]["close"]) * BREAKOUT_BUFFER_PCT, 1e-12):
            return None
    return line, touches


def best_line(c, highs, lows, before, direction):
    candidates = []
    piv = highs if direction == "down" else lows
    # Recent anchors are preferred, but the line is only accepted if it remains intact.
    piv = [i for i in piv if i < before]
    for b in range(len(piv) - 1, max(-1, len(piv) - 14), -1):
        i2 = piv[b]
        for a in range(b - 1, max(-1, b - 13), -1):
            i1 = piv[a]
            x = build_down_line(c, highs, i1, i2, before) if direction == "down" else build_up_line(c, lows, i1, i2, before)
            if x:
                line, touches = x
                # Score: more touches, then more recent second anchor, then tighter anchor span.
                score = (touches, i2, -(i2 - i1))
                candidates.append((score, line, touches))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1], candidates[0][2]


def find_breakout(symbol, c):
    if len(c) < 80:
        return None
    idx = len(c) - 1
    prev = idx - 1
    highs, lows = pivots(c)
    down = best_line(c, highs, lows, idx, "down")
    up = best_line(c, highs, lows, idx, "up")
    candle = c[idx]

    if down:
        line, touches = down
        now_line = line_value(line, idx)
        prev_line = line_value(line, prev)
        buf = max(abs(now_line) * BREAKOUT_BUFFER_PCT, 1e-12)
        if c[prev]["close"] <= prev_line and candle["close"] > now_line + buf:
            lows_before = [i for i in lows if line[2] <= i < idx]
            if lows_before:
                sl_idx = lows_before[-1]
                entry = candle["close"]
                sl = c[sl_idx]["low"]
                risk = entry - sl
                if risk > 0:
                    return make_signal(symbol, "LONG", candle, line, touches, sl_idx, entry, sl, risk)

    if up:
        line, touches = up
        now_line = line_value(line, idx)
        prev_line = line_value(line, prev)
        buf = max(abs(now_line) * BREAKOUT_BUFFER_PCT, 1e-12)
        if c[prev]["close"] >= prev_line and candle["close"] < now_line - buf:
            highs_before = [i for i in highs if line[2] <= i < idx]
            if highs_before:
                sl_idx = highs_before[-1]
                entry = candle["close"]
                sl = c[sl_idx]["high"]
                risk = sl - entry
                if risk > 0:
                    return make_signal(symbol, "SHORT", candle, line, touches, sl_idx, entry, sl, risk)
    return None


def make_signal(symbol, side, candle, line, touches, sl_idx, entry, sl, risk):
    if side == "LONG":
        tp = [entry + risk, entry + 2*risk, entry + 3*risk]
    else:
        tp = [entry - risk, entry - 2*risk, entry - 3*risk]
    return {
        "symbol": symbol, "side": side, "candle_ts": candle["ts"], "entry": entry, "sl": sl,
        "tp1": tp[0], "tp2": tp[1], "tp3": tp[2], "risk": risk,
        "line": line, "touches": touches, "sl_idx": sl_idx,
        "key": (symbol, candle["ts"], side, line[0], line[2]),
    }


def fmt(x):
    ax = abs(x)
    if ax >= 1000: return f"{x:.2f}"
    if ax >= 1: return f"{x:.4f}"
    if ax >= 0.01: return f"{x:.6f}"
    if ax >= 0.0001: return f"{x:.8f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def telegram_api(method, **kwargs):
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    r = s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", timeout=30, **kwargs)
    r.raise_for_status()
    return r.json()


def send_photo(signal, candles):
    global CHAT_ID
    if not CHAT_ID:
        return False
    image = render_chart(signal, candles)
    dt = datetime.fromtimestamp(signal["candle_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    icon = "🟢" if signal["side"] == "LONG" else "🔴"
    caption = (
        f"🚀 SAIWAN TRENDLINE BREAKOUT\n\n"
        f"{icon} {signal['side']}  •  {signal['symbol']}\n"
        f"⏱ 15m  |  Closed-candle confirmation\n"
        f"🕒 {dt}\n\n"
        f"ENTRY  {fmt(signal['entry'])}\n"
        f"SL     {fmt(signal['sl'])}\n"
        f"TP1    {fmt(signal['tp1'])}  •  1R\n"
        f"TP2    {fmt(signal['tp2'])}  •  2R\n"
        f"TP3    {fmt(signal['tp3'])}  •  3R\n\n"
        f"📐 Trendline touches: {signal['touches']}\n"
        f"🏦 Bitget USDT Perpetual\n"
        f"⚠️ Signal only — no automatic order."
    )
    symbol_tv = signal["symbol"].replace("/", "")
    markup = {"inline_keyboard": [[{"text": "📈 Open in TradingView", "url": f"https://www.tradingview.com/chart/?symbol=BITGET%3A{symbol_tv}&interval=15"}]]}
    data = {"chat_id": CHAT_ID, "caption": caption, "parse_mode": "HTML", "reply_markup": __import__("json").dumps(markup)}
    files = {"photo": (f"{signal['symbol']}_15m.png", image, "image/png")}
    telegram_api("sendPhoto", data=data, files=files)
    return True


def render_chart(signal, candles):
    # TradingView-inspired visual language: dark canvas, teal/red candles, subtle grid,
    # right-side price levels, and a single fixed trendline from the actual pivot anchors.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle

    n = min(90, len(candles))
    data = candles[-n:]
    offset = len(candles) - n
    fig, ax = plt.subplots(figsize=(13.5, 7.6), dpi=150)
    fig.patch.set_facecolor("#131722")
    ax.set_facecolor("#131722")

    for x, k in enumerate(data):
        o, h, l, cl = k["open"], k["high"], k["low"], k["close"]
        up = cl >= o
        body_color = "#26a69a" if up else "#ef5350"
        ax.vlines(x, l, h, color=body_color, linewidth=0.9, zorder=2)
        height = max(abs(cl-o), abs(cl) * 0.00002)
        ax.add_patch(Rectangle((x-0.32, min(o, cl)), 0.64, height,
                               facecolor=body_color, edgecolor=body_color, linewidth=0.5, zorder=3))

    line = signal["line"]
    # Draw exactly the mathematical line used by the detector, from its first anchor through the breakout.
    x_start = max(0, line[0] - offset)
    x_end = n - 1
    y_start = line_value(line, offset + x_start)
    y_end = line_value(line, len(candles) - 1)
    if x_end >= x_start:
        ax.plot([x_start, x_end], [y_start, y_end], color="#f0b90b", linewidth=2.2, zorder=6)

    # Entry/SL/TP levels are part of the same chart, not a second message.
    level_specs = [
        (signal["entry"], "ENTRY", "#f5f5f5", "-"),
        (signal["sl"], "SL", "#ef5350", "--"),
        (signal["tp1"], "TP1", "#26a69a", ":"),
        (signal["tp2"], "TP2", "#26a69a", ":"),
        (signal["tp3"], "TP3", "#26a69a", ":"),
    ]
    for price, label, color, style in level_specs:
        ax.axhline(price, color=color, linestyle=style, linewidth=1.0, alpha=0.9, zorder=1)
        ax.text(1.005, price, f" {label}  {fmt(price)}", transform=ax.get_yaxis_transform(),
                va="center", ha="left", color=color, fontsize=8.5,
                bbox=dict(facecolor="#131722", edgecolor="none", pad=1.5, alpha=0.92))

    bx = n - 1
    by = data[-1]["close"]
    color = "#26a69a" if signal["side"] == "LONG" else "#ef5350"
    marker = "^" if signal["side"] == "LONG" else "v"
    ax.scatter([bx], [by], s=95, marker=marker, color=color, edgecolors="#ffffff", linewidths=0.7, zorder=10)
    ax.annotate("BREAKOUT", (bx, by), xytext=(-8, 15 if signal["side"] == "LONG" else -18),
                textcoords="offset points", ha="right", color="#ffffff", fontsize=8.5,
                arrowprops=dict(arrowstyle="-", color="#8b93a1", lw=0.7))

    ax.set_title(f"{signal['symbol']}  ·  15m  ·  {signal['side']} TRENDLINE BREAKOUT",
                 loc="left", color="#f0f3f6", fontsize=15, fontweight="bold", pad=14)
    ax.text(0, 1.01, "SAIWAN  •  Bitget USDT Perpetual  •  fixed pivot trendline",
            transform=ax.transAxes, color="#8b93a1", fontsize=8.5, va="bottom")
    ax.set_xlim(-2, n + 8)
    ax.grid(True, color="#1e222d", linewidth=0.7, alpha=0.9)
    ax.tick_params(colors="#8b93a1", labelsize=8)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.yaxis.tick_right()
    ax.set_xlabel("")
    ax.set_ylabel("")
    fig.tight_layout(pad=1.2)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def telegram_poll_loop():
    global telegram_offset, CHAT_ID
    if not BOT_TOKEN:
        return
    while True:
        try:
            params = {"timeout": 20, "allowed_updates": ["message"]}
            if telegram_offset is not None:
                params["offset"] = telegram_offset
            data = telegram_api("getUpdates", params=params)
            for upd in data.get("result", []):
                telegram_offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                cid = chat.get("id")
                if cid is not None:
                    CHAT_ID = str(cid)
                    state["chat_id"] = CHAT_ID
                text = (msg.get("text") or "").strip().lower()
                if text == "/start" and CHAT_ID:
                    telegram_api("sendMessage", json={"chat_id": CHAT_ID, "text": "✅ SAIWAN is online. 15m Trendline Breakout scanner is running."})
                elif text == "/stop":
                    state["running"] = False
                    telegram_api("sendMessage", json={"chat_id": CHAT_ID, "text": "⏸ Scanner stopped. Send /scan to start."})
                elif text == "/scan":
                    state["running"] = True
                    telegram_api("sendMessage", json={"chat_id": CHAT_ID, "text": "▶️ Scanner started. Strategy: 15m Trendline Breakout only."})
                elif text == "/status":
                    telegram_api("sendMessage", json={"chat_id": CHAT_ID, "text": status_text()})
        except Exception as e:
            state["last_error"] = str(e)
            time.sleep(5)


def status_text():
    return (
        "🤖 SAIWAN STATUS\n"
        f"Bot: ONLINE\nScanner: {'RUNNING' if state['running'] else 'STOPPED'}\n"
        "Market: Bitget USDT Perpetual\n"
        "Timeframe: 15m\n"
        "Strategy: Trendline + CLOSED candle breakout\n"
        f"Symbols: {state['symbols']}\nSignals sent: {state['signals_sent']}\n"
        f"Last scan: {state['last_scan'] or '-'}"
    )


def scan_loop():
    while True:
        if state["running"]:
            try:
                symbols = get_symbols()
                state["symbols"] = len(symbols)
                for symbol in symbols:
                    try:
                        candles = get_candles(symbol)
                        if len(candles) < 80:
                            continue
                        last_ts = candles[-1]["ts"]
                        if last_candle_by_symbol.get(symbol) == last_ts:
                            continue
                        last_candle_by_symbol[symbol] = last_ts
                        signal = find_breakout(symbol, candles)
                        if not signal:
                            continue
                        if signal["key"] in seen_keys:
                            continue
                        previous = last_signal_by_symbol.get(symbol, 0)
                        if time.time() - previous < SYMBOL_COOLDOWN_MIN * 60:
                            continue
                        if send_photo(signal, candles):
                            seen_keys.add(signal["key"])
                            last_signal_by_symbol[symbol] = time.time()
                            state["signals_sent"] += 1
                    except Exception as e:
                        state["last_error"] = f"{symbol}: {e}"
                state["last_scan"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception as e:
                state["last_error"] = str(e)
        time.sleep(SCAN_SECONDS)


@app.get("/")
def home():
    return "SAIWAN Trendline Breakout ONLINE"


@app.get("/status")
def status():
    return jsonify({**state, "strategy": "15m Trendline + CLOSED candle breakout", "market": PRODUCT_TYPE})


def start_threads():
    threading.Thread(target=telegram_poll_loop, daemon=True).start()
    threading.Thread(target=scan_loop, daemon=True).start()


start_threads()
