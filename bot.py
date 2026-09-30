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
BITGET_API = "https://api.bitget.com"
BITGET_PRODUCT = "USDT-FUTURES"
TELEGRAM_API = "https://api.telegram.org/bot"

TF_15M = "15m"
TIMEFRAME = TF_15M
CANDLE_LIMIT = 220
# 0 = scan every eligible Bitget USDT perpetual contract (no top-N cap)
MAX_PAIRS = 0
SCAN_WORKERS = 6
SCAN_INTERVAL = 60
SEND_INTERVAL = 600  # minimum 10 minutes between sent signals
CHART_CANDLES = 70
HTTP_TIMEOUT = 15
MIN_SCORE = 5

# SAIWAN AI Market Radar / risk-aware leverage (informational only)
RADAR_MIN_SCORE = 80
MAX_SUGGESTED_LEVERAGE = 5
MIN_SUGGESTED_LEVERAGE = 2

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
active_signals = {}  # key -> tracked signal state for TP/SL notifications
monitor_thread = None

session = requests.Session()
session.headers.update({"User-Agent": "Trend-RSI-Volatility-Telegram-Bot/4.0", "Accept": "application/json"})
bitget_rate_lock = threading.Lock()
bitget_last_request = 0.0
BITGET_MIN_REQUEST_INTERVAL = 0.06  # ~16.7 requests/sec, below Bitget's 20 req/sec/IP limit


def fmt_price(x):
    x = float(x)
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def bitget_get(path, params=None, retries=3):
    """GET a public Bitget Futures endpoint. No API key is required for market data."""
    global bitget_last_request
    last = None
    for attempt in range(retries + 1):
        try:
            # Keep the whole process below Bitget's documented 20 req/sec/IP market limit.
            with bitget_rate_lock:
                wait = BITGET_MIN_REQUEST_INTERVAL - (time.monotonic() - bitget_last_request)
                if wait > 0:
                    time.sleep(wait)
                bitget_last_request = time.monotonic()
            r = session.get(BITGET_API + path, params=params or {}, timeout=HTTP_TIMEOUT)
            if r.status_code == 429:
                if attempt < retries:
                    time.sleep(min(1.0 * (attempt + 1), 5.0))
                    continue
            if r.status_code in (403, 418, 500, 502, 503, 504):
                if attempt < retries:
                    time.sleep(min(1.5 * (attempt + 1), 6.0))
                    continue
            r.raise_for_status()
            payload = r.json()
            if not isinstance(payload, dict):
                raise RuntimeError("Bitget returned a non-object response")
            if str(payload.get("code")) != "00000":
                raise RuntimeError(f"Bitget API error {payload.get('code')}: {payload.get('msg', 'unknown')}")
            return payload
        except (requests.RequestException, ValueError, RuntimeError) as e:
            last = e
            if attempt < retries:
                time.sleep(min(0.8 * (attempt + 1), 4.0))
    raise last or RuntimeError("Bitget API request failed")


def get_contracts():
    """Return live USDT perpetual futures contracts from Bitget."""
    payload = bitget_get(
        "/api/v2/mix/market/contracts",
        {"productType": BITGET_PRODUCT},
    )
    out = []
    for x in payload.get("data") or []:
        if (
            x.get("symbolStatus") == "normal"
            and str(x.get("symbolType", "")).lower() == "perpetual"
            and x.get("quoteCoin") == "USDT"
            and x.get("symbol", "").endswith("USDT")
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


def get_klines(symbol, interval="15m", limit=CANDLE_LIMIT):
    payload = bitget_get(
        "/api/v2/mix/market/candles",
        {
            "symbol": symbol,
            "productType": BITGET_PRODUCT,
            "granularity": interval,
            "limit": min(limit, 1000),
            "kLineType": "market",
        },
    )
    raw = payload.get("data") or []
    now_ms = int(time.time() * 1000)
    candle_ms = 15 * 60 * 1000
    rows = []
    for v in raw:
        try:
            if len(v) < 6:
                continue
            ts = int(v[0])
            # Exclude the currently forming 15m candle.
            if ts + candle_ms > now_ms:
                continue
            rows.append({
                "time": ts // 1000,
                "open": float(v[1]),
                "high": float(v[2]),
                "low": float(v[3]),
                "close": float(v[4]),
                "vol": float(v[5]),
                "turnover": float(v[6]) if len(v) > 6 else 0.0,
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
    """Return a real breakout/breakdown signal and the matching trendline level.

    LONG uses descending swing-high resistance.
    SHORT uses ascending swing-low support.
    This keeps the signal check and the displayed structure label consistent.
    """
    window = rows[-90:]
    highs, lows = swing_points(window, left=2, right=2)
    now = len(window) - 1
    prev_i = now - 1

    if direction == "LONG" and len(highs) >= 2:
        p1, p2 = highs[-2], highs[-1]
        if p2[1] < p1[1]:
            line_now = line_value(p1, p2, now)
            line_prev = line_value(p1, p2, prev_i)
            crossed = window[-2]["close"] <= line_prev and window[-1]["close"] > line_now
            return crossed, line_now

    if direction == "SHORT" and len(lows) >= 2:
        p1, p2 = lows[-2], lows[-1]
        if p2[1] > p1[1]:
            line_now = line_value(p1, p2, now)
            line_prev = line_value(p1, p2, prev_i)
            crossed = window[-2]["close"] >= line_prev and window[-1]["close"] < line_now
            return crossed, line_now

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



def _base_setup(rows, direction):
    """Detect the compact base/accumulation or distribution that often precedes acceleration."""
    if len(rows) < 35:
        return None
    # Keep the latest 2 closed candles out of the base so the breakout candle
    # cannot manufacture its own support/resistance zone.
    base = rows[-20:-2]
    if len(base) < 12:
        return None
    highs = [x["high"] for x in base]
    lows = [x["low"] for x in base]
    base_high = max(highs)
    base_low = min(lows)
    mid = max((base_high + base_low) / 2.0, 1e-12)
    base_range_pct = (base_high - base_low) / mid * 100.0

    # Compare base candle ranges with the current ATR. A compact range is
    # preferable, but do not require an unrealistically tiny base on volatile coins.
    base_ranges = [x["high"] - x["low"] for x in base]
    avg_base_range = sum(base_ranges) / len(base_ranges)
    a = atr(rows, 14)
    if not a:
        return None
    compression_ratio = avg_base_range / a
    compressed = compression_ratio <= 0.95
    tight_enough = base_range_pct <= 6.0 and compression_ratio <= 1.35

    highs_s, lows_s = swing_points(base, left=2, right=2)
    higher_lows = False
    lower_highs = False
    if len(lows_s) >= 2:
        higher_lows = lows_s[-1][1] > lows_s[-2][1]
    if len(highs_s) >= 2:
        lower_highs = highs_s[-1][1] < highs_s[-2][1]

    # A real base should not be a one-candle spike. The final 3 candles before
    # the trigger should remain inside the zone most of the time.
    pre = rows[-5:-1]
    inside = sum(1 for x in pre if x["close"] <= base_high * 1.003 and x["close"] >= base_low * 0.997)
    stable = inside >= 3

    if direction == "LONG":
        structure = higher_lows
        directional_bias = (base[-1]["close"] >= base[0]["close"] * 0.995)
    else:
        structure = lower_highs
        directional_bias = (base[-1]["close"] <= base[0]["close"] * 1.005)

    quality = 0
    quality += 30 if tight_enough else (15 if compressed else 0)
    quality += 25 if structure else 0
    quality += 20 if stable else 0
    quality += 15 if directional_bias else 0
    quality += 10 if base_range_pct <= 4.0 else 0

    return {
        "high": base_high,
        "low": base_low,
        "range_pct": base_range_pct,
        "compression": compression_ratio,
        "structure": structure,
        "stable": stable,
        "quality": min(100, quality),
    }


def _acceleration_metrics(rows):
    """Measure whether the latest closed candle is starting a genuine expansion."""
    if len(rows) < 25:
        return None
    cur = rows[-1]
    a = atr(rows, 14)
    if not a:
        return None
    bodies = [abs(x["close"] - x["open"]) for x in rows[-12:-1]]
    avg_body = sum(bodies) / len(bodies) if bodies else 0.0
    body_ratio = abs(cur["close"] - cur["open"]) / avg_body if avg_body > 0 else 0.0
    move_atr = abs(cur["close"] - cur["open"]) / a
    return {"body_ratio": body_ratio, "move_atr": move_atr}


def radar_score(direction, trend, rv, vol_ratio, vol_score, base_quality,
                structure_ok, breakout_ok, momentum_ok, acceleration_ok,
                extension_ok, reversal_ok):
    """Score a live setup from 0-100; this is setup quality, not win probability."""
    score = 0
    # Trend is useful context, but a pump radar must also catch reversals from a base.
    if direction == "LONG":
        score += 12 if trend == "BULLISH" else (7 if trend == "NEUTRAL" else 3)
        score += 8 if 48 <= rv <= 68 else 0
    else:
        score += 12 if trend == "BEARISH" else (7 if trend == "NEUTRAL" else 3)
        score += 8 if 32 <= rv <= 52 else 0
    score += round(base_quality * 0.20)
    score += 15 if structure_ok else 0
    score += 15 if breakout_ok else 0
    score += 10 if momentum_ok else 0
    score += 10 if acceleration_ok else 0
    score += 10 if vol_ratio >= 2.0 else (7 if vol_ratio >= 1.5 else (3 if vol_ratio >= 1.2 else 0))
    score += 5 if 4.0 <= vol_score <= 9.0 else (2 if vol_score >= 3.0 else 0)
    score += 4 if extension_ok else 0
    score += 3 if reversal_ok else 0
    return int(max(0, min(100, score)))


def suggested_leverage(radar, volatility, risk_pct):
    if volatility == "EXTREME" or radar < RADAR_MIN_SCORE:
        return 2
    if volatility == "HIGH":
        return 3 if radar < 92 else 4
    if risk_pct >= 2.5:
        return 2
    if risk_pct >= 1.7:
        return 3
    if radar >= 94 and risk_pct <= 1.0:
        return 5
    if radar >= 87 and risk_pct <= 1.5:
        return 4
    return 3


def analyze(symbol, rows15):
    """SAIWAN Pump/Acceleration Radar on 15m CLOSED candles.

    Core sequence:
      BASE -> STRUCTURE -> BREAKOUT -> VOLUME -> ACCELERATION -> SIGNAL
    It intentionally does not wait for a large move to finish. The signal is
    allowed on the first closed expansion candle when the base and expansion
    conditions agree, while an extension filter blocks late entries.
    """
    if len(rows15) < 120:
        return None
    r15 = rows15
    cur, prev = r15[-1], r15[-2]
    closes = [r["close"] for r in r15]
    a = atr(r15, 14)
    rv = rsi(closes, 14)
    if not a or not rv or a <= 0:
        return None

    trend, ema20_now, ema50_now, ema200_now = trend_context(r15)
    vol_score, volatility = volatility_score(r15)
    base_long = _base_setup(r15, "LONG")
    base_short = _base_setup(r15, "SHORT")
    accel = _acceleration_metrics(r15)
    if not base_long or not base_short or not accel:
        return None

    body = abs(cur["close"] - cur["open"])
    rng = max(cur["high"] - cur["low"], 1e-12)
    close_pos = (cur["close"] - cur["low"]) / rng
    momentum_long = cur["close"] > cur["open"] and body >= 0.35 * a and close_pos >= 0.68
    momentum_short = cur["close"] < cur["open"] and body >= 0.35 * a and close_pos <= 0.32
    acceleration_ok = accel["body_ratio"] >= 1.35 and accel["move_atr"] >= 0.45

    avg_vol = sum(x["vol"] for x in r15[-21:-1]) / 20.0
    vol_ratio = cur["vol"] / avg_vol if avg_vol > 0 else 0.0
    volume_ok = vol_ratio >= 1.35

    # The breakout is measured against the base itself, not a generic 20-bar high.
    resistance = base_long["high"]
    support = base_short["low"]
    long_break = cur["close"] > resistance and prev["close"] <= resistance
    short_break = cur["close"] < support and prev["close"] >= support

    long_extension = (cur["close"] - resistance) / a
    short_extension = (support - cur["close"]) / a
    not_extended_long = 0.0 <= long_extension <= 1.15
    not_extended_short = 0.0 <= short_extension <= 1.15

    # Avoid signals where the last several candles already made a large move.
    prior6 = r15[-7:-1]
    if prior6:
        prior_move = abs(prior6[-1]["close"] - prior6[0]["close"]) / a
    else:
        prior_move = 99.0
    reversal_long = trend in ("NEUTRAL", "BEARISH") and base_long["quality"] >= 55
    reversal_short = trend in ("NEUTRAL", "BULLISH") and base_short["quality"] >= 55
    not_late = prior_move <= 3.0

    # Full gate: the base and expansion must both exist. Trend is context, not
    # a mandatory condition, which lets the radar catch early reversal pumps.
    long_ready = (
        base_long["quality"] >= 55
        and base_long["structure"]
        and base_long["stable"]
        and long_break
        and momentum_long
        and acceleration_ok
        and volume_ok
        and not_extended_long
        and not_late
        and vol_score >= 3.5
        and 48 <= rv <= 70
    )
    short_ready = (
        base_short["quality"] >= 55
        and base_short["structure"]
        and base_short["stable"]
        and short_break
        and momentum_short
        and acceleration_ok
        and volume_ok
        and not_extended_short
        and not_late
        and vol_score >= 3.5
        and 30 <= rv <= 52
    )

    if long_ready == short_ready:
        return None
    direction = "LONG" if long_ready else "SHORT"
    base = base_long if direction == "LONG" else base_short
    breakout = long_break if direction == "LONG" else short_break
    momentum = momentum_long if direction == "LONG" else momentum_short
    extension_ok = not_extended_long if direction == "LONG" else not_extended_short
    reversal_ok = reversal_long if direction == "LONG" else reversal_short

    radar = radar_score(direction, trend, rv, vol_ratio, vol_score, base["quality"],
                        base["structure"], breakout, momentum, acceleration_ok,
                        extension_ok, reversal_ok)
    if radar < RADAR_MIN_SCORE:
        return None

    entry = cur["close"]
    if direction == "LONG":
        # Base low is the invalidation anchor; ATR gives a small buffer.
        sl = min(base["low"] - 0.12 * a, entry - 1.05 * a)
        risk = entry - sl
        if risk <= 0 or risk > 3.0 * a:
            return None
        tp1, tp2, tp3 = entry + 1.5*risk, entry + 2.5*risk, entry + 4.0*risk
        structure = "Base + breakout + acceleration"
        trigger_level = resistance
        zone_low, zone_high = base["low"], base["high"]
        checks = {
            "Base / accumulation": base["quality"] >= 55,
            "Higher-low structure": base["structure"],
            "15m breakout close": breakout,
            "Volume expansion": volume_ok,
            "Acceleration": acceleration_ok,
            "Not overextended": extension_ok,
            "RSI healthy": 48 <= rv <= 70,
            "Late-move filter": not_late,
        }
    else:
        sl = max(base["high"] + 0.12 * a, entry + 1.05 * a)
        risk = sl - entry
        if risk <= 0 or risk > 3.0 * a:
            return None
        tp1, tp2, tp3 = entry - 1.5*risk, entry - 2.5*risk, entry - 4.0*risk
        structure = "Base + breakdown + acceleration"
        trigger_level = support
        zone_low, zone_high = base["low"], base["high"]
        checks = {
            "Base / distribution": base["quality"] >= 55,
            "Lower-high structure": base["structure"],
            "15m breakdown close": breakout,
            "Volume expansion": volume_ok,
            "Acceleration": acceleration_ok,
            "Not overextended": extension_ok,
            "RSI healthy": 30 <= rv <= 52,
            "Late-move filter": not_late,
        }

    score = sum(checks.values())
    confidence = min(96, 72 + score * 3 + min(10, max(0, vol_ratio - 1.0) * 2))
    risk_pct = abs(entry - sl) / entry * 100 if entry else 99.0
    leverage = suggested_leverage(radar, volatility, risk_pct)

    return {
        "symbol": symbol,
        "direction": direction,
        "structure": structure,
        "entry": entry,
        "trigger_level": trigger_level,
        "base_low": zone_low,
        "base_high": zone_high,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "score": score,
        "max_score": len(checks),
        "trend15": trend,
        "rsi": rv,
        "volatility": volatility,
        "volatility_score": vol_score,
        "volume_ratio": vol_ratio,
        "acceleration_ratio": accel["body_ratio"],
        "base_quality": base["quality"],
        "confidence": int(confidence),
        "radar_score": radar,
        "risk_pct": risk_pct,
        "suggested_leverage": leverage,
        "ema20": ema20_now,
        "ema50": ema50_now,
        "ema200": ema200_now,
        "time": cur["time"],
        "rows": r15[-CHART_CANDLES:],
        "checks": checks,
    }


def make_chart(sig):
    """Generate the SAIWAN/Finora-inspired dark signal card.

    The Telegram caption is intentionally minimal; the image contains the
    complete setup: candles, base zone, trigger, entry, SL, TP1/TP2/TP3,
    radar/confirmation metrics and risk information.
    """
    rows = sig["rows"]
    direction = sig["direction"]
    entry, sl = sig["entry"], sig["sl"]
    tp1, tp2, tp3 = sig["tp1"], sig["tp2"], sig["tp3"]
    trigger = sig.get("trigger_level", entry)
    base_low = sig.get("base_low", min(r["low"] for r in rows[-12:]))
    base_high = sig.get("base_high", max(r["high"] for r in rows[-12:]))
    symbol = sig["symbol"]

    BG = "#071018"
    PANEL = "#0b151f"
    PANEL2 = "#0e1b26"
    GRID = "#1b2a36"
    TEXT = "#f4f7fa"
    MUTED = "#8fa0ad"
    GREEN = "#35e58b"
    RED = "#ff4d5d"
    CYAN = "#55d6ff"
    YELLOW = "#f5c84b"
    BORDER = "#233542"

    fig = plt.figure(figsize=(10.8, 15.0), dpi=150, facecolor=BG)
    gs = fig.add_gridspec(20, 12, left=.045, right=.955, top=.975, bottom=.035,
                          wspace=.55, hspace=.55)

    # Header.
    axh = fig.add_subplot(gs[0:2, :])
    axh.set_facecolor(BG); axh.axis("off")
    axh.text(.01, .72, "🚀  NEW CONFIRMED SIGNAL", color=TEXT, fontsize=18,
             fontweight="bold", va="center")
    dcolor = GREEN if direction == "LONG" else RED
    axh.text(.01, .20, direction, color="white", fontsize=23, fontweight="bold",
             va="center", bbox=dict(boxstyle="round,pad=.35", facecolor=dcolor,
                                     edgecolor=dcolor, alpha=.95))
    axh.text(.23, .24, symbol, color=TEXT, fontsize=25, fontweight="bold", va="center")
    axh.text(.23, -.08, "BITGET FUTURES  •  15m  •  CRYPTO ONLY", color=MUTED,
             fontsize=9.5, va="center")
    axh.text(.99, .72, "SAIWAN AI MARKET RADAR v3", color=CYAN, fontsize=12,
             fontweight="bold", ha="right")
    axh.text(.99, .38, "A++ CONFIRMED SETUP", color=GREEN, fontsize=10,
             fontweight="bold", ha="right")
    axh.text(.99, .08, "NO STOCKS  •  NO METALS", color=MUTED, fontsize=8.5, ha="right")

    # Left information panel.
    axl = fig.add_subplot(gs[2:16, 0:4])
    axl.set_facecolor(PANEL); axl.set_xlim(0, 1); axl.set_ylim(0, 1); axl.axis("off")
    for spine in axl.spines.values(): spine.set_visible(True); spine.set_color(BORDER)

    radar = sig.get("radar_score", sig.get("score", 0))
    confidence = sig.get("confidence", 0)
    lev = sig.get("suggested_leverage", 2)
    trend = sig.get("trend15", "N/A")
    structure = sig.get("structure", "CONFIRMED")
    vol = sig.get("volume_ratio", 0.0)
    accel = sig.get("acceleration_ratio", 0.0)
    base_q = sig.get("base_quality", 0)

    axl.text(.07, .95, "SIGNAL DETAILS", color=CYAN, fontsize=11, fontweight="bold")
    axl.plot([.07,.93],[.925,.925], color=BORDER, lw=1)

    def row(y, label, value, value_color=TEXT):
        axl.text(.07, y, label.upper(), color=MUTED, fontsize=7.5, va="top")
        axl.text(.93, y, str(value), color=value_color, fontsize=9.2,
                 fontweight="bold", ha="right", va="top")

    row(.885, "Direction", direction, dcolor)
    row(.825, "Symbol", symbol)
    row(.765, "Timeframe", "15m")
    row(.705, "Trend", trend, GREEN if trend == "BULLISH" else RED if trend == "BEARISH" else YELLOW)
    row(.645, "Structure", structure)
    row(.585, "RSI", f"{sig.get('rsi',0):.1f}")
    row(.525, "Volume", f"{vol:.2f}x", GREEN if vol >= 1.35 else YELLOW)
    row(.465, "Acceleration", f"{accel:.2f}x", GREEN if accel >= 1.35 else YELLOW)
    row(.405, "Base Quality", f"{base_q}/100")

    axl.text(.07, .335, "A++ QUALITY", color=GREEN, fontsize=10, fontweight="bold")
    axl.text(.07, .295, f"RADAR  {radar}/100", color=TEXT, fontsize=15, fontweight="bold")
    axl.text(.07, .255, f"CONFIDENCE  {confidence}%", color=CYAN, fontsize=11, fontweight="bold")
    axl.plot([.07,.93],[.225,.225], color=BORDER, lw=1)

    row(.185, "Entry", fmt_price(entry))
    row(.145, "Stop Loss", fmt_price(sl), RED)
    row(.105, "Suggested Leverage", f"{lev}x", YELLOW)
    axl.text(.07, .055, "INFORMATIONAL ONLY", color=MUTED, fontsize=7.5)

    # Main chart.
    ax = fig.add_subplot(gs[2:13, 4:12])
    ax.set_facecolor(PANEL2)
    n = len(rows); width = .58
    for i, r in enumerate(rows):
        c = GREEN if r["close"] >= r["open"] else RED
        ax.vlines(i, r["low"], r["high"], color=c, linewidth=.9, zorder=3)
        lo = min(r["open"], r["close"])
        body = max(abs(r["close"]-r["open"]), abs(r["close"])*1e-5)
        ax.add_patch(Rectangle((i-width/2, lo), width, body, facecolor=c,
                               edgecolor=c, linewidth=.4, zorder=4))

    # Base zone.
    zc = GREEN if direction == "LONG" else RED
    ax.add_patch(Rectangle((-1, base_low), n+1, max(base_high-base_low, abs(entry)*.0003),
                           facecolor=zc, edgecolor=zc, alpha=.10, linewidth=1.0, zorder=0))
    ax.text(1, base_high, "DEMAND / BASE ZONE" if direction == "LONG" else "SUPPLY / BASE ZONE",
            color=zc, fontsize=7.5, fontweight="bold", va="bottom")

    chart_right = n + max(12, int(n*.25))
    ax.axhline(trigger, color=YELLOW, linestyle="--", linewidth=1.1, alpha=.9)
    ax.text(n*.56, trigger, "  CONFIRMED BREAK / CLOSE", color=YELLOW, fontsize=7.5,
            fontweight="bold", va="bottom" if direction == "LONG" else "top")

    # Risk/reward blocks.
    box_x0, box_x1 = n-4, chart_right-1
    ax.add_patch(Rectangle((box_x0, min(entry, sl)), box_x1-box_x0, abs(sl-entry),
                           facecolor=RED, edgecolor=RED, alpha=.16, zorder=1))
    ax.add_patch(Rectangle((box_x0, min(entry, tp3)), box_x1-box_x0, abs(tp3-entry),
                           facecolor=GREEN, edgecolor=GREEN, alpha=.10, zorder=1))
    levels = [(sl,"SL",RED),(entry,"ENTRY",TEXT),(tp1,"TP1",GREEN),(tp2,"TP2",GREEN),(tp3,"TP3",GREEN)]
    for y, label, c in levels:
        ax.axhline(y, color=c, linewidth=1.0 if label in ("SL","TP3") else .75,
                   linestyle="--" if label == "ENTRY" else ":" if label.startswith("TP") else "-", alpha=.95)
        ax.text(chart_right+.3, y, f"{label}  {fmt_price(y)}", color=c, fontsize=7.7,
                fontweight="bold", va="center", ha="left", clip_on=False)

    ax.annotate(direction, xy=(n-1, entry), xytext=(n-15, entry + (tp1-entry)*.08),
                color=dcolor, fontsize=10, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=dcolor, lw=1.6))
    ax.annotate("", xy=(box_x0+(box_x1-box_x0)*.55, tp3), xytext=(box_x0+(box_x1-box_x0)*.55, entry),
                arrowprops=dict(arrowstyle="->", color=dcolor, lw=1.3))
    ax.text(box_x0+1, (entry+tp3)/2, "TP1  •  TP2  •  TP3", color=GREEN if direction=="LONG" else RED,
            fontsize=7.5, fontweight="bold", rotation=90, ha="center", va="center")

    last = rows[-1]["close"]
    ax.text(.01, 1.02, f"{symbol} PERPETUAL CONTRACT  ·  15  ·  BITGET", transform=ax.transAxes,
            color=TEXT, fontsize=11.5, fontweight="bold", va="bottom")
    ax.text(.01, .985, f"Last  {fmt_price(last)}", transform=ax.transAxes, color=MUTED, fontsize=8)
    ax.yaxis.tick_right(); ax.tick_params(axis="y", colors=MUTED, labelsize=7, length=0)
    ax.tick_params(axis="x", colors=MUTED, labelsize=6.5, length=0, pad=5)
    ax.grid(axis="y", color=GRID, linewidth=.5, alpha=.85)
    ax.grid(axis="x", color=GRID, linewidth=.35, alpha=.35)
    for side in ["top","left","bottom"]: ax.spines[side].set_visible(False)
    ax.spines["right"].set_color(BORDER)
    step=max(1,n//6); ticks=list(range(0,n,step))
    if ticks[-1] != n-1: ticks.append(n-1)
    ax.set_xticks(ticks)
    ax.set_xticklabels([datetime.fromtimestamp(rows[i]["time"],tz=timezone.utc).strftime("%d %H:%M") for i in ticks])
    all_lows=[r["low"] for r in rows]+[sl,tp3,base_low]
    all_highs=[r["high"] for r in rows]+[sl,tp3,base_high]
    ymin,ymax=min(all_lows),max(all_highs); span=max(ymax-ymin,abs(last)*.012)
    ax.set_ylim(ymin-span*.05,ymax+span*.12); ax.set_xlim(-1,chart_right+8)

    # Lower technical panels: RSI and volume, matching the reference dashboard feel.
    axr = fig.add_subplot(gs[13:15, 4:12], sharex=ax)
    axr.set_facecolor(PANEL2)
    closes=[r["close"] for r in rows]
    rvals=[]
    for i in range(len(rows)):
        val=rsi(closes[:i+1],14) if i >= 14 else None
        rvals.append(float("nan") if val is None else val)
    axr.plot(range(n), rvals, color=CYAN, linewidth=1.15)
    axr.axhline(70,color=GRID,linewidth=.6,linestyle="--"); axr.axhline(30,color=GRID,linewidth=.6,linestyle="--")
    axr.axhline(50,color=GRID,linewidth=.4)
    axr.set_ylim(0,100); axr.set_ylabel("RSI", color=MUTED, fontsize=7, rotation=0, labelpad=12)
    axr.tick_params(axis="y",colors=MUTED,labelsize=6,length=0); axr.tick_params(axis="x",colors=MUTED,labelsize=0,length=0)
    for side in ["top","left","bottom","right"]: axr.spines[side].set_color(BORDER if side=="right" else "none")
    axr.grid(axis="y",color=GRID,linewidth=.4,alpha=.6)

    axv = fig.add_subplot(gs[15:17, 4:12], sharex=ax)
    axv.set_facecolor(PANEL2)
    vols=[r["vol"] for r in rows]
    avg=sum(vols[-21:-1])/max(1,min(20,len(vols)-1)) if len(vols)>1 else 1
    vr=[v/avg if avg else 0 for v in vols]
    axv.bar(range(n), vr, width=.62, color=[GREEN if rows[i]["close"]>=rows[i]["open"] else RED for i in range(n)], alpha=.72)
    axv.axhline(1.0,color=GRID,linewidth=.6,linestyle="--")
    axv.set_ylabel("VOL", color=MUTED, fontsize=7, rotation=0, labelpad=12)
    axv.tick_params(axis="y",colors=MUTED,labelsize=6,length=0); axv.tick_params(axis="x",colors=MUTED,labelsize=0,length=0)
    for side in ["top","left","bottom","right"]: axv.spines[side].set_color(BORDER if side=="right" else "none")

    # Confirmation checklist.
    axc = fig.add_subplot(gs[17:20, :])
    axc.set_facecolor(PANEL); axc.axis("off")
    axc.text(.02,.88,"A++ CONFIRMATION CHECKLIST",color=GREEN,fontsize=10.5,fontweight="bold")
    checks=sig.get("checks",{})
    items=list(checks.items())
    # Show up to 10 checks in two columns.
    for idx,(name,ok) in enumerate(items[:10]):
        col=0 if idx<5 else 1; row_i=idx if idx<5 else idx-5
        x=.03+col*.49; y=.68-row_i*.13
        axc.text(x,y,"✓" if ok else "×",color=GREEN if ok else RED,fontsize=11,fontweight="bold",va="center")
        axc.text(x+.025,y,name,color=TEXT if ok else MUTED,fontsize=7.4,va="center")
    axc.text(.98,.12,f"{sum(bool(x) for x in checks.values())}/{len(checks)} CONFIRMED",
             color=GREEN,fontsize=10,fontweight="bold",ha="right")
    axc.text(.02,.08,"SAIWAN AI MARKET RADAR  •  100% CRYPTO FOCUS  •  SIGNAL ONLY — NO AUTOMATIC TRADING",
             color=MUTED,fontsize=6.8)

    safe_symbol="".join(ch if ch.isalnum() else "_" for ch in symbol)
    path=f"/tmp/chart_{safe_symbol}_{sig['time']}.png"
    fig.savefig(path,facecolor=BG,edgecolor="none",bbox_inches="tight")
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
    payload = r.json()
    return (payload.get("result") or {}).get("message_id")


def send_photo(chat_id, photo_path, caption, reply_markup=None):
    data = {"chat_id": chat_id, "caption": caption}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    with open(photo_path, "rb") as f:
        r = requests.post(telegram_url("sendPhoto"), data=data, files={"photo": f}, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram sendPhoto {r.status_code}: {r.text[:1000]}")
    payload = r.json()
    return (payload.get("result") or {}).get("message_id")


def status_text():
    with state_lock:
        return (
            "BOT STATUS: ONLINE\n"
            f"Scanner: {'RUNNING' if scanner_running else 'STOPPED'}\n"
            "Market: Bitget USDT Perpetual Futures (full eligible market)\n"
            "Strategy: SAIWAN Pump Radar — Base + Breakout + Acceleration\n"
            "Data source: Bitget Futures market data\n"
            "Scan: 15m only\n"
            f"Pending signals: {len(pending_signals)}\n"
            f"Tracked signals: {len(active_signals)}\n"
            "Signal cooldown: 10 minutes\n"
            "TPs: 1.5R / 2.5R / 4R\n"
            f"Radar threshold: {RADAR_MIN_SCORE}/100\n"
            "Leverage: dynamic 2x-5x (informational)\n"
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
    tv = {x.get("symbol"): x for x in tickers}
    eligible = []
    for c in contracts:
        sym = c.get("symbol", "")
        try:
            liquidity = float(tv.get(sym, {}).get("usdtVolume", tv.get(sym, {}).get("quoteVolume", 0)))
        except (TypeError, ValueError):
            liquidity = 0.0
        if liquidity > 0:
            eligible.append((liquidity, sym))
    eligible.sort(reverse=True)
    # Scan the whole eligible market by default. MAX_PAIRS > 0 can still cap it if needed.
    pairs = [s for _, s in eligible] if MAX_PAIRS <= 0 else [s for _, s in eligible[:MAX_PAIRS]]

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
        active_symbols = {x.get("symbol") for x in active_signals.values()}
        for sig in found:
            seen_signals.add(sig["key"])
            seen_order.append(sig["key"])
            # Do not queue another signal for a symbol that is already being tracked.
            if sig["symbol"] not in active_symbols:
                pending_signals.append(sig)
        # Keep only the strongest Radar candidates so the cooldown never creates
        # a backlog of stale alerts. Radar score is the primary market ranking.
        pending_signals.sort(key=lambda x: (x.get("radar_score", 0), x.get("score", 0), x.get("confidence", 0), x.get("time", 0)), reverse=True)
        del pending_signals[12:]
        while len(seen_order) > 4000:
            seen_signals.discard(seen_order.pop(0))

    total_errors = sum(error_buckets.values())
    summary = ", ".join(f"{name}={count}" for name, count in sorted(error_buckets.items(), key=lambda kv: kv[1], reverse=True)[:4])
    print(f"Bitget 15m scan: universe={len(eligible)}, scanned={len(pairs)}, confirmed={len(found)}, errors={total_errors}, workers={SCAN_WORKERS}")
    if not contracts:
        print("Bitget warning: no contracts returned from /api/v2/mix/market/contracts")
    elif not tickers:
        print("Bitget warning: no tickers returned from /api/v2/mix/market/tickers")
    if summary:
        print(f"Bitget error summary: {summary}")


def signal_caption(sig):
    direction = "🟢 LONG" if sig["direction"] == "LONG" else "🔴 SHORT"
    return f"⭐ {sig["symbol"]}\n{direction}"

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


def track_sent_signal(sig, chat_id, message_id):
    if not message_id:
        return
    with state_lock:
        active_signals[sig["key"]] = {
            "key": sig["key"],
            "chat_id": chat_id,
            "message_id": message_id,
            "symbol": sig["symbol"],
            "direction": sig["direction"],
            "entry": sig["entry"],
            "sl": sig["sl"],
            "tp1": sig["tp1"],
            "tp2": sig["tp2"],
            "tp3": sig["tp3"],
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "closed": False,
        }

def _hit_level(direction, price, level):
    return price >= level if direction == "LONG" else price <= level

def monitor_active_signals():
    global active_signals
    # Monitoring stays alive even when /stop pauses the scanner, so already-sent
    # signals can still receive TP/SL replies.
    while True:
        time.sleep(30)
        with state_lock:
            tracked = list(active_signals.values())
        if not tracked:
            continue
        try:
            tv = {x.get("symbol"): x for x in get_tickers()}
        except Exception as e:
            print(f"TP MONITOR ERROR {_error_bucket(e)}: {e}")
            continue

        for state in tracked:
            if state.get("closed"):
                continue
            ticker = tv.get(state["symbol"]) or {}
            try:
                price = float(ticker.get("lastPr"))
            except (TypeError, ValueError):
                continue

            try:
                # Stop monitoring after SL. This prevents a later TP notification
                # after the original setup has already been invalidated.
                if _hit_level(state["direction"], state["sl"], price):
                    send_message(
                        state["chat_id"],
                        f"🛑 SL Hit\n⭐ {state['symbol']}\n💵 Price: {fmt_price(price)}",
                        reply_to_message_id=state["message_id"],
                    )
                    with state_lock:
                        active_signals.pop(state["key"], None)
                    continue

                for name in ("tp1", "tp2", "tp3"):
                    hit_key = f"{name}_hit"
                    if state[hit_key]:
                        continue
                    if _hit_level(state["direction"], price, state[name]):
                        label = name.upper().replace("TP", "TP")
                        send_message(
                            state["chat_id"],
                            f"🎯 {label} Hit\n⭐ {state['symbol']}\n💵 Price: {fmt_price(price)}",
                            reply_to_message_id=state["message_id"],
                        )
                        with state_lock:
                            if state["key"] in active_signals:
                                active_signals[state["key"]][hit_key] = True
                                if name == "tp3":
                                    active_signals.pop(state["key"], None)
                                    break
            except Exception as e:
                print(f"TP NOTIFY ERROR {state.get('symbol')}: {type(e).__name__}: {e}")

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
                # One new signal per 10-minute window; send the strongest candidate.
                pending_signals.sort(key=lambda x: (x.get("radar_score", 0), x.get("score", 0), x.get("confidence", 0), x.get("time", 0)), reverse=True)
                sig = pending_signals.pop(0)
                pending_signals.clear()
        if not sig:
            continue
        try:
            path = make_chart(sig)
            tv_symbol = sig["symbol"]
            markup = {"inline_keyboard": [[{"text": "📈 TradingView", "url": f"https://www.tradingview.com/chart/?symbol=BITGET:{tv_symbol}"}]]}
            message_id = send_photo(active_chat_id, path, signal_caption(sig), markup)
            track_sent_signal(sig, active_chat_id, message_id)
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
    conflict_wait = 3
    while True:
        try:
            r = requests.get(telegram_url("getUpdates"), params={"timeout": 25, "offset": offset, "allowed_updates": json.dumps(["message"])}, timeout=35)
            if r.status_code == 409:
                print("TELEGRAM 409 CONFLICT: another poller is active; retrying shortly")
                time.sleep(conflict_wait)
                conflict_wait = min(conflict_wait * 2, 30)
                continue
            r.raise_for_status()
            conflict_wait = 3
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
                        "🚀 SAIWAN Pump Radar — Base + Breakout + Acceleration\n\n"
                        "/scan - Start scanner\n"
                        "/stop - Stop scanner\n"
                        "/status - Bot status\n\n"
                        "Market: Bitget USDT Perpetual Futures (full eligible market)\n"
                        "Scan timeframe: 15m only\n"
                        "Confirmation: BASE + 15m CLOSED breakout + Volume + Acceleration\n"
                        "TPs: 1.5R / 2.5R / 4R\n"
                        "Signal cooldown: 10 minutes\n"
                        "TP/SL hit replies: ENABLED\n"
                        "Signal gate: Base + closed-candle expansion confirmation")
                elif text.startswith("/scan"):
                    start_scanner(active_chat_id)
                    send_message(active_chat_id,
                        "🚀 SAIWAN PUMP RADAR STARTED\n\n"
                        "Only 15m is scanned.\n"
                        "No weak/radar-only alerts will be sent.\n"
                        "A signal requires BASE + 15m CLOSED breakout + volume + acceleration + extension filter.\n"
                        "TP1 1.5R • TP2 2.5R • TP3 4R.\n"
                        "New signal cooldown: 10 minutes.\n"
                        "TP/SL hit replies are enabled.\n"
                        "Suggested leverage: dynamic 2x-5x (informational).")
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
    try:
        requests.post(telegram_url("deleteWebhook"), data={"drop_pending_updates": "false"}, timeout=10)
    except Exception as e:
        print(f"TELEGRAM WEBHOOK CLEANUP WARNING: {type(e).__name__}: {e}")
    threading.Thread(target=poll_updates, daemon=True).start()
    threading.Thread(target=sender_loop, daemon=True).start()
    threading.Thread(target=monitor_active_signals, daemon=True).start()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
