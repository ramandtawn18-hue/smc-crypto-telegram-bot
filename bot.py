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
RADAR_MIN_SCORE = 72
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



def radar_score(direction, trend, rv, vol_ratio, vol_score, structure_ok,
                breakout_ok, trendline_ok, momentum_ok, extension_ok,
                zone_ok=True, retest_ok=False, rejection_ok=False):
    """Score setup quality from 0-100; never a profit probability."""
    score = 0.0
    if direction == "LONG":
        score += 15 if trend == "BULLISH" else 0
        score += 8 if 52 <= rv <= 68 else 0
    else:
        score += 15 if trend == "BEARISH" else 0
        score += 8 if 32 <= rv <= 48 else 0
    score += 14 if structure_ok else 0
    score += 14 if breakout_ok else 0
    score += 6 if trendline_ok else 0
    score += 10 if momentum_ok else 0
    score += 9 if vol_ratio >= 1.15 else (4 if vol_ratio >= 1.0 else 0)
    score += 7 if 4.5 <= vol_score <= 8.5 else (3 if vol_score >= 3.5 else 0)
    score += 5 if zone_ok else 0
    score += 7 if retest_ok else 0
    score += 3 if rejection_ok else 0
    score += 2 if extension_ok else 0
    return int(max(0, min(100, round(score))))


def _zone_candidates(rows, atr_value):
    """Find compact 15m demand/supply zones from swing candles plus displacement.

    This is deliberately price-action based: a zone is only kept when a swing
    is followed by a meaningful move away. It is not a generic moving-average
    band.
    """
    if not atr_value or len(rows) < 30:
        return [], []
    highs, lows = swing_points(rows[-100:], left=2, right=2)
    base = rows[-100:]
    demand, supply = [], []
    for idx, low in lows[-8:]:
        if idx + 3 >= len(base):
            continue
        future_high = max(x["high"] for x in base[idx+1:min(len(base), idx+6)])
        if future_high - low < 0.9 * atr_value:
            continue
        r = base[idx]
        zone_low = low
        zone_high = min(max(r["open"], r["close"]), low + 0.75 * atr_value)
        if zone_high > zone_low:
            demand.append({"low": zone_low, "high": zone_high, "index": idx})
    for idx, high in highs[-8:]:
        if idx + 3 >= len(base):
            continue
        future_low = min(x["low"] for x in base[idx+1:min(len(base), idx+6)])
        if high - future_low < 0.9 * atr_value:
            continue
        r = base[idx]
        zone_high = high
        zone_low = max(min(r["open"], r["close"]), high - 0.75 * atr_value)
        if zone_high > zone_low:
            supply.append({"low": zone_low, "high": zone_high, "index": idx})
    return demand, supply


def _zone_touch(row, zone, tolerance):
    return row["low"] <= zone["high"] + tolerance and row["high"] >= zone["low"] - tolerance


def _zone_rejection(row, zone, direction):
    rng = max(row["high"] - row["low"], 1e-12)
    body = abs(row["close"] - row["open"])
    if direction == "LONG":
        lower_wick = min(row["open"], row["close"]) - row["low"]
        return (
            _zone_touch(row, zone, rng * 0.10)
            and row["close"] > zone["high"]
            and lower_wick >= max(body * 0.45, rng * 0.20)
        )
    upper_wick = row["high"] - max(row["open"], row["close"])
    return (
        _zone_touch(row, zone, rng * 0.10)
        and row["close"] < zone["low"]
        and upper_wick >= max(body * 0.45, rng * 0.20)
    )

def suggested_leverage(radar, volatility, risk_pct):
    """Return a conservative informational leverage suggestion (2x-5x).

    Leverage is reduced for high/extreme volatility and wider stops. It does
    not change position size and the bot never places trades automatically.
    """
    if volatility == "EXTREME" or radar < RADAR_MIN_SCORE:
        return 2
    if volatility == "HIGH":
        return 3 if radar < 88 else 4
    if risk_pct >= 2.5:
        return 2
    if risk_pct >= 1.7:
        return 3
    if radar >= 92 and risk_pct <= 1.0:
        return 5
    if radar >= 84 and risk_pct <= 1.4:
        return 4
    return 3

def analyze(symbol, rows15):
    """15m closed-candle setup engine.

    Two valid setup paths are allowed:
      1) breakout -> confirmation/retest -> continuation
      2) fresh demand/supply rejection with structure and momentum

    The engine is intentionally selective, but it does not require every
    optional confirmation at the same time. That was the main reason the old
    version could scan hundreds of contracts and still return zero signals.
    """
    if len(rows15) < 120:
        return None

    r = rows15
    cur, prev = r[-1], r[-2]
    closes = [x["close"] for x in r]
    a = atr(r, 14)
    rv = rsi(closes, 14)
    if not a or not rv or cur["close"] <= 0:
        return None

    trend, ema20, ema50, ema200 = trend_context(r)
    vol_score, volatility = volatility_score(r)

    body = abs(cur["close"] - cur["open"])
    rng = max(cur["high"] - cur["low"], 1e-12)
    body_ratio = body / rng
    close_pos = (cur["close"] - cur["low"]) / rng

    # Volume is deliberately softer than the previous 1.15x hard gate.
    # A 1.00x candle can still be valid when structure + retest are strong.
    avg_vol = sum(x["vol"] for x in r[-21:-1]) / 20.0
    vol_ratio = cur["vol"] / avg_vol if avg_vol else 0.0
    volume_ok = vol_ratio >= 1.00
    volume_strong = vol_ratio >= 1.15

    resistance = max(x["high"] for x in r[-21:-1])
    support = min(x["low"] for x in r[-21:-1])
    long_break = cur["close"] > resistance and prev["close"] <= resistance and cur["close"] > cur["open"]
    short_break = cur["close"] < support and prev["close"] >= support and cur["close"] < cur["open"]

    # Recent breakout/retest detection. A retest can occur up to 4 closed
    # candles after the break; entry is only allowed after the hold/rejection.
    retest_long = retest_short = False
    retest_level_long, retest_level_short = resistance, support
    for j in range(max(1, len(r)-6), len(r)-1):
        prior_res = max(x["high"] for x in r[max(0, j-20):j])
        prior_sup = min(x["low"] for x in r[max(0, j-20):j])
        if r[j]["close"] > prior_res and r[j]["close"] > r[j]["open"]:
            touched = min(x["low"] for x in r[j+1:]) <= prior_res + 0.22*a
            held = cur["close"] > prior_res and cur["close"] > cur["open"]
            if touched and held:
                retest_long, retest_level_long = True, prior_res
        if r[j]["close"] < prior_sup and r[j]["close"] < r[j]["open"]:
            touched = max(x["high"] for x in r[j+1:]) >= prior_sup - 0.22*a
            held = cur["close"] < prior_sup and cur["close"] < cur["open"]
            if touched and held:
                retest_short, retest_level_short = True, prior_sup

    demand, supply = _zone_candidates(r, a)
    demand_zone = demand[-1] if demand else {"low": support-0.30*a, "high": support+0.30*a}
    supply_zone = supply[-1] if supply else {"low": resistance-0.30*a, "high": resistance+0.30*a}
    long_rejection = _zone_rejection(cur, demand_zone, "LONG")
    short_rejection = _zone_rejection(cur, supply_zone, "SHORT")

    long_trigger = retest_level_long if retest_long else resistance
    short_trigger = retest_level_short if retest_short else support
    long_confirm = long_break or retest_long
    short_confirm = short_break or retest_short

    # Structure is based on the last confirmed swings, but we also accept an
    # aligned EMA structure when the market has not printed a perfect swing pair.
    long_structure = structure_bias(r, "LONG")
    short_structure = structure_bias(r, "SHORT")
    ema_long = ema20 > ema50 and ema20 >= ema20 if ema20 and ema50 else False
    ema_short = ema20 < ema50 and ema20 <= ema20 if ema20 and ema50 else False
    # Use actual EMA slopes instead of the tautological expression above.
    e20 = ema([x["close"] for x in r], 20)
    e50 = ema([x["close"] for x in r], 50)
    ema_long = e20[-1] > e50[-1] and e20[-1] > e20[-5]
    ema_short = e20[-1] < e50[-1] and e20[-1] < e20[-5]

    # Momentum must be directional, but does not need to be a huge candle.
    momentum_long = cur["close"] > cur["open"] and body_ratio >= 0.45 and close_pos >= 0.60
    momentum_short = cur["close"] < cur["open"] and body_ratio >= 0.45 and close_pos <= 0.40
    follow_long = cur["close"] > prev["close"] and close_pos >= 0.55
    follow_short = cur["close"] < prev["close"] and close_pos <= 0.45

    # RSI is a directional filter, not an absolute requirement. This avoids
    # rejecting good trend continuation setups simply because RSI is 50.5/49.5.
    long_rsi = 50 <= rv <= 70
    short_rsi = 30 <= rv <= 50

    # Avoid chasing a candle that has already travelled too far from its trigger.
    long_extension = (cur["close"] - long_trigger) / a
    short_extension = (short_trigger - cur["close"]) / a
    not_extended_long = -0.15 <= long_extension <= 0.90
    not_extended_short = -0.15 <= short_extension <= 0.90

    # The current price should still be reasonably connected to the setup zone.
    long_zone_ok = long_rejection or retest_long or abs(cur["close"] - demand_zone["high"]) <= 1.25*a
    short_zone_ok = short_rejection or retest_short or abs(cur["close"] - supply_zone["low"]) <= 1.25*a

    # Reject extreme volatility. Healthy/high volatility can be traded if the
    # structural checks are clean.
    volatility_ok = volatility in ("HEALTHY", "HIGH") and vol_score >= 5.0

    # Score each side instead of requiring every single optional flag.
    def side_score(direction):
        if direction == "LONG":
            trend_ok = trend == "BULLISH" or ema_long
            structure_ok = long_structure or ema_long
            confirm_ok = long_confirm or long_rejection
            momentum_ok = momentum_long or (retest_long and follow_long)
            rsi_ok = long_rsi
            extension_ok = not_extended_long
            zone_ok = long_zone_ok
            score = sum([
                18 if trend == "BULLISH" else (12 if ema_long else 0),
                16 if long_structure else (10 if ema_long else 0),
                20 if long_confirm else (14 if long_rejection else 0),
                12 if momentum_long else (8 if follow_long else 0),
                8 if rsi_ok else 0,
                10 if volume_strong else (6 if volume_ok else 0),
                8 if extension_ok else 0,
                5 if volatility_ok else 0,
                3 if zone_ok else 0,
            ])
            return score, trend_ok, structure_ok, confirm_ok, momentum_ok, rsi_ok, extension_ok, zone_ok
        trend_ok = trend == "BEARISH" or ema_short
        structure_ok = short_structure or ema_short
        confirm_ok = short_confirm or short_rejection
        momentum_ok = momentum_short or (retest_short and follow_short)
        rsi_ok = short_rsi
        extension_ok = not_extended_short
        zone_ok = short_zone_ok
        score = sum([
            18 if trend == "BEARISH" else (12 if ema_short else 0),
            16 if short_structure else (10 if ema_short else 0),
            20 if short_confirm else (14 if short_rejection else 0),
            12 if momentum_short else (8 if follow_short else 0),
            8 if rsi_ok else 0,
            10 if volume_strong else (6 if volume_ok else 0),
            8 if extension_ok else 0,
            5 if volatility_ok else 0,
            3 if zone_ok else 0,
        ])
        return score, trend_ok, structure_ok, confirm_ok, momentum_ok, rsi_ok, extension_ok, zone_ok

    ls = side_score("LONG")
    ss = side_score("SHORT")
    if ls[0] == ss[0] or max(ls[0], ss[0]) < 72:
        return None
    direction = "LONG" if ls[0] > ss[0] else "SHORT"
    vals = ls if direction == "LONG" else ss
    score, trend_ok, structure_ok, confirm_ok, momentum_ok, rsi_ok, extension_ok, zone_ok = vals

    # A signal must have a real confirmation event. This is the one hard gate:
    # breakout/retest or a fresh zone rejection; no radar-only alerts.
    if not confirm_ok or not trend_ok or not structure_ok or not extension_ok or not volatility_ok:
        return None
    if not volume_ok or not momentum_ok or not rsi_ok:
        return None

    trigger = long_trigger if direction == "LONG" else short_trigger
    zone = demand_zone if direction == "LONG" else supply_zone
    retest_ok = retest_long if direction == "LONG" else retest_short
    rejection_ok = long_rejection if direction == "LONG" else short_rejection
    trendline_ok = trendline_signal(r, direction)[0]

    # Risk is based on the setup structure. Reject impractically wide stops.
    if direction == "LONG":
        recent_lows = [x["low"] for x in r[-16:]]
        sl = min(min(recent_lows), zone["low"] - 0.10*a)
        risk = cur["close"] - sl
        if risk <= 0 or risk > 3.0*a:
            return None
        higher = sorted(set(round(x[1], 12) for x in swing_points(r[-100:], 2, 2)[0] if x[1] > cur["close"]))
        tp1 = next((x for x in higher if x >= cur["close"] + 1.2*risk), cur["close"] + 1.5*risk)
        tp2 = next((x for x in higher if x > tp1), cur["close"] + 2.5*risk)
        tp3 = next((x for x in higher if x > tp2), cur["close"] + 4.0*risk)
        structure = "Breakout + higher-low" if confirm_ok else "Demand rejection"
    else:
        recent_highs = [x["high"] for x in r[-16:]]
        sl = max(max(recent_highs), zone["high"] + 0.10*a)
        risk = sl - cur["close"]
        if risk <= 0 or risk > 3.0*a:
            return None
        lower = sorted(set(round(x[1], 12) for x in swing_points(r[-100:], 2, 2)[1] if x[1] < cur["close"]), reverse=True)
        tp1 = next((x for x in lower if x <= cur["close"] - 1.2*risk), cur["close"] - 1.5*risk)
        tp2 = next((x for x in lower if x < tp1), cur["close"] - 2.5*risk)
        tp3 = next((x for x in lower if x < tp2), cur["close"] - 4.0*risk)
        structure = "Breakdown + lower-high" if confirm_ok else "Supply rejection"

    if direction == "LONG":
        tp1 = max(tp1, cur["close"] + 1.2*risk)
        tp2 = max(tp2, tp1 + 0.25*risk)
        tp3 = max(tp3, tp2 + 0.25*risk)
    else:
        tp1 = min(tp1, cur["close"] - 1.2*risk)
        tp2 = min(tp2, tp1 - 0.25*risk)
        tp3 = min(tp3, tp2 - 0.25*risk)

    # Confidence is a setup-quality score, not a probability of profit.
    confidence = min(95, int(70 + (score-72)*0.7 + (4 if retest_ok else 0) + (3 if volume_strong else 0) + (2 if trendline_ok else 0)))
    radar = int(min(100, round(score + (5 if retest_ok else 0) + (3 if trendline_ok else 0))))
    risk_pct = abs(cur["close"] - sl) / cur["close"] * 100
    leverage = suggested_leverage(radar, volatility, risk_pct)

    checks = {
        "Trend aligned": trend_ok,
        "Structure aligned": structure_ok,
        "Closed breakout/retest or rejection": confirm_ok,
        "Momentum": momentum_ok,
        "RSI aligned": rsi_ok,
        "Volume >= 1.0x": volume_ok,
        "Volatility usable": volatility_ok,
        "Not overextended": extension_ok,
    }
    return {
        "symbol": symbol, "direction": direction, "structure": structure,
        "entry": cur["close"], "trigger_level": trigger, "sl": sl,
        "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "entry_zone_low": zone["low"], "entry_zone_high": zone["high"],
        "score": sum(checks.values()), "max_score": len(checks),
        "trend15": trend, "rsi": rv, "volatility": volatility,
        "volatility_score": vol_score, "volume_ratio": vol_ratio,
        "confidence": confidence, "radar_score": radar, "risk_pct": risk_pct,
        "suggested_leverage": leverage, "trendline": trendline_signal(r, direction)[1],
        "ema20": ema20, "ema50": ema50, "ema200": ema200,
        "time": cur["time"], "rows": r[-CHART_CANDLES:], "checks": checks,
        "retest_ok": retest_ok, "rejection_ok": rejection_ok,
        "demand_zones": demand[-3:], "supply_zones": supply[-3:],
    }

def make_chart(sig):
    """TradingView-style chart for the conservative A+ setup scanner.

    Uses the bot's real Bitget OHLC data and signal levels. The visual design is
    intentionally closer to a manual TradingView setup: clean candles, right
    price scale, structure line, support/resistance zone, projected R:R box,
    target arrow, and minimal annotation. No volume subplot is used.
    """
    rows = sig["rows"]
    n = len(rows)
    direction = sig["direction"]
    entry, sl = sig["entry"], sig["sl"]
    trigger = sig.get("trigger_level", entry)
    tp1, tp2, tp3 = sig["tp1"], sig["tp2"], sig["tp3"]

    BG = "#f7f7f8"
    GRID = "#e4e6e8"
    TEXT = "#17191c"
    MUTED = "#73777d"
    UP = "#16a085"
    DOWN = "#e14b55"
    TEAL = "#087f7a"
    GOLD = "#c8a84e"
    BORDER = "#cfd3d7"

    fig, ax = plt.subplots(figsize=(14.4, 7.7), dpi=170, facecolor=BG)
    ax.set_facecolor(BG)

    width = 0.58
    for i, r in enumerate(rows):
        up = r["close"] >= r["open"]
        c = UP if up else DOWN
        ax.vlines(i, r["low"], r["high"], color=c, linewidth=1.0, zorder=3)
        lo = min(r["open"], r["close"])
        body_h = max(abs(r["close"] - r["open"]), abs(r["close"]) * 1e-5)
        ax.add_patch(Rectangle(
            (i - width / 2, lo), width, body_h,
            facecolor=c, edgecolor=c, linewidth=0.5, zorder=4
        ))

    chart_right = n + max(12, int(n * 0.22))

    # Draw the actual price-action zones used by the signal engine.
    # Green = demand, red = supply, matching the reference screenshots.
    window = rows[-min(55, n):]
    highs, lows = swing_points(window, left=2, right=2)
    offset = n - len(window)
    demand_zones = sig.get("demand_zones") or []
    supply_zones = sig.get("supply_zones") or []
    if not demand_zones and not supply_zones:
        av = atr(rows, 14) or abs(rows[-1]["close"]) * 0.005
        demand_zones = [{"low": min(r["low"] for r in rows[-12:]), "high": min(r["low"] for r in rows[-12:]) + 0.5*av}]
        supply_zones = [{"low": max(r["high"] for r in rows[-12:]) - 0.5*av, "high": max(r["high"] for r in rows[-12:])}]
    for z in demand_zones:
        ax.add_patch(Rectangle((-1, z["low"]), chart_right + 1, z["high"]-z["low"],
                               facecolor=UP, edgecolor=UP, linewidth=0.9, alpha=0.14, zorder=0))
    for z in supply_zones:
        ax.add_patch(Rectangle((-1, z["low"]), chart_right + 1, z["high"]-z["low"],
                               facecolor=DOWN, edgecolor=DOWN, linewidth=0.9, alpha=0.13, zorder=0))
    if demand_zones:
        z = demand_zones[-1]
        ax.text(1, z["high"], "DEMAND", color=UP, fontsize=7.8, fontweight="bold", va="bottom", zorder=5)
    if supply_zones:
        z = supply_zones[-1]
        ax.text(1, z["high"], "SUPPLY", color=DOWN, fontsize=7.8, fontweight="bold", va="bottom", zorder=5)

    # Structure trendline uses the same swing logic as the signal check.
    trend_points = None
    if direction == "LONG" and len(highs) >= 2 and highs[-1][1] < highs[-2][1]:
        trend_points = (highs[-2], highs[-1])
    elif direction == "SHORT" and len(lows) >= 2 and lows[-1][1] > lows[-2][1]:
        trend_points = (lows[-2], lows[-1])

    if trend_points:
        p1, p2 = trend_points
        x_a, x_b = p1[0] + offset, p2[0] + offset
        y_a, y_b = p1[1], p2[1]
        y_ext = line_value((x_a, y_a), (x_b, y_b), chart_right)
        ax.plot([x_a, chart_right], [y_a, y_ext], color=TEAL,
                linewidth=2.2, alpha=0.95, zorder=5)
        if sig["checks"].get("Trendline breakout" if direction == "LONG" else "Trendline breakdown"):
            ax.text(min(x_b + 1, chart_right - 8), y_ext,
                    "BREAKOUT" if direction == "LONG" else "BREAKDOWN",
                    color=TEAL, fontsize=8.2, fontweight="bold", va="bottom")

    # The trigger level is the actual breakout/breakdown level used by the
    # scanner. Keep it visually separate from the entry so the chart shows
    # exactly what had to break before the signal was allowed.
    entry_zone_lo = min(trigger, entry)
    entry_zone_hi = max(trigger, entry)
    zone_pad = max((entry_zone_hi - entry_zone_lo) * 0.18, abs(entry) * 0.00015)
    ax.add_patch(Rectangle(
        (-1, entry_zone_lo - zone_pad), n + 1,
        max(entry_zone_hi - entry_zone_lo + 2 * zone_pad, abs(entry) * 0.0003),
        facecolor=TEAL, edgecolor=TEAL, linewidth=0.9, alpha=0.08, zorder=0
    ))
    ax.axhline(trigger, color=GOLD if direction == "LONG" else DOWN,
               linewidth=1.35, linestyle="--", alpha=0.95, zorder=5)
    ax.text(1, trigger,
            f"CONFIRMATION LEVEL  {fmt_price(trigger)}",
            color=GOLD if direction == "LONG" else DOWN, fontsize=7.9,
            fontweight="bold", va="bottom" if direction == "LONG" else "top",
            alpha=0.95, zorder=6)
    ax.scatter([n - 1], [entry], s=34, marker="o",
               facecolor=TEAL, edgecolor="white", linewidth=0.9, zorder=8)
    ax.text(n - 1, entry, "  CLOSED 15m", color=TEAL, fontsize=7.8,
            fontweight="bold", va="bottom" if direction == "LONG" else "top",
            ha="left", zorder=8)

    # Entry and risk/reward projection, like a TradingView long/short position tool.
    box_x0 = n - max(3, int(n * 0.05))
    box_x1 = chart_right - 1
    ax.add_patch(Rectangle(
        (box_x0, min(entry, sl)), box_x1 - box_x0, abs(sl - entry),
        facecolor=DOWN, edgecolor=DOWN, linewidth=0.8, alpha=0.18, zorder=1
    ))
    ax.add_patch(Rectangle(
        (box_x0, min(entry, tp3)), box_x1 - box_x0, abs(tp3 - entry),
        facecolor=UP, edgecolor=UP, linewidth=0.9, alpha=0.16, zorder=1
    ))

    # Horizontal levels: minimal and right-labelled.
    level_specs = [
        (sl, "SL", DOWN, 1.0, "-"),
        (entry, "ENTRY", TEXT, 1.15, "--"),
        (tp1, "TP1", TEAL, 0.9, ":"),
        (tp2, "TP2", TEAL, 0.9, ":"),
        (tp3, "TP3", TEAL, 1.15, "-"),
    ]
    for y, label, c, lw, ls in level_specs:
        ax.axhline(y, color=c, linewidth=lw, linestyle=ls, alpha=0.9, zorder=2)
        ax.text(chart_right + 0.4, y, f"{label}  {fmt_price(y)}",
                color=c, fontsize=8.1, fontweight="bold",
                va="center", ha="left", clip_on=False)

    # Direction arrow + projected target arrow.
    arrow_color = TEAL if direction == "LONG" else DOWN
    if direction == "LONG":
        arrow_y = entry + (tp1 - entry) * 0.05
        target_mid = (entry + tp3) / 2
        ax.annotate("LONG", xy=(n - 1, entry), xytext=(n - 13, arrow_y),
                    arrowprops=dict(arrowstyle="->", color=arrow_color, lw=1.6),
                    color=arrow_color, fontsize=9.5, fontweight="bold")
        ax.annotate("", xy=(box_x0 + (box_x1-box_x0)*0.55, tp3),
                    xytext=(box_x0 + (box_x1-box_x0)*0.55, entry),
                    arrowprops=dict(arrowstyle="->", color=TEAL, lw=1.5), zorder=7)
        ax.text((box_x0 + box_x1)/2, target_mid,
                "TP1 1.5R  •  TP2 2.5R  •  TP3 4R",
                rotation=90, color=TEAL, fontsize=8.0,
                fontweight="bold", ha="center", va="center", alpha=0.9)
    else:
        arrow_y = entry - (entry - tp1) * 0.05
        target_mid = (entry + tp3) / 2
        ax.annotate("SHORT", xy=(n - 1, entry), xytext=(n - 13, arrow_y),
                    arrowprops=dict(arrowstyle="->", color=arrow_color, lw=1.6),
                    color=arrow_color, fontsize=9.5, fontweight="bold")
        ax.annotate("", xy=(box_x0 + (box_x1-box_x0)*0.55, tp3),
                    xytext=(box_x0 + (box_x1-box_x0)*0.55, entry),
                    arrowprops=dict(arrowstyle="->", color=DOWN, lw=1.5), zorder=7)
        ax.text((box_x0 + box_x1)/2, target_mid,
                "TP1 1.5R  •  TP2 2.5R  •  TP3 4R",
                rotation=90, color=DOWN, fontsize=8.0,
                fontweight="bold", ha="center", va="center", alpha=0.9)

    # Header and explanatory line, matching the clean reference style.
    title = f"{sig['symbol']} / TetherUS PERPETUAL CONTRACT · 15 · Bitget"
    ax.text(0.01, 1.055, title, transform=ax.transAxes,
            fontsize=15.2, color=TEXT, fontweight="bold", va="bottom")
    last_close = rows[-1]["close"]
    change = ((last_close / rows[-2]["close"]) - 1.0) * 100 if len(rows) > 1 and rows[-2]["close"] else 0.0
    ax.text(0.01, 1.018, f"{fmt_price(last_close)}  {change:+.2f}%",
            transform=ax.transAxes, fontsize=10.0,
            color=UP if change >= 0 else DOWN, va="bottom")
    if direction == "LONG":
        headline = "LONG favored by the broader bullish context"
    else:
        headline = "SHORT favored by the broader bearish context"
    ax.text(0.50, 0.965, headline, transform=ax.transAxes,
            fontsize=12.2, color=TEAL if direction == "LONG" else DOWN,
            fontweight="bold", ha="center", va="top")

    # Compact footer; no volume panel, matching the reference image's simplicity.
    setup_mode = "RETEST + HOLD" if sig.get("retest_ok") else ("ZONE REJECTION" if sig.get("rejection_ok") else "BREAK + CLOSE")
    footer = (f"{setup_mode}   •   Trend {sig['trend15']}   •   RSI {sig['rsi']:.1f}   •   "
              f"Volume {sig['volume_ratio']:.2f}x   •   Radar {sig['radar_score']}/100   •   "
              f"15m CLOSED CONFIRMATION")
    ax.text(0.01, 0.018, footer, transform=ax.transAxes,
            fontsize=8.2, color=MUTED, va="bottom")

    # Latest-price tag and TradingView-like axes.
    ax.text(1.006, last_close, fmt_price(last_close), transform=ax.get_yaxis_transform(),
            ha="left", va="center", fontsize=8.8, color="white",
            bbox=dict(boxstyle="square,pad=0.28", facecolor=UP if change >= 0 else DOWN,
                      edgecolor="none", alpha=0.96), clip_on=False)
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position("right")
    ax.tick_params(axis="y", colors=TEXT, labelsize=8.5, length=0)
    ax.tick_params(axis="x", colors=MUTED, labelsize=8, length=0, pad=8)
    ax.grid(axis="y", color=GRID, linewidth=0.65, alpha=0.9)
    ax.grid(axis="x", color=GRID, linewidth=0.45, alpha=0.5)
    for side in ["top", "left", "bottom"]:
        ax.spines[side].set_visible(False)
    ax.spines["right"].set_color(BORDER)

    step = max(1, n // 7)
    ticks = list(range(0, n, step))
    if ticks[-1] != n - 1:
        ticks.append(n - 1)
    labels = [datetime.fromtimestamp(rows[i]["time"], tz=timezone.utc).strftime("%d\n%H:%M") for i in ticks]
    ax.set_xticks(ticks)
    ax.set_xticklabels(labels)

    zone_lows = [z["low"] for z in demand_zones + supply_zones] or [entry]
    zone_highs = [z["high"] for z in demand_zones + supply_zones] or [entry]
    all_lows = [r["low"] for r in rows] + [sl, tp3] + zone_lows
    all_highs = [r["high"] for r in rows] + [sl, tp3] + zone_highs
    ymin, ymax = min(all_lows), max(all_highs)
    span = max(ymax - ymin, abs(last_close) * 0.012)
    ax.set_ylim(ymin - span * 0.06, ymax + span * 0.12)
    ax.set_xlim(-1, chart_right + 8)
    fig.subplots_adjust(left=0.035, right=0.86, top=0.89, bottom=0.09)

    safe_symbol = "".join(ch if ch.isalnum() else "_" for ch in sig["symbol"])
    path = f"/tmp/chart_{safe_symbol}_{sig['time']}.png"
    fig.savefig(path, facecolor=BG, edgecolor="none")
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
            "Strategy: AI Market Radar + A++ Confirmation\n"
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
    # Telegram signal is intentionally minimal: chart + coin + direction.
    direction = "🟢 LONG" if sig["direction"] == "LONG" else "🔴 SHORT"
    return f"{sig['symbol']}\n{direction}"


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
                        "🚀 Bitget A+ Structure + Breakout Scanner\n\n"
                        "/scan - Start scanner\n"
                        "/stop - Stop scanner\n"
                        "/status - Bot status\n\n"
                        "Market: Bitget USDT Perpetual Futures (full eligible market)\n"
                        "Scan timeframe: 15m only\n"
                        "Confirmation: 15m CLOSED candle + Structure + Breakout + Volume\n"
                        "TPs: 1.5R / 2.5R / 4R\n"
                        "Signal cooldown: 10 minutes\n"
                        "TP/SL hit replies: ENABLED\n"
                        "Signal gate: A+ closed-candle confirmation")
                elif text.startswith("/scan"):
                    start_scanner(active_chat_id)
                    send_message(active_chat_id,
                        "🚀 BITGET A+ CONFIRMED SIGNAL SCANNER STARTED\n\n"
                        "Only 15m is scanned.\n"
                        "No early/radar-only alerts will be sent.\n"
                        "A signal is sent only after 15m CLOSED candle + structure + breakout + volume confirmation.\n"
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
