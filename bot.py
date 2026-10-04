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
TF_30M = "30m"
TF_1H = "1h"
TF_2H = "2h"
TF_4H = "4h"
TF_5M = "5m"
TIMEFRAME = TF_15M
SUPPORTED_SCAN_TIMEFRAMES = ("1m", TF_5M, TF_15M, TF_30M, TF_1H, TF_2H, TF_4H)
CANDLE_LIMIT = 260
# 0 = scan every eligible Bitget USDT perpetual contract (no top-N cap)
MAX_PAIRS = 0
SCAN_WORKERS = 10
SCAN_INTERVAL = 60
SEND_INTERVAL = 60  # SA-VWAP: deliver as soon as the confirmed 15m trigger is found
SIGNAL_TIMEFRAME = TF_15M
CHART_CANDLES = 80
HTTP_TIMEOUT = 15
MIN_SCORE = 5

# SAIWAN AI Market Radar / risk-aware leverage (informational only)
RADAR_MIN_SCORE = 72
MAX_SUGGESTED_LEVERAGE = 5
MIN_SUGGESTED_LEVERAGE = 2

TP1_R = 1.5
TP2_R = 2.5
TP3_R = 4.0

# SAIWAN Legacy Momentum helpers retained for compatibility; the active signal engine is SA-VWAP close-cross.
MOM_RANGE_LOOKBACK = 20
MOM_BREAKOUT_WINDOW = 12
MOM_MAX_PULLBACK_BARS = 12
MOM_MIN_VOLUME_MULT = 1.15
MOM_TRIGGER_VOLUME_MULT = 0.90
MOM_MIN_BODY_RATIO = 0.45
MOM_MIN_BREAKOUT_ATR = 0.05
MOM_MAX_EXTENSION_ATR = 1.80
MOM_PULLBACK_ATR = 0.10
MOM_SL_ATR_BUFFER = 0.50
MOM_MIN_RISK_ATR = 0.80
MOM_MAX_RISK_ATR = 3.50

# SA-VWAP port from the supplied TradingView Pine source.
# Primary trigger: anchored VWAP retest after price has spent enough bars away
# from VWAP in the current structural leg. Risk preset mirrors the source's
# Signal preset: structure-aware SL with a 1.75 ATR minimum and 1.5R / 3R / 4.5R targets, with BE after TP1.
SA_VWAP_ENABLED = True
SA_VWAP_PIVOT_LEFT = 55
SA_VWAP_PIVOT_RIGHT = 55
SA_VWAP_MIN_SWING_ATR = 1.50
SA_VWAP_RETEST_MIN_AWAY = 5
SA_VWAP_RETEST_TOL_SIGMA = 0.25
SA_VWAP_VOLUME_CLAMP_MEDIAN = 4.0
SA_VWAP_SL_ATR = 1.75
SA_VWAP_MAX_RISK_ATR = 4.00
SA_VWAP_STRUCTURE_BUFFER_ATR = 0.35
SA_VWAP_STRUCTURE_LEFT = 2
SA_VWAP_STRUCTURE_RIGHT = 2
SA_VWAP_MIN_STRUCTURE_RISK_ATR = 0.50
SA_VWAP_TP_R = (1.5, 3.0, 4.5)
SA_VWAP_USE_BE = True
# Fresh signal rule: a confirmed candle must CLOSE across the active SA-VWAP line.
# LONG = previous close at/below VWAP -> current close above VWAP.
# SHORT = previous close at/above VWAP -> current close below VWAP.
SA_VWAP_TRIGGER_MODE = "CLOSE_SIDE"

MOM_EMA_FAST = 9
MOM_EMA_MID = 21
MOM_EMA_SLOW = 50

app = Flask(__name__)
stop_event = threading.Event()
force_scan_event = threading.Event()
state_lock = threading.Lock()
scanner_thread = None
scanner_running = False
active_chat_id = None
active_scan_timeframe = TF_15M
pending_signals = []
seen_signals = set()
seen_order = []
next_send_at = 0
offset = None
active_signals = {}  # key -> tracked signal state for TP/SL notifications
monitor_thread = None

# Smart Watch / history / settings state
watchlist = {}  # symbol -> {symbol, timeframe, chat_id, last_key, last_alert_at}
watch_alerts = []  # recent alert/event records
signal_history = []  # closed signal outcomes for current process
watch_lock = threading.Lock()
watch_stop_event = threading.Event()
watch_thread = None
MAX_WATCH_ITEMS = 20
WATCH_INTERVAL = 60
WATCH_ALERT_COOLDOWN = 300
bot_settings = {
    "alerts": True,
    "watch_interval": WATCH_INTERVAL,
    "watch_cooldown": WATCH_ALERT_COOLDOWN,
}

session = requests.Session()
session.headers.update({"User-Agent": "SAIWAN-Crypto-Signal-Move-Hunter/5.0", "Accept": "application/json"})
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


def get_klines(symbol, interval=TIMEFRAME, limit=CANDLE_LIMIT):
    # Bitget requires 1H/4H for hourly candles; minute intervals stay lowercase.
    api_granularity = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
    }.get(str(interval).lower(), interval)

    payload = bitget_get(
        "/api/v2/mix/market/candles",
        {"symbol": symbol, "productType": BITGET_PRODUCT, "granularity": api_granularity,
         "limit": min(limit, 1000), "kLineType": "market"},
    )
    raw = payload.get("data") or []
    now_ms = int(time.time() * 1000)
    candle_ms = {
        "1m": 60 * 1000,
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "30m": 30 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "2h": 2 * 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
    }.get(str(interval).lower())
    if candle_ms is None:
        raise RuntimeError(f"unsupported timeframe: {interval}")
    rows = []
    for v in raw:
        try:
            if len(v) < 6:
                continue
            ts = int(v[0])
            if ts + candle_ms > now_ms:
                continue
            rows.append({"time": ts // 1000, "open": float(v[1]), "high": float(v[2]),
                         "low": float(v[3]), "close": float(v[4]), "vol": float(v[5]),
                         "turnover": float(v[6]) if len(v) > 6 else 0.0})
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

def _candle_bull(r):
    return r["close"] > r["open"]


def _candle_bear(r):
    return r["close"] < r["open"]


def _body_ratio(r):
    rng = max(r["high"] - r["low"], 1e-12)
    return abs(r["close"] - r["open"]) / rng


def _find_fvg(rows, direction, start_idx, end_idx):
    """Find the latest 3-candle FVG created by displacement."""
    found = []
    for i in range(max(2, start_idx), min(end_idx, len(rows) - 1)):
        a, b, c = rows[i-2], rows[i-1], rows[i]
        if direction == "LONG" and c["low"] > a["high"] and _candle_bull(b):
            found.append({"low": a["high"], "high": c["low"], "index": i, "kind": "BULLISH FVG"})
        elif direction == "SHORT" and c["high"] < a["low"] and _candle_bear(b):
            found.append({"low": c["high"], "high": a["low"], "index": i, "kind": "BEARISH FVG"})
    return found[-1] if found else None


def _find_order_block(rows, direction, before_idx):
    """Last opposite candle before the displacement leg."""
    lo = max(0, before_idx - 8)
    for i in range(before_idx - 1, lo - 1, -1):
        r = rows[i]
        if direction == "LONG" and _candle_bear(r):
            return {"low": r["low"], "high": r["high"], "index": i, "kind": "BULLISH OB"}
        if direction == "SHORT" and _candle_bull(r):
            return {"low": r["low"], "high": r["high"], "index": i, "kind": "BEARISH OB"}
    return None


def _overlap(a, b):
    if not a or not b:
        return None
    lo, hi = max(a["low"], b["low"]), min(a["high"], b["high"])
    if lo <= hi:
        return {"low": lo, "high": hi}
    return None


def _recent_liquidity(rows, upto, direction, lookback=45):
    """Return the nearest prior swing pool that can be swept."""
    w0 = max(0, upto - lookback)
    window = rows[w0:upto]
    highs, lows = swing_points(window, 2, 2)
    if direction == "LONG":
        pools = [(i + w0, p) for i, p in lows]
        return pools[-1] if pools else None
    pools = [(i + w0, p) for i, p in highs]
    return pools[-1] if pools else None


def _structure_break(rows, direction, sweep_idx, end_idx):
    """MSS/CHOCH confirmation from internal swings after the liquidity sweep."""
    start = max(2, sweep_idx + 1)
    end = min(end_idx, len(rows) - 1)
    if end <= start + 1:
        return None
    window = rows[max(0, sweep_idx - 18):end + 1]
    highs, lows = swing_points(window, 1, 1)
    off = max(0, sweep_idx - 18)
    if direction == "LONG":
        prior_highs = [(i + off, p) for i, p in highs if i + off <= sweep_idx]
        if not prior_highs:
            return None
        level_idx, level = prior_highs[-1]
        for i in range(start, end + 1):
            if rows[i]["close"] > level and _candle_bull(rows[i]):
                return {"index": i, "level": level, "type": "MSS + CHOCH"}
    else:
        prior_lows = [(i + off, p) for i, p in lows if i + off <= sweep_idx]
        if not prior_lows:
            return None
        level_idx, level = prior_lows[-1]
        for i in range(start, end + 1):
            if rows[i]["close"] < level and _candle_bear(rows[i]):
                return {"index": i, "level": level, "type": "MSS + CHOCH"}
    return None


def _sweep_candidates(rows):
    """Return recent liquidity sweeps using only price/swing structure."""
    out = []
    start = max(10, len(rows) - 70)
    for i in range(start, len(rows) - 2):
        prior = rows[max(0, i-35):i]
        highs, lows = swing_points(prior, 2, 2)
        if highs:
            high_level = max(p for _, p in highs[-5:])
            if rows[i]["high"] > high_level and rows[i]["close"] < high_level:
                out.append(("SHORT", i, high_level))
        if lows:
            low_level = min(p for _, p in lows[-5:])
            if rows[i]["low"] < low_level and rows[i]["close"] > low_level:
                out.append(("LONG", i, low_level))
    return out


def _context_15m(rows15, direction):
    if not rows15 or len(rows15) < 20:
        return "UNKNOWN"
    recent = rows15[-8:]
    hi = max(r["high"] for r in recent)
    lo = min(r["low"] for r in recent)
    mid = (hi + lo) / 2
    return ("BULLISH CONTEXT" if rows15[-1]["close"] >= mid else "MIXED CONTEXT") if direction == "LONG" else ("BEARISH CONTEXT" if rows15[-1]["close"] <= mid else "MIXED CONTEXT")


def _rolling_mean(values, period):
    if not values:
        return 0.0
    return sum(values[-period:]) / min(period, len(values))


def _atr_at(rows, index, period=14):
    if index < period + 1:
        return None
    chunk = rows[:index + 1]
    return atr(chunk, period)


def _ema_alignment(rows, direction):
    closes = [r["close"] for r in rows]
    if len(closes) < MOM_EMA_SLOW + 2:
        return False, None
    e9 = ema(closes, MOM_EMA_FAST)[-1]
    e21 = ema(closes, MOM_EMA_MID)[-1]
    e50 = ema(closes, MOM_EMA_SLOW)[-1]
    cur = closes[-1]
    if direction == "LONG":
        return cur > e9 > e21 > e50, (e9, e21, e50)
    return cur < e9 < e21 < e50, (e9, e21, e50)


def _momentum_setup(rows, direction, sl_atr_buffer=MOM_SL_ATR_BUFFER,
                    tp_multipliers=(TP1_R, TP2_R, TP3_R)):
    """SAIWAN SA-VWAP Engine: breakout/breakdown -> controlled pullback -> continuation.

    Only closed candles in ``rows`` are used. The current last candle is the
    confirmed trigger. Signals are rejected when price is already too extended,
    so the engine does not chase a long move after several large candles.
    """
    if len(rows) < max(90, MOM_EMA_SLOW + MOM_RANGE_LOOKBACK + 10):
        return None

    cur_i = len(rows) - 1
    cur = rows[cur_i]
    atr_now = _atr_at(rows, cur_i, 14)
    if not atr_now or atr_now <= 0:
        return None
    aligned, emas = _ema_alignment(rows, direction)
    if not aligned:
        return None

    closes = [r["close"] for r in rows]
    vols = [r.get("vol", 0.0) for r in rows]
    avg_vol = _rolling_mean(vols[:-1], 20)
    if avg_vol <= 0:
        return None

    # Search recent confirmed breakout/breakdown candles, newest first.
    first = max(MOM_RANGE_LOOKBACK, cur_i - MOM_BREAKOUT_WINDOW)
    candidates = []
    for b in range(cur_i, first - 1, -1):
        if b < MOM_RANGE_LOOKBACK:
            continue
        atr_b = _atr_at(rows, b, 14)
        if not atr_b or atr_b <= 0:
            continue
        base = rows[b - MOM_RANGE_LOOKBACK:b]
        range_high = max(r["high"] for r in base)
        range_low = min(r["low"] for r in base)
        rb = rows[b]
        body_ratio = _body_ratio(rb)
        prior_avg_vol = _rolling_mean(vols[max(0, b-20):b], 20)
        if prior_avg_vol <= 0:
            continue
        vol_mult = rb.get("vol", 0.0) / prior_avg_vol
        body = abs(rb["close"] - rb["open"])
        if direction == "LONG":
            breakout_ok = rb["close"] > range_high and rb["close"] - range_high >= MOM_MIN_BREAKOUT_ATR * atr_b
            candle_ok = _candle_bull(rb)
        else:
            breakout_ok = rb["close"] < range_low and range_low - rb["close"] >= MOM_MIN_BREAKOUT_ATR * atr_b
            candle_ok = _candle_bear(rb)
        if breakout_ok and candle_ok and body / max(atr_b, 1e-12) >= 0.25 and body_ratio >= MOM_MIN_BODY_RATIO and vol_mult >= MOM_MIN_VOLUME_MULT:
            candidates.append((b, range_high, range_low, atr_b, vol_mult))

    if not candidates:
        return None

    # Prefer the latest valid breakout, then decide whether we have a direct
    # early entry or a controlled pullback/continuation entry.
    for b, range_high, range_low, atr_b, breakout_vol in candidates:
        age = cur_i - b
        if age > MOM_MAX_PULLBACK_BARS:
            continue

        # Direct breakout entry is allowed only on the breakout candle itself.
        if age == 0:
            trigger = cur
            extension = abs(trigger["close"] - (range_high if direction == "LONG" else range_low)) / atr_now
            if extension > MOM_MAX_EXTENSION_ATR:
                continue
            entry = trigger["close"]
            swing_low = min(r["low"] for r in rows[max(0, b-3):b+1])
            swing_high = max(r["high"] for r in rows[max(0, b-3):b+1])
            pullback_idx = None
            pattern = "EXPLOSIVE BREAKOUT" if body_ratio >= 0.65 and breakout_vol >= 1.7 else "BREAKOUT MOMENTUM"
        else:
            post = rows[b+1:cur_i+1]
            if not post:
                continue
            # Pullback must actually retrace part of the impulse, but not destroy
            # the breakout level. This is the anti-chase gate.
            if direction == "LONG":
                pull_low = min(r["low"] for r in post)
                pullback_depth = max(0.0, range_high - pull_low)
                if pull_low < range_high - 0.75 * atr_b:
                    # Too deep: breakout has likely failed.
                    continue
                if pullback_depth < MOM_PULLBACK_ATR * atr_b:
                    # No meaningful reset; avoid buying the top of a straight move.
                    continue
                continuation_level = max(r["high"] for r in rows[b:cur_i]) if cur_i > b else range_high
                trigger_ok = cur["close"] > max(range_high, continuation_level) and _candle_bull(cur)
                swing_low = pull_low
                swing_high = max(r["high"] for r in rows[b:cur_i+1])
            else:
                pull_high = max(r["high"] for r in post)
                pullback_depth = max(0.0, pull_high - range_low)
                if pull_high > range_low + 0.75 * atr_b:
                    continue
                if pullback_depth < MOM_PULLBACK_ATR * atr_b:
                    continue
                continuation_level = min(r["low"] for r in rows[b:cur_i]) if cur_i > b else range_low
                trigger_ok = cur["close"] < min(range_low, continuation_level) and _candle_bear(cur)
                swing_high = pull_high
                swing_low = min(r["low"] for r in rows[b:cur_i+1])
            cur_vol = cur.get("vol", 0.0)
            cur_vol_mult = cur_vol / max(avg_vol, 1e-12)
            if not trigger_ok or cur_vol_mult < MOM_TRIGGER_VOLUME_MULT or _body_ratio(cur) < MOM_MIN_BODY_RATIO:
                continue
            extension = abs(cur["close"] - (range_high if direction == "LONG" else range_low)) / atr_now
            if extension > MOM_MAX_EXTENSION_ATR:
                continue
            entry = cur["close"]
            pullback_idx = cur_i
            pattern = "MOMENTUM CONTINUATION"

        # Risk is volatility/structure based, not a tiny fixed-distance stop.
        # The invalidation level must clear both the pullback swing and the
        # original breakout range, then gets an ATR safety buffer.  A minimum
        # ATR risk floor prevents microscopic SL/TP clusters on quiet 15m moves.
        if direction == "LONG":
            invalidation = min(swing_low, range_low)
            sl = invalidation - sl_atr_buffer * atr_now
            min_sl = entry - MOM_MIN_RISK_ATR * atr_now
            sl = min(sl, min_sl)
            if sl >= entry:
                continue
            risk = entry - sl
            risk_atr = risk / max(atr_now, 1e-12)
            if risk_atr > MOM_MAX_RISK_ATR:
                continue
            t1, t2, t3 = [entry + risk * float(x) for x in tp_multipliers]
        else:
            invalidation = max(swing_high, range_high)
            sl = invalidation + sl_atr_buffer * atr_now
            min_sl = entry + MOM_MIN_RISK_ATR * atr_now
            sl = max(sl, min_sl)
            if sl <= entry:
                continue
            risk = sl - entry
            risk_atr = risk / max(atr_now, 1e-12)
            if risk_atr > MOM_MAX_RISK_ATR:
                continue
            t1, t2, t3 = [entry - risk * float(x) for x in tp_multipliers]

        if risk <= 0 or risk > entry * 0.12:
            continue
        cur_vol_mult = cur.get("vol", 0.0) / max(avg_vol, 1e-12)
        score = 0
        score += 1 if breakout_vol >= 1.30 else 0
        score += 1 if cur_vol_mult >= 1.10 else 0
        score += 1 if body_ratio >= 0.60 else 0
        score += 1 if atr_now >= atr_b * 0.85 else 0
        score += 1 if age <= 4 else 0

        return {
            "symbol": "", "direction": direction,
            "structure": pattern,
            "pattern": pattern,
            "entry": entry, "trigger_level": range_high if direction == "LONG" else range_low,
            "sl": sl, "tp1": t1, "tp2": t2, "tp3": t3,
            "score": score, "max_score": 5, "time": cur["time"],
            "breakout_index": b, "pullback_index": pullback_idx,
            "range_high": range_high, "range_low": range_low,
            "breakout_atr": atr_b, "atr": atr_now,
            "risk_distance": risk, "risk_atr": risk / max(atr_now, 1e-12),
            "risk_pct": (risk / max(entry, 1e-12)) * 100.0,
            "breakout_volume_mult": breakout_vol, "volume_mult": cur_vol_mult,
            "ema9": emas[0], "ema21": emas[1], "ema50": emas[2],
            "extension_atr": extension, "rows": rows[max(0, b-25):], "full_len": len(rows),
            "checks": {"Range": True, "Breakout": True, "Volume": breakout_vol >= MOM_MIN_VOLUME_MULT,
                       "Trend": True, "Pullback": age > 0, "Continuation": age > 0},
            "retest_ok": age > 0, "rejection_ok": True, "early_entry": age <= 4,
            "fvg": {"low": min(range_low, entry), "high": max(range_high, entry), "index": b},
            "ob": {"low": swing_low, "high": swing_high, "index": pullback_idx if pullback_idx is not None else b},
            "entry_zone": {"low": min(range_low, range_high), "high": max(range_low, range_high)},
            "entry_zone_low": min(range_low, range_high), "entry_zone_high": max(range_low, range_high),
        }
    return None


def _pine_rma(values, period):
    """TradingView ta.rma() equivalent for a fully known historical series."""
    out = [None] * len(values)
    if len(values) < period:
        return out
    seed = sum(values[:period]) / float(period)
    out[period - 1] = seed
    alpha = 1.0 / float(period)
    prev = seed
    for i in range(period, len(values)):
        v = values[i]
        prev = alpha * v + (1.0 - alpha) * prev
        out[i] = prev
    return out


def _pine_atr_series(rows, period=13):
    if not rows:
        return []
    trs = []
    for i, r in enumerate(rows):
        if i == 0:
            tr = r["high"] - r["low"]
        else:
            pc = rows[i - 1]["close"]
            tr = max(r["high"] - r["low"], abs(r["high"] - pc), abs(r["low"] - pc))
        trs.append(max(float(tr), 0.0))
    return _pine_rma(trs, period)


def _pine_median(values):
    vals = sorted(float(v) for v in values if v is not None and math.isfinite(float(v)))
    if not vals:
        return 0.0
    m = len(vals)
    mid = m // 2
    if m % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0


def _sa_weight_series(rows):
    """Exact default SA-VWAP weighting: cumulative volume, capped at 4x median(50)."""
    weights = []
    for i, r in enumerate(rows):
        raw = max(float(r.get("vol", 0.0)), 0.0)
        med = _pine_median([rows[k].get("vol", 0.0) for k in range(max(0, i - 49), i + 1)])
        wt = min(raw, med * SA_VWAP_VOLUME_CLAMP_MEDIAN) if med > 0 else raw
        weights.append(wt)
    return weights


def _pivot_at(rows, p, left=55, right=55):
    if p < left or p + right >= len(rows):
        return False, False
    h = rows[p]["high"]
    l = rows[p]["low"]
    hs = [rows[k]["high"] for k in range(p - left, p + right + 1)]
    ls = [rows[k]["low"] for k in range(p - left, p + right + 1)]
    # Pine ta.pivothigh/low accepts equality at the pivot extreme.
    return h >= max(hs), l <= min(ls)


def _sa_build_leg(rows, start, end, direction, weights, atrs, source_points=None):
    """Rebuild one Pine Leg from its anchor through a confirmed bar."""
    sum_w = sum_pw = sum_p2 = 0.0
    away = 0
    retests = 0
    points = []
    for j in range(start, end + 1):
        px = (rows[j]["high"] + rows[j]["low"]) / 2.0
        wt = weights[j]
        sum_w += wt
        sum_pw += px * wt
        sum_p2 += px * px * wt
        if sum_w > 0:
            vwap = sum_pw / sum_w
            sigma = math.sqrt(max(sum_p2 / sum_w - vwap * vwap, 0.0))
        else:
            vwap = None
            sigma = 0.0
        if vwap is None:
            continue
        tol = sigma * SA_VWAP_RETEST_TOL_SIGMA if sigma > 0 else (atrs[j] or 0.0) * 0.1
        r = rows[j]
        touch = r["low"] <= vwap + tol and r["high"] >= vwap - tol
        outside = (r["low"] > vwap + tol) if direction > 0 else (r["high"] < vwap - tol)
        hit = touch and away >= SA_VWAP_RETEST_MIN_AWAY
        if hit:
            retests += 1
        points.append({
            "index": j, "vwap": vwap, "sigma": sigma, "hit": hit,
            "away_before": away, "touch": touch, "outside": outside,
            "retests": retests, "weight": wt,
        })
        # Pine's putPoint commits the current bar on a confirmed historical bar.
        away = 0 if touch else (away + 1 if outside else 0)
    return points


def _sa_vwap_setup(rows, direction):
    """Structure-breakout signal engine.

    The signal is based on the visible market structure shown in the user's
    TradingView examples: a compact consolidation/range is bounded by a
    confirmed swing high and swing low, then a NEW CLOSED candle breaks one
    of those boundaries.

      LONG  -> closed candle breaks above Structure High.
      SHORT -> closed candle breaks below Structure Low.

    Entry is the breakout candle close.  SL is placed beyond the opposite
    structure boundary (with a small ATR buffer), so risk follows the actual
    setup instead of collapsing into a tiny ATR-based cluster.  TP1/TP2/TP3
    are measured from that real structure risk.
    """
    n = len(rows)
    if n < 80:
        return None

    # Work only with confirmed pivots. The newest two candles are excluded
    # from structure discovery so the current candle can be the breakout.
    confirmed = rows[:-2]
    if len(confirmed) < 30:
        return None

    atr_now = atr(rows, 14) or 0.0
    if atr_now <= 0:
        return None

    # Search recent confirmed swing points. We deliberately use a short
    # structural window: this is the local box visible in the examples, not
    # an old all-chart high/low.
    search_start = max(0, len(confirmed) - 55)
    highs, lows = swing_points(confirmed[search_start:], left=2, right=2)
    highs = [(i + search_start, px) for i, px in highs]
    lows = [(i + search_start, px) for i, px in lows]
    if not highs or not lows:
        return None

    cur = rows[-1]
    prev = rows[-2]
    candidates = []

    # Candidate range: use the latest swing high/low pair that forms a compact
    # box with several candles after both pivots. Prefer the most recent pair.
    for hi_i, hi_px in reversed(highs[-8:]):
        for lo_i, lo_px in reversed(lows[-8:]):
            if hi_px <= lo_px:
                continue
            start = max(hi_i, lo_i) + 1
            end = n - 1
            if start >= end:
                continue
            width = hi_px - lo_px
            if width <= 0 or width > atr_now * 8.0:
                continue
            box = rows[start:end]
            if len(box) < 4:
                continue
            # Most of the pre-breakout candles should remain inside/near the
            # box. A small wick outside is allowed; repeated closes outside
            # mean this is no longer the same structure.
            inside = 0
            for r in box:
                if lo_px - atr_now * 0.25 <= r["close"] <= hi_px + atr_now * 0.25:
                    inside += 1
            if inside / max(1, len(box)) < 0.60:
                continue
            candidates.append((max(hi_i, lo_i), hi_i, hi_px, lo_i, lo_px, width))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    _, hi_i, structure_high, lo_i, structure_low, structure_width = candidates[-1]

    # Fresh confirmed breakout only. This is intentionally based on the
    # CLOSED candle, not an intrabar wick.
    long_break = direction == "LONG" and prev["close"] <= structure_high and cur["close"] > structure_high
    short_break = direction == "SHORT" and prev["close"] >= structure_low and cur["close"] < structure_low
    if not (long_break or short_break):
        return None

    actual_direction = "LONG" if long_break else "SHORT"
    entry = float(cur["close"])
    buffer = atr_now * 0.12

    if actual_direction == "LONG":
        sl = structure_low - buffer
        risk = entry - sl
        structure_swing_index = lo_i
        structure_swing_price = structure_low
        structure_swing_type = "LOW"
    else:
        sl = structure_high + buffer
        risk = sl - entry
        structure_swing_index = hi_i
        structure_swing_price = structure_high
        structure_swing_type = "HIGH"

    # Reject only pathological structures. Do NOT squeeze a valid setup back
    # toward Entry; the whole point is that risk comes from structure.
    if risk <= atr_now * 0.30 or risk > atr_now * 12.0:
        return None

    tp1_r, tp2_r, tp3_r = SA_VWAP_TP_R
    if actual_direction == "LONG":
        tp1, tp2, tp3 = entry + risk * tp1_r, entry + risk * tp2_r, entry + risk * tp3_r
    else:
        tp1, tp2, tp3 = entry - risk * tp1_r, entry - risk * tp2_r, entry - risk * tp3_r

    # Informational strength: structure quality + breakout distance. No extra
    # indicator is allowed to veto the structure signal.
    body = abs(cur["close"] - cur["open"])
    body_ratio = body / max(cur["high"] - cur["low"], 1e-12)
    break_dist = (entry - structure_high) if actual_direction == "LONG" else (structure_low - entry)
    strength = 55.0
    strength += min(25.0, max(0.0, break_dist / max(atr_now, 1e-12) * 12.0))
    strength += min(20.0, body_ratio * 20.0)
    strength = min(100.0, strength)

    prior_vol = _rolling_mean([r.get("vol", 0.0) for r in rows[:-1]], 20)
    vol_mult = cur.get("vol", 0.0) / max(prior_vol, 1e-12) if prior_vol > 0 else 0.0

    structure_points = [
        (hi_i, structure_high, "SH", "HIGH"),
        (lo_i, structure_low, "SL", "LOW"),
    ]

    return {
        "symbol": "", "direction": actual_direction,
        "structure": "STRUCTURE BREAKOUT", "pattern": "STRUCTURE BREAKOUT",
        "entry": entry, "trigger_level": structure_high if actual_direction == "LONG" else structure_low,
        "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "score": int(round(strength / 20.0)), "max_score": 5,
        "strength": strength, "sa_vwap": None, "sa_sigma": 0.0,
        "leg_direction": 1 if actual_direction == "LONG" else -1,
        "leg_anchor_index": min(hi_i, lo_i),
        "leg_anchor_px": structure_low if actual_direction == "LONG" else structure_high,
        "anchor_tag": "SH/SL",
        "leg_retests": 0, "leg_balance": 50.0,
        "risk_distance": risk, "risk_atr": risk / atr_now,
        "risk_pct": risk / max(entry, 1e-12) * 100.0,
        "atr": atr_now, "volume_mult": vol_mult,
        "breakout_volume_mult": vol_mult, "extension_atr": abs(entry - (structure_high if actual_direction == "LONG" else structure_low)) / max(atr_now, 1e-12),
        "breakout_index": n - 1, "pullback_index": n - 1,
        "range_high": structure_high, "range_low": structure_low,
        "structure_swing_index": structure_swing_index,
        "structure_swing_price": structure_swing_price,
        "structure_swing_type": structure_swing_type,
        "structure_high": structure_high, "structure_low": structure_low,
        "structure_high_index": hi_i, "structure_low_index": lo_i,
        "structure_width": structure_width,
        "retest_ok": True, "rejection_ok": True, "early_entry": True,
        "checks": {"Structure": True, "Breakout": True, "Closed candle": True},
        "fvg": None, "ob": None,
        "entry_zone_low": structure_low, "entry_zone_high": structure_high,
        "rows": rows, "full_len": len(rows),
        "vwap_series": [], "structure_points": structure_points,
        "leg_dir_series": [], "signal_bar": n - 1,
        "signal_time": cur["time"],
    }

def _move_setup(rows, direction):
    """Use the supplied TradingView SA-VWAP logic as the bot's signal engine."""
    if not SA_VWAP_ENABLED:
        return None
    return _sa_vwap_setup(rows, direction)


def analyze(symbol, rows5=None, rows15=None, timeframe=SIGNAL_TIMEFRAME):
    """SA-VWAP only. Signals are generated from the newest confirmed candle."""
    rows = rows15 if rows15 and len(rows15) >= 120 else rows5
    if not rows or len(rows) < 120:
        return None
    for r in rows:
        r["symbol"] = symbol
    for direction in ("LONG", "SHORT"):
        sig = _move_setup(rows, direction)
        if sig:
            sig["symbol"] = symbol
            sig["timeframe"] = timeframe
            sig["time"] = sig.get("signal_time", rows[-1]["time"])
            sig["candle_time"] = sig["time"]
            return sig
    return None

def make_chart(sig):
    """Clean, wide TradingView-style signal chart: candles + ENTRY + SL only.

    This function changes chart presentation only. Signal logic, Entry/SL/TP
    calculations, scanning, monitoring and Telegram message contents are left
    untouched.
    """
    rows = sig.get("rows", [])[-110:]
    n = len(rows)
    if n < 2:
        raise RuntimeError("not enough candles for chart")

    BG = "#131722"
    GRID = "#2A2E39"
    TEXT = "#E0E0E0"
    MUTED = "#9E9E9E"
    BULL = "#00E676"
    BEAR = "#FF5252"
    ENTRY = "#4FC3F7"
    SL = "#EF5350"

    fig, ax = plt.subplots(figsize=(15.2, 7.9), dpi=160, facecolor=BG)
    ax.set_facecolor(BG)

    # Candles: keep them large enough to read, but show a broad section of
    # price action like the user's TradingView examples.
    width = 0.62
    for i, r in enumerate(rows):
        c = BULL if r["close"] >= r["open"] else BEAR
        ax.vlines(i, r["low"], r["high"], color=c, linewidth=1.0, zorder=4)
        lo = min(r["open"], r["close"])
        bh = max(abs(r["close"] - r["open"]), abs(r["close"]) * 1e-6)
        ax.add_patch(
            Rectangle(
                (i - width / 2, lo), width, bh,
                facecolor=c, edgecolor=c, linewidth=.45, zorder=5
            )
        )

    entry = float(sig["entry"])
    stop = float(sig["sl"])

    # ONLY the two requested trade levels are drawn on the chart.
    # They are short local segments around the signal, never full-width lines.
    signal_i = sig.get("signal_bar")
    full_start = sig.get("full_len", len(rows)) - len(rows)
    local_i = None if signal_i is None else int(signal_i) - int(full_start)
    if local_i is None or not (0 <= local_i < n):
        local_i = n - 8
    line_start = max(0, local_i - 18)
    line_end = min(n - 1, local_i + 10)
    ax.plot([line_start, line_end], [entry, entry], color=ENTRY, linewidth=1.35,
            linestyle=(0, (5, 3)), solid_capstyle="butt", zorder=7)
    ax.plot([line_start, line_end], [stop, stop], color=SL, linewidth=1.35,
            solid_capstyle="butt", zorder=7)

    # Mark the confirmed signal candle without adding extra structure objects.
    if signal_i is not None:
        if 0 <= local_i < n:
            if sig.get("direction") == "LONG":
                y = rows[local_i]["low"]
                ax.scatter([local_i], [y], marker="^", s=78, color=BULL,
                           edgecolors="#FFFFFF", linewidth=.65, zorder=10)
            else:
                y = rows[local_i]["high"]
                ax.scatter([local_i], [y], marker="v", s=78, color=BEAR,
                           edgecolors="#FFFFFF", linewidth=.65, zorder=10)

    # Right-side labels: ENTRY and SL only.
    xlab = n + 1.5
    ax.text(xlab, entry, f"ENTRY {fmt_price(entry)}",
            color=ENTRY, fontsize=8.8, fontweight="bold",
            va="center", ha="left", zorder=15)
    ax.text(xlab, stop, f"SL {fmt_price(stop)}",
            color=SL, fontsize=8.8, fontweight="bold",
            va="center", ha="left", zorder=15)

    # Minimal header, matching the clean TradingView look.
    symbol = sig.get("symbol", "")
    timeframe = sig.get("timeframe", "15m").upper()
    direction = sig.get("direction", "LONG")
    dcol = BULL if direction == "LONG" else BEAR

    ax.text(.018, 1.055, f"{symbol} · {timeframe}",
            transform=ax.transAxes, fontsize=14.5, color=TEXT,
            fontweight="bold", va="top")
    ax.text(.018, 1.018, "SAIWAN · CLOSED CANDLE",
            transform=ax.transAxes, fontsize=8.3, color=MUTED,
            fontweight="bold", va="top")
    ax.text(.985, 1.055, direction,
            transform=ax.transAxes, fontsize=11.5, color=dcol,
            fontweight="bold", va="top", ha="right")

    ax.yaxis.tick_right()
    ax.tick_params(axis="y", colors="#9E9E9E", labelsize=8, length=0, pad=7)
    ax.tick_params(axis="x", colors="#757575", labelsize=7.5, length=0, pad=8)
    ax.grid(axis="y", color=GRID, linewidth=.55, alpha=.75)
    ax.grid(axis="x", color=GRID, linewidth=.25, alpha=.3)

    for side in ("top", "left", "bottom"):
        ax.spines[side].set_visible(False)
    ax.spines["right"].set_color(GRID)

    step = max(1, n // 9)
    ticks = list(range(0, n, step))
    if not ticks or ticks[-1] != n - 1:
        ticks.append(n - 1)
    ax.set_xticks(ticks)
    ax.set_xticklabels([
        datetime.fromtimestamp(rows[i]["time"], tz=timezone.utc).strftime("%d\n%H:%M")
        for i in ticks
    ])

    # IMPORTANT: TP values are intentionally NOT included in chart scaling.
    price_levels = [r["low"] for r in rows] + [r["high"] for r in rows] + [entry, stop]
    ymin, ymax = min(price_levels), max(price_levels)
    span = max(ymax - ymin, abs(rows[-1]["close"]) * .006)
    ax.set_ylim(ymin - span * .06, ymax + span * .08)
    ax.set_xlim(-1, n + 4)

    fig.subplots_adjust(left=.025, right=.84, top=.86, bottom=.085)
    safe = "".join(ch if ch.isalnum() else "_" for ch in symbol)
    path = f"/tmp/chart_{safe}_{sig['time']}.png"
    fig.savefig(path, facecolor=BG, edgecolor="none",
                bbox_inches="tight", pad_inches=.08)
    plt.close(fig)
    return path

def make_analysis_charts(raw_symbol, requested_timeframes=None):
    symbol=_normalize_analysis_symbol(raw_symbol)
    tfs=list(requested_timeframes or [])
    if not tfs:
        tfs=[SIGNAL_TIMEFRAME]
    paths=[]
    for tf in tfs:
        rows=_analysis_tf_data(symbol,tf)
        block=None
        try:
            long_sig=_sa_vwap_setup(rows,"LONG")
            short_sig=_sa_vwap_setup(rows,"SHORT")
            block={"long_sig":long_sig,"short_sig":short_sig}
        except Exception:
            block=None
        paths.append(make_analysis_chart(symbol,tf,rows,block))
    return paths


def telegram_url(method):
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    return TELEGRAM_API + TOKEN + "/" + method


def _inline_button(text, callback_data):
    return {"text": text, "callback_data": callback_data}


def main_menu_markup():
    return {
        "inline_keyboard": [
            [_inline_button("🔥 SCAN MARKET", "menu_scan"), _inline_button("🎯 TOP SA-VWAP", "scan_top")],
            [_inline_button("📈 MOVERS", "movers"), _inline_button("🔎 ANALYSIS", "menu_analysis")],
            [_inline_button("👁 WATCHLIST", "watchlist"), _inline_button("🔔 ALERTS", "alerts")],
            [_inline_button("📜 HISTORY", "history"), _inline_button("📊 STATS", "stats")],
            [_inline_button("💰 RISK", "risk"), _inline_button("⚙️ SETTINGS", "menu_settings")],
            [_inline_button("🟢 BOT STATUS", "status"), _inline_button("🛑 STOP SCANNER", "stop")],
        ]
    }


def scan_menu_markup():
    return {
        "inline_keyboard": [
            [_inline_button("⚡ 1 MIN", "scan_tf:1m"), _inline_button("⚡ 5 MIN", "scan_tf:5m")],
            [_inline_button("⚡ 15 MIN", "scan_tf:15m"), _inline_button("🕐 30 MIN", "scan_tf:30m")],
            [_inline_button("🕐 1 HOUR", "scan_tf:1h"), _inline_button("🕑 2 HOURS", "scan_tf:2h")],
            [_inline_button("🕓 4 HOURS", "scan_tf:4h")],
            [_inline_button("◀️ BACK", "menu_main")],
        ]
    }


def analysis_menu_markup():
    coins = [("BTC", "BTC"), ("ETH", "ETH"), ("SOL", "SOL"), ("BNB", "BNB"), ("XRP", "XRP"), ("AVAX", "AVAX")]
    rows = []
    for i in range(0, len(coins), 2):
        rows.append([_inline_button(f"🔎 {coins[i][0]}", f"analysis_coin:{coins[i][1]}"), _inline_button(f"🔎 {coins[i+1][0]}", f"analysis_coin:{coins[i+1][1]}")])
    rows.append([_inline_button("◀️ BACK", "menu_main")])
    return {"inline_keyboard": rows}


def analysis_timeframe_markup(symbol):
    return {"inline_keyboard": [
        [_inline_button("1 MIN", f"analysis:{symbol}:1m"), _inline_button("5 MIN", f"analysis:{symbol}:5m")],
        [_inline_button("15 MIN", f"analysis:{symbol}:15m"), _inline_button("30 MIN", f"analysis:{symbol}:30m")],
        [_inline_button("1 HOUR", f"analysis:{symbol}:1h"), _inline_button("4 HOURS", f"analysis:{symbol}:4h")],
        [_inline_button("◀️ COINS", "menu_analysis")],
    ]}


def settings_menu_markup():
    return {
        "inline_keyboard": [
            [_inline_button("🔔 ALERTS ON", "settings_alerts_on"), _inline_button("🔕 ALERTS OFF", "settings_alerts_off")],
            [_inline_button("🟢 STATUS", "status")],
            [_inline_button("◀️ BACK", "menu_main")],
        ]
    }


def welcome_text():
    return (
        "🚀 SAIWAN CRYPTO SIGNALS\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📐 SA-VWAP ENGINE\n"
        "📊 15M • BITGET FUTURES\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "🎯 Anchored VWAP • Confirmed Close Cross • Structure SL • ATR Targets • Break-even\n"
        "🔔 Signal + TP/SL monitoring: ACTIVE\n\n"
        "Choose an action from the buttons below."
    )


def edit_message(chat_id, message_id, text, reply_markup=None):
    data = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    r = requests.post(telegram_url("editMessageText"), data=data, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram editMessageText {r.status_code}: {r.text[:500]}")
    return r.json()


def answer_callback(callback_id, text=None):
    data = {"callback_query_id": callback_id}
    if text:
        data["text"] = text
    r = requests.post(telegram_url("answerCallbackQuery"), data=data, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram answerCallbackQuery {r.status_code}: {r.text[:500]}")


def _scan_timeframe_label(timeframe):
    return {
        "1m": "1 MIN",
        "5m": "5 MIN",
        "15m": "15 MIN",
        "30m": "30 MIN",
        "1h": "1 HOUR",
        "2h": "2 HOURS",
        "4h": "4 HOURS",
    }.get(timeframe, str(timeframe).upper())


def _normalize_scan_timeframe(raw):
    tf = (raw or "").strip().lower()
    aliases = {
        "1": "1m", "1m": "1m",
        "5": "5m", "5m": "5m",
        "15": "15m", "15m": "15m",
        "30": "30m", "30m": "30m",
        "1h": "1h", "1hour": "1h", "1hr": "1h",
        "2h": "2h", "2hour": "2h", "2hr": "2h",
        "4h": "4h", "4hour": "4h", "4hr": "4h",
    }
    tf = aliases.get(tf)
    return tf if tf in SUPPORTED_SCAN_TIMEFRAMES else None


def _handle_callback_query(query):
    global active_chat_id
    callback_id = query.get("id")
    data = str(query.get("data") or "")
    msg = query.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    message_id = msg.get("message_id")
    if not chat_id or not message_id:
        if callback_id:
            answer_callback(callback_id)
        return
    active_chat_id = chat_id
    try:
        if data == "menu_main":
            edit_message(chat_id, message_id, welcome_text(), main_menu_markup())
            answer_callback(callback_id)
        elif data == "menu_scan":
            edit_message(chat_id, message_id, "🔥 SCAN MARKET\n\nChoose timeframe:", scan_menu_markup())
            answer_callback(callback_id)
        elif data == "menu_analysis":
            edit_message(chat_id, message_id, "🔎 15M ANALYSIS\n\nChoose a coin:", analysis_menu_markup())
            answer_callback(callback_id)
        elif data == "menu_settings":
            edit_message(chat_id, message_id, "⚙️ SETTINGS\n\nChoose an option:", settings_menu_markup())
            answer_callback(callback_id)
        elif data.startswith("scan_tf:"):
            tf = _normalize_scan_timeframe(data.split(":", 1)[1])
            if not tf:
                answer_callback(callback_id, "Unsupported timeframe")
                return
            label = _scan_timeframe_label(tf)
            start_scanner(chat_id, tf)
            edit_message(
                chat_id, message_id,
                f"🚀 {label} SCANNER STARTED\n\n"
                f"The SA-VWAP Engine is now scanning {label} closed candles.\n\n"
                "📐 Anchored VWAP + Retest + ATR + BE\n"
                "🔒 Anti-chase filter: ON\n"
                "🎯 TP/SL monitoring: ON",
                main_menu_markup(),
            )
            answer_callback(callback_id, f"{label} scanner started")
        elif data == "scan_top":
            answer_callback(callback_id, "Scanning top SA-VWAP setups…")
            report = _smart_scan_report()
            edit_message(chat_id, message_id, report, main_menu_markup())
        elif data == "movers":
            answer_callback(callback_id, "Loading 24H movers…")
            edit_message(chat_id, message_id, _movers_report(), main_menu_markup())
        elif data == "watchlist":
            answer_callback(callback_id)
            edit_message(chat_id, message_id, "👁 WATCHLIST\n\n" + _watch_text(), main_menu_markup())
        elif data == "alerts":
            answer_callback(callback_id)
            edit_message(chat_id, message_id, "🔔 RECENT ALERTS\n\n" + _alerts_text(), main_menu_markup())
        elif data == "history":
            answer_callback(callback_id)
            edit_message(chat_id, message_id, "📜 SIGNAL HISTORY\n\n" + _history_text(None), main_menu_markup())
        elif data == "stats":
            answer_callback(callback_id)
            edit_message(chat_id, message_id, "📊 SIGNAL STATISTICS\n\n" + _stats_text(None), main_menu_markup())
        elif data == "risk":
            answer_callback(callback_id)
            edit_message(chat_id, message_id, "💰 RISK / POSITION SIZE\n\nFor custom position sizing use:\n/risk BTC 1000 1\n\nFor entry/SL based sizing:\n/risk BTC 1000 1 105000 103800", main_menu_markup())
        elif data == "status":
            answer_callback(callback_id)
            edit_message(chat_id, message_id, "🟢 BOT STATUS\n\n" + status_text(), main_menu_markup())
        elif data == "stop":
            stop_scanner()
            answer_callback(callback_id, "Scanner stopped")
            edit_message(chat_id, message_id, "🛑 SCANNER STOPPED\n\nSmart Watch remains available if enabled.", main_menu_markup())
        elif data.startswith("analysis_coin:"):
            symbol = _normalize_analysis_symbol(data.split(":", 1)[1])
            answer_callback(callback_id, f"Choose timeframe for {symbol}")
            edit_message(chat_id, message_id, f"🔎 ANALYSIS — {symbol}\n\nChoose timeframe:", analysis_timeframe_markup(symbol))
        elif data.startswith("analysis:"):
            parts = data.split(":")
            symbol = _normalize_analysis_symbol(parts[1])
            tf = _normalize_analysis_timeframe(parts[2] if len(parts) > 2 else TF_15M) or TF_15M
            answer_callback(callback_id, f"Analyzing {symbol} {tf.upper()}…")
            report = analysis_report(symbol, [tf])
            edit_message(chat_id, message_id, f"🔎 {tf.upper()} ANALYSIS\n\n" + report, analysis_timeframe_markup(symbol))
            try:
                for chart_path in make_analysis_charts(symbol, [tf]):
                    send_photo(chat_id, chart_path, f"📊 SAIWAN CHART — {symbol} · {tf.upper()}")
            except Exception as e:
                print(f"BUTTON ANALYSIS CHART ERROR {type(e).__name__}: {e}")
        elif data == "settings_alerts_on":
            with watch_lock:
                bot_settings["alerts"] = True
            answer_callback(callback_id, "Alerts enabled")
            edit_message(chat_id, message_id, "⚙️ SETTINGS\n\n🔔 Smart Watch alerts: ON", settings_menu_markup())
        elif data == "settings_alerts_off":
            with watch_lock:
                bot_settings["alerts"] = False
            answer_callback(callback_id, "Alerts disabled")
            edit_message(chat_id, message_id, "⚙️ SETTINGS\n\n🔕 Smart Watch alerts: OFF", settings_menu_markup())
        else:
            answer_callback(callback_id, "Unknown button")
    except Exception as e:
        print(f"CALLBACK ERROR {data}: {type(e).__name__}: {e}")
        try:
            answer_callback(callback_id, "Action failed")
        except Exception:
            pass


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


def _normalize_analysis_symbol(raw):
    """Normalize a Telegram /analysis symbol to a Bitget USDT perpetual symbol."""
    symbol = (raw or "").strip().upper().replace("/", "").replace("-", "")
    if not symbol:
        return None
    if symbol.endswith("USDT"):
        return symbol
    return symbol + "USDT"


def _normalize_analysis_timeframe(raw):
    """Normalize on-demand analysis to the supported 15m/30m/1h/4h set."""
    tf = (raw or "").strip().lower()
    aliases = {
        "1": "1m", "1m": "1m",
        "5": TF_5M, "5m": TF_5M,
        "15": TF_15M, "15m": TF_15M,
        "30": TF_30M, "30m": TF_30M,
        "1h": TF_1H, "1hour": TF_1H, "1hr": TF_1H,
        "4h": TF_4H, "4hour": TF_4H, "4hr": TF_4H,
    }
    return aliases.get(tf)


def _analysis_tf_data(symbol, timeframe):
    """Fetch closed candles for an on-demand analysis timeframe."""
    tf = _normalize_analysis_timeframe(timeframe)
    if tf not in ("1m", TF_5M, TF_15M, TF_30M, TF_1H, TF_4H):
        raise RuntimeError("unsupported analysis timeframe")
    rows = get_klines(symbol, tf, max(CANDLE_LIMIT, 180))
    if len(rows) < 120:
        raise RuntimeError(f"not enough {tf} candles")
    return rows


def _timeframe_bias(rows):
    closes = [r["close"] for r in rows]
    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    cur = closes[-1]
    long_score = int(cur > e20) + int(e20 > e50)
    short_score = int(cur < e20) + int(e20 < e50)
    if long_score == 2 and short_score == 0:
        bias = "LONG"
    elif short_score == 2 and long_score == 0:
        bias = "SHORT"
    else:
        bias = "MIXED"
    return cur, e20, e50, long_score, short_score, bias


def _timeframe_setup(rows, direction):
    """Run the same SA-VWAP close-cross engine used by the live scanner."""
    try:
        return _sa_vwap_setup(rows, direction)
    except Exception:
        return None


def _format_htf_block(symbol, timeframe):
    rows = _analysis_tf_data(symbol, timeframe)
    cur, e20, e50, long_score, short_score, bias = _timeframe_bias(rows)
    long_sig = _timeframe_setup(rows, "LONG")
    short_sig = _timeframe_setup(rows, "SHORT")

    if long_sig and not short_sig:
        setup = "🟢 LONG setup confirmed"
    elif short_sig and not long_sig:
        setup = "🔴 SHORT setup confirmed"
    elif long_sig and short_sig:
        setup = "🟡 BOTH directions have setup conditions"
    else:
        setup = "⚪ No fresh SA-VWAP close-cross setup"

    return {
        "timeframe": timeframe,
        "price": cur,
        "ema20": e20,
        "ema50": e50,
        "long_score": long_score,
        "short_score": short_score,
        "bias": bias,
        "setup": setup,
        "long_sig": long_sig,
        "short_sig": short_sig,
    }


def _analysis_verdict(blocks):
    """Combine requested timeframe biases without inventing a trade signal."""
    long_votes = sum(b["bias"] == "LONG" for b in blocks)
    short_votes = sum(b["bias"] == "SHORT" for b in blocks)
    if long_votes and not short_votes:
        return "🟢 LONG bias across requested timeframes"
    if short_votes and not long_votes:
        return "🔴 SHORT bias across requested timeframes"
    if long_votes == short_votes == 0:
        return "🟡 WAIT — higher-timeframe bias is mixed"
    return "🟡 MIXED — higher timeframes disagree"


def analysis_report(raw_symbol, requested_timeframes=None):
    """On-demand SA-VWAP analysis for 1m, 5m, 15m, 30m, 1h and 4h."""
    symbol = _normalize_analysis_symbol(raw_symbol)
    if not symbol or len(symbol) < 6:
        return "❌ تکایە ناوی کۆین بنووسە.\n\nنموونە: /analysis BTC 15m"

    requested = []
    for raw_tf in (requested_timeframes or [SIGNAL_TIMEFRAME]):
        tf = _normalize_analysis_timeframe(raw_tf)
        if tf in ("1m", TF_5M, TF_15M, TF_30M, TF_1H, TF_4H) and tf not in requested:
            requested.append(tf)
    if not requested:
        return "❌ Timeframe ـەکە هەڵەیە.\nبەردەستە: 15m, 30m, 1h, 4h"

    blocks = []
    errors = []
    for tf in requested:
        try:
            blocks.append(_format_htf_block(symbol, tf))
        except Exception as e:
            errors.append(f"{tf.upper()}: {type(e).__name__}")

    if not blocks:
        return f"❌ نەتوانرا شیکاری {symbol} بکرێت. دڵنیابە کۆینەکە لە Bitget USDT Futures هەیە."

    lines = [f"🔎 SAIWAN ANALYSIS — {symbol}", ""]
    if len(blocks) > 1:
        lines += [f"🧭 { _analysis_verdict(blocks) }", ""]

    for block in blocks:
        setup = block["long_sig"] or block["short_sig"]
        tf = block["timeframe"]
        if block["bias"] == "LONG":
            verdict = "🟢 LONG bias"
        elif block["bias"] == "SHORT":
            verdict = "🔴 SHORT bias"
        else:
            verdict = "🟡 MIXED bias"
        lines += [
            f"⏱ {tf.upper()} · CLOSED CANDLES",
            verdict,
            f"💵 Price: {fmt_price(block['price'])}",
            f"EMA20: {fmt_price(block['ema20'])} | EMA50: {fmt_price(block['ema50'])}",
            f"📊 Checks — LONG {block['long_score']}/2 · SHORT {block['short_score']}/2",
            f"Setup: {block['setup']}",
        ]
        if setup:
            lines += [
                f"🎯 Entry: {fmt_price(setup['entry'])}",
                f"🛑 SL: {fmt_price(setup['sl'])}",
                f"🎯 TP1: {fmt_price(setup['tp1'])}",
                f"🎯 TP2: {fmt_price(setup['tp2'])}",
                f"🎯 TP3: {fmt_price(setup['tp3'])}",
                f"📐 Risk: {setup.get('risk_atr', 0):.2f}× ATR · R:R 1.5 / 3.0 / 4.5",
                f"⚡ {setup.get('pattern','SA-VWAP CLOSE CROSS')}",
            ]
        else:
            lines.append("ℹ️ No fresh SA-VWAP close-cross signal on the newest closed candle.")
        lines.append("")

    if errors:
        lines += [f"⚠️ بەشێک شیکاری نەکرا: {', '.join(errors)}"]
    return "\n".join(lines).strip()

def _setup_metrics(sig):
    """Transparent Structure Breakout metrics; quality is a rule count, not probability."""
    entry = float(sig["entry"])
    sl = float(sig["sl"])
    risk = abs(entry - sl)
    if risk <= 0:
        return {"risk":0.0,"rr1":0.0,"rr2":0.0,"rr3":0.0,"quality":0}
    rr1 = abs(float(sig["tp1"])-entry)/risk
    rr2 = abs(float(sig["tp2"])-entry)/risk
    rr3 = abs(float(sig["tp3"])-entry)/risk
    if sig.get("pattern") == "STRUCTURE BREAKOUT":
        checks = sig.get("checks") or {}
        quality = sum(bool(checks.get(k)) for k in ("Structure", "Breakout", "Closed candle"))
        quality += 1 if sig.get("structure_width", 0) > 0 else 0
        quality += 1 if sig.get("risk_atr", 99) >= 0.75 else 0
        quality += 1 if sig.get("volume_mult", 0) >= 1.0 else 0
        return {"risk":risk,"rr1":rr1,"rr2":rr2,"rr3":rr3,"quality":min(10, quality + 3)}
    checks = sig.get("checks") or {}
    quality = sum(bool(checks.get(k)) for k in ("Range","Breakout","Volume","Trend"))
    quality += 1 if sig.get("retest_ok") else 0
    quality += 1 if sig.get("extension_atr",99) <= 1.20 else 0
    quality += 1 if sig.get("volume_mult",0) >= 1.10 else 0
    return {"risk":risk,"rr1":rr1,"rr2":rr2,"rr3":rr3,"quality":min(10,quality)}

def _setup_detail_lines(sig):
    m = _setup_metrics(sig)
    return [
        (f"⭐ SA-VWAP Strength: {sig.get('strength',0):.0f}/100" if str(sig.get('pattern', '')).startswith('SA-VWAP') else f"⭐ Quality: {m['quality']}/10 (rule-based)"),
        f"📐 R:R — TP1 {m['rr1']:.2f}R · TP2 {m['rr2']:.2f}R · TP3 {m['rr3']:.2f}R",
        f"📊 Volume {sig.get('volume_mult',0):.2f}× · Extension {sig.get('extension_atr',0):.2f}×ATR",
    ]


def _record_alert(event, **data):
    item = {"event": event, "time": time.time(), **data}
    with state_lock:
        watch_alerts.append(item)
        del watch_alerts[:-100]


def _record_history(state, outcome, level=None):
    item = {
        "time": time.time(),
        "symbol": state.get("symbol"),
        "direction": state.get("direction"),
        "entry": state.get("entry"),
        "sl": state.get("sl"),
        "tp1": state.get("tp1"),
        "tp2": state.get("tp2"),
        "tp3": state.get("tp3"),
        "outcome": outcome,
        "level": level,
        "source": state.get("source", "scanner"),
        "timeframe": state.get("timeframe", "15m"),
    }
    with state_lock:
        signal_history.append(item)
        del signal_history[:-500]


def _watch_detect(symbol, timeframe):
    rows = get_klines(symbol, timeframe, 260)
    if len(rows) < 80:
        return None
    long_sig = _move_setup(rows, "LONG")
    short_sig = _move_setup(rows, "SHORT")
    if long_sig and not short_sig:
        sig = dict(long_sig)
        sig["direction"] = "LONG"
    elif short_sig and not long_sig:
        sig = dict(short_sig)
        sig["direction"] = "SHORT"
    elif long_sig and short_sig:
        # Do not alert on an ambiguous candle.
        return None
    else:
        return None
    sig["symbol"] = symbol
    sig["timeframe"] = timeframe
    sig["candle_time"] = rows[-1]["time"]
    sig["key"] = f"watch:{symbol}:{timeframe}:{sig['direction']}:{sig['candle_time']}"
    return sig


def _start_watch_thread():
    global watch_thread
    with watch_lock:
        if watch_thread and watch_thread.is_alive():
            return
        watch_stop_event.clear()
        watch_thread = threading.Thread(target=watch_loop, name="smart-watch", daemon=True)
        watch_thread.start()


def watch_loop():
    while not watch_stop_event.is_set():
        with watch_lock:
            items = [dict(v) for v in watchlist.values()]
            interval = int(bot_settings.get("watch_interval", WATCH_INTERVAL))
            cooldown = int(bot_settings.get("watch_cooldown", WATCH_ALERT_COOLDOWN))
            alerts_enabled = bool(bot_settings.get("alerts", True))
        for item in items:
            if not alerts_enabled:
                continue
            try:
                sig = _watch_detect(item["symbol"], item["timeframe"])
                if not sig:
                    continue
                now = time.time()
                with watch_lock:
                    current = watchlist.get(item["symbol"])
                    if not current:
                        continue
                    if current.get("last_key") == sig["key"]:
                        continue
                    if now - float(current.get("last_alert_at", 0)) < cooldown:
                        current["last_key"] = sig["key"]
                        continue
                    current["last_key"] = sig["key"]
                    current["last_alert_at"] = now
                    chat_id = current["chat_id"]
                details = _setup_detail_lines(sig)
                caption = (
                    "📡 SAIWAN SMART WATCH PRO\n\n"
                    f"{'🟢' if sig['direction']=='LONG' else '🔴'} {sig['direction']} — ⭐ {sig['symbol']}\n"
                    f"⏱ {sig['timeframe'].upper()} · CLOSED CANDLES\n"
                    f"💵 Entry: {fmt_price(sig['entry'])}\n"
                    f"🛑 SL: {fmt_price(sig['sl'])}\n"
                    f"🎯 TP1: {fmt_price(sig['tp1'])}\n"
                    f"🎯 TP2: {fmt_price(sig['tp2'])}\n"
                    f"🎯 TP3: {fmt_price(sig['tp3'])}\n"
                    f"{details[0]}\n{details[1]}\n\n"
                    f"Pattern: {sig.get('pattern','STRUCTURE BREAKOUT')} · confirmed closed-candle breakout\n"
                    "🛡️ Anti-chase filter: ON\n"
                    "⚠️ Signal only — no automatic trading."
                )
                path = None
                try:
                    path = make_chart(sig)
                except Exception as chart_error:
                    print(f"WATCH CHART ERROR {sig['symbol']}: {type(chart_error).__name__}: {chart_error}")
                if path:
                    msg_id = send_photo(chat_id, path, caption)
                else:
                    msg_id = send_message(chat_id, caption)
                sig["source"] = "watch"
                sig["key"] = f"watch:{sig['symbol']}:{sig['timeframe']}:{sig['direction']}:{sig['candle_time']}"
                track_sent_signal(sig, chat_id, msg_id, source="watch")
                _record_alert("WATCH", symbol=sig["symbol"], timeframe=sig["timeframe"], direction=sig["direction"], key=sig["key"])
            except Exception as e:
                print(f"WATCH ERROR {item.get('symbol')}: {type(e).__name__}: {e}")
        watch_stop_event.wait(max(15, interval))


def _watch_text():
    with watch_lock:
        items = list(watchlist.values())
    if not items:
        return "📋 WATCHLIST\n\nهیچ کۆینێک لە watchlist ـدا نییە.\n\nنموونە: /watch BTC 5m"
    lines = ["📋 SAIWAN WATCHLIST", ""]
    for i in items:
        lines.append(f"⭐ {i['symbol']} · ⏱ {i['timeframe'].upper()}")
    lines += ["", f"Total: {len(items)}/{MAX_WATCH_ITEMS}"]
    return "\n".join(lines)


def _alerts_text():
    with state_lock:
        items = list(watch_alerts[-10:])
    if not items:
        return "🔔 ALERTS\n\nهێشتا هیچ alert ـێک نییە."
    lines = ["🔔 SAIWAN ALERTS", ""]
    for x in reversed(items):
        dt = datetime.fromtimestamp(x["time"], tz=timezone.utc).strftime("%H:%M")
        if x["event"] == "WATCH":
            lines.append(f"{dt} · {x.get('symbol')} {x.get('timeframe','').upper()} · {x.get('direction')}")
        else:
            lines.append(f"{dt} · {x.get('event')} · {x.get('symbol')}")
    return "\n".join(lines)


def _risk_text(parts):
    if len(parts) not in (4, 6):
        return ("💰 RISK CALCULATOR\n\n"
                "نموونەی سادە: /risk BTC 1000 1\n"
                "بۆ position size: /risk BTC 1000 1 105000 103800")
    symbol = _normalize_analysis_symbol(parts[1])
    try:
        balance = float(parts[2]); risk_pct = float(parts[3])
        if balance <= 0 or risk_pct <= 0:
            raise ValueError
    except ValueError:
        return "❌ Balance و Risk percentage دەبێت ژمارەی positive بن."
    risk_amount = balance * risk_pct / 100.0
    lines = ["💰 SAIWAN RISK CALCULATOR", "", f"⭐ {symbol}", f"Balance: ${balance:,.2f}", f"Risk: {risk_pct:.2f}%", f"Max risk: ${risk_amount:,.2f}"]
    if len(parts) == 6:
        try:
            entry = float(parts[4]); sl = float(parts[5])
            distance = abs(entry - sl)
            if entry <= 0 or sl <= 0 or distance <= 0:
                raise ValueError
            qty = risk_amount / distance
            notional = qty * entry
            lines += [f"Entry: {fmt_price(entry)}", f"SL: {fmt_price(sl)}", f"Stop distance: {fmt_price(distance)}", f"Position size: {qty:.6f} {symbol.replace('USDT','')}", f"Notional: ${notional:,.2f}", "ℹ️ This is risk sizing only; leverage/margin rules are not included."]
        except ValueError:
            return "❌ Entry و SL دەبێت ژمارەی دروست و جیاواز بن."
    else:
        lines.append("ℹ️ بۆ position size، Entry و SL زیاد بکە.")
    return "\n".join(lines)


def _get_historical_klines(symbol, timeframe, days=30):
    """Fetch closed historical candles in backward pages for backtesting.

    Bitget's historical-candle endpoint returns up to 200 rows per request, so
    we walk backward from now. The exact available history depends on timeframe.
    """
    granularity = {"1m":"1m", "5m":"5m", "15m":"15m"}.get(str(timeframe).lower())
    if not granularity:
        raise ValueError("unsupported timeframe")
    candle_ms = {"1m":60000, "5m":300000, "15m":900000}[str(timeframe).lower()]
    now_ms = int(time.time()*1000)
    start_ms = now_ms - int(days*86400000)
    cursor_end = now_ms
    out = {}
    max_pages = max(2, int((days*86400000)/(candle_ms*180))+4)
    for _ in range(max_pages):
        payload = bitget_get("/api/v2/mix/market/history-candles", {
            "symbol": symbol, "productType": BITGET_PRODUCT, "granularity": granularity,
            "endTime": str(cursor_end), "limit": 200, "kLineType": "market"
        })
        data = payload.get("data") or []
        if not data:
            break
        oldest = None
        for v in data:
            try:
                if len(v) < 6: continue
                ts = int(v[0])
                if ts < start_ms: continue
                out[ts] = {"time":ts//1000,"open":float(v[1]),"high":float(v[2]),"low":float(v[3]),"close":float(v[4]),"vol":float(v[5]),"turnover":float(v[6]) if len(v)>6 else 0.0}
                oldest = ts if oldest is None else min(oldest, ts)
            except (TypeError, ValueError, IndexError):
                continue
        if oldest is None or oldest <= start_ms or len(data) < 2:
            break
        cursor_end = oldest - candle_ms
        time.sleep(BITGET_MIN_REQUEST_INTERVAL)
    return sorted(out.values(), key=lambda r:r["time"])


def _backtest_one(rows, direction, sl_buffer, tps, max_hold_bars=48):
    trades=[]
    i=100
    cooldown_until=100
    while i < len(rows)-2:
        if i < cooldown_until:
            i += 1; continue
        hist=rows[:i+1]
        sig=_momentum_setup(hist,direction,sl_atr_buffer=sl_buffer,tp_multipliers=tps)
        if not sig or sig.get("time") != rows[i]["time"]:
            i += 1; continue
        entry=float(sig["entry"]); sl=float(sig["sl"]); risk=abs(entry-sl)
        if risk <= 0:
            i += 1; continue
        weights=[0.50,0.25,0.25]
        levels=[float(sig["tp1"]),float(sig["tp2"]),float(sig["tp3"])]
        hit=[False,False,False]
        realized=0.0; exit_bar=None; outcome="OPEN"
        end=min(len(rows),i+1+max_hold_bars)
        for j in range(i+1,end):
            r=rows[j]
            # Conservative same-candle handling: if SL and a target are both
            # touched, count SL first because intrabar order is unknown.
            sl_hit=(r["low"]<=sl) if direction=="LONG" else (r["high"]>=sl)
            if sl_hit:
                for k in range(3):
                    if not hit[k]: realized += -weights[k]
                outcome="SL"; exit_bar=j; break
            for k,level in enumerate(levels):
                if hit[k]: continue
                tp_hit=(r["high"]>=level) if direction=="LONG" else (r["low"]<=level)
                if tp_hit:
                    realized += weights[k]*tps[k]
                    hit[k]=True
            if all(hit):
                outcome="TP3"; exit_bar=j; break
        if exit_bar is None:
            # Time exit: mark open position to close price in R.
            j=end-1; close=rows[j]["close"]
            move=((close-entry)/risk) if direction=="LONG" else ((entry-close)/risk)
            remaining=sum(weights[k] for k in range(3) if not hit[k])
            realized += remaining*move
            outcome="TIME"; exit_bar=j
        trades.append({"entry_time":rows[i]["time"],"direction":direction,"realized_r":realized,"outcome":outcome,"bars":exit_bar-i})
        cooldown_until=(exit_bar+1 if exit_bar is not None else i+1)
        i=cooldown_until
    return trades


def _run_backtest(symbol, timeframe="15m", days=30):
    rows=_get_historical_klines(symbol,timeframe,days)
    if len(rows)<150:
        raise RuntimeError(f"only {len(rows)} candles available")
    profiles=[]
    for slbuf in (0.15,0.25,0.40,0.60):
        for tps in ((1.0,2.0,3.0),(1.5,2.5,4.0),(1.5,3.0,5.0),(2.0,3.0,5.0)):
            all_trades=_backtest_one(rows,"LONG",slbuf,tps)+_backtest_one(rows,"SHORT",slbuf,tps)
            if not all_trades: continue
            gross=sum(t["realized_r"] for t in all_trades)
            wins=sum(t["realized_r"]>0 for t in all_trades)
            sls=sum(t["outcome"]=="SL" for t in all_trades)
            tp3=sum(t["outcome"]=="TP3" for t in all_trades)
            # Equity curve in R for drawdown.
            eq=peak=dd=0.0
            for t in sorted(all_trades,key=lambda x:x["entry_time"]):
                eq += t["realized_r"]; peak=max(peak,eq); dd=min(dd,eq-peak)
            profiles.append({"sl":slbuf,"tps":tps,"trades":len(all_trades),"gross_r":gross,"win_rate":wins/len(all_trades)*100,"sl_count":sls,"tp3_count":tp3,"max_dd_r":abs(dd)})
    profiles.sort(key=lambda x:(x["gross_r"],-x["max_dd_r"],x["trades"]),reverse=True)
    return rows,profiles


def _backtest_text(parts):
    if len(parts) not in (2,3,4):
        return ("🧪 BACKTEST\n\n"
                "نموونە: /backtest BTC 15m 30\n"
                "Timeframe: 15m only\n"
                "Days: 7–52 · Strategy timeframe: 15m only.")
    symbol=_normalize_analysis_symbol(parts[1])
    tf=_normalize_analysis_timeframe(parts[2]) if len(parts)>=3 else SIGNAL_TIMEFRAME
    try:
        days=int(parts[3]) if len(parts)==4 else 30
        days=max(7,min(52,days))
    except ValueError:
        return "❌ Days دەبێت ژمارە بێت."
    if not symbol or not tf:
        return "❌ Coin یان timeframe هەڵەیە."
    rows,profiles=_run_backtest(symbol,tf,days)
    if not profiles:
        return f"🧪 BACKTEST — {symbol} · {tf.upper()}\n\n🟡 هیچ trade ـێک نەدۆزرایەوە لە {len(rows)} candle ـدا."
    best=profiles[0]
    lines=[f"🧪 SAIWAN SA-VWAP BACKTEST",f"⭐ {symbol} · {tf.upper()} · {days} days",f"Candles: {len(rows)}","",
           "📌 TP/SL sweep (historical, closed candles only)",
           f"Best gross R: {best['gross_r']:+.2f}R",
           f"SL buffer: {best['sl']:.2f}× ATR",
           f"TP: {best['tps'][0]:.1f}R / {best['tps'][1]:.1f}R / {best['tps'][2]:.1f}R",
           f"Trades: {best['trades']} · Win-rate by realized R: {best['win_rate']:.1f}%",
           f"SL exits: {best['sl_count']} · TP3 exits: {best['tp3_count']} · Max DD: {best['max_dd_r']:.2f}R", "",
           "Top parameter sets:"]
    for n,p in enumerate(profiles[:5],1):
        lines.append(f"{n}. SL {p['sl']:.2f} ATR · TP {p['tps'][0]:.1f}/{p['tps'][1]:.1f}/{p['tps'][2]:.1f}R · {p['trades']} trades · {p['gross_r']:+.2f}R · DD {p['max_dd_r']:.2f}R")
    lines += ["", "ℹ️ This is a parameter comparison, not a guarantee. Fees, funding and slippage are not included in gross R."]
    return "\n".join(lines)


def _history_text(symbol_filter=None):
    with state_lock:
        items = list(signal_history)
    if symbol_filter:
        sym = _normalize_analysis_symbol(symbol_filter)
        items = [x for x in items if x.get("symbol") == sym]
    items = items[-10:]
    if not items:
        return "📚 HISTORY\n\nهیچ signal ـێکی داخراو نییە."
    lines = ["📚 SAIWAN HISTORY", ""]
    for x in reversed(items):
        dt = datetime.fromtimestamp(x["time"], tz=timezone.utc).strftime("%m-%d %H:%M")
        lines.append(f"{dt} · {x.get('symbol')} · {x.get('direction')} · {x.get('timeframe')} · {x.get('outcome')}")
    return "\n".join(lines)


def _stats_text(symbol_filter=None):
    sym = _normalize_analysis_symbol(symbol_filter) if symbol_filter else None
    with state_lock:
        items = list(signal_history)
        active_states = list(active_signals.values())
        alerts = list(watch_alerts)
    if sym:
        items = [x for x in items if x.get("symbol") == sym]
        active_states = [x for x in active_states if x.get("symbol") == sym]
        alerts = [x for x in alerts if x.get("symbol") == sym]
    total = len(items)
    sl = sum(x.get("outcome") == "SL" for x in items)
    tp3 = sum(x.get("outcome") == "TP3" for x in items)
    tp1_events = sum(x.get("event") == "TP1" for x in alerts)
    tp2_events = sum(x.get("event") == "TP2" for x in alerts)
    terminal = sl + tp3
    rate = (tp3 / terminal * 100) if terminal else 0.0
    scope = f" · {sym}" if sym else ""
    return (f"📈 SAIWAN STATISTICS{scope}\n\n"
            f"Closed: {total}\nActive: {len(active_states)}\n"
            f"🟢 TP3: {tp3}\n🔴 SL: {sl}\n"
            f"🎯 TP1 events: {tp1_events}\n🎯 TP2 events: {tp2_events}\n"
            f"Terminal TP3 rate: {rate:.1f}%\n\n"
            "ℹ️ TP3 rate = TP3 / (TP3 + SL), using closed terminal records only.\n"
            "Statistics are historical records from the current bot process.")


def _settings_text():
    with watch_lock:
        alerts = bool(bot_settings.get("alerts", True))
        interval = int(bot_settings.get("watch_interval", WATCH_INTERVAL))
        cooldown = int(bot_settings.get("watch_cooldown", WATCH_ALERT_COOLDOWN))
        watched = len(watchlist)
    with state_lock:
        scanner = scanner_running
    return ("⚙️ SAIWAN SETTINGS\n\n"
            f"Scanner: {'ON' if scanner else 'OFF'}\n"
            f"Smart Watch alerts: {'ON' if alerts else 'OFF'}\n"
            f"Watch interval: {interval}s\n"
            f"Alert cooldown: {cooldown}s\n"
            f"Watched coins: {watched}/{MAX_WATCH_ITEMS}\n\n"
            "Change: /settings alerts on\n"
            "Change: /settings alerts off\n"
            "Change: /settings watch_interval 60\n"
            "Change: /settings cooldown 300")


def _quick_scan(raw_symbol):
    symbol = _normalize_analysis_symbol(raw_symbol)
    try:
        rows = get_klines(symbol, SIGNAL_TIMEFRAME, CANDLE_LIMIT)
        if len(rows) < 90:
            return f"❌ داتای پێویست بۆ {symbol} بەردەست نییە."
        cur = rows[-1]["close"]
        *_, bias = _timeframe_bias(rows)
        long_sig = _move_setup(rows, "LONG")
        short_sig = _move_setup(rows, "SHORT")
        setup = long_sig if long_sig and not short_sig else short_sig if short_sig and not long_sig else None
        lines = [f"📊 QUICK SCAN — {symbol}", "", f"💵 Price: {fmt_price(cur)}", f"⏱ {SIGNAL_TIMEFRAME.upper()}", f"Bias: {bias}"]
        if setup:
            lines += [f"🎯 Setup: {'🟢 LONG' if setup['direction']=='LONG' else '🔴 SHORT'}",
                      f"Pattern: {setup.get('pattern')}", f"Entry: {fmt_price(setup['entry'])}", f"SL: {fmt_price(setup['sl'])}",
                      f"TP1: {fmt_price(setup['tp1'])}", f"TP2: {fmt_price(setup['tp2'])}", f"TP3: {fmt_price(setup['tp3'])}"]
            lines += _setup_detail_lines(setup)
        else:
            lines.append("🟡 WAIT — breakout/pullback setupی تەواو نییە.")
        return "\n".join(lines)
    except Exception as e:
        return f"❌ Quick scan سەرکەوتوو نەبوو: {_error_bucket(e)}"


def _smart_scan_report(limit=8):
    """Manual market radar: scan the most liquid contracts and return confirmed setups."""
    contracts = get_contracts()
    tickers = get_tickers()
    tv = {x.get("symbol"): x for x in tickers}
    ranked = []
    for c in contracts:
        sym = c.get("symbol", "")
        try:
            vol = float(tv.get(sym, {}).get("usdtVolume", tv.get(sym, {}).get("quoteVolume", 0)))
        except (TypeError, ValueError):
            vol = 0.0
        if vol > 0:
            ranked.append((vol, sym))
    ranked.sort(reverse=True)
    universe = [sym for _, sym in ranked[:20]]

    def one(symbol):
        try:
            rows15 = get_klines(symbol, SIGNAL_TIMEFRAME, CANDLE_LIMIT)
            sig = analyze(symbol, rows15, None)
            return sig
        except Exception:
            return None

    found = []
    with ThreadPoolExecutor(max_workers=min(SCAN_WORKERS, 8)) as pool:
        futures = [pool.submit(one, sym) for sym in universe]
        for fut in as_completed(futures):
            sig = fut.result()
            if sig:
                m = _setup_metrics(sig)
                sig["quality"] = m["quality"]
                sig["rr3"] = m["rr3"]
                found.append(sig)
    found.sort(key=lambda x: (x.get("quality", 0), x.get("radar_score", 0), x.get("rr3", 0), x.get("time", 0)), reverse=True)
    found = found[:max(1, min(limit, 10))]
    if not found:
        return "📊 SAIWAN SMART SCAN\\n\\n🟡 No complete setup found in the top liquid market contracts right now.\\n\\nTry again after the next closed candles."
    lines = ["📊 SAIWAN SMART SCAN", "", f"Scanned top {len(universe)} liquid contracts", ""]
    for i, sig in enumerate(found, 1):
        m = _setup_metrics(sig)
        d = "🟢 LONG" if sig["direction"] == "LONG" else "🔴 SHORT"
        lines += [
            f"{i}. {d} · ⭐ {sig['symbol']}",
            f"   Entry {fmt_price(sig['entry'])} · SL {fmt_price(sig['sl'])}",
            f"   TP1 {fmt_price(sig['tp1'])} · TP2 {fmt_price(sig['tp2'])} · TP3 {fmt_price(sig['tp3'])}",
            f"   Quality {m['quality']}/10 · TP3 {m['rr3']:.2f}R · {sig.get('pattern','SA-VWAP CLOSE CROSS')}",
            "",
        ]
    lines.append("⚠️ Radar is informational; no automatic trading.")
    return "\\n".join(lines)


def _movers_report(limit=10):
    """Show 24h percentage movers among live USDT perpetuals."""
    tickers = get_tickers()
    rows = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        try:
            change = float(t.get("change24h", t.get("chgUtc", 0))) * 100.0
            price = float(t.get("lastPr"))
            volume = float(t.get("usdtVolume", t.get("quoteVolume", 0)))
        except (TypeError, ValueError):
            continue
        rows.append((change, volume, sym, price))
    rows.sort(key=lambda x: abs(x[0]), reverse=True)
    lines = ["🔥 SAIWAN TOP MOVERS · 24H", ""]
    for i, (change, volume, sym, price) in enumerate(rows[:limit], 1):
        icon = "🟢" if change >= 0 else "🔴"
        lines.append(f"{i}. {icon} {sym} · {change:+.2f}% · {fmt_price(price)}")
    lines.append("\nℹ️ Price movement only; not a trade signal.")
    return "\n".join(lines)


def status_text():
    with state_lock:
        scanner = scanner_running
        pending = len(pending_signals)
        tracked = len(active_signals)
        hist = len(signal_history)
    with watch_lock:
        watched = len(watchlist)
        alerts = len(watch_alerts)
        watch_on = bool(bot_settings.get("alerts", True))
    return (
        "BOT STATUS: ONLINE\n"
        f"Scanner: {'RUNNING' if scanner else 'STOPPED'}\n"
        f"Smart Watch: {'ON' if watch_on else 'OFF'} ({watched}/{MAX_WATCH_ITEMS})\n"
        "Market: Bitget USDT Perpetual Futures\n"
        "Strategy: SAIWAN SA-VWAP — Anchored VWAP close-cross + structure SL + ATR targets + BE\n"
        f"Scan timeframe: {active_scan_timeframe.upper()}\n"
        "Analysis: 15m / 30m / 1h / 4h\n"
        f"Pending signals: {pending}\n"
        f"Tracked signals: {tracked}\n"
        f"History: {hist}\n"
        f"Alerts: {alerts}\n"
        "TP/SL monitoring: ENABLED"
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


def scan_once(timeframe=None):
    global pending_signals
    timeframe = _normalize_scan_timeframe(timeframe) or active_scan_timeframe
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
            rows_tf = get_klines(symbol, timeframe, CANDLE_LIMIT)
            if len(rows_tf) < 90:
                return symbol, None, None
            return symbol, analyze(symbol, rows_tf, None, timeframe), None
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
                key = f"{timeframe}:{symbol}:{sig['direction']}:{sig['time']}"
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
    print(f"Bitget SA-VWAP scan: timeframe={timeframe}, universe={len(eligible)}, scanned={len(pairs)}, confirmed={len(found)}, errors={total_errors}, workers={SCAN_WORKERS}")
    if not contracts:
        print("Bitget warning: no contracts returned from /api/v2/mix/market/contracts")
    elif not tickers:
        print("Bitget warning: no tickers returned from /api/v2/mix/market/tickers")
    if summary:
        print(f"Bitget error summary: {summary}")


def signal_caption(sig):
    d = "🟢 LONG" if sig["direction"] == "LONG" else "🔴 SHORT"
    m = _setup_metrics(sig)
    return (
        f"🚀 SAIWAN STRUCTURE BREAKOUT SIGNAL\n\n{d}\n"
        f"⭐ {sig['symbol']} · Bitget Futures\n"
        f"⏱ {sig.get('timeframe', SIGNAL_TIMEFRAME).upper()} · CLOSED CANDLES\n\n"
        f"Pattern: {sig.get('pattern','STRUCTURE BREAKOUT')}\n"
        + (f"Structure High: {fmt_price(sig.get('structure_high'))} · Structure Low: {fmt_price(sig.get('structure_low'))}\n"
           f"Strength: {sig.get('strength', 0):.0f}/100\n"
           if sig.get('pattern') == 'STRUCTURE BREAKOUT' else
           "Range → Breakout/Breakdown → Pullback/Continuation\n")
        + f"Volume: {sig.get('volume_mult',0):.2f}× avg · Extension: {sig.get('extension_atr',0):.2f}× ATR\n\n"
        f"Entry: {fmt_price(sig['entry'])}\n"
        f"SL: {fmt_price(sig['sl'])}\n"
        f"TP1: {fmt_price(sig['tp1'])}\n"
        f"TP2: {fmt_price(sig['tp2'])}\n"
        f"TP3: {fmt_price(sig['tp3'])}\n"
        f"⭐ Quality: {m['quality']}/10 · R:R {m['rr1']:.2f} / {m['rr2']:.2f} / {m['rr3']:.2f}\n\n"
        + ("🛡️ Structure SL · TP measured from structure risk\n" if sig.get('pattern') == 'STRUCTURE BREAKOUT' else "🛡️ Anti-chase filter: ON\n")
        + "⚠️ Signal only — no automatic trading."
    )


def scanner_loop():
    global scanner_running
    scanner_running=True
    while not stop_event.is_set():
        try: scan_once(active_scan_timeframe)
        except Exception as e: print(f"SCAN LOOP ERROR {type(e).__name__}: {e}")
        force_scan_event.clear()
        for _ in range(SCAN_INTERVAL):
            if stop_event.is_set() or force_scan_event.is_set(): break
            time.sleep(1)
    scanner_running=False


def track_sent_signal(sig, chat_id, message_id, source="scanner"):
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
            "active_sl": sig["sl"],
            "be_active": False,
            "tp1_be_enabled": bool(SA_VWAP_USE_BE and str(sig.get("pattern", "")).startswith("SA-VWAP")),
            "tp1": sig["tp1"],
            "tp2": sig["tp2"],
            "tp3": sig["tp3"],
            "timeframe": sig.get("timeframe", "15m"),
            "source": source,
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
                if _hit_level(state["direction"], state.get("active_sl", state["sl"]), price):
                    sl_label = "BE" if state.get("be_active") else "SL"
                    send_message(
                        state["chat_id"],
                        f"🛑 {sl_label} Hit\n⭐ {state['symbol']}\n💵 Price: {fmt_price(price)}",
                        reply_to_message_id=state["message_id"],
                    )
                    _record_history(state, "SL", "SL")
                    _record_alert("SL", symbol=state["symbol"], timeframe=state.get("timeframe", "15m"), direction=state["direction"])
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
                        if name == "tp3":
                            _record_history(state, "TP3", "TP3")
                            _record_alert("TP3", symbol=state["symbol"], timeframe=state.get("timeframe", "15m"), direction=state["direction"])
                        else:
                            # TP1/TP2 are milestones. SA-VWAP moves SL to entry after TP1.
                            _record_alert(name.upper(), symbol=state["symbol"], timeframe=state.get("timeframe", "15m"), direction=state["direction"])
                            if name == "tp1" and state.get("tp1_be_enabled") and not state.get("be_active"):
                                state["active_sl"] = state["entry"]
                                state["be_active"] = True
                                send_message(
                                    state["chat_id"],
                                    f"🛡️ BREAK-EVEN\n⭐ {state['symbol']}\n💵 SL moved to Entry: {fmt_price(state['entry'])}",
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
                # Send the newest confirmed SA-VWAP trigger as soon as possible.
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


def start_scanner(chat_id, timeframe=TF_15M):
    global scanner_thread, active_chat_id, active_scan_timeframe
    active_chat_id = chat_id
    tf = _normalize_scan_timeframe(timeframe) or TF_15M
    active_scan_timeframe = tf
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
            r = requests.get(telegram_url("getUpdates"), params={"timeout": 25, "offset": offset, "allowed_updates": json.dumps(["message", "callback_query"])}, timeout=35)
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
                if upd.get("callback_query"):
                    _handle_callback_query(upd["callback_query"])
                    continue
                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                text = (msg.get("text") or "").strip()
                if not chat.get("id"):
                    continue
                active_chat_id = chat["id"]
                parts = text.split()
                cmd = parts[0].split("@")[0].lower() if parts else ""
                try:
                    if cmd == "/start":
                        send_message(active_chat_id, welcome_text(), main_menu_markup())
                    elif cmd == "/scan":
                        if len(parts) == 1:
                            start_scanner(active_chat_id, TF_15M)
                            send_message(active_chat_id, "🚀 SAIWAN SA-VWAP SCANNER STARTED\n\n15m closed candles · Anchored VWAP + Retest + ATR + BE. TP/SL monitoring is enabled.")
                        elif parts[1].lower() in ("top", "smart", "smartscan"):
                            send_message(active_chat_id, "📊 Smart Scan خەریکە بازارەکە پشکنین دەکات...")
                            send_message(active_chat_id, _smart_scan_report())
                        else:
                            send_message(active_chat_id, _quick_scan(parts[1]))
                    elif cmd == "/movers":
                        send_message(active_chat_id, _movers_report())
                    elif cmd == "/stop":
                        stop_scanner(); send_message(active_chat_id, "🛑 Scanner stopped. Smart Watch keeps running if it is enabled.")
                    elif cmd == "/watch":
                        if len(parts) != 3:
                            send_message(active_chat_id, "📡 نموونە:\n/watch BTC 5m\n/watch AVAX 15m\n/watch ETH 1h")
                        else:
                            symbol = _normalize_analysis_symbol(parts[1]); tf = _normalize_analysis_timeframe(parts[2])
                            if not symbol or not tf:
                                send_message(active_chat_id, "❌ Coin یان timeframe هەڵەیە. Timeframe: 15m only")
                            else:
                                with watch_lock:
                                    if symbol not in watchlist and len(watchlist) >= MAX_WATCH_ITEMS:
                                        send_message(active_chat_id, f"❌ Watchlist پڕە. Maximum: {MAX_WATCH_ITEMS}")
                                        continue
                                    watchlist[symbol] = {"symbol": symbol, "timeframe": tf, "chat_id": active_chat_id, "last_key": None, "last_alert_at": 0}
                                _start_watch_thread()
                                send_message(active_chat_id, f"📡 Smart Watch enabled\n⭐ {symbol}\n⏱ {tf.upper()}\n\nAlert تەنها بۆ complete setup ـی نوێ دێت.")
                    elif cmd == "/unwatch":
                        if len(parts) != 2:
                            send_message(active_chat_id, "نموونە: /unwatch BTC\nیان /unwatch all")
                        else:
                            target = _normalize_analysis_symbol(parts[1]) if parts[1].lower() != "all" else "all"
                            with watch_lock:
                                if target == "all":
                                    watchlist.clear()
                                    watch_stop_event.set()
                                    reply = "🛑 All Smart Watch items removed."
                                elif target in watchlist:
                                    watchlist.pop(target, None)
                                    if not watchlist: watch_stop_event.set()
                                    reply = f"🛑 {target} removed from watchlist."
                                else:
                                    reply = f"ℹ️ {target} لە watchlist ـدا نییە."
                            send_message(active_chat_id, reply)
                    elif cmd == "/watchlist":
                        send_message(active_chat_id, _watch_text())
                    elif cmd == "/alerts":
                        send_message(active_chat_id, _alerts_text())
                    elif cmd == "/analysis":
                        if len(parts) != 3:
                            send_message(active_chat_id, "🔎 نموونە:\n/analysis BTC 15m\n/analysis AVAX 15m")
                        else:
                            symbol = _normalize_analysis_symbol(parts[1]); tf = _normalize_analysis_timeframe(parts[2])
                            if not tf:
                                send_message(active_chat_id, "❌ Timeframe ـی هەڵەیە. نموونە: /analysis BTC 1m یان /analysis BTC 5m")
                            else:
                                send_message(active_chat_id, f"🔎 خەریکم {symbol} شیکاری دەکەم...\n⏱ {tf.upper()}")
                                send_message(active_chat_id, analysis_report(symbol, [tf]))
                                try:
                                    for chart_path in make_analysis_charts(symbol, [tf]):
                                        send_photo(active_chat_id, chart_path, f"📊 SAIWAN CHART — {symbol} · {tf.upper()}")
                                except Exception as e:
                                    print(f"ANALYSIS CHART ERROR {type(e).__name__}: {e}")
                    elif cmd == "/risk":
                        send_message(active_chat_id, _risk_text(parts))
                    elif cmd == "/history":
                        send_message(active_chat_id, _history_text(parts[1] if len(parts) == 2 else None))
                    elif cmd == "/stats":
                        if len(parts) > 2:
                            send_message(active_chat_id, "نموونە: /stats یان /stats BTC")
                        else:
                            send_message(active_chat_id, _stats_text(parts[1] if len(parts) == 2 else None))
                    elif cmd == "/backtest":
                        if len(parts) not in (2,3,4):
                            send_message(active_chat_id, "نموونە: /backtest BTC 15m 30")
                        else:
                            send_message(active_chat_id, "🧪 Backtest خەریکە دەکرێت...\nTP/SL ـە جیاوازەکان بەراورد دەکرێن.")
                            try:
                                send_message(active_chat_id, _backtest_text(parts))
                            except Exception as e:
                                print(f"BACKTEST ERROR {type(e).__name__}: {e}")
                                send_message(active_chat_id, f"❌ Backtest سەرکەوتوو نەبوو: {_error_bucket(e)}")
                    elif cmd == "/settings":
                        if len(parts) == 1:
                            send_message(active_chat_id, _settings_text())
                        elif len(parts) == 3 and parts[1].lower() == "alerts" and parts[2].lower() in ("on", "off"):
                            with watch_lock: bot_settings["alerts"] = parts[2].lower() == "on"
                            send_message(active_chat_id, f"⚙️ Smart Watch alerts: {parts[2].upper()}")
                        elif len(parts) == 3 and parts[1].lower() == "watch_interval":
                            try:
                                value = max(15, min(900, int(parts[2])))
                                with watch_lock: bot_settings["watch_interval"] = value
                                send_message(active_chat_id, f"⚙️ Watch interval set to {value}s")
                            except ValueError:
                                send_message(active_chat_id, "❌ Interval دەبێت ژمارە بێت.")
                        elif len(parts) == 3 and parts[1].lower() == "cooldown":
                            try:
                                value = max(30, min(3600, int(parts[2])))
                                with watch_lock: bot_settings["watch_cooldown"] = value
                                send_message(active_chat_id, f"⚙️ Alert cooldown set to {value}s")
                            except ValueError:
                                send_message(active_chat_id, "❌ Cooldown دەبێت ژمارە بێت.")
                        else:
                            send_message(active_chat_id, "❌ Settings syntax هەڵەیە. /settings")
                    elif cmd == "/status":
                        send_message(active_chat_id, status_text())
                except Exception as cmd_error:
                    print(f"COMMAND ERROR {cmd}: {type(cmd_error).__name__}: {cmd_error}")
                    send_message(active_chat_id, f"❌ هەڵە لە command ـەکەدا: {type(cmd_error).__name__}")
        except Exception as e:
            print(f"TELEGRAM ERROR {type(e).__name__}: {e}")
            time.sleep(3)


_services_started = False
_services_start_lock = threading.Lock()

def start_background_services():
    """Start Telegram/monitor services once, including when Gunicorn imports bot:app."""
    global _services_started
    if _services_started:
        return
    with _services_start_lock:
        if _services_started:
            return
        if not TOKEN:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
        try:
            requests.post(telegram_url("deleteWebhook"), data={"drop_pending_updates": "false"}, timeout=10)
        except Exception as e:
            print(f"TELEGRAM WEBHOOK CLEANUP WARNING: {type(e).__name__}: {e}")
        threading.Thread(target=poll_updates, name="telegram-poller", daemon=True).start()
        threading.Thread(target=sender_loop, name="signal-sender", daemon=True).start()
        threading.Thread(target=monitor_active_signals, name="tp-sl-monitor", daemon=True).start()
        # Smart Watch thread is started on the first /watch command.
        _services_started = True
        print("SAIWAN services started: Telegram poller + signal sender + TP/SL monitor")


# Gunicorn imports bot:app instead of executing `python bot.py`.
# Start the background services during the worker's module import so Telegram
# commands and monitoring work in the Railway/Gunicorn deployment.
start_background_services()


def main():
    start_background_services()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
