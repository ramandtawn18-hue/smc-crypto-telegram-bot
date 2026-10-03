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
TF_5M = "5m"
TIMEFRAME = TF_5M
CANDLE_LIMIT = 260
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
    payload = bitget_get(
        "/api/v2/mix/market/candles",
        {"symbol": symbol, "productType": BITGET_PRODUCT, "granularity": interval,
         "limit": min(limit, 1000), "kLineType": "market"},
    )
    raw = payload.get("data") or []
    now_ms = int(time.time() * 1000)
    candle_ms = (5 if interval == TF_5M else 15) * 60 * 1000
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


def _move_setup(rows, direction):
    """Early move hunter: sweep -> MSS/CHOCH -> FVG + OB. No retest wait."""
    if len(rows) < 120:
        return None
    candidates = _sweep_candidates(rows)
    for direction0, sweep_idx, liquidity in reversed(candidates):
        if direction0 != direction or sweep_idx >= len(rows) - 1:
            continue
        structure = _structure_break(rows, direction, sweep_idx, len(rows) - 1)
        if not structure:
            continue
        mss_idx = structure["index"]
        # FVG is allowed on the MSS candle or within the next few closed candles.
        fvg = _find_fvg(rows, direction, max(2, mss_idx - 1), min(len(rows), mss_idx + 5))
        if not fvg:
            continue
        ob = _find_order_block(rows, direction, fvg["index"] + 1)
        if not ob:
            continue
        zone = _overlap(fvg, ob) or fvg
        trigger_idx = max(mss_idx, fvg["index"])
        if len(rows) - 1 - trigger_idx > 4:
            continue
        trigger = rows[trigger_idx]
        if direction == "LONG" and not _candle_bull(trigger):
            continue
        if direction == "SHORT" and not _candle_bear(trigger):
            continue
        cur = rows[-1]
        if direction == "LONG" and cur["close"] <= structure["level"]:
            continue
        if direction == "SHORT" and cur["close"] >= structure["level"]:
            continue

        entry = cur["close"]
        if direction == "LONG":
            sl = min(liquidity, ob["low"]) * 0.9995
            if sl >= entry: continue
            risk = entry - sl
            highs, _ = swing_points(rows[:-1], 2, 2)
            targets = sorted({p for _, p in highs if p > entry})
            tp1 = max(targets[0] if targets else entry + risk*1.5, entry + risk*1.5)
            tp2 = max(targets[1] if len(targets)>1 else entry + risk*2.5, tp1 + risk*.5)
            tp3 = max(targets[2] if len(targets)>2 else entry + risk*4.0, tp2 + risk*.5)
        else:
            sl = max(liquidity, ob["high"]) * 1.0005
            if sl <= entry: continue
            risk = sl - entry
            _, lows = swing_points(rows[:-1], 2, 2)
            targets = sorted({p for _, p in lows if p < entry}, reverse=True)
            tp1 = min(targets[0] if targets else entry - risk*1.5, entry - risk*1.5)
            tp2 = min(targets[1] if len(targets)>1 else entry - risk*2.5, tp1 - risk*.5)
            tp3 = min(targets[2] if len(targets)>2 else entry - risk*4.0, tp2 - risk*.5)

        return {
            "symbol": "", "direction": direction,
            "structure": "Liquidity Sweep + MSS + CHOCH + FVG + OB",
            "entry": entry, "trigger_level": structure["level"], "sl": sl,
            "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "entry_zone_low": zone["low"], "entry_zone_high": zone["high"],
            "score": 5, "max_score": 5, "time": cur["time"],
            "liquidity": liquidity, "sweep_index": sweep_idx,
            "mss_index": mss_idx, "mss_level": structure["level"],
            "fvg": fvg, "ob": ob, "entry_zone": zone,
            "rows": rows[max(0, sweep_idx-18):], "full_len": len(rows),
            "checks": {"Liquidity Sweep": True, "MSS": True, "FVG": True, "OB": True, "CHOCH": True},
            "retest_ok": False, "rejection_ok": True, "early_entry": True,
        }
    return None


def analyze(symbol, rows5, rows15=None):
    """SAIWAN Move Hunter: 5m entry hunting with 15m context, price action only."""
    if len(rows5) < 120:
        return None
    for r in rows5: r["symbol"] = symbol
    candidates = []
    for direction in ("LONG", "SHORT"):
        sig = _move_setup(rows5, direction)
        if sig:
            sig["symbol"] = symbol
            sig["context15"] = _context_15m(rows15, direction)
            sig["timeframe"] = "5m Entry · 15m Context"
            candidates.append(sig)
    return max(candidates, key=lambda x: x["time"]) if candidates else None

def make_chart(sig):
    """Render a clean dark TradingView-style ICT/SMC setup chart."""
    rows = sig["rows"]
    n = len(rows)
    direction = sig["direction"]
    entry, sl = sig["entry"], sig["sl"]
    tp1, tp2, tp3 = sig["tp1"], sig["tp2"], sig["tp3"]

    # Dark TradingView-style palette.
    BG = "#07101d"
    PANEL = "#0b1626"
    GRID = "#1a293b"
    TEXT = "#e7eef7"
    MUTED = "#7f93a8"
    UP = "#12d6a0"
    DOWN = "#ff3d57"
    GOLD = "#ffd21f"
    BLUE = "#4f7cff"
    PURPLE = "#7c5cff"
    PINK = "#ff4f87"
    CYAN = "#31d7ff"

    fig, ax = plt.subplots(figsize=(14.4, 7.8), dpi=170, facecolor=BG)
    ax.set_facecolor(BG)

    width = 0.62
    for i, r in enumerate(rows):
        c = UP if r["close"] >= r["open"] else DOWN
        ax.vlines(i, r["low"], r["high"], color=c, linewidth=1.15, zorder=5)
        lo = min(r["open"], r["close"])
        bh = max(abs(r["close"] - r["open"]), abs(r["close"]) * 1e-5)
        ax.add_patch(Rectangle(
            (i - width / 2, lo), width, bh,
            facecolor=c, edgecolor=c, linewidth=.65, zorder=6
        ))

    right = n + 13
    fvg = sig["fvg"]
    ob = sig["ob"]
    zone = sig["entry_zone"]
    offset = sig.get("full_len", n) - n

    def local_idx(z):
        if not isinstance(z, dict) or "index" not in z:
            return max(0, n - 1)
        return int(z.get("index", 0)) - offset

    def zone_box(z, color, alpha, label, text_color=None):
        idx = local_idx(z)
        x0 = max(0, min(n - 1, idx - max(4, n // 12)))
        low, high = float(z["low"]), float(z["high"])
        if high < low:
            low, high = high, low
        ax.add_patch(Rectangle(
            (x0, low), right - x0, max(high - low, 1e-9),
            facecolor=color, edgecolor=color, alpha=alpha,
            linewidth=1.1, zorder=1
        ))
        tc = text_color or color
        ax.text(
            x0 + (right - x0) * .58, high - (high - low) * .22,
            label, color=tc, fontsize=9.2, fontweight="bold",
            ha="center", va="center", zorder=8,
            bbox=dict(boxstyle="round,pad=.28", facecolor=BG,
                      edgecolor=tc, linewidth=.8, alpha=.88)
        )
        return x0

    # Order Block and FVG zones.
    zone_box(ob, PINK, .18, "SUPPLY / ORDER BLOCK", PINK)
    zone_box(fvg, PURPLE, .17, "FVG", "#a995ff")

    # Entry zone is subtle so it does not overpower the actual FVG/OB.
    ez_idx = local_idx(fvg)
    ez_x = max(0, min(n - 1, ez_idx - 2))
    ez_low, ez_high = float(zone["low"]), float(zone["high"])
    ax.add_patch(Rectangle(
        (ez_x, ez_low), right - ez_x, max(ez_high - ez_low, 1e-9),
        facecolor=BLUE, edgecolor=BLUE, alpha=.055, linewidth=.8, zorder=0
    ))

    # Convert setup indices into visible chart indices.
    sweep_local = int(sig.get("sweep_index", n - 1)) - offset
    mss_local = int(sig.get("mss_index", n - 1)) - offset
    sweep_price = float(sig["liquidity"])

    # Liquidity sweep: strong yellow callout above the sweep.
    if 0 <= sweep_local < n:
        ax.scatter(
            [sweep_local], [sweep_price], s=58,
            marker="v" if direction == "SHORT" else "^",
            color=GOLD, edgecolor=BG, linewidth=.7, zorder=10
        )
        tx = max(2, sweep_local - 12)
        ty = sweep_price + (max(r["high"] for r in rows) - min(r["low"] for r in rows)) * .07
        ax.annotate(
            "Liquidity Sweep", xy=(sweep_local, sweep_price),
            xytext=(tx, ty), color=GOLD, fontsize=10, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=GOLD, lw=1.5), zorder=10
        )

    # MSS / CHOCH is a clean structural line rather than a large label over candles.
    if 0 <= mss_local < n:
        ax.axhline(sig["mss_level"], color=CYAN, linestyle="--",
                   linewidth=1.0, alpha=.72, zorder=2)
        ax.annotate(
            "MSS / CHOCH", xy=(mss_local, sig["mss_level"]),
            xytext=(max(1, mss_local - 10), sig["mss_level"]),
            color=CYAN, fontsize=8.8, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=CYAN, lw=1.25), zorder=9
        )

    # Risk/reward levels with right-side pill labels.
    arrow_color = UP if direction == "LONG" else DOWN
    ax.axhline(entry, color=BLUE, linewidth=1.15, linestyle="--", alpha=.95, zorder=3)
    ax.axhline(sl, color=DOWN, linewidth=1.15, alpha=.95, zorder=3)
    for y, lab in [(tp1, "TP1"), (tp2, "TP2"), (tp3, "TP3")]:
        ax.axhline(y, color=UP, linewidth=.95, linestyle="--", alpha=.8, zorder=2)

    def right_label(y, label, color):
        ax.text(
            right + .25, y, f"{label} {fmt_price(y)}",
            color=TEXT, fontsize=8.7, fontweight="bold", va="center", ha="left",
            bbox=dict(boxstyle="round,pad=.34", facecolor=color,
                      edgecolor=color, linewidth=.8, alpha=.95), zorder=12
        )

    right_label(sl, "SL", DOWN)
    right_label(entry, "Entry", BLUE)
    right_label(tp1, "TP1", "#008f70")
    right_label(tp2, "TP2", "#008f70")
    right_label(tp3, "TP3", "#008f70")

    # Entry marker.
    ax.scatter([n - 1], [entry], s=56, color=arrow_color,
               edgecolor=TEXT, linewidth=.9, zorder=11)

    # Header.
    context = sig.get("context15", "")
    context_text = str(context).upper() if context else ""
    ax.text(.018, 1.065, f"{sig['symbol']} · 5m", transform=ax.transAxes,
            fontsize=16, color=TEXT, fontweight="bold", va="top")
    ax.text(.018, 1.025, "SAIWAN CRYPTO SIGNAL  ·  BITGET FUTURES",
            transform=ax.transAxes, fontsize=8.8, color=MUTED,
            fontweight="bold", va="top")
    ax.text(.985, 1.055, direction, transform=ax.transAxes,
            fontsize=12, color=arrow_color, fontweight="bold", ha="right", va="top",
            bbox=dict(boxstyle="round,pad=.38", facecolor=BG,
                      edgecolor=arrow_color, linewidth=1.0))

    # Compact trade summary panel, matching the requested sample style.
    panel_text = (
        f"{sig['symbol']}  ·  {direction}\n"
        f"5m Entry  |  15m Context\n\n"
        f"Entry   :  {fmt_price(entry)}\n"
        f"SL      :  {fmt_price(sl)}\n"
        f"TP1     :  {fmt_price(tp1)}\n"
        f"TP2     :  {fmt_price(tp2)}\n"
        f"TP3     :  {fmt_price(tp3)}\n"
        f"\n✓ Liquidity Sweep\n✓ MSS   ✓ CHOCH\n✓ FVG   ✓ OB"
    )
    ax.text(
        .022, .035, panel_text, transform=ax.transAxes,
        fontsize=8.8, color=TEXT, va="bottom", ha="left", linespacing=1.45,
        bbox=dict(boxstyle="round,pad=.72", facecolor=PANEL,
                  edgecolor="#2a4664", linewidth=1.0, alpha=.97), zorder=20
    )

    if context_text:
        ax.text(.50, .018, f"15m Context: {context_text}", transform=ax.transAxes,
                fontsize=8.5, color=MUTED, ha="center", va="bottom")

    # Axes/grid styling.
    ax.yaxis.tick_right()
    ax.tick_params(axis="y", colors="#9bb0c5", labelsize=8.2, length=0, pad=7)
    ax.tick_params(axis="x", colors="#71879d", labelsize=7.8, length=0, pad=8)
    ax.grid(axis="y", color=GRID, linewidth=.65, alpha=.8)
    ax.grid(axis="x", color=GRID, linewidth=.35, alpha=.35)
    for side in ["top", "left", "bottom"]:
        ax.spines[side].set_visible(False)
    ax.spines["right"].set_color("#22364b")
    ax.spines["right"].set_linewidth(.8)

    step = max(1, n // 7)
    ticks = list(range(0, n, step))
    if not ticks or ticks[-1] != n - 1:
        ticks.append(n - 1)
    ax.set_xticks(ticks)
    ax.set_xticklabels([
        datetime.fromtimestamp(rows[i]["time"], tz=timezone.utc).strftime("%H:%M")
        for i in ticks
    ])

    all_lows = [r["low"] for r in rows] + [sl, tp1, tp2, tp3, ob["low"], fvg["low"]]
    all_highs = [r["high"] for r in rows] + [sl, tp1, tp2, tp3, ob["high"], fvg["high"]]
    ymin, ymax = min(all_lows), max(all_highs)
    span = max(ymax - ymin, abs(rows[-1]["close"]) * .012)
    ax.set_ylim(ymin - span * .06, ymax + span * .16)
    ax.set_xlim(-1, right + 5)

    fig.subplots_adjust(left=.025, right=.865, top=.86, bottom=.085)
    safe = "".join(ch if ch.isalnum() else "_" for ch in sig["symbol"])
    path = f"/tmp/chart_{safe}_{sig['time']}.png"
    fig.savefig(path, facecolor=BG, edgecolor="none", bbox_inches="tight", pad_inches=.08)
    plt.close(fig)
    return path



def make_analysis_chart(symbol, timeframe, rows, block=None):
    """Render an on-demand analysis chart, even when no complete trade setup exists."""
    if not rows:
        raise RuntimeError("no candles for analysis chart")
    rows = rows[-90:]
    n = len(rows)
    BG = "#07101d"
    PANEL = "#0b1626"
    GRID = "#1a293b"
    TEXT = "#e7eef7"
    MUTED = "#7f93a8"
    UP = "#12d6a0"
    DOWN = "#ff3d57"
    GOLD = "#ffd21f"
    BLUE = "#4f7cff"
    PURPLE = "#7c5cff"
    PINK = "#ff4f87"
    CYAN = "#31d7ff"

    fig, ax = plt.subplots(figsize=(14.4, 7.8), dpi=170, facecolor=BG)
    ax.set_facecolor(BG)
    width = .62
    for i, r in enumerate(rows):
        c = UP if r["close"] >= r["open"] else DOWN
        ax.vlines(i, r["low"], r["high"], color=c, linewidth=1.1, zorder=4)
        lo = min(r["open"], r["close"])
        bh = max(abs(r["close"]-r["open"]), abs(r["close"])*1e-5)
        ax.add_patch(Rectangle((i-width/2, lo), width, bh, facecolor=c,
                               edgecolor=c, linewidth=.6, zorder=5))

    closes=[r["close"] for r in rows]
    e20=ema(closes,20)
    e50=ema(closes,50)
    ax.plot(range(n), e20, color=BLUE, linewidth=1.35, alpha=.95, label="EMA20", zorder=6)
    ax.plot(range(n), e50, color=GOLD, linewidth=1.25, alpha=.9, label="EMA50", zorder=6)

    # Recent swing structure for a chart-first analysis.
    highs, lows = swing_points(rows, left=2, right=2)
    for idx, price in highs[-6:]:
        ax.scatter([idx], [price], s=20, facecolors="none", edgecolors=DOWN, linewidth=.9, zorder=8)
    for idx, price in lows[-6:]:
        ax.scatter([idx], [price], s=20, facecolors="none", edgecolors=UP, linewidth=.9, zorder=8)

    # If a complete ICT setup exists, overlay its zones and levels.
    if block:
        setup = block.get("long_sig") or block.get("short_sig")
        if setup:
            direction=setup["direction"]
            fvg=setup.get("fvg") or {}
            ob=setup.get("ob") or {}
            offset=setup.get("full_len", len(rows)) - len(setup.get("rows", rows))
            setup_rows=setup.get("rows", rows)
            # Match setup timestamps to visible chart indices where possible.
            time_to_local={r["time"]:i for i,r in enumerate(rows)}
            def zone_idx(z):
                if not z or "index" not in z:
                    return max(0,n-1)
                full_i=int(z["index"])
                setup_i=full_i-offset
                if setup_rows and 0 <= setup_i < len(setup_rows):
                    ts=setup_rows[setup_i]["time"]
                    return time_to_local.get(ts, max(0,n-1))
                return max(0,min(n-1, full_i))
            for z,color,label,alpha in ((ob,PINK,"ORDER BLOCK",.18),(fvg,PURPLE,"FVG",.18)):
                if z and "low" in z and "high" in z:
                    x0=max(0,min(n-1,zone_idx(z)-8))
                    ax.add_patch(Rectangle((x0,z["low"]),n-x0,z["high"]-z["low"],
                                           facecolor=color,edgecolor=color,alpha=alpha,linewidth=1.0,zorder=1))
                    ax.text(x0+1,z["high"],label,color=color,fontsize=8.5,fontweight="bold",va="bottom",zorder=9)
            if "entry" in setup:
                entry,sl=setup["entry"],setup["sl"]
                ax.axhline(entry,color=BLUE,linestyle="--",linewidth=1.15,zorder=3)
                ax.axhline(sl,color=DOWN,linewidth=1.1,zorder=3)
                for y,lab in ((setup["tp1"],"TP1"),(setup["tp2"],"TP2"),(setup["tp3"],"TP3")):
                    ax.axhline(y,color=UP,linestyle="--",linewidth=.9,alpha=.8,zorder=2)
                    ax.text(n+1,y,f"{lab} {fmt_price(y)}",color=UP,fontsize=8,fontweight="bold",va="center")
                ax.text(n+1,entry,f"ENTRY {fmt_price(entry)}",color=BLUE,fontsize=8,fontweight="bold",va="center")
                ax.text(n+1,sl,f"SL {fmt_price(sl)}",color=DOWN,fontsize=8,fontweight="bold",va="center")
                if "liquidity" in setup:
                    ax.axhline(setup["liquidity"],color=GOLD,linestyle=":",linewidth=1.0,alpha=.9,zorder=2)
                if "mss_level" in setup:
                    ax.axhline(setup["mss_level"],color=CYAN,linestyle="--",linewidth=1.0,alpha=.7,zorder=2)

    cur=rows[-1]["close"]
    e20v=e20[-1]; e50v=e50[-1]
    if cur > e20v and e20v > e50v:
        bias="LONG"
        bias_color=UP
    elif cur < e20v and e20v < e50v:
        bias="SHORT"
        bias_color=DOWN
    else:
        bias="MIXED"
        bias_color=GOLD

    ax.text(.018,1.065,f"{symbol} · {timeframe.upper()}",transform=ax.transAxes,
            fontsize=16,color=TEXT,fontweight="bold",va="top")
    ax.text(.018,1.025,"SAIWAN ANALYSIS · BITGET FUTURES",transform=ax.transAxes,
            fontsize=8.8,color=MUTED,fontweight="bold",va="top")
    ax.text(.985,1.055,bias,transform=ax.transAxes,fontsize=12,color=bias_color,
            fontweight="bold",ha="right",va="top",
            bbox=dict(boxstyle="round,pad=.38",facecolor=BG,edgecolor=bias_color,linewidth=1.0))

    panel=(f"{symbol} · {timeframe.upper()}\\n"
           f"Price  {fmt_price(cur)}\\n"
           f"EMA20  {fmt_price(e20v)}\\n"
           f"EMA50  {fmt_price(e50v)}\\n\\n"
           f"Bias: {bias}\\n"
           f"Candles: CLOSED")
    ax.text(.022,.035,panel,transform=ax.transAxes,fontsize=8.8,color=TEXT,va="bottom",
            ha="left",linespacing=1.45,bbox=dict(boxstyle="round,pad=.72",facecolor=PANEL,
            edgecolor="#2a4664",linewidth=1.0,alpha=.97),zorder=20)
    ax.legend(loc="upper left",bbox_to_anchor=(.36,1.055),frameon=False,labelcolor=TEXT,
              fontsize=8.5,ncol=2)

    ax.yaxis.tick_right()
    ax.tick_params(axis="y",colors="#9bb0c5",labelsize=8.2,length=0,pad=7)
    ax.tick_params(axis="x",colors="#71879d",labelsize=7.8,length=0,pad=8)
    ax.grid(axis="y",color=GRID,linewidth=.65,alpha=.8)
    ax.grid(axis="x",color=GRID,linewidth=.35,alpha=.35)
    for side in ["top","left","bottom"]: ax.spines[side].set_visible(False)
    ax.spines["right"].set_color("#22364b")
    step=max(1,n//7)
    ticks=list(range(0,n,step))
    if not ticks or ticks[-1]!=n-1: ticks.append(n-1)
    ax.set_xticks(ticks)
    ax.set_xticklabels([datetime.fromtimestamp(rows[i]["time"],tz=timezone.utc).strftime("%d\\n%H:%M") for i in ticks])
    all_lows=[r["low"] for r in rows]
    all_highs=[r["high"] for r in rows]
    ymin,ymax=min(all_lows),max(all_highs)
    span=max(ymax-ymin,abs(cur)*.012)
    ax.set_ylim(ymin-span*.06,ymax+span*.16)
    ax.set_xlim(-1,n+10)
    fig.subplots_adjust(left=.025,right=.87,top=.86,bottom=.085)
    safe="".join(ch if ch.isalnum() else "_" for ch in symbol)
    path=f"/tmp/analysis_{safe}_{timeframe}_{int(time.time())}.png"
    fig.savefig(path,facecolor=BG,edgecolor="none",bbox_inches="tight",pad_inches=.08)
    plt.close(fig)
    return path


def make_analysis_charts(raw_symbol, requested_timeframes=None):
    symbol=_normalize_analysis_symbol(raw_symbol)
    tfs=list(requested_timeframes or [])
    if not tfs:
        tfs=["5m","15m"]
    paths=[]
    for tf in tfs:
        rows=_analysis_tf_data(symbol,tf)
        block=None
        if tf in ("1h","4h"):
            try:
                block=_format_htf_block(symbol,tf)
            except Exception:
                block=None
        elif tf=="5m":
            try:
                rows15=_analysis_tf_data(symbol,"15m")
                cur=rows[-1]["close"]
                e20=ema([r["close"] for r in rows],20)[-1]
                e50=ema([r["close"] for r in rows],50)[-1]
                long_sig=_move_setup(rows,"LONG")
                short_sig=_move_setup(rows,"SHORT")
                block={"long_sig":long_sig,"short_sig":short_sig}
            except Exception:
                block=None
        paths.append(make_analysis_chart(symbol,tf,rows,block))
    return paths

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


def _normalize_analysis_symbol(raw):
    """Normalize a Telegram /analysis symbol to a Bitget USDT perpetual symbol."""
    symbol = (raw or "").strip().upper().replace("/", "").replace("-", "")
    if not symbol:
        return None
    if symbol.endswith("USDT"):
        return symbol
    return symbol + "USDT"


def _normalize_analysis_timeframe(raw):
    """Normalize an analysis timeframe. Supports 5m/15m/1h/4h."""
    tf = (raw or "").strip().lower()
    aliases = {
        "5": "5m", "5m": "5m",
        "15": "15m", "15m": "15m",
        "1h": "1h", "1hr": "1h", "60m": "1h", "60": "1h",
        "4h": "4h", "4hr": "4h", "240m": "4h", "240": "4h",
    }
    return aliases.get(tf)


def _analysis_tf_data(symbol, timeframe):
    """Fetch closed candles for an on-demand analysis timeframe."""
    limits = {"5m": CANDLE_LIMIT, "15m": 180, "1h": 180, "4h": 180}
    rows = get_klines(symbol, timeframe, limits.get(timeframe, 180))
    if len(rows) < 60:
        raise RuntimeError(f"not enough {timeframe} candles")
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
    """Run the existing price-action setup model on any supported timeframe."""
    try:
        return _move_setup(rows, direction)
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
        setup = "⚪ No complete ICT setup"

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
    """On-demand multi-timeframe analysis for /analysis SYMBOL [1h] [4h].

    Examples:
      /analysis BTC
      /analysis BTC 1h
      /analysis BTC 4h
      /analysis BTC 1h 4h

    If no timeframe is supplied, the original 5m + 15m analysis is used.
    1h/4h analysis uses the same closed-candle price-action model and EMA bias
    checks as the bot's existing scanner, but does not place trades.
    """
    symbol = _normalize_analysis_symbol(raw_symbol)
    if not symbol or len(symbol) < 6:
        return "❌ تکایە ناوی کۆین بنووسە.\n\nنموونە:\n/analysis BTC\n/analysis BTC 1h\n/analysis BTC 4h\n/analysis BTC 1h 4h"

    requested = []
    for raw_tf in (requested_timeframes or []):
        tf = _normalize_analysis_timeframe(raw_tf)
        if tf and tf not in requested:
            requested.append(tf)
    requested = requested[:3]

    # Default mode keeps the current behavior exactly: 5m entry + 15m context.
    if not requested:
        rows5 = get_klines(symbol, TF_5M, CANDLE_LIMIT)
        rows15 = get_klines(symbol, TF_15M, 180)
        if len(rows5) < 120 or len(rows15) < 30:
            return f"❌ داتای بەشی پێویست بۆ {symbol} بەردەست نییە. دڵنیابە کۆینەکە لە Bitget USDT Futures هەیە."

        cur = rows5[-1]["close"]
        e20_5 = ema([r["close"] for r in rows5], 20)[-1]
        e50_5 = ema([r["close"] for r in rows5], 50)[-1]
        e20_15 = ema([r["close"] for r in rows15], 20)[-1]
        e50_15 = ema([r["close"] for r in rows15], 50)[-1]

        long_sig = _move_setup(rows5, "LONG")
        short_sig = _move_setup(rows5, "SHORT")
        long_score = int(cur > e20_5) + int(e20_5 > e50_5) + int(e20_15 > e50_15)
        short_score = int(cur < e20_5) + int(e20_5 < e50_5) + int(e20_15 < e50_15)

        if long_sig and not short_sig:
            verdict = "🟢 LONG setup موجودە"
            setup = long_sig
        elif short_sig and not long_sig:
            verdict = "🔴 SHORT setup موجودە"
            setup = short_sig
        elif long_sig and short_sig:
            if long_score > short_score:
                verdict = "🟢 LONG bias — بەڵام هەردوو لایەن setup هەیە"
                setup = long_sig
            elif short_score > long_score:
                verdict = "🔴 SHORT bias — بەڵام هەردوو لایەن setup هەیە"
                setup = short_sig
            else:
                verdict = "🟡 WAIT — هەردوو لایەن نزیکن"
                setup = None
        else:
            if long_score >= 2 and short_score == 0:
                verdict = "🟢 LONG bias — setupی تەواو نییە"
            elif short_score >= 2 and long_score == 0:
                verdict = "🔴 SHORT bias — setupی تەواو نییە"
            else:
                verdict = "🟡 WAIT — setupی تەواو نییە"
            setup = None

        context_long = _context_15m(rows15, "LONG")
        context_short = _context_15m(rows15, "SHORT")
        lines = [
            f"🔎 SAIWAN ANALYSIS — {symbol}", "", verdict,
            f"💵 Price: {fmt_price(cur)}",
            "⏱ Timeframe: 5m + 15m context", "",
            f"5m EMA20: {fmt_price(e20_5)} | EMA50: {fmt_price(e50_5)}",
            f"15m EMA20: {fmt_price(e20_15)} | EMA50: {fmt_price(e50_15)}",
            f"15m Long context: {context_long}",
            f"15m Short context: {context_short}", "",
            f"📊 Bias checks — LONG {long_score}/3 · SHORT {short_score}/3",
        ]
        if setup:
            lines += [
                "", f"🎯 Entry: {fmt_price(setup['entry'])}",
                f"🛑 SL: {fmt_price(setup['sl'])}",
                f"🎯 TP1: {fmt_price(setup['tp1'])}",
                f"🎯 TP2: {fmt_price(setup['tp2'])}",
                f"🎯 TP3: {fmt_price(setup['tp3'])}", "",
                "✅ Liquidity Sweep · MSS · CHOCH · FVG · OB",
            ]
        else:
            lines += [
                "",
                "ℹ️ هیچ setupی تەواوی Liquidity Sweep + MSS + FVG + OB لە ئێستادا نییە.",
                "باشترە بۆ triggerی تەواو چاوەڕێ بکرێت لە جیاتی دروستکردنی سیگناڵی ناڕاست.",
            ]
        return "\n".join(lines)

    try:
        blocks = [_format_htf_block(symbol, tf) for tf in requested]
    except Exception as e:
        return f"❌ نەتوانرا شیکاری {symbol} بکرێت بۆ {', '.join(requested)}. دڵنیابە کۆینەکە لە Bitget USDT Futures هەیە."

    lines = [
        f"🔎 SAIWAN HTF ANALYSIS — {symbol}", "",
        _analysis_verdict(blocks),
        "📌 This is market analysis only — no automatic trading.", "",
    ]
    for b in blocks:
        lines += [
            f"━━ {b['timeframe'].upper()} ━━",
            f"💵 Price: {fmt_price(b['price'])}",
            f"EMA20: {fmt_price(b['ema20'])} | EMA50: {fmt_price(b['ema50'])}",
            f"Bias: {'🟢 LONG' if b['bias']=='LONG' else '🔴 SHORT' if b['bias']=='SHORT' else '🟡 MIXED'}",
            f"📊 Checks — LONG {b['long_score']}/2 · SHORT {b['short_score']}/2",
            f"Setup: {b['setup']}",
        ]
        setup = b["long_sig"] or b["short_sig"]
        if setup:
            lines += [
                f"Entry: {fmt_price(setup['entry'])}",
                f"SL: {fmt_price(setup['sl'])}",
                f"TP1: {fmt_price(setup['tp1'])}",
                f"TP2: {fmt_price(setup['tp2'])}",
                f"TP3: {fmt_price(setup['tp3'])}",
            ]
        lines.append("")

    if len(blocks) >= 2:
        b1, b2 = blocks[0], blocks[1]
        if b1["bias"] == b2["bias"] and b1["bias"] in ("LONG", "SHORT"):
            lines += [f"🎯 HTF ALIGNMENT: {b1['bias']} — {b1['timeframe']} + {b2['timeframe']} agree."]
        else:
            lines += ["⚠️ HTF ALIGNMENT: Mixed — wait for the timeframes to agree before treating it as a directional setup."]
    return "\n".join(lines)


def status_text():
    with state_lock:
        return (
            "BOT STATUS: ONLINE\n"
            f"Scanner: {'RUNNING' if scanner_running else 'STOPPED'}\n"
            "Market: Bitget USDT Perpetual Futures (full eligible market)\n"
            "Strategy: SAIWAN CRYPTO SIGNAL — Move Hunter\n"
            "Model: SAIWAN Move Hunter — Liquidity Sweep + MSS + CHOCH + FVG + OB\n"
            "Data source: Bitget Futures market data\n"
            "Scan: 5m closed candles + 15m context\n"
            f"Pending signals: {len(pending_signals)}\n"
            f"Tracked signals: {len(active_signals)}\n"
            "Chart: ICT components annotated\n"
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
            rows5 = get_klines(symbol, TF_5M, CANDLE_LIMIT)
            rows15 = get_klines(symbol, TF_15M, 180)
            if len(rows5) < 120 or len(rows15) < 30:
                return symbol, None, None
            return symbol, analyze(symbol, rows5, rows15), None
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
    print(f"Bitget Move Hunter scan: universe={len(eligible)}, scanned={len(pairs)}, confirmed={len(found)}, errors={total_errors}, workers={SCAN_WORKERS}")
    if not contracts:
        print("Bitget warning: no contracts returned from /api/v2/mix/market/contracts")
    elif not tickers:
        print("Bitget warning: no tickers returned from /api/v2/mix/market/tickers")
    if summary:
        print(f"Bitget error summary: {summary}")


def signal_caption(sig):
    d = "🟢 LONG" if sig["direction"] == "LONG" else "🔴 SHORT"
    return (
        f"🚀 SAIWAN CRYPTO SIGNAL\n\n{d}\n"
        f"⭐ {sig['symbol']} · Bitget Futures\n"
        f"⏱ 5m Entry · 15m Context\n\n"
        "Liquidity Sweep ✓  ·  MSS ✓  ·  CHOCH ✓  ·  FVG ✓  ·  OB ✓\n"
        f"15m Context: {sig.get('context15','UNKNOWN')}\n"
        f"Entry: {fmt_price(sig['entry'])}\n"
        f"SL: {fmt_price(sig['sl'])}\n"
        f"TP1: {fmt_price(sig['tp1'])}\n"
        f"TP2: {fmt_price(sig['tp2'])}\n"
        f"TP3: {fmt_price(sig['tp3'])}\n\n"
        "⚡ Early move setup — closed candles only.\n"
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
                        "🚀 SAIWAN CRYPTO SIGNAL\n\n"
                        "/scan - Start scanner\n"
                        "/stop - Stop scanner\n"
                        "/status - Bot status\n"
                        "/analysis COIN - Analyze one coin now\n"
                        "/analysis COIN 1h - Higher-timeframe analysis\n"
                        "/analysis COIN 4h - Higher-timeframe analysis\n"
                        "/analysis COIN 1h 4h - Multi-timeframe analysis\n\n"
                        "Market: Bitget USDT Perpetual Futures\n"
                        "Timeframe: 5m entry + 15m context\n"
                        "Model: SAIWAN Move Hunter — Liquidity Sweep + MSS + CHOCH + FVG + OB\n"
                        "Chart: professional 5m setup map with all ICT components\n"
                        "TP/SL monitoring: ENABLED")
                elif text.startswith("/scan"):
                    start_scanner(active_chat_id)
                    send_message(active_chat_id,
                        "🚀 SAIWAN CRYPTO SIGNAL SCANNER STARTED\n\n"
                        "5m closed candles for early entries + 15m context.\n"
                        "Signal hunts: Liquidity Sweep + MSS + CHOCH + FVG + OB.\n"
                        "The chart will mark every ICT component used.\n"
                        "TP/SL monitoring is enabled.")
                elif text.startswith("/stop"):
                    stop_scanner(); send_message(active_chat_id, "🛑 Scanner stopped.")
                elif text.startswith("/analysis"):
                    parts = text.split()
                    if len(parts) < 2:
                        send_message(active_chat_id, "🔎 نموونە:\n/analysis BTCUSDT\n/analysis AVAX 1h\n/analysis ETH 4h\n/analysis BTC 1h 4h")
                    else:
                        try:
                            symbol = _normalize_analysis_symbol(parts[1])
                            timeframes = parts[2:]
                            valid_tfs = [_normalize_analysis_timeframe(x) for x in timeframes]
                            invalid = [x for x, tf in zip(timeframes, valid_tfs) if tf is None]
                            if invalid:
                                send_message(active_chat_id, "❌ Timeframe ـی دروست: 5m, 15m, 1h, 4h\n\nنموونە: /analysis BTC 1h 4h")
                            else:
                                mode = " + ".join(valid_tfs) if valid_tfs else "5m + 15m"
                                send_message(active_chat_id, f"🔎 خەریکم {symbol} شیکاری دەکەم...\n⏱ {mode}")
                                report = analysis_report(parts[1], valid_tfs)
                                send_message(active_chat_id, report)
                                try:
                                    chart_paths = make_analysis_charts(parts[1], valid_tfs)
                                    for chart_path in chart_paths:
                                        send_photo(active_chat_id, chart_path, f"📊 SAIWAN CHART — {_normalize_analysis_symbol(parts[1])}")
                                except Exception as chart_error:
                                    print(f"ANALYSIS CHART ERROR {type(chart_error).__name__}: {chart_error}")
                                    send_message(active_chat_id, "⚠️ شیکاریەکە هات، بەڵام چارتەکە نەدروستکرا.")
                        except Exception as e:
                            print(f"ANALYSIS ERROR {type(e).__name__}: {e}")
                            send_message(active_chat_id, f"❌ شیکاری سەرکەوتوو نەبوو: {type(e).__name__}")
                elif text.startswith("/status"):
                    send_message(active_chat_id, status_text())
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
