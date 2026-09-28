#!/usr/bin/env python3
"""
6-month historical SMC V8 backtest using Kraken downloadable OHLCVT archives.

V8 is a research-only backtest. It does NOT change the live Telegram bot.

Compared with V5/V4/V3:
- Replaces the hard 1H+4H gate with soft HTF evidence points, preventing the test set from collapsing to a handful of trades.
- Keeps robust Kraken legacy-symbol/folder matching and diagnostics.
- Tests thresholds 6/10, 7/10, 8/10, 9/10, 10/10.
- Tests three exit models in the same run:
  * TP1-FULL = full position exits at 1.5R.
  * TP2-FULL = full position exits at 2.5R.
  * PARTIAL-BE = 50% exits at 1.5R, then stop moves to breakeven;
    remaining 50% exits at 2.5R or breakeven.
- Uses a chronological 70/30 walk-forward split per pair.
- Selects ONE threshold+exit model using TRAIN only, with a minimum
  training trade-count guard, then evaluates that fixed choice once on unseen TEST.
- Reports every candidate on both TRAIN and TEST so the full experiment is visible.
- Adds cost sensitivity for the selected model (0%, base 0.25% round trip,
  and stressed 0.50% round trip).
- Adds pair/data coverage and total date-range diagnostics.
- Prevents train trades from entering the test period.
- No lookahead: 1H/4H trends use only completed higher-timeframe candles.
- Trades are non-overlapping and max holding time is 24h (96 x 15m bars).
- Same-candle SL is counted before targets.
"""

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

ARCHIVES = [
    ("2026Q1", "https://assets.kraken.com/marketing/institutions/Kraken_OHLCVT_2026Q1.zip"),
    ("2026Q2", "https://assets.kraken.com/marketing/institutions/Kraken_OHLCVT_2026Q2.zip"),
]

PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "LTCUSDT",
    "BCHUSDT", "ATOMUSDT", "UNIUSDT", "AAVEUSDT", "NEARUSDT",
    "ETCUSDT", "FILUSDT", "ALGOUSDT", "XLMUSDT", "TRXUSDT",
]
# Kraken has historically used legacy asset codes in exported data.
# The downloader therefore tries modern symbols plus known legacy codes.
LEGACY_BASE_ALIASES = {
    "BTC": ["BTC", "XBT", "XXBT"],
    "ETH": ["ETH", "XETH"],
    "LTC": ["LTC", "XLTC"],
    "ETC": ["ETC", "XETC"],
    "DOGE": ["DOGE", "XDG", "XXDG"],
    "XLM": ["XLM", "XXLM"],
    "XRP": ["XRP", "XXRP"],
    "XMR": ["XMR", "XXMR"],
    "ZEC": ["ZEC", "XZEC"],
}
QUOTE_ALIASES = {
    "USDT": ["USDT"],
    "USD": ["USD", "ZUSD"],
    "EUR": ["EUR", "ZEUR"],
    "GBP": ["GBP", "ZGBP"],
    "JPY": ["JPY", "ZJPY"],
    "CAD": ["CAD", "ZCAD"],
}
PAIR_ALIASES = {}

def norm(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper())

for _pair in PAIRS:
    _base, _quote = _pair[:-4], _pair[-4:]
    _bases = LEGACY_BASE_ALIASES.get(_base, [_base])
    _quotes = QUOTE_ALIASES.get(_quote, [_quote])
    _variants = []
    for _b in _bases:
        for _q in _quotes:
            _variants.append(norm(_b + _q))
    PAIR_ALIASES[_pair] = list(dict.fromkeys(_variants))

MAX_BARS_AHEAD = 96
THRESHOLDS = (6, 7, 8, 9, 10)
EXIT_MODELS = ("TP1_FULL", "TP2_FULL", "PARTIAL_BE")
TRAIN_RATIO = 0.70
MIN_TRAIN_TRADES_FOR_SELECTION = 100
MIN_TRAIN_NET_PF = 1.05
MIN_TRAIN_NET_R = 0.0
MIN_TRAIN_STRESS_NET_R = 0.0
MIN_TEST_TRADES_REPORT = 30

# Conservative cost-stress assumption; change only for sensitivity testing.
FEE_PER_SIDE_PCT = 0.00100       # 0.10%
SLIPPAGE_PER_SIDE_PCT = 0.00025  # 0.025%
ROUND_TRIP_COST_PCT = 2 * (FEE_PER_SIDE_PCT + SLIPPAGE_PER_SIDE_PCT)

WORKDIR = Path("kraken_history_cache")


@dataclass
class Setup:
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    score: int


@dataclass
class Trade:
    pair: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    score: int
    result: str = "TIMEOUT"
    r: float = 0.0
    net_r: float = 0.0


def aliases(pair: str):
    return PAIR_ALIASES.get(pair, [norm(pair)])


def refresh_aliases_from_assetpairs():
    """Augment pair aliases from Kraken's live AssetPairs symbol map.

    Historical exports can use legacy asset codes while the REST API may
    expose the same market through altname/wsname/base/quote variants.
    This does not replace the historical-file matching rules; it only adds
    more exact candidates when Kraken's symbol set changes.
    """
    try:
        r = requests.get(
            "https://api.kraken.com/0/public/AssetPairs",
            timeout=20,
        )
        r.raise_for_status()
        payload = r.json()
        result = payload.get("result", {})
        for pair in PAIRS:
            base, quote = pair[:-4], pair[-4:]
            variants = set(PAIR_ALIASES.get(pair, []))
            for key, meta in result.items():
                if not isinstance(meta, dict):
                    continue
                vals = [
                    key, meta.get("altname", ""), meta.get("wsname", ""),
                    meta.get("base", ""), meta.get("quote", ""),
                ]
                joined = " ".join(str(v) for v in vals).upper()
                if base in joined and quote in joined:
                    for v in vals[:3]:
                        if v:
                            variants.add(norm(str(v).replace("/", "")))
                    b = meta.get("base", "")
                    q = meta.get("quote", "")
                    if b and q:
                        variants.add(norm(str(b) + str(q)))
            PAIR_ALIASES[pair] = list(variants)
        print("Kraken AssetPairs alias refresh: OK")
    except Exception as exc:
        print(f"Kraken AssetPairs alias refresh skipped: {exc}")


def _is_15m_csv(name: str) -> bool:
    """Accept common Kraken 15-minute CSV filename forms."""
    full = norm(name)
    if not full.endswith("CSV"):
        return False
    return (
        full.endswith("15CSV")
        or full.endswith("15MCSV")
        or full.endswith("15MINCSV")
    )


def find_csv(z: zipfile.ZipFile, pair: str) -> Optional[str]:
    """Find the exact 15m CSV for a pair, including legacy Kraken symbols."""
    wanted = set(aliases(pair))
    members = [name for name in z.namelist() if _is_15m_csv(name)]

    # 1) Exact normalized pair+15m suffix.
    for name in members:
        full = norm(name)
        if any(full.endswith(a + suffix) for a in wanted
               for suffix in ("15CSV", "15MCSV", "15MINCSV")):
            return name

    # 2) Folder layouts such as XDGUSDT/15.csv or XDG/USDT/15.csv.
    for name in members:
        full = norm(name)
        if any(a in full for a in wanted):
            return name

    # 3) Exact base/quote component match, using all known aliases.
    base, quote = pair[:-4], pair[-4:]
    bases = set(LEGACY_BASE_ALIASES.get(base, [base]))
    quotes = set(QUOTE_ALIASES.get(quote, [quote]))
    for name in members:
        full = norm(name)
        if any(norm(b) in full for b in bases) and any(norm(q) in full for q in quotes):
            return name

    return None


def nearby_csv_candidates(z: zipfile.ZipFile, pair: str, limit: int = 8):
    """Return diagnostic candidates without silently using the wrong market."""
    base, _ = pair[:-4], pair[-4:]
    bases = set(LEGACY_BASE_ALIASES.get(base, [base]))
    out = []
    for name in z.namelist():
        if not _is_15m_csv(name):
            continue
        full = norm(name)
        if any(norm(b) in full for b in bases):
            out.append(name)
            if len(out) >= limit:
                break
    return out

def read_candles(z: zipfile.ZipFile, member: str):
    candles = []
    with z.open(member) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
        for row in csv.reader(text):
            if not row or len(row) < 7:
                continue
            try:
                candles.append({
                    "time": int(float(row[0])),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "trades": int(float(row[6])),
                })
            except (ValueError, TypeError):
                continue
    candles.sort(key=lambda x: x["time"])
    out, seen = [], set()
    for c in candles:
        if c["time"] not in seen:
            out.append(c)
            seen.add(c["time"])
    return out


def aggregate_bars(candles, seconds_per_bar):
    groups = {}
    for c in candles:
        bucket = (c["time"] // seconds_per_bar) * seconds_per_bar
        groups.setdefault(bucket, []).append(c)

    bars = []
    for bucket in sorted(groups):
        xs = sorted(groups[bucket], key=lambda x: x["time"])
        expected = seconds_per_bar // 900
        if len(xs) != expected:
            continue
        bars.append({
            "time": bucket + seconds_per_bar - 900,
            "open": xs[0]["open"],
            "high": max(x["high"] for x in xs),
            "low": min(x["low"] for x in xs),
            "close": xs[-1]["close"],
            "volume": sum(x["volume"] for x in xs),
        })
    return bars


def precompute_htf_trend(candles, hours_per_bar, fast=20, slow=50):
    """Compute completed-HTF trend once per candle in O(n), avoiding repeated O(n^2) aggregation."""
    bars = aggregate_bars(candles, 3600 * hours_per_bar)
    closes = [b["close"] for b in bars]
    out = [None] * len(candles)
    j = 0
    for i, c in enumerate(candles):
        current_time = c["time"]
        while j + 1 < len(bars) and bars[j + 1]["time"] < current_time:
            j += 1
        if not bars or bars[j]["time"] >= current_time or j + 1 < slow:
            continue
        end = j + 1
        sma_fast = sum(closes[end-fast:end]) / fast
        sma_slow = sum(closes[end-slow:end]) / slow
        last = closes[j]
        if last > sma_fast and sma_fast > sma_slow:
            out[i] = "LONG"
        elif last < sma_fast and sma_fast < sma_slow:
            out[i] = "SHORT"
        else:
            out[i] = "NEUTRAL"
    return out


def atr(candles, i, n=14):
    if i < n:
        return None
    trs = []
    for k in range(i - n + 1, i + 1):
        prev_close = candles[k - 1]["close"]
        h, l = candles[k]["high"], candles[k]["low"]
        trs.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
    return sum(trs) / n


def structure(candles, i):
    if i < 41:
        return None
    current = candles[i]["close"]
    recent = candles[i - 20:i]
    previous = candles[i - 40:i - 20]
    recent_high = max(x["high"] for x in recent)
    recent_low = min(x["low"] for x in recent)
    previous_high = max(x["high"] for x in previous)
    previous_low = min(x["low"] for x in previous)

    if current > recent_high:
        return "LONG", "BOS", recent_high, recent_low
    if current < recent_low:
        return "SHORT", "BOS", recent_high, recent_low
    if recent_high > previous_high and recent_low > previous_low:
        return "LONG", "CHoCH", recent_high, recent_low
    if recent_high < previous_high and recent_low < previous_low:
        return "SHORT", "CHoCH", recent_high, recent_low
    return None


def liquidity_sweep(candles, i, direction):
    if i < 24:
        return False
    for j in range(max(20, i - 3), i):
        prior_high = max(c["high"] for c in candles[j - 20:j])
        prior_low = min(c["low"] for c in candles[j - 20:j])
        c = candles[j]
        if direction == "LONG" and c["low"] < prior_low and c["close"] > prior_low:
            return True
        if direction == "SHORT" and c["high"] > prior_high and c["close"] < prior_high:
            return True
    return False


def fvg(candles, i, direction):
    if i < 4:
        return False
    for j in range(max(2, i - 4), i):
        a, _, c = candles[j - 2], candles[j - 1], candles[j]
        if direction == "LONG" and c["low"] > a["high"]:
            gap_low, gap_high = a["high"], c["low"]
            if candles[i]["low"] <= gap_high * 1.01 and candles[i]["high"] >= gap_low * 0.99:
                return True
        if direction == "SHORT" and c["high"] < a["low"]:
            gap_low, gap_high = c["high"], a["low"]
            if candles[i]["low"] <= gap_high * 1.01 and candles[i]["high"] >= gap_low * 0.99:
                return True
    return False


def volume_confirm(candles, i):
    if i < 20:
        return False
    avg = sum(c["volume"] for c in candles[i - 20:i]) / 20
    return candles[i]["volume"] >= avg * 1.15


def atr_confirm(candles, i, entry, sl):
    a = atr(candles, i, 14)
    if not a or a <= 0:
        return False
    risk = abs(entry - sl)
    return 0.5 * a <= risk <= 4.0 * a


def order_block(candles, i, direction):
    if i < 7:
        return False
    price = candles[i]["close"]
    for j in range(i - 1, max(1, i - 6), -1):
        c = candles[j]
        body = abs(c["close"] - c["open"])
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        opposite = (
            (direction == "LONG" and c["close"] < c["open"])
            or (direction == "SHORT" and c["close"] > c["open"])
        )
        if not opposite:
            continue
        zone_low = min(c["open"], c["close"])
        zone_high = max(c["open"], c["close"])
        distance = 0.0 if zone_low <= price <= zone_high else min(
            abs(price - zone_low), abs(price - zone_high)
        )
        if distance <= max(body * 2.0, price * 0.015):
            return True
    return False


def candle_confirm(candles, i, direction):
    c = candles[i]
    rng = c["high"] - c["low"]
    if rng <= 0:
        return False
    body = abs(c["close"] - c["open"])
    if body / rng < 0.50:
        return False
    if direction == "LONG":
        return c["close"] >= c["low"] + rng * 0.70
    return c["close"] <= c["high"] - rng * 0.70


def displacement(candles, i, direction):
    if i < 21:
        return False
    c = candles[i]
    body = abs(c["close"] - c["open"])
    prev_bodies = [abs(x["close"] - x["open"]) for x in candles[i - 20:i]]
    avg_body = sum(prev_bodies) / len(prev_bodies)
    if avg_body <= 0 or body < avg_body * 1.25:
        return False
    if direction == "LONG":
        return c["close"] > c["open"]
    return c["close"] < c["open"]


def build_setup(candles, i, h1_trends=None, h4_trends=None):
    s = structure(candles, i)
    if not s:
        return None

    direction, _, recent_high, recent_low = s
    entry = candles[i]["close"]
    sl = recent_low if direction == "LONG" else recent_high
    risk = entry - sl if direction == "LONG" else sl - entry
    if risk <= 0:
        return None

    h1 = h1_trends[i] if h1_trends is not None else None
    h4 = h4_trends[i] if h4_trends is not None else None

    # V8 uses 1H and 4H as soft evidence, not hard gates. A hard gate can
    # shrink the unseen test sample so much that a seemingly "selected" model
    # is based on only a few trades. Soft points preserve sample size while
    # still rewarding multi-timeframe alignment.
    h1_ok = h1 == direction
    h4_ok = h4 == direction

    checks = [
        h1_ok,
        liquidity_sweep(candles, i, direction),
        fvg(candles, i, direction),
        volume_confirm(candles, i),
        atr_confirm(candles, i, entry, sl),
        order_block(candles, i, direction),
        candle_confirm(candles, i, direction),
        displacement(candles, i, direction),
        h4_ok,
    ]

    score = 1 + sum(checks)  # structure + 9 evidence points = 10

    if direction == "LONG":
        tp1, tp2 = entry + 1.5 * risk, entry + 2.5 * risk
    else:
        tp1, tp2 = entry - 1.5 * risk, entry - 2.5 * risk

    return Setup(direction, entry, sl, tp1, tp2, score)

def cost_r(entry, sl):
    """Round-trip fee+slippage converted to R."""
    risk = abs(entry - sl)
    if risk <= 0:
        return 0.0
    cost_abs = entry * ROUND_TRIP_COST_PCT
    return cost_abs / risk


def simulate(candles, i, setup, pair, exit_model, max_end=None):
    end_limit = len(candles) if max_end is None else min(len(candles), max_end)
    end = min(end_limit, i + 1 + MAX_BARS_AHEAD)
    cost = cost_r(setup.entry, setup.sl)

    if exit_model == "TP1_FULL":
        for j in range(i + 1, end):
            h, l = candles[j]["high"], candles[j]["low"]
            if setup.direction == "LONG":
                if l <= setup.sl:
                    gross = -1.0
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "SL", gross, gross - cost)
                if h >= setup.tp1:
                    gross = 1.5
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "TP1", gross, gross - cost)
            else:
                if h >= setup.sl:
                    gross = -1.0
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "SL", gross, gross - cost)
                if l <= setup.tp1:
                    gross = 1.5
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "TP1", gross, gross - cost)

        return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                     setup.tp2, setup.score, "TIMEOUT", 0.0, -cost)

    if exit_model == "TP2_FULL":
        for j in range(i + 1, end):
            h, l = candles[j]["high"], candles[j]["low"]
            if setup.direction == "LONG":
                if l <= setup.sl:
                    gross = -1.0
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "SL", gross, gross - cost)
                if h >= setup.tp2:
                    gross = 2.5
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "TP2", gross, gross - cost)
            else:
                if h >= setup.sl:
                    gross = -1.0
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "SL", gross, gross - cost)
                if l <= setup.tp2:
                    gross = 2.5
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "TP2", gross, gross - cost)

        return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                     setup.tp2, setup.score, "TIMEOUT", 0.0, -cost)

    # PARTIAL_BE: 50% at 1.5R, then the remaining 50% has a breakeven stop.
    hit_tp1 = False
    for j in range(i + 1, end):
        h, l = candles[j]["high"], candles[j]["low"]
        if setup.direction == "LONG":
            if not hit_tp1:
                if l <= setup.sl:
                    gross = -1.0
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "SL", gross, gross - cost)
                if h >= setup.tp1:
                    hit_tp1 = True
                    # If TP1 and TP2 are both inside the same candle, conservative
                    # handling credits only the 50% TP1 fill; the remaining half
                    # cannot be assumed to have reached TP2 without tick data.
                    if h >= setup.tp2:
                        gross = 0.75
                        return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                     setup.tp2, setup.score, "TP1+", gross, gross - cost)
            else:
                if l <= setup.entry:
                    gross = 0.75
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "BE", gross, gross - cost)
                if h >= setup.tp2:
                    gross = 2.0
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "TP2", gross, gross - cost)
        else:
            if not hit_tp1:
                if h >= setup.sl:
                    gross = -1.0
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "SL", gross, gross - cost)
                if l <= setup.tp1:
                    hit_tp1 = True
                    if l <= setup.tp2:
                        gross = 0.75
                        return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                     setup.tp2, setup.score, "TP1+", gross, gross - cost)
            else:
                if h >= setup.entry:
                    gross = 0.75
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "BE", gross, gross - cost)
                if l <= setup.tp2:
                    gross = 2.0
                    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                                 setup.tp2, setup.score, "TP2", gross, gross - cost)

    # If TP1 was reached before timeout, the remaining half is conservatively
    # assumed to finish at breakeven.
    gross = 0.75 if hit_tp1 else 0.0
    result = "BE-TIMEOUT" if hit_tp1 else "TIMEOUT"
    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                 setup.tp2, setup.score, result, gross, gross - cost)


def backtest_pair(pair, candles, threshold, exit_model, start=60, end=None, h1_trends=None, h4_trends=None):
    if end is None:
        end = len(candles) - 1

    trades = []
    i = start
    while i < end:
        setup = build_setup(candles, i, h1_trends, h4_trends)
        if setup and setup.score >= threshold:
            trades.append(simulate(candles, i, setup, pair, exit_model, end))
            i += MAX_BARS_AHEAD
        else:
            i += 1
    return trades


def dedupe(trades):
    unique = {}
    for t in trades:
        key = (
            t.pair, t.direction,
            round(t.entry, 12),
            round(t.sl, 12),
            round(t.tp2, 12),
        )
        unique[key] = t
    return list(unique.values())


def metrics(trades):
    total = len(trades)
    if not total:
        return None

    wins = sum(t.r > 0 for t in trades)
    losses = sum(t.r < 0 for t in trades)
    timeouts = sum(t.result in ("TIMEOUT", "BE-TIMEOUT") for t in trades)
    tp1 = sum(t.result in ("TP1", "TP1+") for t in trades)
    tp2 = sum(t.result == "TP2" for t in trades)

    gross_r = sum(t.r for t in trades)
    net_r = sum(t.net_r for t in trades)

    gross_win = sum(t.r for t in trades if t.r > 0)
    gross_loss = abs(sum(t.r for t in trades if t.r < 0))
    pf = gross_win / gross_loss if gross_loss else float("inf")

    net_win = sum(t.net_r for t in trades if t.net_r > 0)
    net_loss = abs(sum(t.net_r for t in trades if t.net_r < 0))
    net_pf = net_win / net_loss if net_loss else float("inf")

    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "timeouts": timeouts,
        "tp1": tp1,
        "tp2": tp2,
        "winrate": wins / total * 100,
        "gross_r": gross_r,
        "net_r": net_r,
        "avg_r": gross_r / total,
        "net_avg_r": net_r / total,
        "pf": pf,
        "net_pf": net_pf,
        "avg_score": sum(t.score for t in trades) / total,
    }


def print_metrics(label, threshold, trades):
    m = metrics(trades)
    print("\n" + "-" * 76)
    print(f"{label} | MIN SCORE {threshold}/10")
    print("-" * 76)
    if not m:
        print("No trades found.")
        return None

    print(f"Trades        : {m['trades']}")
    print(f"Wins          : {m['wins']}")
    print(f"Losses        : {m['losses']}")
    print(f"Timeouts      : {m['timeouts']}")
    print(f"TP1           : {m['tp1']}")
    print(f"TP2           : {m['tp2']}")
    print(f"Win rate      : {m['winrate']:.2f}%")
    print(f"Gross Total R : {m['gross_r']:.2f}R")
    print(f"Net Total R   : {m['net_r']:.2f}R")
    print(f"Gross Avg R   : {m['avg_r']:.3f}R")
    print(f"Net Avg R     : {m['net_avg_r']:.3f}R")
    print(f"Gross PF      : {m['pf']:.2f}")
    print(f"Net PF        : {m['net_pf']:.2f}")
    print(f"Avg Score     : {m['avg_score']:.2f}/10")
    return m


def print_coverage(candles_by_pair):
    print("\n" + "=" * 76)
    print("DATA COVERAGE")
    print("=" * 76)
    available = 0
    for pair, candles in candles_by_pair.items():
        if candles:
            available += 1
            start = candles[0]["time"]
            end = candles[-1]["time"]
            print(f"{pair:10s} {len(candles):6d} candles | {start} -> {end}")
        else:
            print(f"{pair:10s} NO DATA")
    print(f"Available pairs: {available}/{len(PAIRS)}")


def print_compact(label, threshold, exit_model, trades):
    m = metrics(trades)
    if not m:
        print(f"{label:12s} {exit_model:11s} {threshold}/10 | no trades")
        return None
    print(
        f"{label:12s} {exit_model:11s} {threshold}/10 | "
        f"trades={m['trades']:4d} win={m['winrate']:5.2f}% "
        f"gross={m['gross_r']:7.2f}R net={m['net_r']:7.2f}R "
        f"PF={m['pf']:4.2f} netPF={m['net_pf']:4.2f}"
    )
    return m


def main():
    WORKDIR.mkdir(exist_ok=True)
    candles_by_pair = {p: [] for p in PAIRS}
    refresh_aliases_from_assetpairs()

    for label, url in ARCHIVES:
        path = WORKDIR / f"Kraken_OHLCVT_{label}.zip"
        download(url, path)
        with zipfile.ZipFile(path) as z:
            for n, pair in enumerate(PAIRS, 1):
                member = find_csv(z, pair)
                if not member:
                    candidates = nearby_csv_candidates(z, pair)
                    if candidates:
                        print(f"[{n}/20] {pair}: exact USDT 15m CSV unavailable in {label}; nearby candidates={candidates}")
                    else:
                        print(f"[{n}/20] {pair}: pair not present in {label} 15m export")
                    continue
                candles = read_candles(z, member)
                print(f"[{n}/20] {pair}: {len(candles)} candles in {label} (file={member})")
                candles_by_pair[pair].extend(candles)

    for pair in candles_by_pair:
        candles_by_pair[pair].sort(key=lambda x: x["time"])
        seen, clean = set(), []
        for c in candles_by_pair[pair]:
            if c["time"] not in seen:
                clean.append(c)
                seen.add(c["time"])
        candles_by_pair[pair] = clean

    print_coverage(candles_by_pair)

    print("\n" + "=" * 76)
    print("V7 WALK-FORWARD SMC ROBUSTNESS BACKTEST")
    print("=" * 76)
    print(f"Cost stress base: {ROUND_TRIP_COST_PCT * 100:.3f}% round trip")
    print("Split: 70% train / 30% unseen test")
    print("HTF alignment: completed 1H + 4H are soft evidence points, not hard gates")
    print("Candidates: thresholds 6/10..10/10 x 3 exit models")

    train_results = {}
    test_results = {}

    trend_cache = {}
    for pair, candles in candles_by_pair.items():
        if len(candles) >= 300:
            trend_cache[pair] = (precompute_htf_trend(candles, 1), precompute_htf_trend(candles, 4))

    for exit_model in EXIT_MODELS:
        for threshold in THRESHOLDS:
            train_all, test_all = [], []
            for pair, candles in candles_by_pair.items():
                if len(candles) < 300:
                    continue
                split = int(len(candles) * TRAIN_RATIO)
                h1_trends, h4_trends = trend_cache[pair]
                train_all.extend(backtest_pair(pair, candles, threshold, exit_model, 60, split, h1_trends, h4_trends))
                test_all.extend(backtest_pair(pair, candles, threshold, exit_model, max(60, split), len(candles) - 1, h1_trends, h4_trends))

            train_all = dedupe(train_all)
            test_all = dedupe(test_all)
            key = (exit_model, threshold)
            train_results[key] = train_all
            test_results[key] = test_all

            print_compact("TRAIN", threshold, exit_model, train_all)
            print_compact("TEST", threshold, exit_model, test_all)

    # Select on TRAIN only. There is deliberately NO fallback to a tiny or
    # weak model: if nothing passes the robustness guard, the correct result
    # is "no deployable model" rather than overfitting the backtest.
    candidates = []
    for key, trades in train_results.items():
        m = metrics(trades)
        if not m or m["trades"] < MIN_TRAIN_TRADES_FOR_SELECTION:
            continue

        # Re-price the same TRAIN trades at 0%, base and stressed round-trip
        # costs. This avoids selecting a model that only works before costs.
        stress_net = 0.0
        for t in trades:
            risk = abs(t.entry - t.sl)
            stress_cost = (t.entry * 0.0050 / risk) if risk > 0 else 0.0
            stress_net += t.r - stress_cost

        if (
            m["net_pf"] >= MIN_TRAIN_NET_PF
            and m["net_r"] > MIN_TRAIN_NET_R
            and stress_net > MIN_TRAIN_STRESS_NET_R
        ):
            # Higher stress NET R first, then base NET PF, then sample size.
            candidates.append((stress_net, m["net_pf"], m["trades"], key))

    print("\nSelection guard:")
    print(f"  minimum TRAIN trades : {MIN_TRAIN_TRADES_FOR_SELECTION}")
    print(f"  minimum TRAIN net PF : {MIN_TRAIN_NET_PF:.2f}")
    print(f"  minimum TRAIN base net R : > {MIN_TRAIN_NET_R:.2f}R")
    print(f"  minimum TRAIN stress net R : > {MIN_TRAIN_STRESS_NET_R:.2f}R")

    if not candidates:
        print("\nNO DEPLOYABLE MODEL PASSED THE TRAIN ROBUSTNESS GUARD.")
        print("The backtest will NOT promote a tiny/overfit candidate to the live bot.")
        print("Use the TEST rows above as validation evidence only.")
        return

    _, _, _, selected = max(candidates)
    selection_rule = (
        "highest TRAIN stress NET R among models with >=100 trades, "
        "base net PF >=1.05, positive base net R, and positive 0.50% stress net R"
    )

    selected_exit, selected_threshold = selected
    selected_train = train_results[selected]
    selected_test = test_results[selected]

    print("\n" + "=" * 76)
    print("V8 WALK-FORWARD SELECTION")
    print("=" * 76)
    print(f"Selected from TRAIN only: {selected_exit} @ {selected_threshold}/10")
    print(f"Rule: {selection_rule}")
    print_metrics("SELECTED TRAIN", selected_threshold, selected_train)
    print_metrics("UNSEEN TEST", selected_threshold, selected_test)

    print("\nSelected TEST by pair:")
    by_pair = {}
    for t in selected_test:
        by_pair.setdefault(t.pair, []).append(t)
    for pair in sorted(by_pair):
        m = metrics(by_pair[pair])
        print(f"  {pair:10s} trades={m['trades']:3d} win={m['winrate']:5.2f}% net={m['net_r']:7.2f}R netPF={m['net_pf']:4.2f}")

    # Cost sensitivity is applied to the same selected TEST trades; no test
    # threshold/model selection occurs here.
    print("\n" + "=" * 76)
    print("SELECTED TEST COST SENSITIVITY")
    print("=" * 76)
    for cost_label, cost_pct in (("ZERO COST", 0.0), ("BASE", ROUND_TRIP_COST_PCT), ("STRESS", 0.0050)):
        gross = sum(t.r for t in selected_test)
        net = 0.0
        for t in selected_test:
            risk = abs(t.entry - t.sl)
            cost = (t.entry * cost_pct / risk) if risk > 0 else 0.0
            net += t.r - cost
        avg = net / len(selected_test) if selected_test else 0.0
        print(f"{cost_label:10s}: cost={cost_pct*100:.3f}% RT | net={net:.2f}R | avg={avg:.3f}R")

    print("\n" + "=" * 76)
    print("V8 COMPLETE")
    print("=" * 76)


def download(url: str, path: Path):
    if path.exists() and path.stat().st_size > 0:
        print(f"Using cached {path.name} ({path.stat().st_size/1e6:.1f} MB)")
        return

    print(f"Downloading {path.name} ...")
    tmp = path.with_suffix(".part")

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", "0"))
        done = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    done += len(chunk)
                    if total:
                        print(f"\r  {done/total*100:5.1f}%", end="", flush=True)

    tmp.replace(path)
    print()


if __name__ == "__main__":
    main()
