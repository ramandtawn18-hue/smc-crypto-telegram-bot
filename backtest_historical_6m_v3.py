#!/usr/bin/env python3
"""
6-month historical SMC V3 backtest using Kraken downloadable OHLCVT archives.

V3 is designed as a validation step rather than a promise of better performance.
It keeps the V2 SMC model and adds one confirmation (displacement), then tests
multiple score thresholds in the SAME run so we can see whether stricter signals
actually improve the out-of-sample-like historical results.

Features (9 total):
1. Base 15m BOS/CHoCH structure
2. Last completed 1H trend alignment (EMA-style SMA20/SMA50 proxy)
3. Recent liquidity sweep
4. Fair Value Gap (FVG)
5. Volume expansion
6. ATR/risk sanity filter
7. Simple order-block proximity
8. Signal-candle body/close confirmation
9. Displacement confirmation (large body vs recent average)

Thresholds tested: 5/9, 6/9, 7/9.
The script reports each threshold separately. It does NOT change the live bot.

Targets: TP1=1.5R, TP2=2.5R. The historical simulator uses a conservative
single-exit model: if a candle touches SL and TP, SL is counted first; otherwise
TP2 is counted before TP1 if both are reached in the same candle. Trades are
non-overlapping and max holding time is 24h (96 x 15m candles).
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

PAIR_ALIASES = {"BTCUSDT": ["BTCUSDT", "XBTUSDT"]}
MAX_BARS_AHEAD = 96
THRESHOLDS = (5, 6, 7)
WORKDIR = Path("kraken_history_cache")


@dataclass
class Setup:
    pair: str
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


def norm(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def aliases(pair: str):
    return [norm(x) for x in PAIR_ALIASES.get(pair, [pair])]


def find_csv(z: zipfile.ZipFile, pair: str) -> Optional[str]:
    """Find 15m CSV even when Kraken stores pair name in a folder path."""
    wanted = aliases(pair)
    candidates = []
    for name in z.namelist():
        full = norm(name)
        if not full.endswith("CSV"):
            continue
        if not (full.endswith("15CSV") or full.endswith("15MCSV") or full.endswith("15MINCSV")):
            continue
        if any(a in full for a in wanted):
            candidates.append(name)

    # Prefer direct pair+15 filenames.
    for name in candidates:
        full = norm(name)
        if any(
            full.endswith(a + "15CSV")
            or full.endswith(a + "15MCSV")
            or full.endswith(a + "15MINCSV")
            for a in wanted
        ):
            return name
    return candidates[0] if candidates else None


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

    # De-duplicate quarterly boundary overlap.
    out = []
    seen = set()
    for c in candles:
        if c["time"] not in seen:
            out.append(c)
            seen.add(c["time"])
    return out


def one_hour_bars(candles):
    """Aggregate 15m candles into UTC-aligned completed 1H bars."""
    groups = {}
    for c in candles:
        bucket = (c["time"] // 3600) * 3600
        groups.setdefault(bucket, []).append(c)

    bars = []
    for bucket in sorted(groups):
        xs = sorted(groups[bucket], key=lambda x: x["time"])
        # Only a complete 4-candle hour is accepted.
        if len(xs) != 4:
            continue
        bars.append({
            "time": bucket + 45 * 60,
            "open": xs[0]["open"],
            "high": max(x["high"] for x in xs),
            "low": min(x["low"] for x in xs),
            "close": xs[-1]["close"],
            "volume": sum(x["volume"] for x in xs),
        })
    return bars


def h1_trend(candles, i):
    """Use only the last COMPLETED UTC hour available before signal candle."""
    if i < 220:
        return None
    current_time = candles[i]["time"]
    hours = one_hour_bars(candles[: i + 1])
    completed = [h for h in hours if h["time"] < current_time]
    if len(completed) < 50:
        return None
    closes = [h["close"] for h in completed]
    sma20 = sum(closes[-20:]) / 20
    sma50 = sum(closes[-50:]) / 50
    last = closes[-1]
    if last > sma20 and sma20 > sma50:
        return "LONG"
    if last < sma20 and sma20 < sma50:
        return "SHORT"
    return "NEUTRAL"


def atr(candles, i, n=14):
    if i < n:
        return None
    trs = []
    for k in range(i - n + 1, i + 1):
        prev_close = candles[k - 1]["close"] if k > 0 else candles[k]["open"]
        h, l = candles[k]["high"], candles[k]["low"]
        trs.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
    return sum(trs) / n


def structure(candles, i):
    if i < 41:
        return None
    highs = [x["high"] for x in candles]
    lows = [x["low"] for x in candles]
    current = candles[i]["close"]
    recent_high = max(highs[i - 20:i])
    recent_low = min(lows[i - 20:i])
    previous_high = max(highs[i - 40:i - 20])
    previous_low = min(lows[i - 40:i - 20])

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
    """Large directional body relative to the previous 20 candle bodies."""
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


def build_setup(candles, i):
    s = structure(candles, i)
    if not s:
        return None
    direction, _, recent_high, recent_low = s
    entry = candles[i]["close"]
    sl = recent_low if direction == "LONG" else recent_high
    risk = entry - sl if direction == "LONG" else sl - entry
    if risk <= 0:
        return None

    checks = [
        h1_trend(candles, i) == direction,
        liquidity_sweep(candles, i, direction),
        fvg(candles, i, direction),
        volume_confirm(candles, i),
        atr_confirm(candles, i, entry, sl),
        order_block(candles, i, direction),
        candle_confirm(candles, i, direction),
        displacement(candles, i, direction),
    ]
    score = 1 + sum(checks)  # 1 structure + 8 confirmations = 9

    if direction == "LONG":
        tp1, tp2 = entry + 1.5 * risk, entry + 2.5 * risk
    else:
        tp1, tp2 = entry - 1.5 * risk, entry - 2.5 * risk
    return Setup("", direction, entry, sl, tp1, tp2, score)


def simulate(candles, i, setup, pair):
    end = min(len(candles), i + 1 + MAX_BARS_AHEAD)
    for j in range(i + 1, end):
        h, l = candles[j]["high"], candles[j]["low"]
        if setup.direction == "LONG":
            if l <= setup.sl:
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1, setup.tp2, setup.score, "SL", -1.0)
            if h >= setup.tp2:
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1, setup.tp2, setup.score, "TP2", 2.5)
            if h >= setup.tp1:
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1, setup.tp2, setup.score, "TP1", 1.5)
        else:
            if h >= setup.sl:
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1, setup.tp2, setup.score, "SL", -1.0)
            if l <= setup.tp2:
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1, setup.tp2, setup.score, "TP2", 2.5)
            if l <= setup.tp1:
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1, setup.tp2, setup.score, "TP1", 1.5)
    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1, setup.tp2, setup.score)


def backtest_pair(pair, candles, threshold):
    trades = []
    i = 60
    while i < len(candles) - 1:
        setup = build_setup(candles, i)
        if setup and setup.score >= threshold:
            trades.append(simulate(candles, i, setup, pair))
            i += MAX_BARS_AHEAD
        else:
            i += 1
    return trades


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


def dedupe(trades):
    unique = {}
    for t in trades:
        key = (t.pair, t.direction, round(t.entry, 12), round(t.sl, 12), round(t.tp2, 12))
        unique[key] = t
    return list(unique.values())


def metrics(trades):
    total = len(trades)
    if not total:
        return None
    wins = sum(t.result in ("TP1", "TP2") for t in trades)
    losses = sum(t.result == "SL" for t in trades)
    timeouts = sum(t.result == "TIMEOUT" for t in trades)
    tp1 = sum(t.result == "TP1" for t in trades)
    tp2 = sum(t.result == "TP2" for t in trades)
    total_r = sum(t.r for t in trades)
    gross_win = sum(t.r for t in trades if t.r > 0)
    gross_loss = abs(sum(t.r for t in trades if t.r < 0))
    pf = gross_win / gross_loss if gross_loss else float("inf")
    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "timeouts": timeouts,
        "tp1": tp1,
        "tp2": tp2,
        "winrate": wins / total * 100,
        "total_r": total_r,
        "avg_r": total_r / total,
        "pf": pf,
        "avg_score": sum(t.score for t in trades) / total,
    }


def report(threshold, trades):
    m = metrics(trades)
    print("\n" + "=" * 76)
    print(f"V3 RESULTS — MIN SCORE {threshold}/9")
    print("=" * 76)
    if not m:
        print("No trades found.")
        return
    print(f"Trades     : {m['trades']}")
    print(f"Wins       : {m['wins']}")
    print(f"Losses     : {m['losses']}")
    print(f"Timeouts   : {m['timeouts']}")
    print(f"TP1        : {m['tp1']}")
    print(f"TP2        : {m['tp2']}")
    print(f"Win rate   : {m['winrate']:.2f}%")
    print(f"Total R    : {m['total_r']:.2f}R")
    print(f"Average R  : {m['avg_r']:.3f}R")
    print(f"Profit Fac.: {m['pf']:.2f}")
    print(f"Avg Score  : {m['avg_score']:.2f}/9")
    print("\nBy pair:")
    for pair in sorted(set(t.pair for t in trades)):
        ts = [t for t in trades if t.pair == pair]
        w = sum(t.result in ("TP1", "TP2") for t in ts)
        r = sum(t.r for t in ts)
        print(f"  {pair:9s} trades={len(ts):4d} wins={w:4d} winrate={w/len(ts)*100:6.2f}% totalR={r:7.2f}")


def main():
    WORKDIR.mkdir(exist_ok=True)
    candles_by_pair = {p: [] for p in PAIRS}

    # Merge both quarterly archives per pair before backtesting so signals can
    # naturally cross the quarter boundary.
    for label, url in ARCHIVES:
        path = WORKDIR / f"Kraken_OHLCVT_{label}.zip"
        download(url, path)
        with zipfile.ZipFile(path) as z:
            for n, pair in enumerate(PAIRS, 1):
                member = find_csv(z, pair)
                if not member:
                    print(f"[{n}/20] {pair}: CSV not found in {label}")
                    continue
                candles = read_candles(z, member)
                print(f"[{n}/20] {pair}: {len(candles)} candles in {label}")
                candles_by_pair[pair].extend(candles)

    for pair in candles_by_pair:
        candles_by_pair[pair].sort(key=lambda x: x["time"])
        seen = set()
        clean = []
        for c in candles_by_pair[pair]:
            if c["time"] not in seen:
                clean.append(c)
                seen.add(c["time"])
        candles_by_pair[pair] = clean

    for threshold in THRESHOLDS:
        all_trades = []
        for pair, candles in candles_by_pair.items():
            if candles:
                all_trades.extend(backtest_pair(pair, candles, threshold))
        report(threshold, dedupe(all_trades))


if __name__ == "__main__":
    main()
