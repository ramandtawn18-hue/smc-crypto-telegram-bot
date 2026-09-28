#!/usr/bin/env python3
"""
6-month historical SMC V2 backtest using Kraken downloadable OHLCVT archives.

V2 keeps the original 15m BOS/CHoCH entry/SL/TP model and adds confirmations:
- 1H trend alignment
- liquidity sweep in the preceding 3 candles
- 3-candle Fair Value Gap (FVG)
- volume confirmation
- ATR/volatility filter
- simple order-block proximity
- signal-candle body/close confirmation

A signal is accepted only when SCORE >= MIN_SCORE (default 5/8).
All confirmation checks use information available at or before the signal candle;
the 1H trend uses the last COMPLETED 1H candle to avoid lookahead.

Targets remain TP1=1.5R and TP2=2.5R, with a 24h maximum holding period and
conservative same-candle SL-first handling.
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
MIN_SCORE = 5
WORKDIR = Path("kraken_history_cache")


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
    for name in candidates:
        full = norm(name)
        if any(full.endswith(a + "15CSV") or full.endswith(a + "15MCSV") or full.endswith(a + "15MINCSV") for a in wanted):
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
    # De-duplicate timestamps in case quarterly archives overlap at a boundary.
    out = []
    seen = set()
    for c in candles:
        if c["time"] not in seen:
            out.append(c)
            seen.add(c["time"])
    return out


def sma(values, n):
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def atr(candles, i, n=14):
    if i < n:
        return None
    trs = []
    for k in range(i - n + 1, i + 1):
        prev_close = candles[k - 1]["close"] if k > 0 else candles[k]["open"]
        h, l = candles[k]["high"], candles[k]["low"]
        trs.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
    return sum(trs) / n


def one_hour_bars(candles):
    """Aggregate 4 consecutive 15m candles into completed 1H bars."""
    bars = []
    bucket = []
    for c in candles:
        bucket.append(c)
        if len(bucket) == 4:
            bars.append({
                "time": bucket[-1]["time"],
                "open": bucket[0]["open"],
                "high": max(x["high"] for x in bucket),
                "low": min(x["low"] for x in bucket),
                "close": bucket[-1]["close"],
                "volume": sum(x["volume"] for x in bucket),
            })
            bucket = []
    return bars


def h1_trend(candles, i):
    """Return 1H trend using only the last completed 1H candle."""
    completed = i // 4 - 1
    if completed < 50:
        return None
    hbars = one_hour_bars(candles[: i + 1])
    if len(hbars) < 51:
        return None
    # Last completed hourly bar is hbars[-2] because the current 15m candle may
    # belong to the still-forming hour.
    closes = [b["close"] for b in hbars[:-1]]
    if len(closes) < 50:
        return None
    e20 = sum(closes[-20:]) / 20
    e50 = sum(closes[-50:]) / 50
    last = closes[-1]
    if last > e20 and e20 > e50:
        return "LONG"
    if last < e20 and e20 < e50:
        return "SHORT"
    return "NEUTRAL"


def structure(candles, i):
    if i < 41:
        return None
    high = [x["high"] for x in candles]
    low = [x["low"] for x in candles]
    current = candles[i]["close"]
    recent_high = max(high[i - 20:i])
    recent_low = min(low[i - 20:i])
    previous_high = max(high[i - 40:i - 20])
    previous_low = min(low[i - 40:i - 20])
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
    """Sweep of a prior 20-candle level during one of the 3 candles before i."""
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
    """Detect a recent 3-candle imbalance ending before signal candle."""
    if i < 4:
        return False
    for j in range(max(2, i - 4), i):
        a, b, c = candles[j - 2], candles[j - 1], candles[j]
        if direction == "LONG" and c["low"] > a["high"]:
            # Signal price should be at/near the imbalance rather than far away.
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
    """Simple non-lookahead OB: last opposite candle in prior 5 bars."""
    if i < 7:
        return False
    price = candles[i]["close"]
    for j in range(i - 1, max(1, i - 6), -1):
        c = candles[j]
        body = abs(c["close"] - c["open"])
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        opposite = (direction == "LONG" and c["close"] < c["open"]) or (direction == "SHORT" and c["close"] > c["open"])
        if not opposite:
            continue
        # Current price within 1.5% of the candle's body/zone.
        zone_low = min(c["open"], c["close"])
        zone_high = max(c["open"], c["close"])
        distance = 0.0 if zone_low <= price <= zone_high else min(abs(price-zone_low), abs(price-zone_high))
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


def analyze_at(candles, i):
    s = structure(candles, i)
    if not s:
        return None
    direction, kind, recent_high, recent_low = s
    entry = candles[i]["close"]
    sl = recent_low if direction == "LONG" else recent_high
    risk = entry - sl if direction == "LONG" else sl - entry
    if risk <= 0:
        return None

    htrend = h1_trend(candles, i)
    score = 1  # base structure signal
    details = {"structure": True}

    if htrend == direction:
        score += 1
        details["h1"] = True
    else:
        details["h1"] = False

    details["sweep"] = liquidity_sweep(candles, i, direction)
    score += int(details["sweep"])
    details["fvg"] = fvg(candles, i, direction)
    score += int(details["fvg"])
    details["volume"] = volume_confirm(candles, i)
    score += int(details["volume"])
    details["atr"] = atr_confirm(candles, i, entry, sl)
    score += int(details["atr"])
    details["ob"] = order_block(candles, i, direction)
    score += int(details["ob"])
    details["candle"] = candle_confirm(candles, i, direction)
    score += int(details["candle"])

    if score < MIN_SCORE:
        return None

    if direction == "LONG":
        tp1, tp2 = entry + 1.5 * risk, entry + 2.5 * risk
    else:
        tp1, tp2 = entry - 1.5 * risk, entry - 2.5 * risk
    return direction, entry, sl, tp1, tp2, score


def simulate(candles, i, signal, pair):
    direction, entry, sl, tp1, tp2, score = signal
    end = min(len(candles), i + 1 + MAX_BARS_AHEAD)
    for j in range(i + 1, end):
        h, l = candles[j]["high"], candles[j]["low"]
        if direction == "LONG":
            if l <= sl:
                return Trade(pair, direction, entry, sl, tp1, tp2, score, "SL", -1.0)
            if h >= tp2:
                return Trade(pair, direction, entry, sl, tp1, tp2, score, "TP2", 2.5)
            if h >= tp1:
                return Trade(pair, direction, entry, sl, tp1, tp2, score, "TP1", 1.5)
        else:
            if h >= sl:
                return Trade(pair, direction, entry, sl, tp1, tp2, score, "SL", -1.0)
            if l <= tp2:
                return Trade(pair, direction, entry, sl, tp1, tp2, score, "TP2", 2.5)
            if l <= tp1:
                return Trade(pair, direction, entry, sl, tp1, tp2, score, "TP1", 1.5)
    return Trade(pair, direction, entry, sl, tp1, tp2, score)


def backtest_pair(pair, candles):
    trades = []
    i = 60
    while i < len(candles) - 1:
        signal = analyze_at(candles, i)
        if signal:
            trades.append(simulate(candles, i, signal, pair))
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


def report(trades):
    print("\n" + "=" * 72)
    print("6-MONTH HISTORICAL SMC V2 BACKTEST")
    print(f"MIN SCORE: {MIN_SCORE}/8")
    print("=" * 72)
    if not trades:
        print("No trades found.")
        return
    total = len(trades)
    wins = sum(t.result in ("TP1", "TP2") for t in trades)
    losses = sum(t.result == "SL" for t in trades)
    timeouts = sum(t.result == "TIMEOUT" for t in trades)
    tp1 = sum(t.result == "TP1" for t in trades)
    tp2 = sum(t.result == "TP2" for t in trades)
    total_r = sum(t.r for t in trades)
    avg_r = total_r / total
    win_rate = wins / total * 100
    gross_win = sum(t.r for t in trades if t.r > 0)
    gross_loss = abs(sum(t.r for t in trades if t.r < 0))
    pf = gross_win / gross_loss if gross_loss else float("inf")
    avg_score = sum(t.score for t in trades) / total
    print(f"Trades     : {total}")
    print(f"Wins       : {wins}")
    print(f"Losses     : {losses}")
    print(f"Timeouts   : {timeouts}")
    print(f"TP1        : {tp1}")
    print(f"TP2        : {tp2}")
    print(f"Win rate   : {win_rate:.2f}%")
    print(f"Total R    : {total_r:.2f}R")
    print(f"Average R  : {avg_r:.3f}R")
    print(f"Profit Fac.: {pf:.2f}")
    print(f"Avg Score  : {avg_score:.2f}/8")
    print("\nBy pair:")
    for pair in sorted(set(t.pair for t in trades)):
        ts = [t for t in trades if t.pair == pair]
        w = sum(t.result in ("TP1", "TP2") for t in ts)
        r = sum(t.r for t in ts)
        print(f"  {pair:9s} trades={len(ts):4d} wins={w:4d} winrate={w/len(ts)*100:6.2f}% totalR={r:7.2f}")


def main():
    WORKDIR.mkdir(exist_ok=True)
    all_trades = []
    for label, url in ARCHIVES:
        path = WORKDIR / f"Kraken_OHLCVT_{label}.zip"
        download(url, path)
        with zipfile.ZipFile(path) as z:
            for n, pair in enumerate(PAIRS, 1):
                member = find_csv(z, pair)
                if not member:
                    print(f"[{n}/20] {pair}: CSV not found")
                    continue
                candles = read_candles(z, member)
                print(f"[{n}/20] {pair}: {len(candles)} candles")
                all_trades.extend(backtest_pair(pair, candles))
    # Remove duplicate trades caused by quarter boundary overlap.
    unique = {}
    for t in all_trades:
        key = (t.pair, t.direction, round(t.entry, 12), round(t.sl, 12), round(t.tp2, 12))
        unique[key] = t
    report(list(unique.values()))


if __name__ == "__main__":
    main()
