#!/usr/bin/env python3
"""
6-month historical SMC backtest using Kraken's downloadable OHLCVT archives.

Uses Kraken 15-minute CSV candles from:
  2026 Q1 (Jan-Mar)
  2026 Q2 (Apr-Jun)

It mirrors the current baseline strategy:
- 20/40-candle BOS/CHoCH structure
- entry at signal candle close
- SL at recent swing
- TP1 = 1.5R, TP2 = 2.5R
- max holding time = 24h (96 candles)
- same-candle SL is checked before TP
- trades are non-overlapping

Only standard-library Python plus requests is required.
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
INTERVAL = 15
MIN_CANDLES = 50
MAX_BARS_AHEAD = 96
WORKDIR = Path("kraken_history_cache")


@dataclass
class Trade:
    pair: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
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
        base = norm(Path(name).name)
        if not base.endswith("15CSV"):
            continue
        stem = base[:-6]
        if any(a in stem for a in wanted):
            candidates.append(name)

    for name in candidates:
        base = norm(Path(name).name)
        if any(base == a + "15CSV" for a in wanted):
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
    return candles


def analyze_at(candles, i):
    if i < 41:
        return None

    close = [x["close"] for x in candles]
    high = [x["high"] for x in candles]
    low = [x["low"] for x in candles]

    current = close[i]
    recent_high = max(high[i - 20:i])
    recent_low = min(low[i - 20:i])
    previous_high = max(high[i - 40:i - 20])
    previous_low = min(low[i - 40:i - 20])

    if current > recent_high:
        direction = "LONG"
    elif current < recent_low:
        direction = "SHORT"
    elif recent_high > previous_high and recent_low > previous_low:
        direction = "LONG"
    elif recent_high < previous_high and recent_low < previous_low:
        direction = "SHORT"
    else:
        return None

    entry = current
    if direction == "LONG":
        sl = recent_low
        risk = entry - sl
        if risk <= 0:
            return None
        tp1, tp2 = entry + 1.5 * risk, entry + 2.5 * risk
    else:
        sl = recent_high
        risk = sl - entry
        if risk <= 0:
            return None
        tp1, tp2 = entry - 1.5 * risk, entry - 2.5 * risk

    return direction, entry, sl, tp1, tp2


def simulate(candles, i, signal, pair):
    direction, entry, sl, tp1, tp2 = signal
    end = min(len(candles), i + 1 + MAX_BARS_AHEAD)

    for j in range(i + 1, end):
        h, l = candles[j]["high"], candles[j]["low"]

        if direction == "LONG":
            if l <= sl:
                return Trade(pair, direction, entry, sl, tp1, tp2, "SL", -1.0)
            if h >= tp2:
                return Trade(pair, direction, entry, sl, tp1, tp2, "TP2", 2.5)
            if h >= tp1:
                return Trade(pair, direction, entry, sl, tp1, tp2, "TP1", 1.5)
        else:
            if h >= sl:
                return Trade(pair, direction, entry, sl, tp1, tp2, "SL", -1.0)
            if l <= tp2:
                return Trade(pair, direction, entry, sl, tp1, tp2, "TP2", 2.5)
            if l <= tp1:
                return Trade(pair, direction, entry, sl, tp1, tp2, "TP1", 1.5)

    return Trade(pair, direction, entry, sl, tp1, tp2)


def backtest_pair(pair, candles):
    trades = []
    i = 41
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
    print("6-MONTH HISTORICAL SMC BACKTEST")
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

    print("\nBy pair:")
    for pair in sorted(set(t.pair for t in trades)):
        pt = [t for t in trades if t.pair == pair]
        pw = sum(t.result in ("TP1", "TP2") for t in pt)
        pr = sum(t.r for t in pt)
        print(f"  {pair:12s} trades={len(pt):4d} wins={pw:4d} "
              f"winrate={pw/len(pt)*100:6.2f}% totalR={pr:8.2f}")


def main():
    WORKDIR.mkdir(exist_ok=True)
    all_trades = []

    for label, url in ARCHIVES:
        path = WORKDIR / f"Kraken_OHLCVT_{label}.zip"
        download(url, path)

        print(f"\nProcessing {label}...")
        with zipfile.ZipFile(path) as z:
            for n, pair in enumerate(PAIRS, 1):
                member = find_csv(z, pair)
                if not member:
                    print(f"[{n}/{len(PAIRS)}] {pair}: CSV not found")
                    continue

                candles = read_candles(z, member)
                print(f"[{n}/{len(PAIRS)}] {pair}: {len(candles)} candles")
                if len(candles) >= MIN_CANDLES:
                    all_trades.extend(backtest_pair(pair, candles))

    # Avoid duplicate boundary signals if archives overlap.
    seen = set()
    unique = []
    for t in all_trades:
        key = (t.pair, t.direction, t.entry, t.sl, t.tp1, t.tp2)
        if key not in seen:
            seen.add(key)
            unique.append(t)

    report(unique)


if __name__ == "__main__":
    main()
