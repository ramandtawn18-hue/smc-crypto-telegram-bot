#!/usr/bin/env python3
"""
6-month historical SMC V4 backtest using Kraken downloadable OHLCVT archives.

V5 is a research-only backtest. It does NOT change the live Telegram bot.

Compared with V4/V3:
- Keeps the 10-point V4 score, including completed 4H trend alignment.
- Fixes Kraken legacy asset-code matching (e.g. DOGE=XDG/XXDG, ETC=XETC, XLM=XXLM).
- Adds a hard 1H+4H trend-alignment gate to reduce counter-trend signals.
- Tests thresholds 7/10, 8/10, 9/10, 10/10.
- Uses a chronological 70/30 walk-forward split:
  * train = first 70% of each pair's history
  * test  = final 30%
  * the script reports all thresholds on both periods
  * it also selects the best training threshold by NET R and evaluates that
    fixed threshold on the unseen test period.
- Reports gross R and a conservative cost-stress NET R.
- Cost assumption is configurable: 0.10% fee + 0.025% slippage per side.
- No lookahead: 1H/4H trend uses only completed higher-timeframe candles.
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
THRESHOLDS = (7, 8, 9, 10)
TRAIN_RATIO = 0.70

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
    """Find a pair's 15m CSV using modern + legacy Kraken asset codes.

    Kraken history exports may use legacy asset codes such as XDG/XXDG
    for DOGE, XETC for ETC and XXLM for XLM.  The archive may also put
    the interval in a folder (for example PAIR/15.csv), so matching is
    done against the full archive member path, not just the basename.
    """
    wanted = aliases(pair)
    candidates = []

    for name in z.namelist():
        if not _is_15m_csv(name):
            continue
        full = norm(name)

        # Exact pair+15m suffix is the safest match.
        if any(
            full.endswith(a + "15CSV")
            or full.endswith(a + "15MCSV")
            or full.endswith(a + "15MINCSV")
            for a in wanted
        ):
            return name

        # Folder layout such as XDGUSDT/15.csv or XDG/USDT/15.csv.
        if any(a in full for a in wanted):
            candidates.append(name)

    if candidates:
        return candidates[0]

    # Last-resort pair-component matching. This handles exports where the
    # pair identifier is separated by folders or punctuation in unusual ways.
    base, quote = pair[:-4], pair[-4:]
    bases = LEGACY_BASE_ALIASES.get(base, [base])
    quotes = QUOTE_ALIASES.get(quote, [quote])

    for name in z.namelist():
        if not _is_15m_csv(name):
            continue
        full = norm(name)
        if any(norm(b) in full for b in bases) and any(norm(q) in full for q in quotes):
            return name

    return None

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


def completed_htf_trend(candles, i, hours_per_bar, fast=20, slow=50):
    """Higher-timeframe trend using only completed HTF bars before candle i."""
    current_time = candles[i]["time"]
    bars = aggregate_bars(candles[: i + 1], 3600 * hours_per_bar)
    completed = [b for b in bars if b["time"] < current_time]
    if len(completed) < slow:
        return None
    closes = [b["close"] for b in completed]
    sma_fast = sum(closes[-fast:]) / fast
    sma_slow = sum(closes[-slow:]) / slow
    last = closes[-1]
    if last > sma_fast and sma_fast > sma_slow:
        return "LONG"
    if last < sma_fast and sma_fast < sma_slow:
        return "SHORT"
    return "NEUTRAL"


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

    h1 = completed_htf_trend(candles, i, 1)
    h4 = completed_htf_trend(candles, i, 4)

    # V5 hard gate: only trade when both completed 1H and 4H trends
    # agree with the 15m structure direction. This is deliberately a gate,
    # not an extra score point, so the score measures the remaining evidence.
    if h1 != direction or h4 != direction:
        return None

    checks = [
        True,  # 1H trend gate already satisfied
        liquidity_sweep(candles, i, direction),
        fvg(candles, i, direction),
        volume_confirm(candles, i),
        atr_confirm(candles, i, entry, sl),
        order_block(candles, i, direction),
        candle_confirm(candles, i, direction),
        displacement(candles, i, direction),
        True,  # 4H trend gate already satisfied
    ]

    score = 1 + sum(checks)  # structure + 9 confirmations = 10

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


def simulate(candles, i, setup, pair, max_end=None):
    end_limit = len(candles) if max_end is None else min(len(candles), max_end)
    end = min(end_limit, i + 1 + MAX_BARS_AHEAD)

    for j in range(i + 1, end):
        h, l = candles[j]["high"], candles[j]["low"]

        if setup.direction == "LONG":
            if l <= setup.sl:
                gross = -1.0
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                             setup.tp2, setup.score, "SL", gross,
                             gross - cost_r(setup.entry, setup.sl))
            if h >= setup.tp2:
                gross = 2.5
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                             setup.tp2, setup.score, "TP2", gross,
                             gross - cost_r(setup.entry, setup.sl))
            if h >= setup.tp1:
                gross = 1.5
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                             setup.tp2, setup.score, "TP1", gross,
                             gross - cost_r(setup.entry, setup.sl))
        else:
            if h >= setup.sl:
                gross = -1.0
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                             setup.tp2, setup.score, "SL", gross,
                             gross - cost_r(setup.entry, setup.sl))
            if l <= setup.tp2:
                gross = 2.5
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                             setup.tp2, setup.score, "TP2", gross,
                             gross - cost_r(setup.entry, setup.sl))
            if l <= setup.tp1:
                gross = 1.5
                return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                             setup.tp2, setup.score, "TP1", gross,
                             gross - cost_r(setup.entry, setup.sl))

    return Trade(pair, setup.direction, setup.entry, setup.sl, setup.tp1,
                 setup.tp2, setup.score, "TIMEOUT", 0.0,
                 -cost_r(setup.entry, setup.sl))


def backtest_pair(pair, candles, threshold, start=60, end=None):
    if end is None:
        end = len(candles) - 1

    trades = []
    i = start
    while i < end:
        setup = build_setup(candles, i)
        if setup and setup.score >= threshold:
            trades.append(simulate(candles, i, setup, pair, end))
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

    wins = sum(t.result in ("TP1", "TP2") for t in trades)
    losses = sum(t.result == "SL" for t in trades)
    timeouts = sum(t.result == "TIMEOUT" for t in trades)
    tp1 = sum(t.result == "TP1" for t in trades)
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


def main():
    WORKDIR.mkdir(exist_ok=True)
    candles_by_pair = {p: [] for p in PAIRS}

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

    print("\n" + "=" * 76)
    print("V4 WALK-FORWARD SMC BACKTEST")
    print("=" * 76)
    print(f"Cost stress: {ROUND_TRIP_COST_PCT * 100:.3f}% round trip "
          f"({FEE_PER_SIDE_PCT * 100:.3f}% fee + "
          f"{SLIPPAGE_PER_SIDE_PCT * 100:.3f}% slippage per side)")
    print("Split: 70% train / 30% unseen test")
    print("V5: hard 1H + 4H trend gate; Kraken legacy-symbol CSV matching")

    train_results = {}
    test_results = {}

    for threshold in THRESHOLDS:
        train_all, test_all = [], []

        for pair, candles in candles_by_pair.items():
            if len(candles) < 300:
                continue

            split = int(len(candles) * TRAIN_RATIO)

            # Keep enough warm-up candles on both sides.
            train_all.extend(backtest_pair(pair, candles, threshold, 60, split))
            test_all.extend(backtest_pair(pair, candles, threshold, max(60, split), len(candles) - 1))

        train_all = dedupe(train_all)
        test_all = dedupe(test_all)

        train_results[threshold] = train_all
        test_results[threshold] = test_all

        print_metrics("TRAIN", threshold, train_all)
        print_metrics("TEST ", threshold, test_all)

    scored = []
    for threshold, trades in train_results.items():
        m = metrics(trades)
        if m:
            scored.append((m["net_r"], threshold))

    if not scored:
        print("\nNo usable training results.")
        return

    selected_net_r, selected_threshold = max(scored)
    selected_test = test_results[selected_threshold]

    print("\n" + "=" * 76)
    print("WALK-FORWARD SELECTION")
    print("=" * 76)
    print(f"Selected threshold from TRAIN only: {selected_threshold}/10")
    print(f"Training NET R at selected threshold: {selected_net_r:.2f}R")
    print("The selected threshold is now evaluated unchanged on unseen TEST data.")

    print_metrics("UNSEEN TEST", selected_threshold, selected_test)

    print("\n" + "=" * 76)
    print("V5 COMPLETE")
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
