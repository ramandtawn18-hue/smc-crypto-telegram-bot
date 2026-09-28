#!/usr/bin/env python3
"""
Baseline SMC backtest for the Telegram bot.

- Kraken public OHLC endpoint
- 15m candles
- Mirrors the live bot's BOS/CHoCH logic
- Entry at signal candle close
- SL at recent swing
- TP1 = 1.5R, TP2 = 2.5R
- Max holding time = 24h (96 candles)
- Conservative same-candle rule: SL is counted before TP
"""

import argparse
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import requests

KRAKEN_API = "https://api.kraken.com/0/public"
INTERVAL = 15
MIN_CANDLES = 50
MAX_BARS_AHEAD = 96
DEFAULT_PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "LTCUSDT",
    "BCHUSDT", "ATOMUSDT", "UNIUSDT", "AAVEUSDT", "NEARUSDT",
    "ETCUSDT", "FILUSDT", "ALGOUSDT", "XLMUSDT", "TRXUSDT",
]


@dataclass
class Trade:
    pair: str
    direction: str
    index: int
    entry: float
    sl: float
    tp1: float
    tp2: float
    result: str = "TIMEOUT"
    r: float = 0.0


def kraken_get(path: str, params=None):
    r = requests.get(KRAKEN_API + path, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(data["error"])
    return data["result"]


def resolve_pair(pair: str, pairs: dict) -> Optional[str]:
    target = pair.upper().replace("/", "")
    for key, meta in pairs.items():
        alt = str(meta.get("altname", "")).upper().replace("/", "")
        ws = str(meta.get("wsname", "")).upper().replace("/", "")
        if target in (alt, ws, key.upper().replace("/", "")):
            return key
    return None


def get_pair_map():
    result = kraken_get("/AssetPairs")
    return result


def get_candles(pair: str, pair_map: dict):
    key = resolve_pair(pair, pair_map)
    if not key:
        return []

    result = kraken_get("/OHLC", {"pair": key, "interval": INTERVAL})
    rows = result.get(key, [])
    candles = []
    for row in rows:
        candles.append({
            "time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[6]),
        })
    return candles


def analyze_at(candles: List[dict], i: int):
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

    direction = None
    structure = None

    if current > recent_high:
        direction = "LONG"
        structure = "BULLISH BOS"
    elif current < recent_low:
        direction = "SHORT"
        structure = "BEARISH BOS"
    elif recent_high > previous_high and recent_low > previous_low:
        direction = "LONG"
        structure = "BULLISH CHoCH"
    elif recent_high < previous_high and recent_low < previous_low:
        direction = "SHORT"
        structure = "BEARISH CHoCH"
    else:
        return None

    entry = current

    if direction == "LONG":
        sl = recent_low
        risk = entry - sl
        if risk <= 0:
            return None
        tp1 = entry + 1.5 * risk
        tp2 = entry + 2.5 * risk
    else:
        sl = recent_high
        risk = sl - entry
        if risk <= 0:
            return None
        tp1 = entry - 1.5 * risk
        tp2 = entry - 2.5 * risk

    return {
        "direction": direction,
        "structure": structure,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "time": candles[i]["time"],
    }


def simulate_trade(candles: List[dict], i: int, signal: dict) -> Trade:
    t = Trade(
        pair="",
        direction=signal["direction"],
        index=i,
        entry=signal["entry"],
        sl=signal["sl"],
        tp1=signal["tp1"],
        tp2=signal["tp2"],
    )

    risk = abs(t.entry - t.sl)

    end = min(len(candles), i + 1 + MAX_BARS_AHEAD)

    for j in range(i + 1, end):
        h = candles[j]["high"]
        l = candles[j]["low"]

        if t.direction == "LONG":
            # Conservative: if both happen in one candle, count SL first.
            if l <= t.sl:
                t.result = "SL"
                t.r = -1.0
                return t
            if h >= t.tp2:
                t.result = "TP2"
                t.r = 2.5
                return t
            if h >= t.tp1:
                t.result = "TP1"
                t.r = 1.5
                return t
        else:
            if h >= t.sl:
                t.result = "SL"
                t.r = -1.0
                return t
            if l <= t.tp2:
                t.result = "TP2"
                t.r = 2.5
                return t
            if l <= t.tp1:
                t.result = "TP1"
                t.r = 1.5
                return t

    # Mark timeout as 0R because no exit rule was hit.
    t.result = "TIMEOUT"
    t.r = 0.0
    return t


def backtest_pair(pair: str, candles: List[dict]) -> List[Trade]:
    trades = []
    i = 41

    while i < len(candles) - 1:
        signal = analyze_at(candles, i)
        if signal:
            trade = simulate_trade(candles, i, signal)
            trade.pair = pair
            trades.append(trade)

            # Keep trades non-overlapping.
            if trade.result != "TIMEOUT":
                i += MAX_BARS_AHEAD
            else:
                i += MAX_BARS_AHEAD
        else:
            i += 1

    return trades


def print_report(trades: List[Trade]):
    print("\n" + "=" * 70)
    print("BASELINE SMC BACKTEST")
    print("=" * 70)

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
    pairs = sorted(set(t.pair for t in trades))
    for pair in pairs:
        pt = [t for t in trades if t.pair == pair]
        pwin = sum(t.result in ("TP1", "TP2") for t in pt)
        pr = sum(t.r for t in pt)
        print(f"  {pair:12s} trades={len(pt):3d} wins={pwin:3d} winrate={pwin/len(pt)*100:6.2f}% totalR={pr:7.2f}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pairs",
        nargs="*",
        default=DEFAULT_PAIRS,
        help="USDT pairs, e.g. BTCUSDT ETHUSDT SOLUSDT",
    )
    args = parser.parse_args()

    pair_map = get_pair_map()
    all_trades = []

    print("Downloading Kraken 15m candles...")
    print(f"Pairs: {len(args.pairs)}")

    for n, pair in enumerate(args.pairs, 1):
        try:
            candles = get_candles(pair, pair_map)
            print(f"[{n}/{len(args.pairs)}] {pair}: {len(candles)} candles")

            if len(candles) >= MIN_CANDLES:
                trades = backtest_pair(pair, candles)
                all_trades.extend(trades)

            time.sleep(0.25)
        except Exception as e:
            print(f"  ERROR {pair}: {e}")

    print_report(all_trades)


if __name__ == "__main__":
    main()
