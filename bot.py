import os
import io
import time
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify

# ============================================================
# SAIWAN — 1H Trendline Breakout / Retest Signal Bot
# Bitget USDT Perpetual + Telegram
#
# Signal logic:
#   converging trendlines -> closed-candle breakout -> retest
#   -> confirmation -> structural SL -> TP1/TP2/TP3
#
# The TP1/TP2/TP3 tracking system is intentionally kept.
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BASE = "https://api.bitget.com"
PRODUCT = "USDT-FUTURES"
TF = "1H"

SCAN_SECONDS = max(30, int(os.getenv("SCAN_SECONDS", "60")))
CANDLE_LIMIT = min(1000, max(220, int(os.getenv("CANDLE_LIMIT", "420"))))
PIVOT_WINDOW = max(2, int(os.getenv("PIVOT_WINDOW", "4")))
ATR_PERIOD = max(10, int(os.getenv("ATR_PERIOD", "14")))
BREAKOUT_ATR = float(os.getenv("BREAKOUT_ATR", "0.10"))
RETEST_ATR = float(os.getenv("RETEST_ATR", "0.45"))
SL_ATR_BUFFER = float(os.getenv("SL_ATR_BUFFER", "0.20"))
MIN_RR = max(1.5, float(os.getenv("MIN_RR", "2.0")))
TP2_R = max(MIN_RR, float(os.getenv("TP2_R", "3.0")))
TP3_R = max(TP2_R, float(os.getenv("TP3_R", "4.0")))
LIVE_PRICE_SECONDS = max(1, float(os.getenv("LIVE_PRICE_SECONDS", "3")))
MAX_RETEST_BARS = max(2, int(os.getenv("MAX_RETEST_BARS", "6")))
MAX_PATTERN_BARS = max(40, int(os.getenv("MAX_PATTERN_BARS", "100")))
MIN_SCORE = max(60, min(95, int(os.getenv("MIN_SCORE", "72"))))
COOLDOWN_HOURS = max(0.0, float(os.getenv("COOLDOWN_HOURS", "6")))
MAX_SYMBOLS = max(0, int(os.getenv("MAX_SYMBOLS", "0")))
SEND_CHART = os.getenv("SEND_CHART", "1") == "1"
CHART_BARS = min(150, max(80, int(os.getenv("CHART_BARS", "110"))))

app = Flask(__name__)
http = requests.Session()
http.headers.update({"User-Agent": "SAIWAN-1H-Pattern-Bot/4.0"})

state = {
    "running": False,
    "last_scan": None,
    "last_error": None,
    "signals_sent": 0,
    "symbols": 0,
    "last_signal": None,
}
state_lock = threading.Lock()
scan_lock = threading.Lock()
processed = set()
last_signal_at = {}
telegram_offset = None

# Active signals are kept so TP1/TP2/TP3/SL can be reported as replies
# to the original signal message.
active_signals = {}
active_lock = threading.Lock()


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt(x):
    x = float(x)
    a = abs(x)
    if a >= 1000:
        return f"{x:.2f}"
    if a >= 1:
        return f"{x:.4f}"
    if a >= .01:
        return f"{x:.6f}"
    if a >= .0001:
        return f"{x:.8f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def api(path, params=None, timeout=20):
    r = http.get(BASE + path, params=params, timeout=timeout)
    r.raise_for_status()
    d = r.json()
    if d.get("code") != "00000":
        raise RuntimeError(f"Bitget {d.get('code')}: {d.get('msg')}")
    return d.get("data")


def get_live_price(symbol):
    data = api("/api/v2/mix/market/ticker", {
        "productType": PRODUCT,
        "symbol": symbol,
    }) or []
    if isinstance(data, dict):
        data = [data]
    if not data:
        return None, None
    row = data[0]
    price = row.get("lastPr") or row.get("markPrice")
    if price is None:
        return None, row.get("ts")
    return float(price), int(row.get("ts") or int(time.time() * 1000))


def get_symbols():
    rows = api("/api/v2/mix/market/contracts", {"productType": PRODUCT}) or []
    out = []
    for x in rows:
        s = str(x.get("symbol", ""))
        if not s.endswith("USDT"):
            continue
        if str(x.get("quoteCoin", "")).upper() not in ("", "USDT"):
            continue
        if str(x.get("symbolStatus", "")).lower() not in ("", "normal"):
            continue
        if str(x.get("symbolType", "")).lower() not in ("", "perpetual"):
            continue
        base = str(x.get("baseCoin", "")).upper()
        if not base:
            continue
        if str(x.get("isRwa", "NO")).upper() == "YES":
            continue
        non_crypto = ("XAU", "XAG", "XPT", "XPD", "GOLD", "SILVER")
        if base.startswith(non_crypto):
            continue
        if base in {"AAPL", "AMZN", "GOOG", "GOOGL", "META", "MSFT", "NVDA", "TSLA", "COIN", "MSTR"}:
            continue
        out.append(s)
    out = sorted(set(out))
    return out[:MAX_SYMBOLS] if MAX_SYMBOLS else out


def get_candles(symbol):
    rows = api("/api/v2/mix/market/candles", {
        "symbol": symbol,
        "productType": PRODUCT,
        "granularity": TF,
        "limit": CANDLE_LIMIT,
    }) or []
    c = []
    for r in rows:
        if len(r) < 6:
            continue
        c.append({
            "ts": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]),
        })
    c.sort(key=lambda x: x["ts"])
    # Never analyze the still-forming 1H candle.
    return c[:-1] if len(c) > 2 else c


def atrs(c, period=ATR_PERIOD):
    tr = []
    for i, k in enumerate(c):
        if i == 0:
            tr.append(k["high"] - k["low"])
        else:
            p = c[i - 1]["close"]
            tr.append(max(
                k["high"] - k["low"],
                abs(k["high"] - p),
                abs(k["low"] - p),
            ))
    out = [None] * len(c)
    if len(c) < period:
        return out
    a = sum(tr[:period]) / period
    out[period - 1] = a
    for i in range(period, len(c)):
        a = ((a * (period - 1)) + tr[i]) / period
        out[i] = a
    return out


def median(values):
    v = sorted(x for x in values if x is not None)
    if not v:
        return 0.0
    m = len(v) // 2
    return v[m] if len(v) % 2 else (v[m - 1] + v[m]) / 2


def pivots(c):
    hs, ls = [], []
    n = PIVOT_WINDOW
    for i in range(n, len(c) - n):
        h = c[i]["high"]
        lo = c[i]["low"]
        if h > max(c[j]["high"] for j in range(i - n, i)) and h >= max(c[j]["high"] for j in range(i + 1, i + n + 1)):
            hs.append(i)
        if lo < min(c[j]["low"] for j in range(i - n, i)) and lo <= min(c[j]["low"] for j in range(i + 1, i + n + 1)):
            ls.append(i)
    return hs, ls


def line_value(line, i):
    x1, y1, x2, y2 = line
    if x1 == x2:
        return y2
    return y1 + (y2 - y1) * (i - x1) / (x2 - x1)


def line_slope(line):
    x1, y1, x2, y2 = line
    if x2 == x1:
        return 0.0
    return (y2 - y1) / (x2 - x1)


def candle_strength(k, a, bullish):
    a = max(a or 0.0, 1e-12)
    rng = max(k["high"] - k["low"], 1e-12)
    body = abs(k["close"] - k["open"])
    if body < a * 0.20:
        return False
    if bullish:
        return k["close"] > k["open"] and (k["close"] - k["low"]) / rng >= 0.60
    return k["close"] < k["open"] and (k["high"] - k["close"]) / rng >= 0.60


def candidate_lines(c, indices, kind, atr):
    """Return good support/resistance lines from recent pivots."""
    if len(indices) < 2:
        return []
    pts = indices[-8:]
    result = []
    typical_atr = median(atr[max(0, len(c) - 50):]) or c[-1]["close"] * .005
    for ai in range(len(pts) - 1):
        for bi in range(ai + 1, len(pts)):
            i1, i2 = pts[ai], pts[bi]
            if i2 - i1 < 6:
                continue
            y1 = c[i1][kind]
            y2 = c[i2][kind]
            line = (i1, y1, i2, y2)
            tol = max(typical_atr * .55, c[i2]["close"] * .0015)
            touches = 0
            errors = []
            for j in pts:
                y = c[j][kind]
                e = abs(y - line_value(line, j))
                if e <= tol:
                    touches += 1
                errors.append(e)
            if touches < 2:
                continue
            # Prefer lines that explain more pivots with less error.
            result.append({
                "line": line,
                "touches": touches,
                "error": sum(errors) / len(errors),
            })
    result.sort(key=lambda x: (-x["touches"], x["error"], -x["line"][2]))
    return result[:12]


def pattern_candidates(c, hs, ls, atr):
    """Find the triangle/wedge structures used by the reference charts."""
    cur = len(c) - 1
    left = max(0, cur - MAX_PATTERN_BARS)
    highs = [i for i in hs if i >= left and i <= cur - 2]
    lows = [i for i in ls if i >= left and i <= cur - 2]
    if len(highs) < 2 or len(lows) < 2:
        return []

    ups = candidate_lines(c, highs, "high", atr)
    downs = candidate_lines(c, lows, "low", atr)
    out = []
    current_price = c[cur]["close"]

    for u in ups:
        for d in downs:
            ul = u["line"]
            dl = d["line"]
            start = max(ul[0], dl[0])
            end = min(cur - 2, max(ul[2], dl[2]) + 2)
            if end - start < 20:
                continue
            width_start = line_value(ul, start) - line_value(dl, start)
            width_end = line_value(ul, end) - line_value(dl, end)
            if width_start <= 0 or width_end <= 0:
                continue
            contraction = width_end / width_start
            if contraction > 0.90:
                continue

            su = line_slope(ul)
            sl = line_slope(dl)
            ref = max(abs(current_price), 1e-12)
            nsu = su / ref
            nsl = sl / ref

            # Main reference setup: descending resistance + rising support.
            if nsu < -0.00005 and nsl > 0.00005:
                typ = "Symmetrical Triangle"
                bias = "BULLISH"
                score = 76
            # Rising support + slightly rising resistance: ascending triangle.
            elif abs(nsu) <= 0.00005 and nsl > 0.00005:
                typ = "Ascending Triangle"
                bias = "BULLISH"
                score = 74
            # Falling resistance + flat support: descending triangle.
            elif nsu < -0.00005 and abs(nsl) <= 0.00005:
                typ = "Descending Triangle"
                bias = "BEARISH"
                score = 74
            # Both lines falling while converging.
            elif nsu < -0.00005 and nsl < -0.00005 and abs(nsu) > abs(nsl):
                typ = "Falling Wedge"
                bias = "BULLISH"
                score = 73
            # Both lines rising while converging.
            elif nsu > 0.00005 and nsl > 0.00005 and nsl > nsu:
                typ = "Rising Wedge"
                bias = "BEARISH"
                score = 73
            else:
                continue

            score += min(12, (u["touches"] + d["touches"] - 4) * 3)
            score += 7 if contraction < .65 else 4 if contraction < .78 else 2
            score = min(95, int(score))
            if score < MIN_SCORE:
                continue

            out.append({
                "type": typ,
                "bias": bias,
                "upper": ul,
                "lower": dl,
                "upper_touches": u["touches"],
                "lower_touches": d["touches"],
                "touches": u["touches"] + d["touches"],
                "score": score,
                "start": start,
                "end": end,
                "contraction": contraction,
            })

    # Deduplicate nearly identical structures.
    unique = []
    seen = set()
    for p in sorted(out, key=lambda x: (-x["score"], -x["touches"], x["start"])):
        key = (
            p["type"],
            round(line_slope(p["upper"]), 10),
            round(line_slope(p["lower"]), 10),
            p["start"] // 5,
        )
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique[:8]


def breakout_and_retest(c, p, atr):
    """Return LONG/SHORT breakout, retest and confirmation indexes."""
    cur = len(c) - 1
    # Pattern must be mature before breakout.
    search_from = min(cur - 1, p["end"] + 1)
    if search_from >= cur:
        return None

    candidates = []
    for i in range(search_from, cur):
        a = atr[i] or median(atr[max(0, i - 20):i + 1]) or c[i]["close"] * .005
        upper = line_value(p["upper"], i)
        lower = line_value(p["lower"], i)
        prev = c[i - 1]
        k = c[i]

        if p["bias"] == "BULLISH":
            if prev["close"] <= upper + a * .05 and k["close"] > upper + a * BREAKOUT_ATR and candle_strength(k, a, True):
                candidates.append((i, "LONG", upper))
        elif p["bias"] == "BEARISH":
            if prev["close"] >= lower - a * .05 and k["close"] < lower - a * BREAKOUT_ATR and candle_strength(k, a, False):
                candidates.append((i, "SHORT", lower))

    if not candidates:
        return None

    # Use the latest valid breakout so an old breakout cannot generate a late signal.
    bi, direction, level = candidates[-1]
    retest = None
    confirm = None
    for r in range(bi + 1, min(cur, bi + MAX_RETEST_BARS) + 1):
        a = atr[r] or atr[bi] or c[r]["close"] * .005
        k = c[r]
        touched = k["low"] <= level + a * RETEST_ATR and k["high"] >= level - a * RETEST_ATR
        if not touched:
            continue
        if direction == "LONG" and k["close"] >= level - a * .10:
            retest = r
            break
        if direction == "SHORT" and k["close"] <= level + a * .10:
            retest = r
            break

    if retest is None:
        return None

    # Confirmation may be the retest candle itself when it rejects the level,
    # or one of the following two closed candles. The latest closed candle must
    # be the confirmation used for the Telegram signal.
    for q in range(retest, min(cur, retest + 2) + 1):
        a = atr[q] or atr[retest] or c[q]["close"] * .005
        k = c[q]
        if direction == "LONG" and k["close"] > level and candle_strength(k, a, True):
            confirm = q
        elif direction == "SHORT" and k["close"] < level and candle_strength(k, a, False):
            confirm = q
        if confirm is not None and confirm == cur:
            break

    if confirm != cur:
        return None
    return {
        "direction": direction,
        "breakout_idx": bi,
        "retest_idx": retest,
        "confirm_idx": confirm,
        "breakout_level": level,
    }


def make_signal(symbol, c, p, atr, hs, ls):
    event = breakout_and_retest(c, p, atr)
    if event is None:
        return None

    direction = event["direction"]
    retest = event["retest_idx"]
    cur = len(c) - 1
    entry = c[cur]["close"]
    pattern_start = max(p["start"], retest - 15)

    if direction == "LONG":
        local_lows = [i for i in ls if pattern_start <= i <= retest]
        if local_lows:
            base = c[local_lows[-1]]["low"]
        else:
            base = min(x["low"] for x in c[max(0, retest - 8):retest + 1])
        sl = base - (atr[retest] or atr[cur]) * SL_ATR_BUFFER
        risk = entry - sl
        if risk <= 0:
            return None
        tp1 = entry + risk * MIN_RR
        tp2 = entry + risk * TP2_R
        tp3 = entry + risk * TP3_R
    else:
        local_highs = [i for i in hs if pattern_start <= i <= retest]
        if local_highs:
            base = c[local_highs[-1]]["high"]
        else:
            base = max(x["high"] for x in c[max(0, retest - 8):retest + 1])
        sl = base + (atr[retest] or atr[cur]) * SL_ATR_BUFFER
        risk = sl - entry
        if risk <= 0:
            return None
        tp1 = entry - risk * MIN_RR
        tp2 = entry - risk * TP2_R
        tp3 = entry - risk * TP3_R

    risk_pct = abs(entry - sl) / max(abs(entry), 1e-12) * 100
    if risk_pct < .15 or risk_pct > 12:
        return None

    return {
        "symbol": symbol,
        "side": direction,
        "pattern": p["type"],
        "score": p["score"],
        "touches": p["touches"],
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr": abs(tp2 - entry) / max(abs(entry - sl), 1e-12),
        "risk_pct": risk_pct,
        "candle_ts": c[cur]["ts"],
        "breakout_idx": event["breakout_idx"],
        "retest_idx": event["retest_idx"],
        "confirm_idx": event["confirm_idx"],
        "breakout_level": event["breakout_level"],
        "pattern_data": p,
        "key": (symbol, c[cur]["ts"], direction, p["type"], event["breakout_idx"]),
    }


def find_signal(symbol, c):
    if len(c) < 140:
        return None
    atr = atrs(c)
    if not atr[-1]:
        return None
    hs, ls = pivots(c)
    patterns = pattern_candidates(c, hs, ls, atr)
    found = []
    for p in patterns:
        s = make_signal(symbol, c, p, atr, hs, ls)
        if s:
            found.append(s)
    if not found:
        return None
    return max(found, key=lambda x: (x["score"], x["rr"], x["touches"]))


def _draw_volume_profile(ax, data, bins=24):
    """Simple price-volume profile, similar to the reference charts."""
    try:
        lo = min(k["low"] for k in data)
        hi = max(k["high"] for k in data)
        if hi <= lo:
            return
        step = (hi - lo) / bins
        vols = [0.0] * bins
        for k in data:
            idx = int((k["close"] - lo) / step)
            idx = max(0, min(bins - 1, idx))
            vols[idx] += max(k["volume"], 0.0)
        vmax = max(vols) if vols else 0.0
        if vmax <= 0:
            return
        max_width = max(8, len(data) * .12)
        for j, v in enumerate(vols):
            if v <= 0:
                continue
            y = lo + j * step
            width = max_width * v / vmax
            ax.barh(y + step / 2, width, height=step * .72, left=0,
                    alpha=.16, color="#4d7cff", edgecolor="none", zorder=0)
    except Exception:
        return


def chart(c, s):
    if not SEND_CHART:
        return None
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle

        n = min(CHART_BARS, len(c))
        data = c[-n:]
        off = len(c) - n
        fig, ax = plt.subplots(figsize=(13.5, 7.6), dpi=150)
        fig.patch.set_facecolor("#ffffff")
        ax.set_facecolor("#ffffff")

        _draw_volume_profile(ax, data)

        for x, k in enumerate(data):
            o, h, l, cl = k["open"], k["high"], k["low"], k["close"]
            col = "#19a974" if cl >= o else "#d9534f"
            ax.vlines(x, l, h, color=col, lw=.75, zorder=2)
            body = max(abs(cl - o), (h - l) * .004)
            ax.add_patch(Rectangle(
                (x - .30, min(o, cl)), .60, body,
                facecolor=col, edgecolor=col, lw=.25, zorder=3,
            ))

        p = s["pattern_data"]

        def draw_line(line, color, lw=2.0):
            x1, y1, x2, y2 = line
            xx1 = max(0, x1 - off)
            xx2 = n - 1
            if xx2 < 0 or xx1 > n - 1:
                return
            ax.plot(
                [xx1, xx2],
                [line_value(line, x1), line_value(line, len(c) - 1)],
                color=color, lw=lw, zorder=4,
            )

        # Reference-style blue trendlines.
        draw_line(p["upper"], "#3977d7", 2.2)
        draw_line(p["lower"], "#3977d7", 2.2)

        # Breakout / entry / retest levels.
        level = s["breakout_level"]
        ax.axhline(level, color="#d14b4b", lw=1.4, alpha=.85, zorder=3)

        bx = s["breakout_idx"] - off
        rx = s["retest_idx"] - off
        ex = s["confirm_idx"] - off
        if 0 <= bx < n:
            ax.annotate("BREAKOUT", (bx, c[s["breakout_idx"]]["close"]),
                        xytext=(max(0, bx - 13), c[s["breakout_idx"]]["close"]),
                        arrowprops=dict(arrowstyle="->", color="#333333"),
                        color="#333333", fontsize=8.5, fontweight="bold")
        if 0 <= rx < n:
            ax.annotate("RETEST", (rx, c[s["retest_idx"]]["close"]),
                        xytext=(max(0, rx - 10), c[s["retest_idx"]]["close"]),
                        arrowprops=dict(arrowstyle="->", color="#333333"),
                        color="#333333", fontsize=8.5, fontweight="bold")
        if 0 <= ex < n:
            ax.scatter([ex], [s["entry"]], s=42, color="#111111", zorder=7)
            ax.annotate("ENTRY", (ex, s["entry"]),
                        xytext=(min(n - 12, ex + 2), s["entry"]),
                        color="#111111", fontsize=9, fontweight="bold")

        # The same risk/target structure is preserved: TP1, TP2, TP3.
        entry = s["entry"]
        if s["side"] == "LONG":
            y_top = s["tp3"]
            y_bottom = s["sl"]
            green_bottom = entry
            red_top = entry
        else:
            y_top = entry
            y_bottom = s["tp3"]
            green_bottom = entry
            red_top = entry

        x0 = max(0, ex - 1)
        x1 = min(n - 1, ex + max(12, int(n * .12)))
        if s["side"] == "LONG":
            ax.add_patch(Rectangle((x0, entry), x1 - x0, s["tp3"] - entry,
                                   facecolor="#27ae60", alpha=.18, edgecolor="none", zorder=1))
            ax.add_patch(Rectangle((x0, s["sl"]), x1 - x0, entry - s["sl"],
                                   facecolor="#e74c3c", alpha=.18, edgecolor="none", zorder=1))
        else:
            ax.add_patch(Rectangle((x0, s["tp3"]), x1 - x0, entry - s["tp3"],
                                   facecolor="#27ae60", alpha=.18, edgecolor="none", zorder=1))
            ax.add_patch(Rectangle((x0, entry), x1 - x0, s["sl"] - entry,
                                   facecolor="#e74c3c", alpha=.18, edgecolor="none", zorder=1))

        levels = [
            (s["entry"], "ENTRY", "#555555"),
            (s["sl"], "SL", "#d9534f"),
            (s["tp1"], "TP1", "#159957"),
            (s["tp2"], "TP2", "#159957"),
            (s["tp3"], "TP3", "#159957"),
        ]
        for y, label, col in levels:
            ax.axhline(y, color=col, lw=1.05, ls="--", alpha=.9, zorder=5)
            ax.text(n + .5, y, f"{label} {fmt(y)}", va="center",
                    color=col, fontsize=8.5, fontweight="bold")

        ax.set_title(
            f"SAIWAN • {s['symbol']} • 1H • {s['pattern']} • {s['side']}",
            color="#222222", fontsize=13, fontweight="bold", loc="left",
        )
        ax.text(
            .01, .965,
            f"Trendline → Breakout → Retest → Confirmation   |   Score {s['score']}/100   |   RR {s['rr']:.2f}",
            transform=ax.transAxes, va="top", color="#555555", fontsize=8.5,
        )
        ax.set_xlim(-1, n + 10)
        ax.grid(True, alpha=.15, color="#9aa3ad")
        ax.tick_params(colors="#555555", labelsize=8)
        for sp in ax.spines.values():
            sp.set_color("#d0d5da")
        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception as e:
        with state_lock:
            state["last_error"] = f"chart: {e}"
        return None


def tg_text(s):
    icon = "🟢" if s["side"] == "LONG" else "🔴"
    dt = datetime.fromtimestamp(s["candle_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"📊 <b>SAIWAN 1H PATTERN SIGNAL</b>\n\n"
        f"{icon} <b>{s['side']}</b> • {s['symbol']}\n"
        f"🔎 Pattern: <b>{s['pattern']}</b>\n"
        f"⭐ Score: {s['score']}/100\n"
        f"📐 Trendline touches: {s['touches']}\n"
        f"⏱ Timeframe: 1H CLOSED\n\n"
        f"🎯 <b>ENTRY:</b> {fmt(s['entry'])}\n"
        f"🛑 <b>SL:</b> {fmt(s['sl'])}\n"
        f"✅ <b>TP1:</b> {fmt(s['tp1'])} ({MIN_RR:.1f}R)\n"
        f"✅ <b>TP2:</b> {fmt(s['tp2'])} ({TP2_R:.1f}R)\n"
        f"🚀 <b>TP3:</b> {fmt(s['tp3'])} ({TP3_R:.1f}R)\n\n"
        f"📏 Risk: {s['risk_pct']:.2f}%\n"
        f"⚖️ RR: {s['rr']:.2f}\n\n"
        f"🔹 Breakout → Retest → Confirmation\n"
        f"🏦 Bitget USDT Perpetual\n"
        f"🕒 {dt}\n\n"
        f"⚠️ Signal only — no automatic order."
    )


def tg_send(text, reply_to=None, parse_mode=None):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Telegram variables are missing")
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to:
        payload["reply_parameters"] = {
            "message_id": int(reply_to),
            "allow_sending_without_reply": True,
        }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    r = http.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json=payload,
        timeout=20,
    )
    r.raise_for_status()
    return (r.json().get("result") or {}).get("message_id")


def tg_reply(text, message_id):
    return tg_send(text, reply_to=message_id, parse_mode="HTML")


def send_signal(s, c):
    # One Telegram message: chart + caption. TP/SL updates reply to this message.
    im = chart(c, s)
    if im is not None:
        try:
            payload = {
                "chat_id": CHAT_ID,
                "caption": tg_text(s),
                "parse_mode": "HTML",
            }
            r = http.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data=payload,
                files={"photo": ("saiwan_1h.png", im, "image/png")},
                timeout=30,
            )
            r.raise_for_status()
            msg_id = (r.json().get("result") or {}).get("message_id")
            if not msg_id:
                raise RuntimeError("Telegram did not return message_id")
            s["telegram_message_id"] = int(msg_id)
            return
        except Exception as e:
            with state_lock:
                state["last_error"] = f"telegram chart: {e}"
    msg_id = tg_send(tg_text(s), parse_mode="HTML")
    s["telegram_message_id"] = int(msg_id) if msg_id else None


def register_active_signal(s):
    mid = s.get("telegram_message_id")
    if not mid:
        return
    key = f"{s['symbol']}|{s['candle_ts']}|{s['side']}|{s['pattern']}|{s['breakout_idx']}"
    item = dict(s)
    item["tracking_key"] = key
    item["tp1_hit"] = False
    item["tp2_hit"] = False
    item["tp3_hit"] = False
    item["sl_hit"] = False
    item["last_checked_ts"] = 0
    with active_lock:
        active_signals[key] = item


def hit_update_text(s, event, price, ts):
    icon = {"TP1": "🎯", "TP2": "🏁", "TP3": "🚀", "SL": "🛑"}[event]
    when = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"{icon} <b>{event} HIT</b>\n"
        f"{s['symbol']} • {s['side']} • {s['pattern']}\n"
        f"Price: <b>{fmt(price)}</b>\n"
        f"Time: {when}"
    )


def track_active_signals():
    """Monitor live price for TP1/TP2/TP3/SL updates."""
    with active_lock:
        snapshot = list(active_signals.items())
    for key, s in snapshot:
        try:
            price, ts = get_live_price(s["symbol"])
            if price is None:
                continue
            events = []
            if s["side"] == "LONG":
                if price >= s["tp3"] and not s.get("tp3_hit"):
                    if not s.get("tp1_hit"):
                        events.append(("TP1", s["tp1"])); s["tp1_hit"] = True
                    if not s.get("tp2_hit"):
                        events.append(("TP2", s["tp2"])); s["tp2_hit"] = True
                    events.append(("TP3", s["tp3"])); s["tp3_hit"] = True
                elif price >= s["tp2"] and not s.get("tp2_hit"):
                    if not s.get("tp1_hit"):
                        events.append(("TP1", s["tp1"])); s["tp1_hit"] = True
                    events.append(("TP2", s["tp2"])); s["tp2_hit"] = True
                elif price >= s["tp1"] and not s.get("tp1_hit"):
                    events.append(("TP1", s["tp1"])); s["tp1_hit"] = True
                if price <= s["sl"] and not s.get("sl_hit"):
                    events.append(("SL", s["sl"])); s["sl_hit"] = True
            else:
                if price <= s["tp3"] and not s.get("tp3_hit"):
                    if not s.get("tp1_hit"):
                        events.append(("TP1", s["tp1"])); s["tp1_hit"] = True
                    if not s.get("tp2_hit"):
                        events.append(("TP2", s["tp2"])); s["tp2_hit"] = True
                    events.append(("TP3", s["tp3"])); s["tp3_hit"] = True
                elif price <= s["tp2"] and not s.get("tp2_hit"):
                    if not s.get("tp1_hit"):
                        events.append(("TP1", s["tp1"])); s["tp1_hit"] = True
                    events.append(("TP2", s["tp2"])); s["tp2_hit"] = True
                elif price <= s["tp1"] and not s.get("tp1_hit"):
                    events.append(("TP1", s["tp1"])); s["tp1_hit"] = True
                if price >= s["sl"] and not s.get("sl_hit"):
                    events.append(("SL", s["sl"])); s["sl_hit"] = True

            for event, _level in events:
                tg_reply(hit_update_text(s, event, price, ts), s["telegram_message_id"])

            s["last_live_price"] = price
            s["last_live_ts"] = ts
            if s.get("sl_hit") or s.get("tp3_hit"):
                with active_lock:
                    active_signals.pop(key, None)
            else:
                with active_lock:
                    if key in active_signals:
                        active_signals[key] = s
        except Exception as e:
            with state_lock:
                state["last_error"] = f"live tracker {s['symbol']}: {e}"


def live_tracker_loop():
    while True:
        try:
            track_active_signals()
        except Exception as e:
            with state_lock:
                state["last_error"] = f"live tracker: {e}"
        time.sleep(LIVE_PRICE_SECONDS)


def scan_once():
    if not scan_lock.acquire(False):
        return
    try:
        symbols = get_symbols()
        with state_lock:
            state["symbols"] = len(symbols)
        for symbol in symbols:
            try:
                c = get_candles(symbol)
                if len(c) < 140:
                    continue
                candle_key = (symbol, c[-1]["ts"])
                if candle_key in processed:
                    continue
                processed.add(candle_key)
                s = find_signal(symbol, c)
                if not s:
                    continue
                if s["key"] in processed:
                    continue
                if COOLDOWN_HOURS and time.time() - last_signal_at.get(symbol, 0) < COOLDOWN_HOURS * 3600:
                    continue
                send_signal(s, c)
                register_active_signal(s)
                processed.add(s["key"])
                last_signal_at[symbol] = time.time()
                with state_lock:
                    state["signals_sent"] += 1
                    state["last_signal"] = f"{symbol} {s['side']} {s['pattern']} @ {fmt(s['entry'])}"
            except Exception as e:
                with state_lock:
                    state["last_error"] = f"{symbol}: {e}"
    finally:
        scan_lock.release()


def scanner_loop():
    with state_lock:
        state["running"] = True
    while True:
        try:
            scan_once()
            with state_lock:
                state["last_scan"] = now_utc()
        except Exception as e:
            with state_lock:
                state["last_error"] = str(e)
        time.sleep(SCAN_SECONDS)


def telegram_loop():
    global telegram_offset
    if not BOT_TOKEN or not CHAT_ID:
        with state_lock:
            state["last_error"] = "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing"
        return

    while True:
        try:
            r = http.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"timeout": 25, "offset": telegram_offset},
                timeout=35,
            )
            r.raise_for_status()
            d = r.json()
            for u in d.get("result", []):
                telegram_offset = u["update_id"] + 1
                m = u.get("message") or {}
                text = (m.get("text") or "").strip().lower()
                if str(m.get("chat", {}).get("id")) != str(CHAT_ID):
                    continue
                if text == "/start":
                    tg_send(
                        "🤖 <b>SAIWAN 1H Pattern Bot is online.</b>\n\n"
                        "Trendline → Breakout → Retest → Confirmation\n"
                        "Use /status or /scan",
                        parse_mode="HTML",
                    )
                elif text == "/scan":
                    tg_send("🔎 Manual 1H scan started...")
                    try:
                        scan_once()
                        tg_send("✅ 1H scan completed.")
                    except Exception as e:
                        tg_send(f"❌ Scan error: {e}")
                elif text == "/status":
                    with state_lock:
                        s = dict(state)
                    with active_lock:
                        active_count = len(active_signals)
                    tg_send(
                        "🤖 <b>SAIWAN STATUS</b>\n\n"
                        "Bot: ONLINE\n"
                        "Scanner: %s\n"
                        "Market: Bitget USDT Perpetual\n"
                        "Timeframe: 1H\n"
                        "Symbols: %s\n"
                        "Signals sent: %s\n"
                        "Active tracked signals: %s\n"
                        "Last scan: %s\n"
                        "Last signal: %s\n"
                        "Last error: %s" % (
                            "RUNNING" if s["running"] else "STARTING",
                            s["symbols"],
                            s["signals_sent"],
                            active_count,
                            s["last_scan"] or "not yet",
                            s["last_signal"] or "none",
                            s["last_error"] or "none",
                        ),
                        parse_mode="HTML",
                    )
        except Exception as e:
            with state_lock:
                state["last_error"] = f"Telegram: {e}"
            time.sleep(5)


@app.get("/")
def home():
    with state_lock:
        return jsonify({
            "bot": "SAIWAN",
            "strategy": "1H Trendline Breakout + Retest + Confirmation",
            "status": "online",
            **state,
        })


@app.get("/status")
def status():
    with state_lock:
        return jsonify(dict(state))


# Railway/Gunicorn: ONE worker only, because each worker would start its own
# scanner, Telegram polling loop and TP/SL tracker.
if not getattr(app, "_saiwan_started", False):
    app._saiwan_started = True
    threading.Thread(target=scanner_loop, daemon=True, name="scanner").start()
    threading.Thread(target=telegram_loop, daemon=True, name="telegram").start()
    threading.Thread(target=live_tracker_loop, daemon=True, name="live-tp-sl").start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
