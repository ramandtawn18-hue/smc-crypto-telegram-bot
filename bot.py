import os
import io
import time
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify

# ============================================================
# SAIWAN — 1H Chart Pattern Breakout Bot
# Bitget USDT-M Perpetual + Telegram
# Rule-based: closed 1H candle -> pattern -> breakout -> retest
# -> confirmation -> structural SL -> RR-based TP.
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BASE = "https://api.bitget.com"
PRODUCT = "USDT-FUTURES"
TF = "1H"

SCAN_SECONDS = max(30, int(os.getenv("SCAN_SECONDS", "60")))
CANDLE_LIMIT = min(1000, max(220, int(os.getenv("CANDLE_LIMIT", "420"))))
PIVOT_WINDOW = max(2, int(os.getenv("PIVOT_WINDOW", "4")))
MIN_TOUCHES = max(3, int(os.getenv("MIN_TOUCHES", "3")))
ATR_PERIOD = max(10, int(os.getenv("ATR_PERIOD", "14")))
BREAKOUT_ATR = float(os.getenv("BREAKOUT_ATR", "0.15"))
RETEST_ATR = float(os.getenv("RETEST_ATR", "0.35"))
SL_ATR_BUFFER = float(os.getenv("SL_ATR_BUFFER", "0.25"))
MIN_RR = max(1.5, float(os.getenv("MIN_RR", "2.0")))
TP2_R = max(MIN_RR, float(os.getenv("TP2_R", "3.0")))
MAX_RETEST_BARS = max(1, int(os.getenv("MAX_RETEST_BARS", "5")))
MAX_PATTERN_BARS = max(30, int(os.getenv("MAX_PATTERN_BARS", "90")))
MIN_SCORE = max(60, min(95, int(os.getenv("MIN_SCORE", "70"))))
COOLDOWN_HOURS = max(0.0, float(os.getenv("COOLDOWN_HOURS", "6")))
MAX_SYMBOLS = max(0, int(os.getenv("MAX_SYMBOLS", "0")))
SEND_CHART = os.getenv("SEND_CHART", "1") == "1"
CHART_BARS = min(150, max(70, int(os.getenv("CHART_BARS", "110"))))

app = Flask(__name__)
http = requests.Session()
http.headers.update({"User-Agent": "SAIWAN-1H-Pattern-Bot/3.0"})

state = {"running": False, "last_scan": None, "last_error": None,
         "signals_sent": 0, "symbols": 0, "last_signal": None}
state_lock = threading.Lock()
scan_lock = threading.Lock()
processed = set()
last_signal_at = {}
telegram_offset = None

# Active signals are tracked after they are sent. Each signal stores the
# Telegram message id so TP/SL updates can be posted as replies to that
# exact signal message.
active_signals = {}
active_lock = threading.Lock()


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt(x):
    x = float(x)
    a = abs(x)
    if a >= 1000: return f"{x:.2f}"
    if a >= 1: return f"{x:.4f}"
    if a >= .01: return f"{x:.6f}"
    if a >= .0001: return f"{x:.8f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def api(path, params=None, timeout=15):
    r = http.get(BASE + path, params=params, timeout=timeout)
    r.raise_for_status()
    d = r.json()
    if d.get("code") != "00000":
        raise RuntimeError(f"Bitget {d.get('code')}: {d.get('msg')}")
    return d.get("data")


def get_symbols():
    rows = api("/api/v2/mix/market/contracts", {"productType": PRODUCT}) or []
    out = []
    for x in rows:
        s = str(x.get("symbol", ""))
        if not s.endswith("USDT"): continue
        if str(x.get("quoteCoin", "")).upper() not in ("", "USDT"): continue
        if str(x.get("symbolStatus", "")).lower() not in ("", "normal"): continue
        if str(x.get("symbolType", "")).lower() not in ("", "perpetual"): continue
        out.append(s)
    out = sorted(set(out))
    return out[:MAX_SYMBOLS] if MAX_SYMBOLS else out


def get_candles(symbol):
    rows = api("/api/v2/mix/market/candles", {
        "symbol": symbol, "productType": PRODUCT, "granularity": TF,
        "limit": CANDLE_LIMIT,
    }) or []
    c = []
    for r in rows:
        if len(r) < 6: continue
        c.append({"ts": int(r[0]), "open": float(r[1]), "high": float(r[2]),
                  "low": float(r[3]), "close": float(r[4]), "volume": float(r[5])})
    c.sort(key=lambda x: x["ts"])
    # Do not analyze the still-forming candle.
    return c[:-1] if len(c) > 2 else c


def atrs(c, period=ATR_PERIOD):
    tr = []
    for i, k in enumerate(c):
        if i == 0: tr.append(k["high"] - k["low"])
        else:
            p = c[i-1]["close"]
            tr.append(max(k["high"]-k["low"], abs(k["high"]-p), abs(k["low"]-p)))
    out = [None] * len(c)
    if len(c) < period: return out
    a = sum(tr[:period]) / period
    out[period-1] = a
    for i in range(period, len(c)):
        a = ((a*(period-1)) + tr[i]) / period
        out[i] = a
    return out


def med(vals):
    v = sorted(x for x in vals if x is not None)
    if not v: return 0.0
    m = len(v)//2
    return v[m] if len(v)%2 else (v[m-1]+v[m])/2


def pivots(c):
    hs, ls = [], []
    n = PIVOT_WINDOW
    for i in range(n, len(c)-n):
        h = c[i]["high"]; lo = c[i]["low"]
        if h > max(c[j]["high"] for j in range(i-n, i)) and h >= max(c[j]["high"] for j in range(i+1, i+n+1)):
            hs.append(i)
        if lo < min(c[j]["low"] for j in range(i-n, i)) and lo <= min(c[j]["low"] for j in range(i+1, i+n+1)):
            ls.append(i)
    return hs, ls


def lv(line, i):
    a,p1,b,p2 = line
    return p1 if a == b else p1 + (p2-p1)*(i-a)/(b-a)


def slope(points):
    if len(points) < 2: return 0.0
    xb = sum(x for x,_ in points)/len(points)
    yb = sum(y for _,y in points)/len(points)
    den = sum((x-xb)**2 for x,_ in points)
    return 0 if den == 0 else sum((x-xb)*(y-yb) for x,y in points)/den


def _line_score(c, indices, atr, mode, line, direction):
    """Score a trendline using pivot touches, spacing and price violations."""
    a, p1, b, p2 = line
    if b <= a:
        return None
    span = b - a
    base_atr = med([atr[i] for i in indices if i < len(atr) and atr[i]]) or abs(p1) * 0.002
    tol = max(base_atr * 0.38, abs(p1) * 0.0008)

    touches = []
    violations = 0
    for i in indices:
        if i < a or i > b:
            continue
        y = c[i]["high"] if mode == "high" else c[i]["low"]
        d = y - lv(line, i)
        if abs(d) <= tol:
            touches.append(i)

    if len(touches) < MIN_TOUCHES:
        return None

    # Touches must be spread over the line, not clustered together.
    if touches[-1] - touches[0] < span * 0.58:
        return None
    gaps = [touches[i+1] - touches[i] for i in range(len(touches)-1)]
    if gaps and max(gaps) > span * 0.72:
        return None

    # A clean trendline cannot have repeated meaningful closes through it
    # during the pattern. Wicks may slightly pierce the line.
    for i in range(a + 1, b + 1):
        k = c[i]
        aa = atr[i] or base_atr
        y = lv(line, i)
        if direction == "resistance" and k["close"] > y + aa * 0.18:
            violations += 1
        elif direction == "support" and k["close"] < y - aa * 0.18:
            violations += 1
    if violations > max(1, int(span * 0.04)):
        return None

    # Prefer more touches, cleaner lines and longer spans.
    score = len(touches) * 22 + min(span, 120) * 0.20 - violations * 12
    return {"line": line, "touches": touches, "tol": tol, "score": score}


def best_line(c, indices, atr, mode, direction):
    """Find a strong, clean trendline from meaningful swing points.

    For resistance the line stays above price; for support it stays below.
    The returned tuple remains compatible with the rest of the bot.
    """
    pts = indices[-12:]
    candidates = []
    for ai in range(max(0, len(pts) - 10), len(pts) - 2):
        for bi in range(ai + 1, len(pts)):
            i1, i2 = pts[ai], pts[bi]
            if i2 - i1 < 8:
                continue
            p1 = c[i1]["high"] if mode == "high" else c[i1]["low"]
            p2 = c[i2]["high"] if mode == "high" else c[i2]["low"]

            # Reject practically horizontal candidates.  A wedge boundary
            # must visibly travel from one swing to another; otherwise the
            # scanner can accidentally select a flat line made from several
            # similarly-priced pivots.  The threshold is normalized to price
            # and is intentionally modest so shallow but real 1H trendlines
            # are still allowed.
            avg_price = max((abs(p1) + abs(p2)) / 2.0, 1e-12)
            move_pct = abs(p2 - p1) / avg_price
            if move_pct < 0.004:
                continue

            line = (i1, p1, i2, p2)
            result = _line_score(c, pts, atr, mode, line, direction)
            if result:
                candidates.append(result)
    if not candidates:
        return None
    best = max(candidates, key=lambda z: (z["score"], z["touches"][-1], z["line"][2]))
    return (len(best["touches"]), best["touches"][-1], best["line"])

def detect_patterns(c, hs, ls, atr):
    out = []
    rh = [i for i in hs if i < len(c) - 2][-12:]
    rl = [i for i in ls if i < len(c) - 2][-12:]

    # ------------------------------------------------------------
    # Two-boundary patterns: build a real upper line from swing highs
    # and a real lower line from swing lows. Wedges require both lines
    # to slope in the same direction and to converge.
    # ------------------------------------------------------------
    if len(rh) >= 3 and len(rl) >= 3:
        upper = best_line(c, rh, atr, "high", "resistance")
        lower = best_line(c, rl, atr, "low", "support")
        if upper and lower:
            ul, ll = upper[2], lower[2]
            start_i = max(ul[0], ll[0])
            # The pattern ends at the later second anchor. Do not add a
            # forward buffer here: breakout/retest candles must remain outside
            # the pattern so they can be validated independently.
            end_i = min(len(c) - 2, max(ul[2], ll[2]))
            if end_i > start_i + 8:
                w0 = lv(ul, start_i) - lv(ll, start_i)
                w1 = lv(ul, end_i) - lv(ll, end_i)
                su = slope([(i, c[i]["high"]) for i in rh if ul[0] <= i <= ul[2]])
                sl = slope([(i, c[i]["low"]) for i in rl if ll[0] <= i <= ll[2]])
                if w0 > 0 and w1 > 0 and w1 < w0 * 0.82:
                    # Falling wedge: both boundaries descend, upper descends faster.
                    if su < 0 and sl < 0 and su < sl:
                        typ, bias = "Falling Wedge", "BULLISH"
                    # Rising wedge: both boundaries rise, lower rises faster.
                    elif su > 0 and sl > 0 and sl > su:
                        typ, bias = "Rising Wedge", "BEARISH"
                    # Opposite slopes = triangle geometry, not a wedge.
                    elif su < 0 and sl > 0:
                        typ, bias = "Symmetrical Triangle", "NEUTRAL"
                    else:
                        typ = bias = None
                    if typ:
                        convergence = 1 - (w1 / w0)
                        touch_count = upper[0] + lower[0]
                        score = min(95, 68 + min(18, (touch_count - 6) * 4) + min(9, convergence * 20))
                        out.append({
                            "type": typ, "bias": bias,
                            "upper": ul, "lower": ll,
                            "upper_touches": upper[0], "lower_touches": lower[0],
                            "upper_points": [i for i in rh if ul[0] <= i <= ul[2]],
                            "lower_points": [i for i in rl if ll[0] <= i <= ll[2]],
                            "touches": touch_count, "score": round(score),
                            "start": start_i, "end": end_i,
                            "width_start": w0, "width_end": w1,
                        })

    # Descending triangle: falling resistance + approximately horizontal support.
    if len(rh) >= 3 and len(rl) >= 3:
        upper = best_line(c, rh, atr, "high", "resistance")
        lows = rl[-6:]
        if upper and len(lows) >= 3:
            level = med([c[i]["low"] for i in lows])
            spread = max(abs(c[i]["low"] - level) for i in lows)
            aa = med([atr[i] for i in lows]) or c[-1]["close"] * .005
            us = slope([(i, c[i]["high"]) for i in rh if upper[2][0] <= i <= upper[2][2]])
            if spread <= aa * .9 and us < 0:
                score = min(95, 70 + (upper[0] - 3) * 5)
                out.append({"type": "Descending Triangle", "bias": "BEARISH",
                            "upper": upper[2], "lower_level": level,
                            "lower_points": lows, "touches": upper[0] + len(lows),
                            "score": score, "start": min(rh[-5], rl[-5]),
                            "end": max(rh[-1], rl[-1])})

    # Double / triple bottom and double top.
    tol_base = 0.003
    if len(rl) >= 2:
        for k in range(max(0, len(rl) - 5), len(rl) - 1):
            a, b = rl[k], rl[k + 1]
            if b - a < 5:
                continue
            tol = max((atr[b] or 0) * .9, c[b]["close"] * tol_base)
            if abs(c[a]["low"] - c[b]["low"]) <= tol:
                neckline = max(x["high"] for x in c[a:b + 1])
                typ = "Double Bottom"
                pts = [a, b]
                if k > 0:
                    z = rl[k - 1]
                    if abs(c[z]["low"] - c[a]["low"]) <= tol:
                        typ, pts = "Triple Bottom", [z, a, b]
                out.append({"type": typ, "bias": "BULLISH", "level": neckline,
                            "points": pts, "touches": len(pts),
                            "score": 78 if typ == "Triple Bottom" else 70,
                            "start": pts[0], "end": pts[-1]})
    if len(rh) >= 2:
        for k in range(max(0, len(rh) - 5), len(rh) - 1):
            a, b = rh[k], rh[k + 1]
            if b - a < 5:
                continue
            tol = max((atr[b] or 0) * .9, c[b]["close"] * tol_base)
            if abs(c[a]["high"] - c[b]["high"]) <= tol:
                neckline = min(x["low"] for x in c[a:b + 1])
                out.append({"type": "Double Top", "bias": "BEARISH",
                            "level": neckline, "points": [a, b], "touches": 2,
                            "score": 70, "start": a, "end": b})
    return [p for p in out if p["score"] >= MIN_SCORE]

def levels(p, i):
    t=p["type"]
    if t in ("Falling Wedge","Rising Wedge","Descending Channel","Symmetrical Triangle"):
        return lv(p["upper"],i),lv(p["lower"],i)
    if t=="Descending Triangle": return lv(p["upper"],i),p["lower_level"]
    if t in ("Double Bottom","Triple Bottom"): return p["level"],None
    if t=="Double Top": return None,p["level"]
    return None,None


def bull_candle(k,a):
    rng=max(k["high"]-k["low"],1e-12)
    return k["close"]>k["open"] and abs(k["close"]-k["open"])>=a*.30 and (k["close"]-k["low"])/rng>=.55


def bear_candle(k,a):
    rng=max(k["high"]-k["low"],1e-12)
    return k["close"]<k["open"] and abs(k["close"]-k["open"])>=a*.30 and (k["high"]-k["close"])/rng>=.55


def make_signal(symbol,c,p,atr,hs,ls):
    cur=len(c)-1
    end=min(p.get("end",cur-3),cur-2)
    start=max(p.get("start",0),cur-MAX_PATTERN_BARS)
    if end<=start: return None
    direction=bi_level=None; bi=None
    for i in range(end+1,cur):
        a=atr[i] or med(atr[-20:]) or c[i]["close"]*.005
        u,l=levels(p,i)
        prev=c[i-1]; k=c[i]
        if p["bias"] in ("BULLISH","NEUTRAL") and u is not None:
            if prev["close"]<=u+a*.10 and k["close"]>u+a*BREAKOUT_ATR and bull_candle(k,a):
                direction,bi,bi_level="LONG",i,u; break
        if p["bias"] in ("BEARISH","NEUTRAL") and l is not None:
            if prev["close"]>=l-a*.10 and k["close"]<l-a*BREAKOUT_ATR and bear_candle(k,a):
                direction,bi,bi_level="SHORT",i,l; break
    if bi is None: return None

    # Retest must interact with the *broken boundary itself*. For a wedge
    # this is a sloped trendline, not a frozen horizontal breakout price.
    breakout_line = None
    if direction == "LONG":
        breakout_line = p.get("upper")
    elif direction == "SHORT":
        breakout_line = p.get("lower")

    retest=None; confirm=None; retest_level=None
    for r in range(bi+1,min(cur,bi+MAX_RETEST_BARS)+1):
        a=atr[r] or atr[bi] or c[r]["close"]*.005
        k=c[r]
        test_level = lv(breakout_line, r) if breakout_line else bi_level
        touched=k["low"]<=test_level+a*RETEST_ATR and k["high"]>=test_level-a*RETEST_ATR
        if not touched: continue
        if direction=="LONG" and k["close"]>=test_level:
            retest=r; retest_level=test_level
            if r+1<=cur and bull_candle(c[r+1],atr[r+1] or a): confirm=r+1; break
        if direction=="SHORT" and k["close"]<=test_level:
            retest=r; retest_level=test_level
            if r+1<=cur and bear_candle(c[r+1],atr[r+1] or a): confirm=r+1; break
    if confirm!=cur: return None

    entry=c[cur]["close"]
    if direction=="LONG":
        lows=[i for i in ls if max(p.get("start",0),retest-12)<=i<=retest]
        base=c[lows[-1]]["low"] if lows else min(x["low"] for x in c[max(0,retest-8):retest+1])
        sl=base-(atr[retest] or atr[cur]) * SL_ATR_BUFFER
        risk=entry-sl
        if risk<=0:return None
        tp1=entry+risk*MIN_RR; tp2=entry+risk*TP2_R
    else:
        highs=[i for i in hs if max(p.get("start",0),retest-12)<=i<=retest]
        base=c[highs[-1]]["high"] if highs else max(x["high"] for x in c[max(0,retest-8):retest+1])
        sl=base+(atr[retest] or atr[cur]) * SL_ATR_BUFFER
        risk=sl-entry
        if risk<=0:return None
        tp1=entry-risk*MIN_RR; tp2=entry-risk*TP2_R
    risk_pct=abs(entry-sl)/entry*100
    if risk_pct<.15 or risk_pct>12: return None
    return {"symbol":symbol,"side":direction,"pattern":p["type"],"score":p["score"],
            "touches":p["touches"],"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,
            "rr":abs(tp2-entry)/abs(entry-sl),"risk_pct":risk_pct,"candle_ts":c[cur]["ts"],
            "breakout_idx":bi,"retest_idx":retest,"confirm_idx":confirm,
            "breakout_level":bi_level,"retest_level":retest_level,
            "breakout_line":breakout_line,
            "pattern_data":p,"key":(symbol,c[cur]["ts"],direction,p["type"],bi)}


def find_signal(symbol,c):
    if len(c)<120:return None
    atr=atrs(c)
    if not atr[-1]:return None
    hs,ls=pivots(c)
    patterns=detect_patterns(c,hs,ls,atr)
    found=[]
    for p in patterns:
        s=make_signal(symbol,c,p,atr,hs,ls)
        if s:found.append(s)
    return max(found,key=lambda x:(x["score"],x["rr"],x["touches"])) if found else None


def chart(c, s):
    """Professional dark TradingView-style signal chart."""
    if not SEND_CHART:
        return None
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle, FancyBboxPatch
        from matplotlib.ticker import MaxNLocator
        from datetime import datetime, timezone

        p = s["pattern_data"]
        # Keep the whole detected pattern visible, with a small context margin.
        pat_start = max(0, int(p.get("start", 0)) - 12)
        pat_end = min(len(c) - 1, int(p.get("end", len(c) - 1)) + 18)
        span = pat_end - pat_start + 1
        n = min(len(c), max(CHART_BARS, span))
        off = max(0, min(len(c) - n, pat_start))
        data = c[off:off + n]

        bg = "#07111d"
        panel = "#0b1726"
        grid = "#294054"
        text = "#e8f0f7"
        muted = "#91a4b7"
        bull = "#20d39b"
        bear = "#ff536d"
        blue = "#1ea7ff"
        cyan = "#44c7ff"
        gold = "#f4c45f"

        fig, ax = plt.subplots(figsize=(15.5, 8.7), dpi=160)
        fig.patch.set_facecolor(bg)
        ax.set_facecolor(bg)

        # Candles.
        for x, k in enumerate(data):
            o, h, lo, cl = k["open"], k["high"], k["low"], k["close"]
            col = bull if cl >= o else bear
            ax.vlines(x, lo, h, color=col, lw=0.9, alpha=0.95, zorder=2)
            body = max(abs(cl - o), (h - lo) * 0.012)
            ax.add_patch(Rectangle((x - 0.31, min(o, cl)), 0.62, body,
                                   facecolor=col, edgecolor=col, lw=0.25, zorder=3))

        def plot_line(line, color, lw=2.4, ls="-", end_abs=None):
            if not line:
                return
            a, p1, b, p2 = line
            # Draw the real boundary from its first anchor. Never extend it
            # to the right edge of the chart. The broken boundary may receive
            # a short dashed extension through the retest.
            x_start_abs = max(a, off)
            x_end_abs = min(end_abs if end_abs is not None else p.get("end", b), len(c) - 1)
            x_start = x_start_abs - off
            x_end = x_end_abs - off
            if x_end < 0 or x_start > n - 1 or x_end <= x_start:
                return
            ax.plot([x_start, x_end], [lv(line, x_start_abs), lv(line, x_end_abs)],
                    color=color, lw=lw, ls=ls, solid_capstyle="round", zorder=4)

        broken_line = s.get("breakout_line")
        pattern_end = int(p.get("end", 0))
        retest_end = int(s.get("retest_idx", pattern_end))

        if p["type"] in ("Falling Wedge", "Rising Wedge", "Symmetrical Triangle", "Descending Channel"):
            # Both boundaries stop at the pattern. The broken boundary gets a
            # short dashed continuation only as far as the retest.
            plot_line(p.get("upper"), blue, end_abs=pattern_end)
            plot_line(p.get("lower"), cyan, end_abs=pattern_end)
            if broken_line and retest_end > pattern_end:
                plot_line(broken_line, blue if broken_line == p.get("upper") else cyan,
                          lw=2.0, ls="--", end_abs=retest_end)
        elif p["type"] == "Descending Triangle":
            plot_line(p.get("upper"), blue, end_abs=pattern_end)
            ax.axhline(p["lower_level"], color=cyan, lw=2.2, zorder=4)
        elif p["type"] in ("Double Bottom", "Triple Bottom"):
            ax.axhline(p["level"], color=cyan, lw=2.2, zorder=4)
        elif p["type"] == "Double Top":
            ax.axhline(p["level"], color=gold, lw=2.2, zorder=4)

        # Touch markers for actual boundary pivots.
        upper_pts = p.get("upper_points", [])
        lower_pts = p.get("lower_points", [])
        def three_touch_points(points):
            pts = [i for i in points if off <= i < off + n]
            if len(pts) <= 3:
                return pts
            return [pts[0], pts[len(pts)//2], pts[-1]]

        for num, i in enumerate(three_touch_points(upper_pts), 1):
            x = i - off; y = c[i]["high"]
            ax.scatter(x, y, s=72, facecolors=bg, edgecolors=blue, lw=1.8, zorder=7)
            ax.annotate(f"Touch {num}", (x, y), xytext=(0, 16), textcoords="offset points",
                        ha="center", color=blue, fontsize=9, fontweight="bold", zorder=8)
        for num, i in enumerate(three_touch_points(lower_pts), 1):
            x = i - off; y = c[i]["low"]
            ax.scatter(x, y, s=72, facecolors=bg, edgecolors=cyan, lw=1.8, zorder=7)
            ax.annotate(f"Touch {num}", (x, y), xytext=(0, -18), textcoords="offset points",
                        ha="center", color=cyan, fontsize=9, fontweight="bold", zorder=8)

        # Breakout, retest and confirmation.
        bx = s["breakout_idx"] - off
        rx = s["retest_idx"] - off
        ex = s["confirm_idx"] - off
        if 0 <= bx < n:
            y = c[s["breakout_idx"]]["close"]
            ax.annotate("Breakout", (bx, y), xytext=(bx - 7, y + (ax.get_ylim()[1] - ax.get_ylim()[0]) * .09),
                        color=text, fontsize=10, fontweight="bold",
                        arrowprops=dict(arrowstyle="->", color=blue, lw=1.5), zorder=10)
        if 0 <= rx < n:
            y = c[s["retest_idx"]]["close"]
            ax.scatter(rx, y, s=90, facecolors=bg, edgecolors=blue, lw=2.0, zorder=8)
            ax.annotate("Retest", (rx, y), xytext=(rx + 3, y - (ax.get_ylim()[1] - ax.get_ylim()[0]) * .07),
                        color=blue, fontsize=10, fontweight="bold",
                        arrowprops=dict(arrowstyle="->", color=blue, lw=1.3), zorder=10)
        if 0 <= ex < n:
            y = s["entry"]
            ax.scatter(ex, y, s=90, facecolors=bull, edgecolors=bg, lw=1.5, zorder=9)
            ax.annotate("Entry (Buy)" if s["side"] == "LONG" else "Entry (Sell)",
                        (ex, y), xytext=(ex + 3, y + (ax.get_ylim()[1] - ax.get_ylim()[0]) * .035),
                        color=bull if s["side"] == "LONG" else bear, fontsize=10.5, fontweight="bold",
                        bbox=dict(boxstyle="round,pad=.28", fc=panel, ec=bull if s["side"] == "LONG" else bear, lw=1.2),
                        arrowprops=dict(arrowstyle="->", color=bull if s["side"] == "LONG" else bear, lw=1.2), zorder=10)

        # Subtle risk/reward shading, matching the reference chart while
        # keeping candles and trendlines visually dominant.
        if 0 <= ex < n:
            rr_x = ex
            x_to = min(n - 1, ex + max(8, int(n * 0.16)))
            ax.add_patch(Rectangle((rr_x, min(s["entry"], s["sl"])), x_to - rr_x, abs(s["entry"] - s["sl"]),
                                   facecolor=bear, edgecolor="none", alpha=.055, zorder=0))
            ax.add_patch(Rectangle((rr_x, min(s["entry"], s["tp2"])), x_to - rr_x, abs(s["tp2"] - s["entry"]),
                                   facecolor=bull, edgecolor="none", alpha=.055, zorder=0))

        # Price levels with right-side labels.
        levels_to_draw = [
            (s["sl"], bear, "SL"),
            (s["tp1"], bull, "TP1"),
            (s["tp2"], bull, "TP2"),
        ]
        ax.axhline(s["entry"], color=text, lw=1.1, ls=(0, (4, 4)), alpha=.75, zorder=1)
        for y, col, label in levels_to_draw:
            ax.axhline(y, color=col, lw=1.25, ls=(0, (6, 4)), alpha=.9, zorder=1)
            ax.text(1.005, y, f" {label}  {fmt(y)}", transform=ax.get_yaxis_transform(),
                    va="center", ha="left", color=col, fontsize=9.5, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=.22", fc=panel, ec=col, lw=.8))
        ax.text(1.005, s["entry"], f" ENTRY  {fmt(s['entry'])}", transform=ax.get_yaxis_transform(),
                va="center", ha="left", color=text, fontsize=9.2, fontweight="bold")

        # Header and pattern badge.
        side_name = "Bullish Reversal" if s["side"] == "LONG" else "Bearish Reversal"
        fig.text(.035, .958, s["symbol"], color=text, fontsize=17, fontweight="bold", va="top")
        fig.text(.155, .958, "1H", color=bg, fontsize=10, fontweight="bold", va="top",
                 bbox=dict(boxstyle="round,pad=.35", fc=blue, ec=blue))
        last = c[-1]
        fig.text(.205, .958,
                 f"O {fmt(last['open'])}   H {fmt(last['high'])}   L {fmt(last['low'])}   C {fmt(last['close'])}",
                 color=muted, fontsize=10.5, va="top")
        fig.text(.965, .958, f"{s['side']}  •  {s['pattern']}", color=bull if s["side"] == "LONG" else bear,
                 fontsize=11, fontweight="bold", ha="right", va="top")

        fig.text(.035, .900, s["pattern"].upper(), color=text, fontsize=22, fontweight="bold", va="top",
                 bbox=dict(boxstyle="round,pad=.42", fc=panel, ec=blue, lw=1.2, alpha=.96))
        fig.text(.038, .855, f"({side_name})", color=muted, fontsize=11, va="top")

        # Bottom-left checklist panel.
        checks = [
            f"Pattern: {s['pattern']}",
            f"{p.get('upper_touches', 0)} upper / {p.get('lower_touches', 0)} lower touches",
            "Trendlines converge",
            "Breakout + Retest",
            "Confirmation candle",
            "Entry / SL / TP",
        ]
        box = FancyBboxPatch((.035, .065), .255, .245, transform=fig.transFigure,
                             boxstyle="round,pad=.012", facecolor=panel, edgecolor=grid, lw=1.0, alpha=.97)
        fig.add_artist(box)
        fig.text(.052, .286, "SETUP CHECK", color=text, fontsize=11.5, fontweight="bold")
        for j, item in enumerate(checks):
            fig.text(.052, .253 - j * .034, f"✓  {item}", color=muted if j else text, fontsize=9.2)

        # Axes styling.
        start_dt = datetime.fromtimestamp(data[0]["ts"] / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(data[-1]["ts"] / 1000, tz=timezone.utc)
        tick_count = min(7, max(4, n // 18))
        ticks = [round(i) for i in __import__("numpy").linspace(0, n - 1, tick_count)]
        ax.set_xticks(ticks)
        labels = []
        for x in ticks:
            dt = datetime.fromtimestamp(data[int(x)]["ts"] / 1000, tz=timezone.utc)
            labels.append(dt.strftime("%b %d\n%H:%M"))
        ax.set_xticklabels(labels, color=muted, fontsize=8.5)
        ax.yaxis.tick_right()
        ax.yaxis.set_label_position("right")
        ax.tick_params(axis="y", colors=muted, labelsize=8.5, length=0)
        ax.grid(True, color=grid, alpha=.34, lw=.55)
        ax.set_xlim(-1, n + 9)
        ax.margins(y=.08)
        for side in ("top", "left", "bottom"):
            ax.spines[side].set_visible(False)
        ax.spines["right"].set_color(grid)
        ax.spines["right"].set_alpha(.8)
        ax.set_title(f"Breakout → Retest → Confirmation     •     RR {s['rr']:.2f}     •     Risk {s['risk_pct']:.2f}%",
                     color=muted, fontsize=9.5, pad=13, loc="left")

        fig.subplots_adjust(left=.035, right=.925, top=.775, bottom=.085)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=160, facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception as e:
        with state_lock:
            state["last_error"] = f"chart: {e}"
        return None

def tg_text(s):
    icon="🟢" if s["side"]=="LONG" else "🔴"
    dt=datetime.fromtimestamp(s["candle_ts"]/1000,tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (f"📊 SAIWAN 1H PATTERN SIGNAL\n\n{icon} {s['side']} • {s['symbol']}\n"
            f"🔎 Pattern: {s['pattern']}\n⭐ Score: {s['score']}/100\n📐 Touches: {s['touches']}\n"
            f"⏱ Timeframe: 1H CLOSED\n\n🎯 ENTRY: {fmt(s['entry'])}\n🛑 SL: {fmt(s['sl'])}\n"
            f"✅ TP1: {fmt(s['tp1'])} ({MIN_RR:.1f}R)\n✅ TP2: {fmt(s['tp2'])} ({TP2_R:.1f}R)\n"
            f"📏 Risk: {s['risk_pct']:.2f}%\n⚖️ RR: {s['rr']:.2f}\n\n"
            f"🔹 Breakout → Retest → Confirmation\n🏦 Bitget USDT Perpetual\n🕒 {dt}\n\n⚠️ Signal only — no automatic order.")


def tg_send(text, reply_to=None, parse_mode=None):
    if not BOT_TOKEN or not CHAT_ID: raise RuntimeError("Telegram variables are missing")
    payload={"chat_id":CHAT_ID,"text":text,"disable_web_page_preview":True}
    if reply_to:
        payload["reply_parameters"]={"message_id":int(reply_to),"allow_sending_without_reply":True}
    if parse_mode:
        payload["parse_mode"]=parse_mode
    r=http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",json=payload,timeout=20); r.raise_for_status()
    return (r.json().get("result") or {}).get("message_id")


def tg_reply(text, message_id):
    return tg_send(text, reply_to=message_id, parse_mode="HTML")


def send_signal(s,c):
    # One Telegram message: the chart carries the full signal as its caption.
    # This gives every future TP/SL update a single message to reply to.
    im=chart(c,s)
    if im is not None:
        try:
            payload={"chat_id":CHAT_ID,"caption":tg_text(s),"parse_mode":"HTML"}
            r=http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",data=payload,files={"photo":("saiwan_1h.png",im,"image/png")},timeout=30)
            r.raise_for_status()
            msg_id=(r.json().get("result") or {}).get("message_id")
            if not msg_id: raise RuntimeError("Telegram did not return message_id")
            s["telegram_message_id"]=int(msg_id)
            return
        except Exception as e:
            with state_lock: state["last_error"]=f"telegram chart: {e}"
    msg_id=tg_send(tg_text(s))
    s["telegram_message_id"]=int(msg_id) if msg_id else None


def register_active_signal(s):
    mid=s.get("telegram_message_id")
    if not mid:
        return
    key=f"{s['symbol']}|{s['candle_ts']}|{s['side']}|{s['pattern']}|{s['breakout_idx']}"
    item=dict(s)
    item["tracking_key"]=key
    item["tp1_hit"]=False
    item["tp2_hit"]=False
    item["sl_hit"]=False
    item["last_checked_ts"]=0
    with active_lock:
        active_signals[key]=item


def hit_update_text(s, event, price, ts):
    icon={"TP1":"🎯","TP2":"🏁","SL":"🛑"}[event]
    when=datetime.fromtimestamp(ts/1000,tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (f"{icon} <b>{event} HIT</b>\n"
            f"{s['symbol']} • {s['side']} • {s['pattern']}\n"
            f"Price: <b>{fmt(price)}</b>\n"
            f"Time: {when}")


def track_active_signals():
    with active_lock:
        snapshot=list(active_signals.items())
    for key,s in snapshot:
        try:
            c=get_candles(s["symbol"])
            if not c: continue
            # Only inspect candles that appeared after the signal candle.
            candles=[k for k in c if k["ts"]>s["candle_ts"]]
            if not candles: continue
            for k in candles:
                ts=k["ts"]
                if ts<=s.get("last_checked_ts",0): continue
                high,low=k["high"],k["low"]
                events=[]
                if s["side"]=="LONG":
                    tp1_hit=high>=s["tp1"]
                    tp2_hit=high>=s["tp2"]
                    sl_hit=low<=s["sl"]
                else:
                    tp1_hit=low<=s["tp1"]
                    tp2_hit=low<=s["tp2"]
                    sl_hit=high>=s["sl"]

                # If SL and a target are both inside the same 1H candle, the
                # API gives OHLC but not the intrabar order. Do not invent an
                # order; report the candle as ambiguous and keep tracking.
                both_target_sl = sl_hit and (tp1_hit or tp2_hit)
                if both_target_sl:
                    if not s.get("ambiguous_logged"):
                        tg_reply(f"⚠️ <b>Same 1H candle touched SL and target</b>\n{ s['symbol'] }\nIntrabar order is unknown, so no HIT is declared for this candle.", s["telegram_message_id"])
                        s["ambiguous_logged"]=True
                    s["last_checked_ts"]=ts
                    continue

                if tp1_hit and not s.get("tp1_hit"):
                    events.append(("TP1",s["tp1"]))
                    s["tp1_hit"]=True
                if tp2_hit and not s.get("tp2_hit"):
                    # If TP2 is reached directly, TP1 is necessarily crossed
                    # for normal long/short geometry, so report both in order.
                    if not s.get("tp1_hit"):
                        events.append(("TP1",s["tp1"]))
                        s["tp1_hit"]=True
                    events.append(("TP2",s["tp2"]))
                    s["tp2_hit"]=True
                if sl_hit and not s.get("sl_hit"):
                    events.append(("SL",s["sl"]))
                    s["sl_hit"]=True

                for event,price in events:
                    tg_reply(hit_update_text(s,event,price,ts),s["telegram_message_id"])

                s["last_checked_ts"]=ts
                # SL ends the trade. TP2 also ends the trade. TP1 remains
                # active so a later TP2 or SL can still be reported.
                if s.get("sl_hit") or s.get("tp2_hit"):
                    with active_lock:
                        active_signals.pop(key,None)
                    break
            with active_lock:
                if key in active_signals:
                    active_signals[key]=s
        except Exception as e:
            with state_lock: state["last_error"]=f"tracker {s['symbol']}: {e}"


def scan_once():
    if not scan_lock.acquire(False):return
    try:
        symbols=get_symbols()
        with state_lock: state["symbols"]=len(symbols)
        for symbol in symbols:
            try:
                c=get_candles(symbol)
                if len(c)<120:continue
                candle_key=(symbol,c[-1]["ts"])
                if candle_key in processed:continue
                processed.add(candle_key)
                s=find_signal(symbol,c)
                if not s:continue
                if s["key"] in processed:continue
                if COOLDOWN_HOURS and time.time()-last_signal_at.get(symbol,0)<COOLDOWN_HOURS*3600:continue
                send_signal(s,c); register_active_signal(s); processed.add(s["key"]); last_signal_at[symbol]=time.time()
                with state_lock:
                    state["signals_sent"]+=1; state["last_signal"]=f"{symbol} {s['side']} {s['pattern']} @ {fmt(s['entry'])}"
            except Exception as e:
                with state_lock: state["last_error"]=f"{symbol}: {e}"
    finally:scan_lock.release()


def scanner_loop():
    with state_lock:state["running"]=True
    while True:
        try:
            scan_once()
            track_active_signals()
            with state_lock:state["last_scan"]=now_utc()
        except Exception as e:
            with state_lock:state["last_error"]=str(e)
        time.sleep(SCAN_SECONDS)


def telegram_loop():
    global telegram_offset
    if not BOT_TOKEN or not CHAT_ID:
        with state_lock:state["last_error"]="TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing"
        return
    while True:
        try:
            r=http.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",params={"timeout":25,"offset":telegram_offset},timeout=35); r.raise_for_status(); d=r.json()
            for u in d.get("result",[]):
                telegram_offset=u["update_id"]+1; m=u.get("message") or {}; text=(m.get("text") or "").strip().lower()
                if str(m.get("chat",{}).get("id"))!=str(CHAT_ID):continue
                if text=="/start":tg_send("🤖 SAIWAN 1H Pattern Bot is online.\n\nBreakout → Retest → Confirmation\nUse /status or /scan")
                elif text=="/scan":
                    tg_send("🔎 Manual 1H scan started...")
                    try:scan_once();tg_send("✅ 1H scan completed.")
                    except Exception as e:tg_send(f"❌ Scan error: {e}")
                elif text=="/status":
                    with state_lock:s=dict(state)
                    with active_lock: active_count=len(active_signals)
                    tg_send("🤖 SAIWAN STATUS\n\nBot: ONLINE\nScanner: %s\nMarket: Bitget USDT Perpetual\nTimeframe: 1H\nSymbols: %s\nSignals sent: %s\nActive tracked signals: %s\nLast scan: %s\nLast signal: %s\nLast error: %s" % ("RUNNING" if s["running"] else "STARTING",s["symbols"],s["signals_sent"],active_count,s["last_scan"] or "not yet",s["last_signal"] or "none",s["last_error"] or "none"))
        except Exception as e:
            with state_lock:state["last_error"]=f"Telegram: {e}"
            time.sleep(5)


@app.get("/")
def home():
    with state_lock:return jsonify({"bot":"SAIWAN","strategy":"1H Pattern Breakout + Retest","status":"online",**state})

@app.get("/status")
def status():
    with state_lock:return jsonify(dict(state))

# Railway/Gunicorn: use ONE worker, otherwise every worker would start a scanner.
if not getattr(app,"_saiwan_started",False):
    app._saiwan_started=True
    threading.Thread(target=scanner_loop,daemon=True,name="scanner").start()
    threading.Thread(target=telegram_loop,daemon=True,name="telegram").start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")))
