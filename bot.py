"""
Crypto Signal Bot (Bitget USDT Futures, public data only, signal-only)

Features:
- Closed-candle signals only (no repainting)
- Higher-timeframe (1h) trend filter + BTC filter
- LONG and SHORT, ATR-based stop loss (adapts to volatility)
- Honest scoring (no fake "confidence %")
- Every signal saved to SQLite; outcomes tracked automatically (TP1/TP2/TP3/SL)
- /stats shows REAL win rate and R results
- Cooldown per symbol, max open signals
"""
import os
import time
import math
import sqlite3
import logging
import threading
import requests
import pandas as pd
from collections import Counter

# ---------------- CONFIG (env variables) ----------------
TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = str(os.environ["CHAT_ID"])
DB_PATH = os.getenv("DB_PATH", "signals.db")      # on Railway: mount a volume, e.g. /data/signals.db
TF = os.getenv("TF", "15m")                       # signal timeframe
HTF = os.getenv("HTF", "1h")                      # trend timeframe
TOP_N = int(os.getenv("TOP_N", "40"))             # scan top N coins by volume
MIN_24H_VOLUME = float(os.getenv("MIN_24H_VOLUME", "5000000"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "5"))      # out of 6 extra checks
ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "1.5"))
MAX_LEV = int(os.getenv("MAX_LEV", "3"))
COOLDOWN_H = float(os.getenv("COOLDOWN_H", "8"))
MAX_OPEN = int(os.getenv("MAX_OPEN", "5"))
EXPIRE_H = float(os.getenv("EXPIRE_H", "48"))
MIN_VOL_RATIO = float(os.getenv("MIN_VOL_RATIO", "1.5"))
ENABLE_CHART = os.getenv("ENABLE_CHART", "1") == "1"

BASE = "https://api.bitget.com"
TF_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000}
PRODUCT = "USDT-FUTURES"
# Bitget wants uppercase H for hours (1H, 4H)
BG_GRAN = {"1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1H", "4h": "4H"}
SCAN_STATS = Counter()
LAST_SCAN = {}
scan_lock = threading.Lock()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")
db_lock = threading.Lock()


# ---------------- DATABASE ----------------
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


conn = db()
conn.execute("""CREATE TABLE IF NOT EXISTS signals(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER, symbol TEXT, side TEXT,
    entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
    score INTEGER, rsi REAL, vol REAL,
    status TEXT DEFAULT 'OPEN', tp_hit INTEGER DEFAULT 0,
    stop REAL, last_ts INTEGER, closed_ts INTEGER, r REAL)""")
try:
    conn.execute("ALTER TABLE signals ADD COLUMN msg_id INTEGER")
except sqlite3.OperationalError:
    pass  # column already exists
conn.commit()


def q(sql, args=(), commit=False):
    with db_lock:
        cur = conn.execute(sql, args)
        if commit:
            conn.commit()
        return cur.fetchall()


# ---------------- TELEGRAM ----------------
def tg(method, **payload):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TOKEN}/{method}", json=payload, timeout=40)
        return r.json()
    except Exception as e:
        log.warning("telegram error: %s", e)
        return {}


def send(text, symbol=None, reply_to=None):
    """Sends a message; if reply_to is given, replies to that original signal message."""
    payload = dict(chat_id=CHAT_ID, text=text, parse_mode="HTML", disable_web_page_preview=True)
    if symbol:
        url = f"https://www.tradingview.com/chart/?symbol=BITGET:{symbol}.P"
        payload["reply_markup"] = {"inline_keyboard": [[{"text": "📈 TradingView", "url": url}]]}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
        payload["allow_sending_without_reply"] = True
    res = tg("sendMessage", **payload)
    return (res.get("result") or {}).get("message_id")


def fmt(p):
    dec = max(2, 4 - int(math.floor(math.log10(abs(p)))))
    return f"{p:.{dec}f}"


# ---------------- DATA ----------------
def get_json(path, params):
    last = None
    for _ in range(3):
        try:
            r = requests.get(BASE + path, params=params, timeout=15)
            d = r.json()
            if d.get("code") == "00000":
                return d["data"]
            last = f"{d.get('code')} {d.get('msg')}"
            break                      # API rejected the request: retrying will not help
        except Exception as e:
            last = str(e)
            time.sleep(1)
    log.warning("API failed %s %s -> %s", path, params, last)
    SCAN_STATS["api_errors"] += 1
    return None


def candles(symbol, tf, limit=250, closed_only=True):
    data = get_json("/api/v2/mix/market/candles",
                    {"symbol": symbol, "productType": PRODUCT, "granularity": BG_GRAN[tf], "limit": limit})
    if not data:
        return None
    df = pd.DataFrame(data).iloc[:, :6]
    df.columns = ["ts", "o", "h", "l", "c", "v"]
    df = df.astype(float).sort_values("ts").reset_index(drop=True)
    df["ts"] = df["ts"].astype("int64")
    if closed_only:
        df = df[df["ts"] + TF_MS[tf] <= time.time() * 1000].reset_index(drop=True)
    return df if len(df) > 210 else None


def top_symbols():
    data = get_json("/api/v2/mix/market/tickers", {"productType": PRODUCT})
    if not data:
        return []
    rows = []
    for t in data:
        vol = float(t.get("usdtVolume") or t.get("quoteVolume") or 0)
        if t["symbol"].endswith("USDT") and vol >= MIN_24H_VOLUME:
            rows.append((vol, t["symbol"]))
    rows.sort(reverse=True)
    return [s for _, s in rows[:TOP_N]]


# ---------------- INDICATORS ----------------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def rsi(s, n=14):
    d = s.diff()
    g = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + g / l.replace(0, 1e-12))


def atr(df, n=14):
    pc = df["c"].shift()
    tr = pd.concat([df["h"] - df["l"], (df["h"] - pc).abs(), (df["l"] - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def htf_bias(df):
    """+1 bullish, -1 bearish, 0 neutral (EMA50 vs EMA200 and price)"""
    c = df["c"]
    e50, e200 = ema(c, 50).iloc[-1], ema(c, 200).iloc[-1]
    if c.iloc[-1] > e200 and e50 > e200:
        return 1
    if c.iloc[-1] < e200 and e50 < e200:
        return -1
    return 0


# ---------------- STRATEGY ----------------
def analyze(symbol, btc_bias):
    st = SCAN_STATS
    df = candles(symbol, TF)
    if df is None:
        st["no_data"] += 1
        return None
    c, h, l, o, v = df["c"], df["h"], df["l"], df["o"], df["v"]
    price = c.iloc[-1]
    rng = h.iloc[-1] - l.iloc[-1]
    avg_v = v.iloc[-21:-1].mean()
    if rng <= 0 or avg_v <= 0:
        st["no_data"] += 1
        return None
    vol_ratio = v.iloc[-1] / avg_v

    # ---- cheap gates on the signal timeframe first ----
    up = price > h.iloc[-21:-1].max()
    dn = price < l.iloc[-21:-1].min()
    if not (up or dn):
        st["no_breakout"] += 1
        return None
    side = "LONG" if up else "SHORT"
    d = 1 if up else -1
    if side == "LONG":
        strong = price > o.iloc[-1] and (price - l.iloc[-1]) / rng >= 0.7
    else:
        strong = price < o.iloc[-1] and (h.iloc[-1] - price) / rng >= 0.7
    if not strong:
        st["weak_candle"] += 1
        return None
    if vol_ratio < MIN_VOL_RATIO:
        st["low_volume"] += 1
        return None

    # ---- higher-timeframe trend (only fetched for real candidates) ----
    hdf = candles(symbol, HTF)
    if hdf is None:
        st["no_data"] += 1
        return None
    if htf_bias(hdf) != d:
        st["against_htf"] += 1
        return None

    e20, e50 = ema(c, 20), ema(c, 50)
    r = rsi(c)
    a_now = atr(df).iloc[-1]

    # ---- scored checks ----
    if side == "LONG":
        checks = {
            "EMA alignment (20>50)": e20.iloc[-1] > e50.iloc[-1],
            "RSI 50-68": 50 <= r.iloc[-1] <= 68,
            "Higher-low structure": l.iloc[-5:].min() > l.iloc[-15:-5].min(),
        }
    else:
        checks = {
            "EMA alignment (20<50)": e20.iloc[-1] < e50.iloc[-1],
            "RSI 32-50": 32 <= r.iloc[-1] <= 50,
            "Lower-high structure": h.iloc[-5:].max() < h.iloc[-15:-5].max(),
        }
    checks["Not overextended (<2 ATR from EMA20)"] = abs(price - e20.iloc[-1]) < 2 * a_now
    checks["No spike candle (<2.5 ATR)"] = rng < 2.5 * a_now
    checks["BTC not against"] = btc_bias in (0, d)
    score = sum(checks.values())
    if score < MIN_SCORE:
        st["low_score"] += 1
        return None

    sl_dist = ATR_SL_MULT * a_now
    entry = price
    sl = entry - d * sl_dist
    tps = [entry + d * sl_dist * m for m in (1.0, 2.0, 3.0)]
    sl_pct = sl_dist / entry
    lev = max(1, min(MAX_LEV, int(0.5 / sl_pct)))
    return dict(symbol=symbol, side=side, entry=entry, sl=sl, tp=tps, score=score,
                rsi=float(r.iloc[-1]), vol=float(vol_ratio), sl_pct=sl_pct, lev=lev,
                checks=checks, candle_ts=int(df["ts"].iloc[-1]), df=df)


def make_chart(s, bars=90):
    """Returns PNG bytes: candlesticks + EMA20/50 + entry/SL/TP zones."""
    import io
    from datetime import datetime, timezone
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle

    full = s["df"]
    df = full.tail(bars).reset_index(drop=True)
    e20 = ema(full["c"], 20).tail(bars).reset_index(drop=True)
    e50 = ema(full["c"], 50).tail(bars).reset_index(drop=True)
    n = len(df)
    bg, up, dn, txt = "#0f1419", "#26a69a", "#ef5350", "#d1d4dc"

    fig, ax = plt.subplots(figsize=(10, 6), dpi=110)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)
    for i in range(n):
        o, h, l, c = df.at[i, "o"], df.at[i, "h"], df.at[i, "l"], df.at[i, "c"]
        col = up if c >= o else dn
        ax.vlines(i, l, h, color=col, linewidth=1)
        ax.add_patch(Rectangle((i - 0.35, min(o, c)), 0.7, max(abs(c - o), (h - l) * 0.01 + 1e-12),
                               color=col, linewidth=0))
    ax.plot(range(n), e20, color="#f5a623", linewidth=1, label="EMA20")
    ax.plot(range(n), e50, color="#4aa3ff", linewidth=1, label="EMA50")

    entry, sl, tps = s["entry"], s["sl"], s["tp"]
    x0, x1 = n - 1, n + 11
    ax.fill_between([x0, x1], entry, tps[2], color=up, alpha=0.13, linewidth=0)
    ax.fill_between([x0, x1], sl, entry, color=dn, alpha=0.13, linewidth=0)
    levels = [(entry, "ENTRY", "#ffffff"), (sl, "SL", dn),
              (tps[0], "TP1", up), (tps[1], "TP2", up), (tps[2], "TP3", up)]
    for price, name, col in levels:
        ax.hlines(price, 0, x1, colors=col, linestyles="dashed", linewidth=0.8, alpha=0.8)
        ax.text(x1 + 0.3, price, f"{name} {fmt(price)}", color=col, fontsize=8, va="center")

    lo = min(df["l"].min(), sl, tps[0])
    hi = max(df["h"].max(), sl, tps[2])
    pad = (hi - lo) * 0.04
    ax.set_ylim(lo - pad, hi + pad)
    ax.set_xlim(-1, n + 22)
    step = max(1, n // 8)
    ticks = list(range(0, n, step))
    ax.set_xticks(ticks)
    ax.set_xticklabels([datetime.fromtimestamp(df.at[i, "ts"] / 1000, tz=timezone.utc).strftime("%d %H:%M")
                        for i in ticks], color=txt, fontsize=8)
    ax.tick_params(axis="y", colors=txt, labelsize=8)
    ax.yaxis.tick_left()
    ax.grid(color="#2a2e39", linewidth=0.5, alpha=0.6)
    for sp in ax.spines.values():
        sp.set_visible(False)
    arrow = "LONG" if s["side"] == "LONG" else "SHORT"
    ax.set_title(f"{s['symbol']} · {TF} · {arrow} · Bitget Futures (UTC)", color=txt, fontsize=11, loc="left")
    ax.legend(loc="upper left", facecolor=bg, edgecolor="none", labelcolor=txt, fontsize=8)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=bg, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def send_photo(png, caption):
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendPhoto",
                      data={"chat_id": CHAT_ID, "caption": caption, "parse_mode": "HTML"},
                      files={"photo": ("chart.png", png, "image/png")}, timeout=60)
    except Exception as e:
        log.warning("sendPhoto error: %s", e)


def format_signal(s):
    icon = "🟢" if s["side"] == "LONG" else "🔴"
    ck = "\n".join(f"{'✅' if ok else '❌'} {k}" for k, ok in s["checks"].items())
    wr = stats_text(short=True)
    return (
        f"🚀 <b>NEW SIGNAL</b>\n\n"
        f"{icon} <b>{s['side']}</b>  {s['symbol']} (Bitget Futures)\n"
        f"⏱ {TF}  |  Trend filter: {HTF}\n\n"
        f"Entry: <code>{fmt(s['entry'])}</code>  (only if price is still near)\n"
        f"Stop Loss: <code>{fmt(s['sl'])}</code>  ({s['sl_pct']*100:.2f}%)\n"
        f"TP1: <code>{fmt(s['tp'][0])}</code>  (1R) → move SL to entry\n"
        f"TP2: <code>{fmt(s['tp'][1])}</code>  (2R)\n"
        f"TP3: <code>{fmt(s['tp'][2])}</code>  (3R)\n\n"
        f"RSI {s['rsi']:.1f} | Volume {s['vol']:.2f}x\n"
        f"Score: {s['score']}/6\n{ck}\n\n"
        f"💰 Risk max 1% of account. Max leverage: {s['lev']}x\n"
        f"📊 {wr}\n\n"
        f"⚠️ Signal only. Not financial advice. No win is guaranteed."
    )


# ---------------- OUTCOME TRACKING ----------------
R_BY_TP_ON_STOP = {0: -1.0, 1: 0.33, 2: 1.0}   # 1/3 closed at each TP, rest stopped at breakeven


def track():
    rows = q("SELECT * FROM signals WHERE status='OPEN'")
    for s in rows:
        df = candles(s["symbol"], TF, limit=300, closed_only=True)
        time.sleep(0.15)
        if df is None:
            continue
        d = 1 if s["side"] == "LONG" else -1
        stop, tp_hit, last_ts = s["stop"], s["tp_hit"], s["last_ts"]
        tps = [s["tp1"], s["tp2"], s["tp3"]]
        final = None
        for _, k in df[df["ts"] > last_ts].iterrows():
            last_ts = int(k["ts"])
            # conservative: stop is checked first inside the same candle
            hit_stop = k["l"] <= stop if d == 1 else k["h"] >= stop
            if hit_stop:
                final = ("SL" if tp_hit == 0 else "BE", R_BY_TP_ON_STOP[tp_hit])
                break
            while tp_hit < 3 and ((k["h"] >= tps[tp_hit]) if d == 1 else (k["l"] <= tps[tp_hit])):
                tp_hit += 1
                send(f"🎯 <b>TP{tp_hit} HIT</b> — {s['symbol']} {s['side']}"
                     + ("\nMove SL to entry." if tp_hit == 1 else ""),
                     reply_to=s["msg_id"])
                if tp_hit == 1:
                    stop = s["entry"]
            if tp_hit == 3:
                final = ("TP3", 2.0)
                break
        if final is None and (time.time() * 1000 - s["ts"]) > EXPIRE_H * 3_600_000:
            final = ("EXPIRED", R_BY_TP_ON_STOP.get(tp_hit, 0.0) if tp_hit else 0.0)
        if final:
            status, rr = final
            q("UPDATE signals SET status=?, tp_hit=?, stop=?, last_ts=?, closed_ts=?, r=? WHERE id=?",
              (status, tp_hit, stop, last_ts, int(time.time() * 1000), rr, s["id"]), commit=True)
            if status == "SL":
                send(f"🛑 <b>SL HIT</b> — {s['symbol']} {s['side']}  ({rr:+.2f}R)", reply_to=s["msg_id"])
            elif status == "BE":
                send(f"⚪ <b>Closed at breakeven after TP{tp_hit}</b> — {s['symbol']} ({rr:+.2f}R)", reply_to=s["msg_id"])
            elif status == "TP3":
                send(f"🏆 <b>ALL TARGETS HIT</b> — {s['symbol']} {s['side']} ({rr:+.2f}R)", reply_to=s["msg_id"])
            else:
                send(f"⌛ Signal expired — {s['symbol']}", reply_to=s["msg_id"])
        else:
            q("UPDATE signals SET tp_hit=?, stop=?, last_ts=? WHERE id=?",
              (tp_hit, stop, last_ts, s["id"]), commit=True)


# ---------------- STATS ----------------
def stats_text(short=False):
    rows = q("SELECT * FROM signals WHERE status!='OPEN' AND status!='EXPIRED'")
    n = len(rows)
    if n < 20:
        return f"Real stats: not enough data yet ({n}/20 closed signals)"
    wins = sum(1 for r in rows if r["r"] > 0)
    tot = sum(r["r"] for r in rows)
    base = f"Real win rate: {wins/n*100:.0f}% ({n} signals) | Avg {tot/n:+.2f}R"
    if short:
        return base
    longs = [r for r in rows if r["side"] == "LONG"]
    shorts = [r for r in rows if r["side"] == "SHORT"]

    def part(name, lst):
        if not lst:
            return ""
        w = sum(1 for r in lst if r["r"] > 0)
        return f"\n{name}: {w}/{len(lst)} wins, {sum(r['r'] for r in lst):+.1f}R"
    sl = sum(1 for r in rows if r["status"] == "SL")
    tp3 = sum(1 for r in rows if r["status"] == "TP3")
    return (f"📊 <b>REAL STATS</b>\n{base}\nTotal: {tot:+.1f}R\n"
            f"SL: {sl} | Full TP3: {tp3}" + part("LONG", longs) + part("SHORT", shorts))


# ---------------- SCANNER ----------------
LABELS = [("no_data", "No data"), ("no_breakout", "No breakout"), ("weak_candle", "Weak candle"),
          ("low_volume", "Low volume"), ("against_htf", "Against HTF trend"),
          ("low_score", "Score too low"), ("cooldown", "Cooldown"),
          ("skipped_timeout", "Skipped (time limit)"), ("api_errors", "API errors")]


def scan_summary():
    if not LAST_SCAN:
        return "No scan has run yet."
    s = LAST_SCAN["stats"]
    age = int(time.time() - LAST_SCAN["ts"])
    lines = [f"Last scan: {age}s ago, took {LAST_SCAN['secs']:.0f}s",
             f"Checked: {s['checked']} | Signals: {s['signals']}"]
    lines += [f"• {label}: {s[k]}" for k, label in LABELS if s.get(k)]
    return "\n".join(lines)


def scan():
    global SCAN_STATS, LAST_SCAN
    SCAN_STATS = Counter()
    t0 = time.time()
    deadline = t0 + max(40, TF_MS[TF] / 1000 * 0.9)
    open_n = q("SELECT COUNT(*) c FROM signals WHERE status='OPEN'")[0]["c"]
    found = 0
    if open_n >= MAX_OPEN:
        log.info("max open reached")
        return -1
    btc = candles("BTCUSDT", HTF)
    btc_b = htf_bias(btc) if btc is not None else 0
    syms = top_symbols()
    if not syms:
        log.warning("no symbols returned from Bitget tickers")
    for i, sym in enumerate(syms):
        if time.time() > deadline:
            SCAN_STATS["skipped_timeout"] = len(syms) - i
            break
        if open_n >= MAX_OPEN:
            break
        recent = q("SELECT 1 FROM signals WHERE symbol=? AND ts>?",
                   (sym, int((time.time() - COOLDOWN_H * 3600) * 1000)))
        if recent:
            SCAN_STATS["cooldown"] += 1
            continue
        SCAN_STATS["checked"] += 1
        try:
            s = analyze(sym, btc_b)
        except Exception as e:
            log.warning("%s analyze error: %s", sym, e)
            continue
        time.sleep(0.1)
        if not s:
            continue
        q("""INSERT INTO signals(ts,symbol,side,entry,sl,tp1,tp2,tp3,score,rsi,vol,stop,last_ts)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (int(time.time() * 1000), sym, s["side"], s["entry"], s["sl"], *s["tp"],
           s["score"], s["rsi"], s["vol"], s["sl"], s["candle_ts"]), commit=True)
        if ENABLE_CHART:
            try:
                icon = "🟢" if s["side"] == "LONG" else "🔴"
                send_photo(make_chart(s), f"{icon} <b>{s['side']}</b> {sym} · {TF}")
            except Exception as e:
                log.warning("chart error %s: %s", sym, e)
        mid = send(format_signal(s), symbol=sym)
        q("UPDATE signals SET msg_id=? WHERE id=(SELECT MAX(id) FROM signals WHERE symbol=?)",
          (mid, sym), commit=True)
        open_n += 1
        found += 1
        log.info("SIGNAL %s %s", sym, s["side"])
    SCAN_STATS["signals"] = found
    LAST_SCAN = dict(ts=time.time(), secs=time.time() - t0, stats=SCAN_STATS)
    log.info("scan done in %.0fs | %s", LAST_SCAN["secs"], dict(SCAN_STATS))
    return found


def run_cycle():
    with scan_lock:                  # never two scans at the same time
        track()
        scan()


def scanner_loop():
    step = TF_MS[TF] / 1000
    first = True
    while True:
        try:
            if first:
                time.sleep(5)        # first scan right after startup
                first = False
            else:
                time.sleep(step - (time.time() % step) + 8)   # then after every candle close
            run_cycle()
        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(30)


# ---------------- COMMANDS ----------------
def manual_scan():
    if not scan_lock.acquire(blocking=False):
        send("⏳ A scan is already running.")
        return
    try:
        send("🔍 Scanning market now...")
        track()
        n = scan()
        if n == -1:
            send(f"Max open signals reached ({MAX_OPEN}). No new scan.")
        else:
            head = "✅ Scan finished. " + (f"{n} new signal(s) sent." if n else "No signal meets all conditions right now.")
            send(head + "\n\n" + scan_summary())
    except Exception as e:
        log.exception("manual scan error")
        send(f"❌ Scan error: {e}")
    finally:
        scan_lock.release()

def commands_loop():
    offset = 0
    while True:
        res = tg("getUpdates", offset=offset, timeout=30)
        for u in res.get("result", []):
            offset = u["update_id"] + 1
            m = u.get("message") or {}
            if str(m.get("chat", {}).get("id")) != CHAT_ID:
                continue
            t = (m.get("text") or "").split("@")[0].strip().lower()
            if t == "/stats":
                send(stats_text())
            elif t == "/scan":
                threading.Thread(target=manual_scan, daemon=True).start()
            elif t == "/status":
                n_open = q("SELECT COUNT(*) c FROM signals WHERE status='OPEN'")[0]["c"]
                send(f"📡 <b>Status</b> (auto-scan every {TF} candle)\n"
                     f"TF {TF} | HTF {HTF} | min score {MIN_SCORE} | min vol {MIN_VOL_RATIO}x | top {TOP_N}\n"
                     f"Open signals: {n_open}/{MAX_OPEN}\n\n" + scan_summary())
            elif t == "/open":
                rows = q("SELECT * FROM signals WHERE status='OPEN'")
                send("\n".join(f"{r['symbol']} {r['side']} entry {fmt(r['entry'])} TP hit: {r['tp_hit']}"
                               for r in rows) or "No open signals.")
            elif t in ("/start", "/help"):
                send("Commands:\n/status — what the bot is doing\n/scan — scan now\n/stats — real results\n/open — open signals")
        time.sleep(1)


if __name__ == "__main__":
    log.info("Bot started")
    send(f"✅ Bot started. Auto-scanning every {TF} candle (trend filter {HTF}). Use /status anytime.")
    threading.Thread(target=commands_loop, daemon=True).start()
    scanner_loop()

