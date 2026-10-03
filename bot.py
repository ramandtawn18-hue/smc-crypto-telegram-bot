# SAIWAN 4H Triangle Telegram Bot
# Strict signal flow:
# Symmetrical Triangle -> Breakout -> Retest -> Confirmation -> LONG signal
# Uses CLOSED 4H candles only.

import os, time, logging
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import ccxt
import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TIMEFRAME = "4h"
SCAN_INTERVAL_MIN = int(os.getenv("SCAN_INTERVAL_MIN", "15"))
LOOKBACK = int(os.getenv("LOOKBACK", "220"))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "500"))
MIN_SCORE = float(os.getenv("MIN_SCORE", "80"))

exchange = ccxt.bitget({
    "enableRateLimit": True,
    "options": {"defaultType": "swap"},
})

last_signal_key = None
last_scan = None
last_error = None
signals_sent = 0

def tg_send(text, image_path=None):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logging.warning("Telegram variables are missing.")
        return False
    base = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/"
    try:
        if image_path:
            with open(image_path, "rb") as f:
                r = requests.post(
                    base + "sendPhoto",
                    data={"chat_id": CHAT_ID, "caption": text},
                    files={"photo": f},
                    timeout=30,
                )
        else:
            r = requests.post(
                base + "sendMessage",
                data={"chat_id": CHAT_ID, "text": text},
                timeout=30,
            )
        r.raise_for_status()
        return True
    except Exception as e:
        logging.exception("Telegram send failed: %s", e)
        return False

def fmt(x):
    if x is None or not np.isfinite(x):
        return "-"
    if x >= 100: return f"{x:.2f}"
    if x >= 1: return f"{x:.4f}"
    if x >= 0.1: return f"{x:.5f}"
    if x >= 0.01: return f"{x:.6f}"
    return f"{x:.8f}"

def atr(df, n=14):
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def pivots(df, left=3, right=3):
    highs, lows = [], []
    H, L = df["high"].values, df["low"].values
    for i in range(left, len(df)-right):
        if H[i] == max(H[i-left:i+right+1]):
            highs.append(i)
        if L[i] == min(L[i-left:i+right+1]):
            lows.append(i)
    return highs, lows

def line_from_points(i1, y1, i2, y2):
    if i2 == i1:
        return None
    m = (y2-y1)/(i2-i1)
    b = y1-m*i1
    return m, b

def line_y(line, x):
    return line[0]*x + line[1]

def triangle_setup(df):
    n = len(df)
    if n < 100:
        return None

    start = max(0, n-110)
    sub = df.iloc[start:].reset_index(drop=True)
    hs, ls = pivots(sub, 3, 3)
    if len(hs) < 3 or len(ls) < 3:
        return None

    hs, ls = hs[-6:], ls[-6:]
    candidates = []

    for hi1 in range(max(0, len(hs)-5), len(hs)-1):
        for hi2 in range(hi1+1, len(hs)):
            h1, h2 = hs[hi1], hs[hi2]
            if h2-h1 < 8:
                continue
            if sub["high"].iloc[h2] >= sub["high"].iloc[h1] * 0.995:
                continue

            upper = line_from_points(h1, sub["high"].iloc[h1], h2, sub["high"].iloc[h2])
            if not upper or upper[0] >= 0:
                continue

            for li1 in range(max(0, len(ls)-5), len(ls)-1):
                for li2 in range(li1+1, len(ls)):
                    l1, l2 = ls[li1], ls[li2]
                    if l2-l1 < 8:
                        continue
                    if sub["low"].iloc[l2] <= sub["low"].iloc[l1] * 1.005:
                        continue

                    lower = line_from_points(l1, sub["low"].iloc[l1], l2, sub["low"].iloc[l2])
                    if not lower or lower[0] <= 0:
                        continue

                    if upper[0] == lower[0]:
                        continue
                    apex = (lower[1]-upper[1])/(upper[0]-lower[0])

                    if apex <= max(h2, l2)+5 or apex > n-start+120:
                        continue

                    left_x = max(h1, l1)
                    right_x = n-start-1
                    width_left = line_y(upper,left_x)-line_y(lower,left_x)
                    width_right = line_y(upper,right_x)-line_y(lower,right_x)

                    if width_left <= 0 or width_right <= 0:
                        continue
                    if width_right/width_left > 0.65:
                        continue

                    # Price must stay inside the triangle (small wick tolerance).
                    inside = True
                    for k in range(left_x, right_x+1):
                        if sub["close"].iloc[k] > line_y(upper,k)*1.012:
                            inside = False
                            break
                        if sub["close"].iloc[k] < line_y(lower,k)*0.988:
                            inside = False
                            break
                    if not inside:
                        continue

                    score = 70
                    score += min(15, 5*min(3, len(hs)+len(ls)-4))
                    score += 10 if width_right/width_left < 0.45 else 5
                    candidates.append(
                        (score, upper, lower, start, h1, h2, l1, l2, apex)
                    )

    if not candidates:
        return None

    candidates.sort(key=lambda x:x[0], reverse=True)
    return candidates[0]

def analyze_symbol(symbol):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=LOOKBACK)
    if not ohlcv or len(ohlcv) < 130:
        return None

    df = pd.DataFrame(
        ohlcv,
        columns=["ts","open","high","low","close","volume"]
    )

    # IMPORTANT: never generate a signal from the currently forming 4H candle.
    df = df.iloc[:-1].reset_index(drop=True)
    df["atr"] = atr(df)

    setup = triangle_setup(df)
    if not setup:
        return None

    score, upper, lower, start, h1,h2,l1,l2,apex = setup
    sub = df.iloc[start:].reset_index(drop=True)
    last_i = len(sub)-1
    a = float(sub["atr"].iloc[-1])

    if not np.isfinite(a) or a <= 0:
        return None

    # 1) REAL BREAKOUT above the descending resistance trendline.
    breakout = None
    for i in range(max(5,last_i-12), last_i+1):
        u = line_y(upper,i)
        c = sub["close"].iloc[i]
        prev = sub["close"].iloc[i-1]
        vol_med = sub["volume"].iloc[max(0,i-30):i].median()

        if (
            c > u + 0.15*a
            and prev <= line_y(upper,i-1) + 0.05*a
            and sub["volume"].iloc[i] >= 1.15*vol_med
        ):
            breakout = i
            break

    if breakout is None:
        return None

    # 2) RETEST of the broken trendline.
    retest = None
    for i in range(breakout+1, min(last_i, breakout+7)+1):
        u = line_y(upper,i)
        if sub["low"].iloc[i] <= u + 0.35*a and sub["close"].iloc[i] > u:
            retest = i

    if retest is None or retest >= last_i:
        return None

    # 3) CONFIRMATION candle must close above retest high.
    confirm = last_i
    u = line_y(upper, confirm)

    if sub["close"].iloc[confirm] <= u:
        return None
    if sub["close"].iloc[confirm] <= sub["high"].iloc[retest]:
        return None

    entry = float(sub["close"].iloc[confirm])
    sl = float(sub["low"].iloc[retest] - 0.25*a)

    if sl >= entry:
        return None

    risk = entry-sl

    # Measured-move target from triangle width.
    bx = breakout
    tri_width = max(
        line_y(upper,bx) - line_y(lower,bx),
        0
    )

    tp3 = max(entry + tri_width, entry + 3*risk)
    tp1 = entry + (tp3-entry)*0.35
    tp2 = entry + (tp3-entry)*0.68
    rr = (tp3-entry)/risk

    if score < MIN_SCORE or rr < 2.5:
        return None

    return {
        "symbol": symbol,
        "df": df,
        "sub": sub,
        "setup": setup,
        "breakout": breakout,
        "retest": retest,
        "confirm": confirm,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr": rr,
        "score": score,
        "direction": "LONG",
    }

def draw_chart(s):
    symbol = s["symbol"]
    sub = s["sub"]
    score, upper, lower, start, h1,h2,l1,l2,apex = s["setup"]

    d = s["df"].iloc[max(0, len(s["df"])-125):].copy().reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(16,9), dpi=120)

    # Candlesticks.
    for i,row in d.iterrows():
        ax.vlines(i,row.low,row.high,linewidth=0.8)
        ax.vlines(i,row.open,row.close,linewidth=5.5)

    # Triangle trendlines.
    abs_start = len(s["df"]) - len(sub)
    offset = len(s["df"]) - len(d)

    for line, p1, p2 in ((upper,h1,h2),(lower,l1,l2)):
        xs = np.array([abs_start+p1, abs_start+len(sub)-1])
        ys = np.array([line_y(line,p1), line_y(line,len(sub)-1)])
        ax.plot(xs-offset, ys, linewidth=2.2)

    # Simple volume-profile style histogram on the left.
    prices=np.linspace(d.low.min(), d.high.max(), 35)
    vols=np.zeros(len(prices)-1)
    for _,r in d.iterrows():
        mask=(prices[:-1] <= r.high) & (prices[1:] >= r.low)
        vols[mask]+=r.volume
    if vols.max()>0:
        widths=vols/vols.max()*12
        centers=(prices[:-1]+prices[1:])/2
        ax.barh(
            centers,widths,left=-1,
            height=(prices[1]-prices[0])*0.8,
            alpha=0.18
        )

    # Entry / SL / TP levels.
    ax.axhline(s["entry"], linestyle="--", linewidth=1.2)
    ax.axhline(s["sl"], linestyle="--", linewidth=1.2)
    for tp in (s["tp1"],s["tp2"],s["tp3"]):
        ax.axhline(tp, linestyle="--", linewidth=1.0)

    # Position box.
    x0=len(d)-10
    width=18
    ax.add_patch(
        Rectangle(
            (x0,s["entry"]),
            width,
            s["tp3"]-s["entry"],
            alpha=0.18
        )
    )
    ax.add_patch(
        Rectangle(
            (x0,s["sl"]),
            width,
            s["entry"]-s["sl"],
            alpha=0.15
        )
    )

    ax.text(x0+width-1,s["tp3"],f"TP3 {fmt(s['tp3'])}",
            ha="right",va="bottom",fontsize=10)
    ax.text(x0+width-1,s["tp2"],f"TP2 {fmt(s['tp2'])}",
            ha="right",va="bottom",fontsize=10)
    ax.text(x0+width-1,s["tp1"],f"TP1 {fmt(s['tp1'])}",
            ha="right",va="bottom",fontsize=10)
    ax.text(x0+width-1,s["entry"],f"ENTRY {fmt(s['entry'])}",
            ha="right",va="bottom",fontsize=10,fontweight="bold")
    ax.text(x0+width-1,s["sl"],f"SL {fmt(s['sl'])}",
            ha="right",va="top",fontsize=10)

    ax.set_title(
        f"SAIWAN • {symbol} • 4H • Symmetrical Triangle • LONG",
        loc="left",fontsize=16,fontweight="bold"
    )
    ax.text(
        0.01,0.965,
        f"Trendline → Breakout → Retest → Confirmation   |   "
        f"Score {s['score']:.0f}/100   |   RR {s['rr']:.2f}",
        transform=ax.transAxes,fontsize=10,va="top"
    )
    ax.grid(alpha=0.18)
    ax.set_xlim(-2,len(d)+8)

    fig.tight_layout()
    path="/tmp/saiwan_signal.png"
    fig.savefig(path,bbox_inches="tight")
    plt.close(fig)
    return path

def scan_all():
    global last_scan,last_error,signals_sent,last_signal_key

    last_error=None
    last_scan=datetime.now(timezone.utc)

    markets=exchange.load_markets()
    symbols=[
        m["symbol"] for m in markets.values()
        if m.get("active",True)
        and m.get("swap")
        and m.get("linear")
        and m.get("quote")=="USDT"
    ]
    symbols=symbols[:MAX_SYMBOLS]

    logging.info(
        "Scanning %d Bitget USDT perpetual symbols on %s",
        len(symbols), TIMEFRAME
    )

    found=[]

    for idx,symbol in enumerate(symbols,1):
        try:
            result=analyze_symbol(symbol)
            if result:
                found.append(result)
                logging.info(
                    "VALID SETUP %s score=%s rr=%.2f",
                    symbol,result["score"],result["rr"]
                )
        except Exception as e:
            logging.warning("%s failed: %s",symbol,e)

        if idx % 25 == 0:
            logging.info("progress %d/%d",idx,len(symbols))

    found.sort(
        key=lambda x:(x["score"],x["rr"]),
        reverse=True
    )

    if found:
        s=found[0]
        key=f"{s['symbol']}:{s['confirm']}:{s['entry']:.10g}"

        if key != last_signal_key:
            caption=(
                f"🚨 SAIWAN 4H SIGNAL\n\n"
                f"{s['symbol']} • LONG\n"
                f"Pattern: Symmetrical Triangle\n"
                f"Structure: Breakout → Retest → Confirmation\n\n"
                f"ENTRY: {fmt(s['entry'])}\n"
                f"SL: {fmt(s['sl'])}\n"
                f"TP1: {fmt(s['tp1'])}\n"
                f"TP2: {fmt(s['tp2'])}\n"
                f"TP3: {fmt(s['tp3'])}\n"
                f"RR: {s['rr']:.2f}\n"
                f"Score: {s['score']:.0f}/100"
            )

            path=draw_chart(s)

            if tg_send(caption,path):
                last_signal_key=key
                signals_sent += 1

    return len(found)

def status_text():
    return (
        f"🤖 SAIWAN STATUS\n\n"
        f"Bot: ONLINE\n"
        f"Scanner: RUNNING\n"
        f"Market: Bitget USDT Perpetual\n"
        f"Timeframe: 4H\n"
        f"Signals sent: {signals_sent}\n"
        f"Last scan: {last_scan.isoformat() if last_scan else 'not yet'}\n"
        f"Last error: {last_error or 'none'}"
    )

def telegram_poll():
    offset=0
    base=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

    while True:
        try:
            r=requests.get(
                base+"/getUpdates",
                params={"timeout":25,"offset":offset},
                timeout=35
            )
            data=r.json()

            for u in data.get("result",[]):
                offset=u["update_id"]+1
                msg=u.get("message",{})
                text=msg.get("text","").strip()
                chat=str(msg.get("chat",{}).get("id",""))

                if chat != CHAT_ID:
                    continue

                if text=="/start":
                    tg_send(
                        "🤖 SAIWAN 4H Pattern Bot is online.\n\n"
                        "Symmetrical Triangle → Breakout → Retest → Confirmation\n"
                        "Use /status or /scan"
                    )

                elif text=="/status":
                    tg_send(status_text())

                elif text=="/scan":
                    tg_send("🔎 Manual 4H scan started...")
                    count=scan_all()
                    tg_send(
                        f"✅ 4H scan completed. "
                        f"Valid setups found: {count}"
                    )

        except Exception as e:
            logging.warning("Telegram polling: %s",e)
            time.sleep(3)

def main():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        raise RuntimeError(
            "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Railway Variables."
        )

    import threading
    threading.Thread(
        target=telegram_poll,
        daemon=True
    ).start()

    while True:
        try:
            scan_all()
        except Exception as e:
            global last_error
            last_error=str(e)
            logging.exception("scan error")

        time.sleep(SCAN_INTERVAL_MIN*60)

if __name__=="__main__":
    main()
