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
MIN_TOUCHES = max(2, int(os.getenv("MIN_TOUCHES", "2")))
ATR_PERIOD = max(10, int(os.getenv("ATR_PERIOD", "14")))
BREAKOUT_ATR = float(os.getenv("BREAKOUT_ATR", "0.15"))
RETEST_ATR = float(os.getenv("RETEST_ATR", "0.35"))
SL_ATR_BUFFER = float(os.getenv("SL_ATR_BUFFER", "0.25"))
MIN_RR = max(1.5, float(os.getenv("MIN_RR", "2.0")))
TP2_R = max(MIN_RR, float(os.getenv("TP2_R", "3.0")))
TP3_R = max(TP2_R, float(os.getenv("TP3_R", "4.0")))
LIVE_PRICE_SECONDS = max(1, float(os.getenv("LIVE_PRICE_SECONDS", "3")))
MAX_RETEST_BARS = max(1, int(os.getenv("MAX_RETEST_BARS", "8")))
MAX_PATTERN_BARS = max(30, int(os.getenv("MAX_PATTERN_BARS", "90")))
MIN_SCORE = max(60, min(95, int(os.getenv("MIN_SCORE", "65"))))
COOLDOWN_HOURS = max(0.0, float(os.getenv("COOLDOWN_HOURS", "6")))
MAX_SYMBOLS = max(0, int(os.getenv("MAX_SYMBOLS", "0")))
SEND_CHART = os.getenv("SEND_CHART", "1") == "1"
CHART_BARS = min(150, max(70, int(os.getenv("CHART_BARS", "110"))))

app = Flask(__name__)
http = requests.Session()
http.headers.update({"User-Agent": "SAIWAN-1H-Pattern-Bot/3.0"})

state = {"running": False, "last_scan": None, "last_error": None,
         "signals_sent": 0, "symbols": 0, "last_signal": None,
         "signals_checked": 0, "last_scan_candidates": 0}
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


def get_live_price(symbol):
    data = api("/api/v2/mix/market/ticker", {"productType": PRODUCT, "symbol": symbol}) or []
    if isinstance(data, dict):
        data = [data]
    if not data:
        return None, None
    row = data[0]
    price = row.get("lastPr") or row.get("markPrice")
    if price is None:
        return None, row.get("ts")
    return float(price), int(row.get("ts") or int(time.time()*1000))


def get_symbols():
    rows = api("/api/v2/mix/market/contracts", {"productType": PRODUCT}) or []
    out = []
    for x in rows:
        s = str(x.get("symbol", ""))
        if not s.endswith("USDT"): continue
        if str(x.get("quoteCoin", "")).upper() not in ("", "USDT"): continue
        if str(x.get("symbolStatus", "")).lower() not in ("", "normal"): continue
        if str(x.get("symbolType", "")).lower() not in ("", "perpetual"): continue
        # Crypto-only universe: Bitget's USDT-FUTURES product is the source,
        # but explicitly exclude known non-crypto/asset-token style contracts.
        base = str(x.get("baseCoin", "")).upper()
        if not base: continue
        if str(x.get("isRwa", "NO")).upper() == "YES": continue
        non_crypto_prefixes = ("XAU", "XAG", "XPT", "XPD", "GOLD", "SILVER")
        if base.startswith(non_crypto_prefixes): continue
        if any(base == z for z in {"AAPL", "AMZN", "GOOG", "GOOGL", "META", "MSFT", "NVDA", "TSLA", "COIN", "MSTR"}): continue
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


def best_line(c, indices, atr, mode, direction):
    pts = indices[-9:]
    cand = []
    for ai in range(max(0, len(pts)-7), len(pts)-1):
        for bi in range(ai+1, len(pts)):
            i1,i2 = pts[ai],pts[bi]
            if i2-i1 < 6: continue
            p1 = c[i1]["high"] if mode=="high" else c[i1]["low"]
            p2 = c[i2]["high"] if mode=="high" else c[i2]["low"]
            if direction=="resistance" and p2 >= p1: continue
            if direction=="support" and p2 <= p1: continue
            # For general trend-line structures, support may slope either up
            # or down. The breakout direction is decided later from price
            # crossing the line, not from the line slope.
            if direction in ("resistance_any","support_any"):
                pass
            line=(i1,p1,i2,p2)
            tol = max(med([atr[i] for i in pts])*0.45, abs(p2)*0.001)
            touches = sum(abs((c[i]["high"] if mode=="high" else c[i]["low"])-lv(line,i)) <= tol for i in pts)
            if touches < MIN_TOUCHES: continue
            clean=True
            for j in range(i2+1, len(c)-1):
                a=atr[j] or atr[-1] or c[j]["close"]*.005
                if direction in ("resistance","resistance_any") and c[j]["close"] > lv(line,j)+a*.20: clean=False; break
                if direction in ("support","support_any") and c[j]["close"] < lv(line,j)-a*.20: clean=False; break
            if clean: cand.append((touches,i2,line))
    if not cand: return None
    return max(cand, key=lambda z:(z[0],z[1]))


def detect_patterns(c, hs, ls, atr):
    """
    Trend-line pattern detector.
    Signal sequence remains:
    Trendline/Pattern -> Breakout -> Retest -> Confirmation.
    """
    out=[]
    rh=[i for i in hs if i < len(c)-2][-9:]
    rl=[i for i in ls if i < len(c)-2][-9:]

    if len(rh)>=MIN_TOUCHES and len(rl)>=MIN_TOUCHES:
        upper=best_line(c,rh,atr,"high","resistance_any")
        lower=best_line(c,rl,atr,"low","support_any")
        if upper and lower:
            ul,ll=upper[2],lower[2]
            start=max(ul[0],ll[0])
            end=min(len(c)-2,max(ul[2],ll[2])+12)
            d1=abs(lv(ul,start)-lv(ll,start))
            d2=abs(lv(ul,end)-lv(ll,end))
            su=slope([(i,c[i]["high"]) for i in rh])
            sl=slope([(i,c[i]["low"]) for i in rl])
            price=med([x["close"] for x in c[-40:]]) or c[-1]["close"]
            sp_u=su/price
            sp_l=sl/price

            if d1>0 and d2<d1*.90:
                if sp_u<0 and sp_l>0:
                    typ,bias="Falling Wedge","BULLISH"
                elif sp_u>0 and sp_l>0:
                    typ,bias="Rising Wedge","BEARISH"
                elif sp_u<0 and sp_l<0:
                    typ,bias="Descending Channel","NEUTRAL"
                else:
                    typ,bias="Symmetrical Triangle","NEUTRAL"
            elif sp_u>0 and sp_l>0:
                typ,bias="Ascending Channel","NEUTRAL"
            elif sp_u<0 and sp_l<0:
                typ,bias="Descending Channel","NEUTRAL"
            else:
                typ,bias="Trendline Structure","NEUTRAL"

            convergence_bonus=8 if d1>0 and d2<d1*.75 else 3
            score=min(95,62+(upper[0]+lower[0]-2)*4+convergence_bonus)
            out.append({"type":typ,"bias":bias,"upper":ul,"lower":ll,
                        "touches":upper[0]+lower[0],"score":score,
                        "start":start,"end":end})

    if len(rh)>=MIN_TOUCHES and len(rl)>=MIN_TOUCHES:
        upper=best_line(c,rh,atr,"high","resistance")
        lows=rl[-5:]
        if upper and len(lows)>=MIN_TOUCHES:
            level=med([c[i]["low"] for i in lows])
            spread=max(abs(c[i]["low"]-level) for i in lows)
            a=med([atr[i] for i in lows]) or c[-1]["close"]*.005
            if spread<=a*1.15:
                score=min(95,66+(upper[0]-MIN_TOUCHES)*4+
                          (9 if spread<a*.55 else 3))
                out.append({"type":"Descending Triangle","bias":"BEARISH",
                            "upper":upper[2],"lower_level":level,
                            "lower_points":lows,"touches":upper[0]+len(lows),
                            "score":score,"start":min(rh[-5],rl[-5]),
                            "end":max(rh[-1],rl[-1])})

    tol_base=.003
    if len(rl)>=2:
        for k in range(max(0,len(rl)-5),len(rl)-1):
            a,b=rl[k],rl[k+1]
            if b-a<5: continue
            tol=max((atr[b] or 0)*.9,c[b]["close"]*tol_base)
            if abs(c[a]["low"]-c[b]["low"])<=tol:
                neckline=max(x["high"] for x in c[a:b+1])
                typ="Double Bottom"; pts=[a,b]
                if k>0:
                    z=rl[k-1]
                    if abs(c[z]["low"]-c[a]["low"])<=tol:
                        typ="Triple Bottom"; pts=[z,a,b]
                out.append({"type":typ,"bias":"BULLISH","level":neckline,
                            "points":pts,"touches":len(pts),
                            "score":78 if typ=="Triple Bottom" else 70,
                            "start":pts[0],"end":pts[-1]})

    if len(rh)>=2:
        for k in range(max(0,len(rh)-5),len(rh)-1):
            a,b=rh[k],rh[k+1]
            if b-a<5: continue
            tol=max((atr[b] or 0)*.9,c[b]["close"]*tol_base)
            if abs(c[a]["high"]-c[b]["high"])<=tol:
                neckline=min(x["low"] for x in c[a:b+1])
                out.append({"type":"Double Top","bias":"BEARISH",
                            "level":neckline,"points":[a,b],"touches":2,
                            "score":70,"start":a,"end":b})
    return [p for p in out if p["score"]>=MIN_SCORE]

def levels(p, i):
    t=p["type"]
    if t in ("Falling Wedge","Rising Wedge","Descending Channel","Ascending Channel","Symmetrical Triangle","Trendline Structure"):
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

    retest=None; confirm=None
    for r in range(bi+1,min(cur-1,bi+MAX_RETEST_BARS)+1):
        a=atr[r] or atr[bi] or c[r]["close"]*.005
        k=c[r]
        touched=k["low"]<=bi_level+a*RETEST_ATR and k["high"]>=bi_level-a*RETEST_ATR
        if not touched:
            continue
        if direction=="LONG" and k["close"]>=bi_level:
            if r+1==cur and bull_candle(c[cur],atr[cur] or a):
                retest=r
                confirm=cur
                break
        if direction=="SHORT" and k["close"]<=bi_level:
            if r+1==cur and bear_candle(c[cur],atr[cur] or a):
                retest=r
                confirm=cur
                break
    if confirm!=cur: return None

    entry=c[cur]["close"]
    if direction=="LONG":
        lows=[i for i in ls if max(p.get("start",0),retest-12)<=i<=retest]
        base=c[lows[-1]]["low"] if lows else min(x["low"] for x in c[max(0,retest-8):retest+1])
        sl=base-(atr[retest] or atr[cur]) * SL_ATR_BUFFER
        risk=entry-sl
        if risk<=0:return None
        tp1=entry+risk*MIN_RR; tp2=entry+risk*TP2_R; tp3=entry+risk*TP3_R
    else:
        highs=[i for i in hs if max(p.get("start",0),retest-12)<=i<=retest]
        base=c[highs[-1]]["high"] if highs else max(x["high"] for x in c[max(0,retest-8):retest+1])
        sl=base+(atr[retest] or atr[cur]) * SL_ATR_BUFFER
        risk=sl-entry
        if risk<=0:return None
        tp1=entry-risk*MIN_RR; tp2=entry-risk*TP2_R; tp3=entry-risk*TP3_R
    risk_pct=abs(entry-sl)/entry*100
    if risk_pct<.15 or risk_pct>12: return None
    return {"symbol":symbol,"side":direction,"pattern":p["type"],"score":p["score"],
            "touches":p["touches"],"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,"tp3":tp3,
            "rr":abs(tp2-entry)/abs(entry-sl),"risk_pct":risk_pct,"candle_ts":c[cur]["ts"],
            "breakout_idx":bi,"retest_idx":retest,"confirm_idx":confirm,"breakout_level":bi_level,
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


def chart(c,s):
    if not SEND_CHART:return None
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
        n=min(CHART_BARS,len(c)); data=c[-n:]; off=len(c)-n
        fig,ax=plt.subplots(figsize=(13,7.2),dpi=150)
        fig.patch.set_facecolor("#0b0f14"); ax.set_facecolor("#0b0f14")
        for x,k in enumerate(data):
            o,h,l,cl=k["open"],k["high"],k["low"],k["close"]
            col="#00d084" if cl>=o else "#ff4d6d"
            ax.vlines(x,l,h,color=col,lw=.9)
            ax.add_patch(Rectangle((x-.31,min(o,cl)),.62,max(abs(cl-o),(h-l)*.006),facecolor=col,edgecolor=col,lw=.3))
        p=s["pattern_data"]
        def pl(line,label,col):
            if not line:return
            x1=line[0]-off; x2=n-1
            if x2<0 or x1>n-1:return
            ax.plot([x1,x2],[lv(line,line[0]),lv(line,len(c)-1)],color=col,lw=2,label=label)
        if p["type"] in ("Falling Wedge","Rising Wedge","Descending Channel","Ascending Channel","Symmetrical Triangle","Trendline Structure"):
            pl(p["upper"],"Resistance","#f0c36a"); pl(p["lower"],"Support","#5ec8ff")
        elif p["type"]=="Descending Triangle":
            pl(p["upper"],"Resistance","#f0c36a"); ax.axhline(p["lower_level"],color="#5ec8ff",lw=2)
        elif p["type"] in ("Double Bottom","Triple Bottom"):
            ax.axhline(p["level"],color="#5ec8ff",lw=2)
        elif p["type"]=="Double Top":
            ax.axhline(p["level"],color="#f0c36a",lw=2)
        for i in p.get("points",[]):
            if off<=i<len(c):
                y=c[i]["low"] if "Bottom" in p["type"] else c[i]["high"]
                ax.scatter([i-off],[y],s=48,facecolors="none",edgecolors="white",lw=1.2,zorder=5)
        bx=s["breakout_idx"]-off; rx=s["retest_idx"]-off; ex=s["confirm_idx"]-off
        if 0<=bx<n: ax.annotate("BREAKOUT",(bx,c[s["breakout_idx"]]["close"]),xytext=(max(0,bx-10),c[s["breakout_idx"]]["close"]),arrowprops=dict(arrowstyle="->",color="white"),color="white",fontsize=9,fontweight="bold")
        if 0<=rx<n: ax.annotate("RETEST",(rx,c[s["retest_idx"]]["close"]),xytext=(max(0,rx-9),c[s["retest_idx"]]["close"]),arrowprops=dict(arrowstyle="->",color="white"),color="white",fontsize=9,fontweight="bold")
        if 0<=ex<n: ax.scatter([ex],[s["entry"]],s=70,color="white",zorder=6); ax.annotate("ENTRY",(ex,s["entry"]),xytext=(min(n-15,ex+2),s["entry"]),color="white",fontsize=10,fontweight="bold")
        for y,col,label in [(s["entry"],"white","ENTRY"),(s["sl"],"#ff4d6d","SL"),(s["tp1"],"#00d084",f"TP1 {MIN_RR:.1f}R"),(s["tp2"],"#00d084",f"TP2 {TP2_R:.1f}R"),(s["tp3"],"#00d084",f"TP3 {TP3_R:.1f}R")]:
            ax.axhline(y,color=col,lw=1.15,ls="--"); ax.text(n-1,y,f"  {label} {fmt(y)}",va="center",color=col,fontsize=8.5,fontweight="bold")
        ax.set_title(f"SAIWAN • {s['symbol']} • 1H • {s['pattern']} • {s['side']} • Score {s['score']}/100",color="white",fontsize=13,fontweight="bold")
        ax.text(.01,.98,f"Breakout → Retest → Confirmation | RR {s['rr']:.2f} | Risk {s['risk_pct']:.2f}%",transform=ax.transAxes,va="top",color="#cfd8e3",fontsize=9)
        ax.set_xlim(-1,n+8); ax.grid(True,alpha=.12,color="#8a98a8"); ax.tick_params(colors="#aeb8c4",labelsize=8)
        for sp in ax.spines.values():sp.set_color("#26313c")
        fig.tight_layout(); buf=io.BytesIO(); fig.savefig(buf,format="png",bbox_inches="tight",facecolor=fig.get_facecolor()); plt.close(fig); buf.seek(0); return buf
    except Exception as e:
        with state_lock: state["last_error"]=f"chart: {e}"
        return None


def tg_text(s):
    icon="🟢" if s["side"]=="LONG" else "🔴"
    dt=datetime.fromtimestamp(s["candle_ts"]/1000,tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (f"📊 SAIWAN 1H PATTERN SIGNAL\n\n{icon} {s['side']} • {s['symbol']}\n"
            f"🔎 Pattern: {s['pattern']}\n⭐ Score: {s['score']}/100\n📐 Touches: {s['touches']}\n"
            f"⏱ Timeframe: 1H CLOSED\n\n🎯 ENTRY: {fmt(s['entry'])}\n🛑 SL: {fmt(s['sl'])}\n"
            f"✅ TP1: {fmt(s['tp1'])} ({MIN_RR:.1f}R)\n✅ TP2: {fmt(s['tp2'])} ({TP2_R:.1f}R)\n🚀 TP3: {fmt(s['tp3'])} ({TP3_R:.1f}R)\n"
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
    item["tp3_hit"]=False
    item["sl_hit"]=False
    item["last_checked_ts"]=0
    with active_lock:
        active_signals[key]=item


def hit_update_text(s, event, price, ts):
    icon={"TP1":"🎯","TP2":"🏁","TP3":"🚀","SL":"🛑"}[event]
    when=datetime.fromtimestamp(ts/1000,tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (f"{icon} <b>{event} HIT</b>\n"
            f"{s['symbol']} • {s['side']} • {s['pattern']}\n"
            f"Price: <b>{fmt(price)}</b>\n"
            f"Time: {when}")


def track_active_signals():
    """Live TP/SL monitor. Uses Bitget ticker, not 1H candles."""
    with active_lock:
        snapshot=list(active_signals.items())
    for key,s in snapshot:
        try:
            price, ts = get_live_price(s["symbol"])
            if price is None:
                continue
            if s.get("last_live_price") is None:
                s["last_live_price"]=price
            events=[]
            if s["side"]=="LONG":
                if price >= s["tp3"] and not s.get("tp3_hit"):
                    if not s.get("tp1_hit"): events.append(("TP1",s["tp1"])); s["tp1_hit"]=True
                    if not s.get("tp2_hit"): events.append(("TP2",s["tp2"])); s["tp2_hit"]=True
                    events.append(("TP3",s["tp3"])); s["tp3_hit"]=True
                elif price >= s["tp2"] and not s.get("tp2_hit"):
                    if not s.get("tp1_hit"): events.append(("TP1",s["tp1"])); s["tp1_hit"]=True
                    events.append(("TP2",s["tp2"])); s["tp2_hit"]=True
                elif price >= s["tp1"] and not s.get("tp1_hit"):
                    events.append(("TP1",s["tp1"])); s["tp1_hit"]=True
                if price <= s["sl"] and not s.get("sl_hit"):
                    events.append(("SL",s["sl"])); s["sl_hit"]=True
            else:
                if price <= s["tp3"] and not s.get("tp3_hit"):
                    if not s.get("tp1_hit"): events.append(("TP1",s["tp1"])); s["tp1_hit"]=True
                    if not s.get("tp2_hit"): events.append(("TP2",s["tp2"])); s["tp2_hit"]=True
                    events.append(("TP3",s["tp3"])); s["tp3_hit"]=True
                elif price <= s["tp2"] and not s.get("tp2_hit"):
                    if not s.get("tp1_hit"): events.append(("TP1",s["tp1"])); s["tp1_hit"]=True
                    events.append(("TP2",s["tp2"])); s["tp2_hit"]=True
                elif price <= s["tp1"] and not s.get("tp1_hit"):
                    events.append(("TP1",s["tp1"])); s["tp1_hit"]=True
                if price >= s["sl"] and not s.get("sl_hit"):
                    events.append(("SL",s["sl"])); s["sl_hit"]=True

            # If one live quote jumps across SL and a target simultaneously,
            # the exchange quote does not reveal the intrabar path. Keep the
            # signal active and do not fabricate the order.
            if ((s["side"]=="LONG" and price>=s["tp1"] and price<=s["sl"]) or
                (s["side"]=="SHORT" and price<=s["tp1"] and price>=s["sl"])):
                events=[]
                if not s.get("ambiguous_live_logged"):
                    tg_reply(f"⚠️ <b>Live price crossed SL/target range</b>\n{s['symbol']}\nExact intrabar order is unknown; no HIT is declared from this quote.", s["telegram_message_id"])
                    s["ambiguous_live_logged"]=True

            for event,level in events:
                tg_reply(hit_update_text(s,event,price,ts),s["telegram_message_id"])

            s["last_live_price"]=price
            s["last_live_ts"]=ts
            if s.get("sl_hit") or s.get("tp3_hit"):
                with active_lock: active_signals.pop(key,None)
            else:
                with active_lock:
                    if key in active_signals: active_signals[key]=s
        except Exception as e:
            with state_lock: state["last_error"]=f"live tracker {s['symbol']}: {e}"


def live_tracker_loop():
    while True:
        try:
            track_active_signals()
        except Exception as e:
            with state_lock: state["last_error"]=f"live tracker: {e}"
        time.sleep(LIVE_PRICE_SECONDS)


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
                s=find_signal(symbol,c)
                with state_lock:
                    state["signals_checked"] += 1
                if not s:
                    # Re-check the latest closed candle on the next scan.
                    # This helps when exchange data arrives slightly late.
                    continue
                with state_lock:
                    state["last_scan_candidates"] += 1
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
    threading.Thread(target=live_tracker_loop,daemon=True,name="live-tp-sl").start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")))
