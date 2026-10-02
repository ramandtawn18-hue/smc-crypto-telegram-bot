import os
import time
import json
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from flask import Flask, jsonify

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BITGET_API = "https://api.bitget.com"
BITGET_PRODUCT = "USDT-FUTURES"
TELEGRAM_API = "https://api.telegram.org/bot"

TF_15M = "15m"
TF_5M = "5m"
TIMEFRAME = TF_5M
CANDLE_LIMIT = 260
# 0 = scan every eligible Bitget USDT perpetual contract (no top-N cap)
MAX_PAIRS = 0
SCAN_WORKERS = 6
SCAN_INTERVAL = 60
SEND_INTERVAL = 600  # minimum 10 minutes between sent signals
CHART_CANDLES = 70
HTTP_TIMEOUT = 15

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
AI_REQUIRED = os.getenv("AI_REQUIRED", "true").strip().lower() not in {"0", "false", "no", "off"}
AI_TIMEOUT = 20
# Groq free/on-demand limits are organization-wide. Serialize AI calls and
# keep a small gap between requests so a full 466-symbol scan does not burst
# 20+ candidate requests into the same minute.
AI_MIN_INTERVAL = float(os.getenv("AI_MIN_INTERVAL", "2.0"))
AI_MAX_RETRIES = 1
ai_call_lock = threading.Lock()
ai_last_call = 0.0
ai_cache_lock = threading.Lock()
ai_review_cache = {}
AI_CACHE_TTL = 12 * 60
groq_client = bool(GROQ_API_KEY)


app = Flask(__name__)
stop_event = threading.Event()
force_scan_event = threading.Event()
state_lock = threading.Lock()
scanner_thread = None
scanner_running = False
active_chat_id = None
pending_signals = []
seen_signals = set()
seen_order = []
next_send_at = 0
offset = None
active_signals = {}  # key -> tracked signal state for TP/SL notifications
signal_history = []  # sent signal summaries for /search
MAX_SIGNAL_HISTORY = 500
monitor_thread = None

session = requests.Session()
session.headers.update({"User-Agent": "SAIWAN-Crypto-Signal-Move-Hunter/5.0", "Accept": "application/json"})
bitget_rate_lock = threading.Lock()
bitget_last_request = 0.0
BITGET_MIN_REQUEST_INTERVAL = 0.06  # ~16.7 requests/sec, below Bitget's 20 req/sec/IP limit


def fmt_price(x):
    x = float(x)
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.10f}".rstrip("0").rstrip(".")


def bitget_get(path, params=None, retries=3):
    """GET a public Bitget Futures endpoint. No API key is required for market data."""
    global bitget_last_request
    last = None
    for attempt in range(retries + 1):
        try:
            # Keep the whole process below Bitget's documented 20 req/sec/IP market limit.
            with bitget_rate_lock:
                wait = BITGET_MIN_REQUEST_INTERVAL - (time.monotonic() - bitget_last_request)
                if wait > 0:
                    time.sleep(wait)
                bitget_last_request = time.monotonic()
            r = session.get(BITGET_API + path, params=params or {}, timeout=HTTP_TIMEOUT)
            if r.status_code == 429:
                if attempt < retries:
                    time.sleep(min(1.0 * (attempt + 1), 5.0))
                    continue
            if r.status_code in (403, 418, 500, 502, 503, 504):
                if attempt < retries:
                    time.sleep(min(1.5 * (attempt + 1), 6.0))
                    continue
            r.raise_for_status()
            payload = r.json()
            if not isinstance(payload, dict):
                raise RuntimeError("Bitget returned a non-object response")
            if str(payload.get("code")) != "00000":
                raise RuntimeError(f"Bitget API error {payload.get('code')}: {payload.get('msg', 'unknown')}")
            return payload
        except (requests.RequestException, ValueError, RuntimeError) as e:
            last = e
            if attempt < retries:
                time.sleep(min(0.8 * (attempt + 1), 4.0))
    raise last or RuntimeError("Bitget API request failed")


def get_contracts():
    """Return live USDT perpetual futures contracts from Bitget."""
    payload = bitget_get(
        "/api/v2/mix/market/contracts",
        {"productType": BITGET_PRODUCT},
    )
    out = []
    for x in payload.get("data") or []:
        if (
            x.get("symbolStatus") == "normal"
            and str(x.get("symbolType", "")).lower() == "perpetual"
            and x.get("quoteCoin") == "USDT"
            and x.get("symbol", "").endswith("USDT")
            and str(x.get("isRwa", "NO")).upper() != "YES"
        ):
            out.append(x)
    return out


def get_tickers():
    payload = bitget_get(
        "/api/v2/mix/market/tickers",
        {"productType": BITGET_PRODUCT},
    )
    return payload.get("data") or []


def get_klines(symbol, interval=TIMEFRAME, limit=CANDLE_LIMIT):
    payload = bitget_get(
        "/api/v2/mix/market/candles",
        {"symbol": symbol, "productType": BITGET_PRODUCT, "granularity": interval,
         "limit": min(limit, 1000), "kLineType": "market"},
    )
    raw = payload.get("data") or []
    now_ms = int(time.time() * 1000)
    candle_ms = (5 if interval == TF_5M else 15) * 60 * 1000
    rows = []
    for v in raw:
        try:
            if len(v) < 6:
                continue
            ts = int(v[0])
            if ts + candle_ms > now_ms:
                continue
            rows.append({"time": ts // 1000, "open": float(v[1]), "high": float(v[2]),
                         "low": float(v[3]), "close": float(v[4]), "vol": float(v[5]),
                         "turnover": float(v[6]) if len(v) > 6 else 0.0})
        except (TypeError, ValueError, IndexError):
            continue
    rows.sort(key=lambda x: x["time"])
    return rows[-limit:]


def _candle_bull(r):
    return r["close"] > r["open"]


def _candle_bear(r):
    return r["close"] < r["open"]


def _body_ratio(r):
    rng = max(r["high"] - r["low"], 1e-12)
    return abs(r["close"] - r["open"]) / rng


def _find_fvg(rows, direction, start_idx, end_idx):
    """Find the latest 3-candle FVG created by displacement."""
    found = []
    for i in range(max(2, start_idx), min(end_idx, len(rows) - 1)):
        a, b, c = rows[i-2], rows[i-1], rows[i]
        if direction == "LONG" and c["low"] > a["high"] and _candle_bull(b):
            found.append({"low": a["high"], "high": c["low"], "index": i, "kind": "BULLISH FVG"})
        elif direction == "SHORT" and c["high"] < a["low"] and _candle_bear(b):
            found.append({"low": c["high"], "high": a["low"], "index": i, "kind": "BEARISH FVG"})
    return found[-1] if found else None


def _find_order_block(rows, direction, before_idx):
    """Last opposite candle before the displacement leg."""
    lo = max(0, before_idx - 8)
    for i in range(before_idx - 1, lo - 1, -1):
        r = rows[i]
        if direction == "LONG" and _candle_bear(r):
            return {"low": r["low"], "high": r["high"], "index": i, "kind": "BULLISH OB"}
        if direction == "SHORT" and _candle_bull(r):
            return {"low": r["low"], "high": r["high"], "index": i, "kind": "BEARISH OB"}
    return None


def _overlap(a, b):
    if not a or not b:
        return None
    lo, hi = max(a["low"], b["low"]), min(a["high"], b["high"])
    if lo <= hi:
        return {"low": lo, "high": hi}
    return None


def _recent_liquidity(rows, upto, direction, lookback=45):
    """Return the nearest prior swing pool that can be swept."""
    w0 = max(0, upto - lookback)
    window = rows[w0:upto]
    highs, lows = swing_points(window, 2, 2)
    if direction == "LONG":
        pools = [(i + w0, p) for i, p in lows]
        return pools[-1] if pools else None
    pools = [(i + w0, p) for i, p in highs]
    return pools[-1] if pools else None


def _structure_break(rows, direction, sweep_idx, end_idx):
    """MSS/CHOCH confirmation from internal swings after the liquidity sweep."""
    start = max(2, sweep_idx + 1)
    end = min(end_idx, len(rows) - 1)
    if end <= start + 1:
        return None
    window = rows[max(0, sweep_idx - 18):end + 1]
    highs, lows = swing_points(window, 1, 1)
    off = max(0, sweep_idx - 18)
    if direction == "LONG":
        prior_highs = [(i + off, p) for i, p in highs if i + off <= sweep_idx]
        if not prior_highs:
            return None
        level_idx, level = prior_highs[-1]
        for i in range(start, end + 1):
            if rows[i]["close"] > level and _candle_bull(rows[i]):
                return {"index": i, "level": level, "type": "MSS + CHOCH"}
    else:
        prior_lows = [(i + off, p) for i, p in lows if i + off <= sweep_idx]
        if not prior_lows:
            return None
        level_idx, level = prior_lows[-1]
        for i in range(start, end + 1):
            if rows[i]["close"] < level and _candle_bear(rows[i]):
                return {"index": i, "level": level, "type": "MSS + CHOCH"}
    return None


def _sweep_candidates(rows):
    """Return recent liquidity sweeps using only price/swing structure."""
    out = []
    start = max(10, len(rows) - 70)
    for i in range(start, len(rows) - 2):
        prior = rows[max(0, i-35):i]
        highs, lows = swing_points(prior, 2, 2)
        if highs:
            high_level = max(p for _, p in highs[-5:])
            if rows[i]["high"] > high_level and rows[i]["close"] < high_level:
                out.append(("SHORT", i, high_level))
        if lows:
            low_level = min(p for _, p in lows[-5:])
            if rows[i]["low"] < low_level and rows[i]["close"] > low_level:
                out.append(("LONG", i, low_level))
    return out


def _context_15m(rows15, direction):
    if not rows15 or len(rows15) < 20:
        return "UNKNOWN"
    recent = rows15[-8:]
    hi = max(r["high"] for r in recent)
    lo = min(r["low"] for r in recent)
    mid = (hi + lo) / 2
    return ("BULLISH CONTEXT" if rows15[-1]["close"] >= mid else "MIXED CONTEXT") if direction == "LONG" else ("BEARISH CONTEXT" if rows15[-1]["close"] <= mid else "MIXED CONTEXT")


def _move_setup(rows, direction):
    """Deterministic ICT Move Hunter.

    Chain: liquidity sweep -> reaction/displacement -> MSS/CHOCH -> FVG + OB.
    The current candle must still be close enough to the origin of the move.
    No indicators are used here.
    """
    if len(rows) < 120:
        return None

    candidates = _sweep_candidates(rows)
    for direction0, sweep_idx, liquidity in reversed(candidates):
        if direction0 != direction or sweep_idx >= len(rows) - 2:
            continue

        structure = _structure_break(rows, direction, sweep_idx, len(rows) - 1)
        if not structure:
            continue
        mss_idx = structure["index"]
        if mss_idx <= sweep_idx:
            continue

        # The displacement/FVG must happen soon after the MSS. This prevents
        # old structure from becoming a late signal several candles later.
        fvg = _find_fvg(rows, direction, max(2, mss_idx - 1), min(len(rows), mss_idx + 5))
        if not fvg:
            continue
        ob = _find_order_block(rows, direction, fvg["index"] + 1)
        if not ob:
            continue

        zone = _overlap(fvg, ob) or fvg
        trigger_idx = max(mss_idx, fvg["index"])
        age = len(rows) - 1 - trigger_idx
        if age > 3:
            continue

        trigger = rows[trigger_idx]
        if direction == "LONG" and not _candle_bull(trigger):
            continue
        if direction == "SHORT" and not _candle_bear(trigger):
            continue

        cur = rows[-1]
        if direction == "LONG" and cur["close"] <= structure["level"]:
            continue
        if direction == "SHORT" and cur["close"] >= structure["level"]:
            continue

        entry = cur["close"]
        trigger_range = max(trigger["high"] - trigger["low"], 1e-12)
        zone_width = max(zone["high"] - zone["low"], 1e-12)

        # Structural invalidation: LONG below the protected low; SHORT above
        # the protected high. A small candle-structure buffer is used only to
        # keep the stop outside the invalidation wick, never as a fixed %.
        if direction == "LONG":
            invalidation = min(liquidity, ob["low"], zone["low"])
            sl = invalidation - trigger_range * 0.15
            if sl >= entry:
                continue
            risk = entry - sl
            if risk < zone_width * 0.50:
                continue

            # Liquidity/structure targets first; R-multiples are only a final
            # fallback when there is genuinely no visible opposing swing.
            highs, _ = swing_points(rows[:-1], 2, 2)
            targets = sorted({p for _, p in highs if p > entry * 1.002})
            if targets:
                tp1 = targets[0]
                remaining = [p for p in targets[1:] if p > tp1 * 1.002]
                tp2 = remaining[0] if remaining else entry + risk * 2.0
                remaining2 = [p for p in remaining[1:] if p > tp2 * 1.002]
                tp3 = remaining2[0] if remaining2 else entry + risk * 3.0
            else:
                tp1, tp2, tp3 = entry + risk * 1.5, entry + risk * 2.5, entry + risk * 4.0
        else:
            invalidation = max(liquidity, ob["high"], zone["high"])
            sl = invalidation + trigger_range * 0.15
            if sl <= entry:
                continue
            risk = sl - entry
            if risk < zone_width * 0.50:
                continue

            _, lows = swing_points(rows[:-1], 2, 2)
            targets = sorted({p for _, p in lows if p < entry * 0.998}, reverse=True)
            if targets:
                tp1 = targets[0]
                remaining = [p for p in targets[1:] if p < tp1 * 0.998]
                tp2 = remaining[0] if remaining else entry - risk * 2.0
                remaining2 = [p for p in remaining[1:] if p < tp2 * 0.998]
                tp3 = remaining2[0] if remaining2 else entry - risk * 3.0
            else:
                tp1, tp2, tp3 = entry - risk * 1.5, entry - risk * 2.5, entry - risk * 4.0

        # Reject impossible/behind-market targets.
        if direction == "LONG" and not (sl < entry < tp1 <= tp2 <= tp3):
            continue
        if direction == "SHORT" and not (tp3 <= tp2 <= tp1 < entry < sl):
            continue

        # Deterministic timing/freshness used to rank candidates before AI.
        move_origin = max(sweep_idx, trigger_idx)
        bars_since_sweep = len(rows) - 1 - sweep_idx
        extension = abs(entry - trigger["close"]) / max(trigger_range, 1e-12)
        freshness = max(0.0, 10.0 - bars_since_sweep * 1.8 - age * 1.5)
        origin_proximity = max(0.0, 10.0 - extension * 1.5)
        ict_rank = freshness + origin_proximity + (3.0 if age <= 1 else 0.0)

        return {
            "symbol": "", "direction": direction,
            "structure": "Liquidity Sweep + MSS + CHOCH + FVG + OB",
            "entry": entry, "trigger_level": structure["level"], "sl": sl,
            "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "entry_zone_low": zone["low"], "entry_zone_high": zone["high"],
            "time": cur["time"],
            "liquidity": liquidity, "sweep_index": sweep_idx,
            "mss_index": mss_idx, "mss_level": structure["level"],
            "fvg": fvg, "ob": ob, "entry_zone": zone,
            "rows": rows[max(0, sweep_idx-18):], "full_len": len(rows),
            "checks": {"Liquidity Sweep": True, "MSS": True, "FVG": True, "OB": True, "CHOCH": True},
            "retest_ok": False, "rejection_ok": True, "early_entry": age <= 1,
            "setup_age_bars": age, "bars_since_sweep": bars_since_sweep,
            "trigger_range": trigger_range, "zone_width": zone_width,
            "ict_rank": ict_rank, "setup_key": f"{direction}:{rows[sweep_idx]['time']}:{rows[mss_idx]['time']}:{rows[fvg['index']]['time']}",
        }
    return None

def _compact_candles(rows, count=36):
    """Keep the AI prompt small: recent closed candles only."""
    out = []
    for r in rows[-count:]:
        out.append({
            "t": r["time"],
            "o": round(r["open"], 10),
            "h": round(r["high"], 10),
            "l": round(r["low"], 10),
            "c": round(r["close"], 10),
        })
    return out


def _ai_json(text):
    """Extract a JSON object from the model's text response."""
    text = (text or "").strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    try:
        return json.loads(text)
    except Exception:
        start, end = text.find("{"), text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start:end + 1])
    raise ValueError("AI returned invalid JSON")


def _groq_chat_json(system, user_payload, schema_name="saiwan_ai_review", schema=None, max_tokens=300):
    """Call Groq safely with serialized requests and 429 backoff."""
    global ai_last_call
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY is missing")

    body = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_payload if isinstance(user_payload, str) else json.dumps(user_payload, separators=(",", ":"))},
        ],
        "reasoning_effort": "low",
        "include_reasoning": False,
        "temperature": 0.1,
        "max_completion_tokens": max_tokens,
    }
    # Do not enable Groq server-side JSON validation here. GPT-OSS is a reasoning
    # model, and constrained JSON generation can fail with json_validate_failed
    # even when the request is otherwise valid. We hide reasoning and validate
    # the final JSON locally with _ai_json().

    # One AI request at a time. This is the important fix for the 8K TPM
    # organization limit seen during the 466-symbol scan.
    with ai_call_lock:
        wait = AI_MIN_INTERVAL - (time.monotonic() - ai_last_call)
        if wait > 0:
            time.sleep(wait)
        for attempt in range(AI_MAX_RETRIES + 1):
            try:
                r = requests.post(
                    GROQ_API_URL,
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                    json=body,
                    timeout=AI_TIMEOUT,
                )
                ai_last_call = time.monotonic()
            except requests.RequestException as e:
                ai_last_call = time.monotonic()
                if attempt >= AI_MAX_RETRIES:
                    raise RuntimeError(f"Groq request failed: {type(e).__name__}: {e}")
                time.sleep(min(5.0, 1.5 * (attempt + 1)))
                continue

            if r.ok:
                data = r.json()
                choices = data.get("choices") or []
                if not choices:
                    raise RuntimeError("Groq returned no choices")
                content = ((choices[0].get("message") or {}).get("content") or "").strip()
                return _ai_json(content)

            if r.status_code == 429 and attempt < AI_MAX_RETRIES:
                retry_after = r.headers.get("retry-after")
                try:
                    delay = float(retry_after) if retry_after is not None else 5.0
                except ValueError:
                    delay = 5.0
                # Do not spin on a minute-level TPM limit.
                time.sleep(max(1.0, min(delay, 65.0)))
                continue

            try:
                detail = r.json()
            except Exception:
                detail = r.text[:800]
            raise RuntimeError(f"Groq HTTP {r.status_code}: {detail}")

    raise RuntimeError("Groq request failed after retries")


AI_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["CONFIRM", "WAIT", "REJECT"]},
        "timing": {"type": "string", "enum": ["EARLY", "READY", "LATE", "INVALID"]},
        "direction": {"type": "string", "enum": ["LONG", "SHORT"]},
        "reason": {"type": "string"},
        "reversal_watch": {"type": "boolean"}
    },
    "required": ["decision", "timing", "direction", "reason", "reversal_watch"],
    "additionalProperties": False
}


def ai_review_setup(sig, rows5, rows15):
    """Small AI timing gate. One request per fresh ICT setup, cached afterwards."""
    if not groq_client:
        if AI_REQUIRED:
            return None, "AI unavailable: GROQ_API_KEY is missing"
        return {"decision": "CONFIRM", "timing": "READY", "direction": sig["direction"], "reason": "ICT-only mode", "reversal_watch": False}, None

    cache_key = f"{sig['symbol']}:{sig['setup_key']}:{rows5[-1]['time']}"
    now = time.time()
    with ai_cache_lock:
        cached = ai_review_cache.get(cache_key)
        if cached and now - cached[0] < AI_CACHE_TTL:
            return dict(cached[1]), None

    payload = {
        "symbol": sig["symbol"],
        "direction": sig["direction"],
        "timing_data": {
            "bars_since_sweep": sig.get("bars_since_sweep"),
            "setup_age_bars": sig.get("setup_age_bars"),
            "ict_rank": round(float(sig.get("ict_rank", 0)), 2),
            "entry_zone": sig.get("entry_zone"),
            "entry": sig.get("entry"),
            "sl": sig.get("sl"),
            "context15": sig.get("context15"),
        },
        "ict": {
            "liquidity_sweep": True,
            "mss": True,
            "choch": True,
            "fvg": sig.get("fvg"),
            "ob": sig.get("ob"),
        },
        "recent_5m": _compact_candles(rows5, 8),
        "recent_15m": _compact_candles(rows15, 3),
    }
    system = (
        "You are SAIWAN Move Hunter's timing gate. The deterministic engine already proved "
        "Liquidity Sweep + MSS/CHOCH + FVG + OB. Decide only whether this is early enough to alert now. "
        "CONFIRM only when the move is still near its origin and structure is valid. WAIT when structure is "
        "valid but needs a fresh candle/retest. REJECT when clearly extended or invalidated. "
        "Never use RSI, volume, MACD, Fibonacci, ATR, EMA, indicators, scores, confidence or predictions. "
        "Return one compact JSON object with decision, timing, direction, reason, reversal_watch."
    )
    try:
        result = _groq_chat_json(system, payload, "saiwan_ai_review", AI_REVIEW_SCHEMA, max_tokens=128)
        decision = str(result.get("decision", "REJECT")).upper()
        timing = str(result.get("timing", "INVALID")).upper()
        direction = str(result.get("direction", sig["direction"])).upper()
        if decision not in {"CONFIRM", "WAIT", "REJECT"}: decision = "REJECT"
        if timing not in {"EARLY", "READY", "LATE", "INVALID"}: timing = "INVALID"
        if direction != sig["direction"]: decision = "REJECT"
        if timing == "LATE": decision = "REJECT"
        result.update({"decision": decision, "timing": timing, "direction": direction})
        with ai_cache_lock:
            ai_review_cache[cache_key] = (time.time(), dict(result))
            if len(ai_review_cache) > 500:
                cutoff = time.time() - AI_CACHE_TTL
                for k, v in list(ai_review_cache.items()):
                    if v[0] < cutoff:
                        ai_review_cache.pop(k, None)
        return result, None
    except Exception as e:
        return None, f"AI review failed: {type(e).__name__}: {e}"


def collect_ict_candidates(symbol, rows5, rows15=None):
    """Phase 1: deterministic ICT scan only. No AI calls."""
    if len(rows5) < 120:
        return []
    for r in rows5:
        r["symbol"] = symbol
    out = []
    for direction in ("LONG", "SHORT"):
        sig = _move_setup(rows5, direction)
        if not sig:
            continue
        sig["symbol"] = symbol
        sig["context15"] = _context_15m(rows15, direction)
        sig["timeframe"] = "5m Entry · 15m Context"
        # If 15m context directly contradicts the setup, do not spend AI budget.
        if direction == "LONG" and sig["context15"] == "BEARISH CONTEXT":
            continue
        if direction == "SHORT" and sig["context15"] == "BULLISH CONTEXT":
            continue
        out.append(sig)
    return out


def analyze(symbol, rows5, rows15=None):
    """Compatibility wrapper: deterministic ICT candidate + AI gate."""
    candidates = collect_ict_candidates(symbol, rows5, rows15)
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.get("ict_rank", 0), reverse=True)
    sig = candidates[0]
    ai, err = ai_review_setup(sig, rows5, rows15 or [])
    if err:
        print(f"AI REVIEW {symbol} {sig['direction']}: {err}")
        return None
    if not ai or ai.get("decision") != "CONFIRM":
        return None
    sig["ai_timing"] = ai.get("timing", "READY")
    sig["ai_reason"] = ai.get("reason", "ICT setup confirmed")
    sig["ai_reversal_watch"] = bool(ai.get("reversal_watch", False))
    return sig

def make_chart(sig):
    """Render the SAIWAN Move Hunter setup with every ICT component annotated."""
    rows = sig["rows"]
    n = len(rows)
    direction = sig["direction"]
    entry, sl = sig["entry"], sig["sl"]
    tp1, tp2, tp3 = sig["tp1"], sig["tp2"], sig["tp3"]
    BG, GRID, TEXT, MUTED = "#f7f7f8", "#e4e6e8", "#17191c", "#73777d"
    UP, DOWN, GOLD, PURPLE = "#16a085", "#e14b55", "#c8a84e", "#7957d5"
    fig, ax = plt.subplots(figsize=(14.4, 7.8), dpi=170, facecolor=BG)
    ax.set_facecolor(BG)
    width = 0.58
    for i, r in enumerate(rows):
        c = UP if r["close"] >= r["open"] else DOWN
        ax.vlines(i, r["low"], r["high"], color=c, linewidth=1.0, zorder=3)
        lo = min(r["open"], r["close"])
        bh = max(abs(r["close"]-r["open"]), abs(r["close"])*1e-5)
        ax.add_patch(Rectangle((i-width/2, lo), width, bh, facecolor=c, edgecolor=c, linewidth=.5, zorder=4))

    right = n + 14
    fvg = sig["fvg"]; ob = sig["ob"]; zone = sig["entry_zone"]
    def box(z, color, alpha, label, yoff=0):
        local_index = z.get("index", 0) - (sig.get("full_len", n) - n) if "index" in z else 0
        x0 = max(0, min(n-1, local_index - max(3, n//10)))
        ax.add_patch(Rectangle((x0, z["low"]), right-x0, z["high"]-z["low"], facecolor=color, edgecolor=color, alpha=alpha, linewidth=1.0, zorder=1))
        ax.text(x0+1, z["high"]+yoff, label, color=color, fontsize=8.2, fontweight="bold", va="bottom", zorder=6)

    box(ob, GOLD, .13, "ORDER BLOCK")
    box(fvg, PURPLE, .15, "FVG")
    ax.add_patch(Rectangle((max(0, fvg["index"]-2), zone["low"]), right-max(0, fvg["index"]-2), zone["high"]-zone["low"], facecolor=PURPLE, edgecolor=PURPLE, alpha=.08, linewidth=1.2, zorder=0))
    ax.text(max(0, fvg["index"]-1), zone["high"], "ENTRY ZONE", color=PURPLE, fontsize=8, fontweight="bold", va="bottom")

    # Map stored indices from full series to chart-local indices using timestamp.
    times = {r["time"]: i for i, r in enumerate(rows)}
    full_rows = rows
    sweep_price = sig["liquidity"]
    # Sweep and MSS indices are converted approximately from the setup's latest
    # chart window by matching the closest candle timestamp when possible.
    sweep_local = max(0, n-1)
    mss_local = max(0, n-1)
    # The stored setup indices refer to the full scan; derive their local offset
    # from the visible window size.
    full_len_hint = sig.get("full_len", n)
    sweep_local = sig["sweep_index"] - (full_len_hint - n)
    mss_local = sig["mss_index"] - (full_len_hint - n)
    if 0 <= sweep_local < n:
        ax.scatter([sweep_local], [sweep_price], s=55, marker="v" if direction == "SHORT" else "^", color=DOWN if direction == "SHORT" else UP, zorder=8)
        ax.annotate("LIQUIDITY SWEEP", xy=(sweep_local, sweep_price), xytext=(max(0,sweep_local-10), sweep_price), arrowprops=dict(arrowstyle="->", color=DOWN if direction=="SHORT" else UP, lw=1.4), color=DOWN if direction=="SHORT" else UP, fontsize=8.4, fontweight="bold")
    if 0 <= mss_local < n:
        ax.axhline(sig["mss_level"], color=GOLD, linestyle="--", linewidth=1.0, alpha=.85)
        ax.annotate("MSS / CHOCH", xy=(mss_local, sig["mss_level"]), xytext=(max(0,mss_local-10), sig["mss_level"]), arrowprops=dict(arrowstyle="->", color=GOLD, lw=1.4), color=GOLD, fontsize=8.4, fontweight="bold")

    ax.axhline(entry, color=TEXT, linewidth=1.15, linestyle="--")
    ax.axhline(sl, color=DOWN, linewidth=1.0)
    for y, lab, c in [(tp1,"TP1",UP),(tp2,"TP2",UP),(tp3,"TP3",UP)]:
        ax.axhline(y, color=c, linewidth=.9, linestyle=":")
        ax.text(right+.3, y, f"{lab} {fmt_price(y)}", color=c, fontsize=8, fontweight="bold", va="center")
    ax.text(right+.3, entry, f"ENTRY {fmt_price(entry)}", color=TEXT, fontsize=8, fontweight="bold", va="center")
    ax.text(right+.3, sl, f"SL {fmt_price(sl)}", color=DOWN, fontsize=8, fontweight="bold", va="center")

    arrow_color = UP if direction == "LONG" else DOWN
    ax.scatter([n-1], [entry], s=42, color=arrow_color, edgecolor="white", linewidth=.8, zorder=9)
    ax.annotate(direction, xy=(n-1, entry), xytext=(max(0,n-15), entry), arrowprops=dict(arrowstyle="->", color=arrow_color, lw=1.7), color=arrow_color, fontsize=10, fontweight="bold")
    ax.text(.01, 1.055, f"{sig['symbol']} · SAIWAN CRYPTO SIGNAL · 5m ENTRY · Bitget Futures", transform=ax.transAxes, fontsize=15, color=TEXT, fontweight="bold")
    ax.text(.01, 1.018, "LIQUIDITY SWEEP → MSS → CHOCH → FVG → OB → ENTRY", transform=ax.transAxes, fontsize=9.5, color=PURPLE, fontweight="bold")
    ax.text(.99, 1.018, direction, transform=ax.transAxes, fontsize=11, color=arrow_color, fontweight="bold", ha="right")
    ax.text(.01, .018, "SAIWAN Move Hunter · 5m closed entry · 15m context · ICT price action only", transform=ax.transAxes, fontsize=8.2, color=MUTED)

    ax.yaxis.tick_right(); ax.tick_params(axis="y", colors=TEXT, labelsize=8.3, length=0)
    ax.tick_params(axis="x", colors=MUTED, labelsize=8, length=0, pad=8)
    ax.grid(axis="y", color=GRID, linewidth=.6); ax.grid(axis="x", color=GRID, linewidth=.4, alpha=.5)
    for side in ["top","left","bottom"]: ax.spines[side].set_visible(False)
    ax.spines["right"].set_color("#cfd3d7")
    step=max(1,n//7); ticks=list(range(0,n,step))
    if ticks[-1] != n-1: ticks.append(n-1)
    ax.set_xticks(ticks); ax.set_xticklabels([datetime.fromtimestamp(rows[i]["time"], tz=timezone.utc).strftime("%d\\n%H:%M") for i in ticks])
    all_lows=[r["low"] for r in rows]+[sl,tp3,ob["low"],fvg["low"]]
    all_highs=[r["high"] for r in rows]+[sl,tp3,ob["high"],fvg["high"]]
    ymin,ymax=min(all_lows),max(all_highs); span=max(ymax-ymin,abs(rows[-1]["close"])*.012)
    ax.set_ylim(ymin-span*.06,ymax+span*.12); ax.set_xlim(-1,right+8)
    fig.subplots_adjust(left=.035,right=.86,top=.89,bottom=.09)
    safe="".join(ch if ch.isalnum() else "_" for ch in sig["symbol"])
    path=f"/tmp/chart_{safe}_{sig['time']}.png"; fig.savefig(path,facecolor=BG,edgecolor="none"); plt.close(fig); return path


def telegram_url(method):
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    return TELEGRAM_API + TOKEN + "/" + method


def send_message(chat_id, text, reply_markup=None, reply_to_message_id=None):
    data = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    if reply_to_message_id is not None:
        data["reply_to_message_id"] = str(reply_to_message_id)
    r = requests.post(telegram_url("sendMessage"), data=data, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram sendMessage {r.status_code}: {r.text[:500]}")
    payload = r.json()
    return (payload.get("result") or {}).get("message_id")


def send_photo(chat_id, photo_path, caption, reply_markup=None):
    data = {"chat_id": chat_id, "caption": caption}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    with open(photo_path, "rb") as f:
        r = requests.post(telegram_url("sendPhoto"), data=data, files={"photo": f}, timeout=HTTP_TIMEOUT)
    if not r.ok:
        raise RuntimeError(f"Telegram sendPhoto {r.status_code}: {r.text[:1000]}")
    payload = r.json()
    return (payload.get("result") or {}).get("message_id")


def search_signals(query):
    q = (query or "").strip().upper().replace("/SEARCH", "").strip()
    if not q:
        return "Usage: /search SYMBOL\nExample: /search XRP"
    with state_lock:
        matches = [x.copy() for x in signal_history if q in x["symbol"].upper()]
    if not matches:
        return f"🔎 No saved SAIWAN signal found for {q}."
    matches = matches[-8:][::-1]
    lines = [f"🔎 SAIWAN SIGNAL SEARCH: {q}", ""]
    for x in matches:
        dt = datetime.fromtimestamp(x["time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        arrow = "🟢 LONG" if x["direction"] == "LONG" else "🔴 SHORT"
        lines.append(f"{arrow} {x['symbol']} · {dt}")
        lines.append(f"Entry {fmt_price(x['entry'])} · SL {fmt_price(x['sl'])} · TP1 {fmt_price(x['tp1'])}")
        if x.get("ai_reason"):
            lines.append(f"AI: {x['ai_reason']}")
        lines.append("")
    return "\n".join(lines).strip()


def ai_test():
    """Small Telegram diagnostic proving the Groq key/model are reachable."""
    if not groq_client:
        return "❌ AI TEST FAILED\nGROQ_API_KEY is missing."
    schema = {
        "type": "object",
        "properties": {"ok": {"type": "boolean"}, "reply": {"type": "string"}},
        "required": ["ok", "reply"],
        "additionalProperties": False
    }
    try:
        result = _groq_chat_json(
            "You are a connectivity test. Return JSON only.",
            "Reply with ok=true and a short reply saying SAIWAN AI OK.",
            "saiwan_ai_test", schema, max_tokens=80
        )
        if result.get("ok") is True:
            return f"✅ AI TEST OK\nModel: {GROQ_MODEL}\nReply: {result.get('reply', 'SAIWAN AI OK')}"
        return f"❌ AI TEST FAILED\nUnexpected response: {result}"
    except Exception as e:
        return f"❌ AI TEST FAILED\n{type(e).__name__}: {e}"


def status_text():
    with state_lock:
        return (
            "BOT STATUS: ONLINE\n"
            f"Scanner: {'RUNNING' if scanner_running else 'STOPPED'}\n"
            "Market: Bitget USDT Perpetual Futures (full eligible market)\n"
            "Strategy: SAIWAN CRYPTO SIGNAL — Move Hunter\n"
            "Model: SAIWAN Move Hunter — Liquidity Sweep + MSS + CHOCH + FVG + OB\n"
            "Data source: Bitget Futures market data\n"
            "Scan: 5m closed candles + 15m context\n"
            f"Pending signals: {len(pending_signals)}\n"
            f"Tracked signals: {len(active_signals)}\n"
            f"AI: {GROQ_MODEL if groq_client else 'NOT CONNECTED'}\n"
            "Chart: ICT components annotated\n"
            "TradingView: chart link only"
        )

def _error_bucket(exc):
    msg = str(exc).replace("\n", " ").strip()
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        code = getattr(resp, "status_code", None)
        if code:
            return f"HTTP {code}"
    if isinstance(exc, requests.Timeout):
        return "TIMEOUT"
    if isinstance(exc, requests.ConnectionError):
        return "CONNECTION"
    return type(exc).__name__


def scan_once():
    """Two-phase market scan: ICT all-market -> AI only top fresh candidates."""
    global pending_signals
    contracts = get_contracts()
    tickers = get_tickers()
    tv = {x.get("symbol"): x for x in tickers}
    eligible = []
    for c in contracts:
        sym = c.get("symbol", "")
        try:
            liquidity = float(tv.get(sym, {}).get("usdtVolume", tv.get(sym, {}).get("quoteVolume", 0)))
        except (TypeError, ValueError):
            liquidity = 0.0
        if liquidity > 0:
            eligible.append((liquidity, sym))
    eligible.sort(reverse=True)
    pairs = [s for _, s in eligible] if MAX_PAIRS <= 0 else [s for _, s in eligible[:MAX_PAIRS]]

    def check_symbol(symbol):
        try:
            rows5 = get_klines(symbol, TF_5M, CANDLE_LIMIT)
            rows15 = get_klines(symbol, TF_15M, 180)
            if len(rows5) < 120 or len(rows15) < 30:
                return symbol, [], None
            return symbol, collect_ict_candidates(symbol, rows5, rows15), None
        except Exception as e:
            return symbol, [], e

    phase1 = []
    error_buckets = {}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = [pool.submit(check_symbol, symbol) for symbol in pairs]
        for fut in as_completed(futures):
            symbol, candidates, err = fut.result()
            if err is not None:
                key = _error_bucket(err)
                error_buckets[key] = error_buckets.get(key, 0) + 1
                continue
            phase1.extend(candidates)

    # Rank ONLY by ICT freshness/origin quality. No radar, confidence or score.
    phase1.sort(key=lambda x: (x.get("ict_rank", 0), x.get("time", 0)), reverse=True)
    ai_candidates = phase1[:3]
    confirmed = []
    ai_errors = 0
    for sig in ai_candidates:
        try:
            # Do not spend another request on a setup already sent/tracked.
            setup_key = f"{sig['symbol']}:{sig['setup_key']}"
            if any(k.startswith(setup_key) for k in seen_signals):
                continue
            rows5 = get_klines(sig["symbol"], TF_5M, CANDLE_LIMIT)
            rows15 = get_klines(sig["symbol"], TF_15M, 180)
            ai, err = ai_review_setup(sig, rows5, rows15)
            if err:
                ai_errors += 1
                print(f"AI REVIEW {sig['symbol']} {sig['direction']}: {err}")
                continue
            if not ai or ai.get("decision") != "CONFIRM":
                continue
            sig["ai_timing"] = ai.get("timing", "READY")
            sig["ai_reason"] = ai.get("reason", "ICT setup confirmed")
            sig["ai_reversal_watch"] = bool(ai.get("reversal_watch", False))
            sig["key"] = f"{sig['symbol']}:{sig['direction']}:{sig['time']}"
            if sig["key"] not in seen_signals:
                confirmed.append(sig)
        except Exception as e:
            ai_errors += 1
            print(f"AI REVIEW {sig.get('symbol')} {sig.get('direction')}: {_error_bucket(e)} {e}")

    with state_lock:
        active_symbols = {x.get("symbol") for x in active_signals.values()}
        for sig in confirmed:
            seen_signals.add(sig["key"])
            seen_order.append(sig["key"])
            if sig["symbol"] not in active_symbols:
                pending_signals.append(sig)
        # Keep only fresh ICT candidates; no legacy score/radar ordering.
        pending_signals.sort(key=lambda x: (x.get("ict_rank", 0), x.get("time", 0)), reverse=True)
        del pending_signals[8:]
        while len(seen_order) > 4000:
            seen_signals.discard(seen_order.pop(0))

    total_errors = sum(error_buckets.values())
    summary = ", ".join(f"{name}={count}" for name, count in sorted(error_buckets.items(), key=lambda kv: kv[1], reverse=True)[:4])
    print(f"Bitget Move Hunter scan: universe={len(eligible)}, scanned={len(pairs)}, ICT={len(phase1)}, AI_TOP={len(ai_candidates)}, confirmed={len(confirmed)}, AI_errors={ai_errors}, market_errors={total_errors}, workers={SCAN_WORKERS}")
    if summary:
        print(f"Bitget error summary: {summary}")

def signal_caption(sig):
    d = "🟢 LONG" if sig["direction"] == "LONG" else "🔴 SHORT"
    return (
        f"🚀 SAIWAN CRYPTO SIGNAL\n\n{d}\n"
        f"⭐ {sig['symbol']} · Bitget Futures\n"
        f"⏱ 5m Entry · 15m Context\n\n"
        "Liquidity Sweep ✓  ·  MSS ✓  ·  CHOCH ✓  ·  FVG ✓  ·  OB ✓\n"
        f"15m Context: {sig.get('context15','UNKNOWN')}\n"
        f"Entry: {fmt_price(sig['entry'])}\n"
        f"SL: {fmt_price(sig['sl'])}\n"
        f"TP1: {fmt_price(sig['tp1'])}\n"
        f"TP2: {fmt_price(sig['tp2'])}\n"
        f"TP3: {fmt_price(sig['tp3'])}\n\n"
        f"🧠 AI timing: {sig.get('ai_timing', 'READY')}\n"
        f"AI note: {sig.get('ai_reason', 'ICT setup confirmed')}\n\n"
        "⚡ Early move setup — closed candles only.\n"
        "⚠️ Signal only — no automatic trading."
    )

def scanner_loop():
    global scanner_running
    scanner_running=True
    while not stop_event.is_set():
        try: scan_once()
        except Exception as e: print(f"SCAN LOOP ERROR {type(e).__name__}: {e}")
        force_scan_event.clear()
        for _ in range(SCAN_INTERVAL):
            if stop_event.is_set() or force_scan_event.is_set(): break
            time.sleep(1)
    scanner_running=False


def track_sent_signal(sig, chat_id, message_id):
    global signal_history
    if not message_id:
        return
    with state_lock:
        active_signals[sig["key"]] = {
            "key": sig["key"],
            "chat_id": chat_id,
            "message_id": message_id,
            "symbol": sig["symbol"],
            "direction": sig["direction"],
            "entry": sig["entry"],
            "sl": sig["sl"],
            "tp1": sig["tp1"],
            "tp2": sig["tp2"],
            "tp3": sig["tp3"],
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "closed": False,
        }
        signal_history.append({
            "key": sig["key"], "symbol": sig["symbol"], "direction": sig["direction"],
            "time": sig["time"], "entry": sig["entry"], "sl": sig["sl"],
            "tp1": sig["tp1"], "tp2": sig["tp2"], "tp3": sig["tp3"],
            "ai_timing": sig.get("ai_timing", "READY"), "ai_reason": sig.get("ai_reason", ""),
        })
        if len(signal_history) > MAX_SIGNAL_HISTORY:
            del signal_history[:-MAX_SIGNAL_HISTORY]

def _hit_level(direction, price, level):
    return price >= level if direction == "LONG" else price <= level

def monitor_active_signals():
    global active_signals
    # Monitoring stays alive even when /stop pauses the scanner, so already-sent
    # signals can still receive TP/SL replies.
    while True:
        time.sleep(30)
        with state_lock:
            tracked = list(active_signals.values())
        if not tracked:
            continue
        try:
            tv = {x.get("symbol"): x for x in get_tickers()}
        except Exception as e:
            print(f"TP MONITOR ERROR {_error_bucket(e)}: {e}")
            continue

        for state in tracked:
            if state.get("closed"):
                continue
            ticker = tv.get(state["symbol"]) or {}
            try:
                price = float(ticker.get("lastPr"))
            except (TypeError, ValueError):
                continue

            try:
                # Stop monitoring after SL. This prevents a later TP notification
                # after the original setup has already been invalidated.
                if _hit_level(state["direction"], state["sl"], price):
                    send_message(
                        state["chat_id"],
                        f"🛑 SL Hit\n⭐ {state['symbol']}\n💵 Price: {fmt_price(price)}",
                        reply_to_message_id=state["message_id"],
                    )
                    with state_lock:
                        active_signals.pop(state["key"], None)
                    continue

                for name in ("tp1", "tp2", "tp3"):
                    hit_key = f"{name}_hit"
                    if state[hit_key]:
                        continue
                    if _hit_level(state["direction"], price, state[name]):
                        label = name.upper().replace("TP", "TP")
                        send_message(
                            state["chat_id"],
                            f"🎯 {label} Hit\n⭐ {state['symbol']}\n💵 Price: {fmt_price(price)}",
                            reply_to_message_id=state["message_id"],
                        )
                        with state_lock:
                            if state["key"] in active_signals:
                                active_signals[state["key"]][hit_key] = True
                                if name == "tp3":
                                    active_signals.pop(state["key"], None)
                                    break
            except Exception as e:
                print(f"TP NOTIFY ERROR {state.get('symbol')}: {type(e).__name__}: {e}")

def sender_loop():
    global next_send_at
    while True:
        time.sleep(1)
        if not active_chat_id:
            continue
        now = time.time()
        if now < next_send_at:
            continue
        sig = None
        with state_lock:
            if pending_signals:
                # One new signal per 10-minute window; send the strongest candidate.
                pending_signals.sort(key=lambda x: (x.get("ict_rank", 0), x.get("time", 0)), reverse=True)
                sig = pending_signals.pop(0)
                pending_signals.clear()
        if not sig:
            continue
        try:
            path = make_chart(sig)
            tv_symbol = sig["symbol"]
            markup = {"inline_keyboard": [[{"text": "📈 TradingView", "url": f"https://www.tradingview.com/chart/?symbol=BITGET:{tv_symbol}"}]]}
            message_id = send_photo(active_chat_id, path, signal_caption(sig), markup)
            track_sent_signal(sig, active_chat_id, message_id)
            next_send_at = time.time() + SEND_INTERVAL
        except Exception as e:
            print(f"SEND ERROR {type(e).__name__}: {e}")


def start_scanner(chat_id):
    global scanner_thread, active_chat_id
    active_chat_id = chat_id
    with state_lock:
        running = scanner_running
    if not running:
        stop_event.clear()
        scanner_thread = threading.Thread(target=scanner_loop, daemon=True)
        scanner_thread.start()
    force_scan_event.set()


def stop_scanner():
    stop_event.set()


def poll_updates():
    global offset, active_chat_id
    conflict_wait = 3
    while True:
        try:
            r = requests.get(telegram_url("getUpdates"), params={"timeout": 25, "offset": offset, "allowed_updates": json.dumps(["message"])}, timeout=35)
            if r.status_code == 409:
                print("TELEGRAM 409 CONFLICT: another poller is active; retrying shortly")
                time.sleep(conflict_wait)
                conflict_wait = min(conflict_wait * 2, 30)
                continue
            r.raise_for_status()
            conflict_wait = 3
            data = r.json()
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                text = (msg.get("text") or "").strip()
                if not chat.get("id"):
                    continue
                active_chat_id = chat["id"]
                if text.startswith("/start"):
                    send_message(active_chat_id,
                        "🚀 SAIWAN CRYPTO SIGNAL\n\n"
                        "/scan - Start scanner\n"
                        "/stop - Stop scanner\n"
                        "/status - Bot status\n"
                        "/aitest - Test Groq AI connection\n"
                        "/search SYMBOL - Find saved signals\n\n"
                        "Market: Bitget USDT Perpetual Futures\n"
                        "Timeframe: 5m entry + 15m context\n"
                        "Model: SAIWAN Move Hunter — Liquidity Sweep + MSS + CHOCH + FVG + OB\n"
                        "Chart: professional 5m setup map with all ICT components\n"
                        "TP/SL monitoring: ENABLED")
                elif text.startswith("/scan"):
                    start_scanner(active_chat_id)
                    send_message(active_chat_id,
                        "🚀 SAIWAN CRYPTO SIGNAL SCANNER STARTED\n\n"
                        "5m closed candles for early entries + 15m context.\n"
                        "Signal hunts: Liquidity Sweep + MSS + CHOCH + FVG + OB.\n"
                        "The chart will mark every ICT component used.\n"
                        "TP/SL monitoring is enabled.")
                elif text.startswith("/stop"):
                    stop_scanner(); send_message(active_chat_id, "🛑 Scanner stopped.")
                elif text.startswith("/status"):
                    send_message(active_chat_id, status_text())
                elif text.startswith("/aitest"):
                    send_message(active_chat_id, ai_test())
                elif text.startswith("/search"):
                    send_message(active_chat_id, search_signals(text))
        except Exception as e:
            print(f"TELEGRAM ERROR {type(e).__name__}: {e}")
            time.sleep(3)


_services_started = False
_services_start_lock = threading.Lock()

def start_background_services():
    """Start Telegram/monitor services once, including when Gunicorn imports bot:app."""
    global _services_started
    if _services_started:
        return
    with _services_start_lock:
        if _services_started:
            return
        if not TOKEN:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
        try:
            requests.post(telegram_url("deleteWebhook"), data={"drop_pending_updates": "false"}, timeout=10)
        except Exception as e:
            print(f"TELEGRAM WEBHOOK CLEANUP WARNING: {type(e).__name__}: {e}")
        threading.Thread(target=poll_updates, name="telegram-poller", daemon=True).start()
        threading.Thread(target=sender_loop, name="signal-sender", daemon=True).start()
        threading.Thread(target=monitor_active_signals, name="tp-sl-monitor", daemon=True).start()
        _services_started = True
        print("SAIWAN services started: Telegram poller + signal sender + TP/SL monitor")


# Gunicorn imports bot:app instead of executing `python bot.py`.
# Start the background services during the worker's module import so Telegram
# commands and monitoring work in the Railway/Gunicorn deployment.
start_background_services()


def main():
    start_background_services()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
