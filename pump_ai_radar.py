import time

try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False


class EarlyPumpRadar:
    """Quantitative early-momentum detector with an optional trained ML layer.

    This is an experimental probability model, not a guarantee of future price movement.
    """
    def __init__(self, get_klines, train_pairs=25, train_bars=1600):
        self.get_klines = get_klines
        self.train_pairs = train_pairs
        self.train_bars = train_bars
        self.long_model = None
        self.short_model = None
        self.ready = False
        self.last_train = 0

    @staticmethod
    def ema(values, period):
        if not values:
            return []
        k = 2.0 / (period + 1.0)
        out = [values[0]]
        for x in values[1:]:
            out.append(x * k + out[-1] * (1 - k))
        return out

    @staticmethod
    def atr(rows, period=14):
        if len(rows) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(rows)):
            r, p = rows[i], rows[i - 1]
            trs.append(max(r["high"] - r["low"], abs(r["high"] - p["close"]), abs(r["low"] - p["close"])))
        return sum(trs[-period:]) / period

    @staticmethod
    def rsi(rows, period=14):
        if len(rows) < period + 2:
            return 50.0
        closes = [x["close"] for x in rows]
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i - 1]
            gains.append(max(d, 0.0))
            losses.append(max(-d, 0.0))
        ag = sum(gains[-period:]) / period
        al = sum(losses[-period:]) / period
        if al <= 1e-12:
            return 100.0
        return 100.0 - 100.0 / (1.0 + ag / al)

    def features(self, rows, idx=None):
        if idx is None:
            idx = len(rows) - 1
        if idx < 70:
            return None
        sub = rows[:idx + 1]
        cur = sub[-1]
        closes = [x["close"] for x in sub]
        vols = [x["vol"] for x in sub]
        a = self.atr(sub, 14)
        avg20 = sum(vols[-21:-1]) / 20.0
        avg60 = sum(vols[-61:-1]) / 60.0
        e9 = self.ema(closes, 9)[-1]
        e21 = self.ema(closes, 21)[-1]
        rng = max(cur["high"] - cur["low"], 1e-12)
        body = abs(cur["close"] - cur["open"])
        return [
            (closes[-1] / closes[-2] - 1) * 100,
            (closes[-1] / closes[-4] - 1) * 100,
            (closes[-1] / closes[-7] - 1) * 100,
            (closes[-1] / closes[-13] - 1) * 100,
            cur["vol"] / avg20 if avg20 else 0,
            cur["vol"] / avg60 if avg60 else 0,
            body / a if a else 0,
            rng / a if a else 0,
            (cur["close"] - cur["low"]) / rng,
            (e9 / e21 - 1) * 100 if e21 else 0,
            self.rsi(sub, 14),
        ]

    def train(self, pairs):
        if not SKLEARN_OK:
            print("AI RADAR: scikit-learn unavailable; heuristic mode only")
            return False
        X, yl, ys = [], [], []
        for symbol in pairs[:self.train_pairs]:
            try:
                rows = self.get_klines(symbol, "Min5", self.train_bars)
                if len(rows) < 300:
                    continue
                for i in range(80, len(rows) - 8):
                    f = self.features(rows, i)
                    future = rows[i + 1:i + 7]
                    if not f or len(future) < 6:
                        continue
                    base = rows[i]["close"]
                    up = max(x["high"] / base - 1 for x in future)
                    dn = min(x["low"] / base - 1 for x in future)
                    X.append(f)
                    yl.append(1 if up >= 0.020 else 0)
                    ys.append(1 if dn <= -0.020 else 0)
            except Exception as e:
                print(f"AI RADAR TRAIN SKIP {symbol}: {type(e).__name__}: {e}")
        if len(X) < 500 or len(set(yl)) < 2 or len(set(ys)) < 2:
            print(f"AI RADAR: not enough training diversity, samples={len(X)}")
            return False
        try:
            self.long_model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=500, class_weight="balanced"))
            self.short_model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=500, class_weight="balanced"))
            self.long_model.fit(X, yl)
            self.short_model.fit(X, ys)
            self.ready = True
            self.last_train = time.time()
            print(f"AI RADAR trained: {len(X)} samples")
            return True
        except Exception as e:
            print(f"AI RADAR TRAIN ERROR {type(e).__name__}: {e}")
            self.ready = False
            return False

    def probabilities(self, rows):
        if not self.ready:
            return 0.0, 0.0
        f = self.features(rows)
        if not f:
            return 0.0, 0.0
        try:
            return (
                float(self.long_model.predict_proba([f])[0][1]),
                float(self.short_model.predict_proba([f])[0][1]),
            )
        except Exception:
            return 0.0, 0.0

    def score(self, rows, direction):
        if len(rows) < 80:
            return 0, {}
        f = self.features(rows)
        if not f:
            return 0, {}
        r1, r5, r30, r60, vr20, vr60, body_atr, range_atr, close_pos, ema_gap, rsi = f
        points = 0.0
        reasons = []
        if direction == "LONG":
            if r1 >= 0.35: points += 12; reasons.append("1m acceleration")
            if r5 >= 0.60: points += 12; reasons.append("5m momentum")
            if r30 >= 1.00: points += 10; reasons.append("30m momentum")
            if vr20 >= 2.0: points += 15; reasons.append("volume 2x")
            elif vr20 >= 1.5: points += 9; reasons.append("volume 1.5x")
            if vr60 >= 1.5: points += 8; reasons.append("volume expansion")
            if body_atr >= 1.2: points += 8; reasons.append("displacement")
            if close_pos >= 0.70: points += 6; reasons.append("close strength")
            if ema_gap > 0: points += 5
            if 55 <= rsi <= 82: points += 5
            lp, _ = self.probabilities(rows)
            if lp >= 0.60: points += 19 * lp; reasons.append(f"ML {lp:.0%}")
            elif lp >= 0.50: points += 8
            return min(100, round(points)), {"ml_prob": lp, "volume_ratio": vr20, "ret5m": r5, "ret30m": r30, "reasons": reasons}
        else:
            if r1 <= -0.35: points += 12; reasons.append("1m acceleration")
            if r5 <= -0.60: points += 12; reasons.append("5m momentum")
            if r30 <= -1.00: points += 10; reasons.append("30m momentum")
            if vr20 >= 2.0: points += 15; reasons.append("volume 2x")
            elif vr20 >= 1.5: points += 9; reasons.append("volume 1.5x")
            if vr60 >= 1.5: points += 8; reasons.append("volume expansion")
            if body_atr >= 1.2: points += 8; reasons.append("displacement")
            if close_pos <= 0.30: points += 6; reasons.append("close weakness")
            if ema_gap < 0: points += 5
            if 18 <= rsi <= 45: points += 5
            _, sp = self.probabilities(rows)
            if sp >= 0.60: points += 19 * sp; reasons.append(f"ML {sp:.0%}")
            elif sp >= 0.50: points += 8
            return min(100, round(points)), {"ml_prob": sp, "volume_ratio": vr20, "ret5m": r5, "ret30m": r30, "reasons": reasons}

    def detect(self, rows1m, rows5m, gate=72):
        if len(rows5m) < 80:
            return None
        r5 = rows5m[:-1] if len(rows5m) > 90 else rows5m
        r1 = rows1m[:-1] if len(rows1m) > 90 else rows1m
        lp_score, lp = self.score(r5, "LONG")
        sp_score, sp = self.score(r5, "SHORT")
        # Extra 1m acceleration bonus, so an alert can arrive before a 15m candle closes.
        if len(r1) >= 20:
            last = r1[-1]["close"]
            ret1m = (last / r1[-2]["close"] - 1) * 100
            ret10m = (last / r1[-11]["close"] - 1) * 100
            if lp_score > sp_score:
                if ret1m >= 0.25: lp_score += 5
                if ret10m >= 0.70: lp_score += 5
            else:
                if ret1m <= -0.25: sp_score += 5
                if ret10m <= -0.70: sp_score += 5
        score = max(lp_score, sp_score)
        if score < gate:
            return None
        direction = "LONG" if lp_score >= sp_score else "SHORT"
        meta = lp if direction == "LONG" else sp
        return {
            "direction": direction,
            "score": min(100, score),
            "ml_prob": meta["ml_prob"],
            "volume_ratio": meta["volume_ratio"],
            "ret5m": meta["ret5m"],
            "ret30m": meta["ret30m"],
            "reasons": meta["reasons"],
            "time": r5[-1]["time"],
            "rows5": r5[-60:],
            "rows1m": r1[-60:],
        }
