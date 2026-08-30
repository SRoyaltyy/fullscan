"""Turn per-day cell colors into formal, backtestable cluster definitions.

Pipeline: grid JSON (values + fills per day per column)
  -> color classification (green/red families with intensity)
  -> per-day features (column-A regime, weighted row score)
  -> parameterized cluster detectors (D1 strict, D2 tolerant, D3 hysteresis)
"""
import colorsys
import json


# ------------------------------------------------------- color taxonomy ---
def classify_fill(rgb):
    """RRGGBB -> (family, signed_score). Green +, red -, specials 0."""
    if not rgb:
        return "none", 0.0
    r, g, b = (int(rgb[i:i + 2], 16) / 255 for i in (0, 2, 4))
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    hue = h * 360
    if s < 0.12:
        return "neutral", 0.0
    if 70 <= hue <= 170:                       # green family
        if l < 0.45:
            return "green", 2.0                # deep green
        if l < 0.72:
            return "green", 1.5
        return "green", 1.0                    # light green
    if hue < 16 or hue > 335:                  # red family
        if l < 0.5:
            return "red", -2.0                 # deep/pure red
        if l < 0.72:
            return "red", -1.5
        return "red", -1.0                     # pink
    if 270 <= hue <= 335:
        return "purple", 0.0
    if 180 <= hue < 270:
        return "blue", 0.0
    if 16 <= hue < 70:
        return "orange", 0.0
    return "neutral", 0.0


# ------------------------------------------------------- daily features ---
def load_days(grid_json_path, weights=None):
    """Extract per-day records: date serial, close, col-A score, row score."""
    d = json.load(open(grid_json_path))
    days = []
    for row in d["grid"][1:]:  # skip header
        a = row[0]
        if not isinstance(a["value"], (int, float)) or a["value"] < 30000:
            continue
        close = row[1]["value"]
        if not isinstance(close, (int, float)):
            continue
        fams, scores = [], []
        for cell in row:
            fam, sc = classify_fill(cell["fill"])
            fams.append(fam)
            scores.append(sc)
        if weights:
            row_score = sum(s * weights.get(i, 1.0) for i, s in enumerate(scores))
        else:
            row_score = sum(scores)
        days.append({
            "date": a["value"], "close": close,
            "a_family": fams[0], "a_score": scores[0],
            "families": fams, "scores": scores, "row_score": row_score,
        })
    return days


# ----------------------------------------------------- cluster detectors ---
def detect(days, method="d2", **kw):
    """Return list of clusters: {start, end, side} as day indices [start, end)."""
    if method == "d1":
        regimes = [_regime_a(day) for day in days]
        return _runs(regimes, min_len=kw.get("min_len", 1))
    if method == "d2":
        tol = kw.get("tolerance", 2)
        min_len = kw.get("min_len", 3)
        regimes = [_regime_a(day) for day in days]
        return _tolerant_runs(regimes, tol, min_len)
    if method == "d3":
        enter = kw.get("enter", 4.0)
        exit_ = kw.get("exit", 0.0)
        min_len = kw.get("min_len", 3)
        return _hysteresis([d["row_score"] for d in days], enter, exit_, min_len)
    raise ValueError(method)


def _regime_a(day):
    if day["a_family"] == "green":
        return 1
    if day["a_family"] == "red":
        return -1
    return 0


def _runs(regimes, min_len):
    out, i, n = [], 0, len(regimes)
    while i < n:
        r = regimes[i]
        j = i
        while j < n and regimes[j] == r:
            j += 1
        if r != 0 and j - i >= min_len:
            out.append({"start": i, "end": j, "side": r,
                        "entry_idx": i + min_len - 1,
                        # flip knowable at close of first opposite day
                        "exit_idx": j if j < n else None})
        i = j
    return out


def _tolerant_runs(regimes, tol, min_len):
    """Regime persists through up to `tol`-1 consecutive opposite/neutral days."""
    out = []
    i, n = 0, len(regimes)
    while i < n:
        r = regimes[i]
        if r == 0:
            i += 1
            continue
        j = i
        last_good = i
        opp = 0
        exit_idx = None
        while j < n:
            if regimes[j] == r:
                last_good = j
                opp = 0
            else:
                opp += 1
            if opp >= tol:
                exit_idx = j  # flip confirmed at close of this day
                break
            j += 1
        end = last_good + 1
        if end - i >= min_len:
            out.append({"start": i, "end": end, "side": r,
                        "entry_idx": i + min_len - 1, "exit_idx": exit_idx})
        i = max(end, i + 1)
    return out


def _hysteresis(scores, enter, exit_, min_len):
    """Enter green when score>=enter, exit when score<=exit_ (mirror for red)."""
    out = []
    state, start, entry_idx = 0, 0, 0
    for i, s in enumerate(scores):
        if state == 0:
            if s >= enter:
                state, start, entry_idx = 1, i, i
            elif s <= -enter:
                state, start, entry_idx = -1, i, i
        elif state == 1 and s <= exit_:
            if i - start >= min_len:
                out.append({"start": start, "end": i, "side": 1,
                            "entry_idx": entry_idx, "exit_idx": i})
            state = 0
        elif state == -1 and s >= -exit_:
            if i - start >= min_len:
                out.append({"start": start, "end": i, "side": -1,
                            "entry_idx": entry_idx, "exit_idx": i})
            state = 0
    if state != 0 and len(scores) - start >= min_len:
        out.append({"start": start, "end": len(scores), "side": state,
                    "entry_idx": entry_idx, "exit_idx": None})
    return out


def cluster_trades(days, clusters):
    """Buy at close of first cluster day, sell at close of last (long green,
    short red). Returns trade list with returns."""
    trades = []
    for c in clusters:
        i, j = c["start"], c["end"] - 1
        entry, exitp = days[i]["close"], days[j]["close"]
        ret = (exitp - entry) / entry * c["side"]
        trades.append({**c, "entry": entry, "exit": exitp, "ret": ret,
                       "entry_date": days[i]["date"], "exit_date": days[j]["date"]})
    return trades
