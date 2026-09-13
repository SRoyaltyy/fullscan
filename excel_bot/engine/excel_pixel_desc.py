"""Dense per-letter descriptors for Simple View--Calculation.xlsx A1:JO364."""
from __future__ import annotations

from excel_clock_gate import FILL_OPEN, VALUE_OPEN_44
from signals import classify_fill

WINDOWS = (5, 10, 20)
STREAK_GATES = (2, 3, 5, 8)
COUNT_GATES = (3, 6, 8, 12)
LAGS = (0, 1, 2)
RATIO_ANCHORS = ("C", "J", "H")


def col_letters(end="JO"):
    out, s, n = [], "A", 0
    while _idx(s) <= _idx(end):
        out.append(s)
        s = _next(s)
        n += 1
        if n > 400:
            break
    return out


def _idx(s):
    n = 0
    for ch in s:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n


def _next(s):
    chars = list(s)
    i = len(chars) - 1
    while i >= 0:
        if chars[i] != "Z":
            chars[i] = chr(ord(chars[i]) + 1)
            return "".join(chars)
        chars[i] = "A"
        i -= 1
    return "A" + "".join(chars)


ALL_LETTERS = tuple(col_letters("JO"))

BANDS = (
    ("AO", [c for c in ALL_LETTERS if _idx(c) <= 15]),
    ("PZ", [c for c in ALL_LETTERS if 16 <= _idx(c) <= 26]),
    ("AAAZ", [c for c in ALL_LETTERS if 27 <= _idx(c) <= 52]),
    ("BABZ", [c for c in ALL_LETTERS if 53 <= _idx(c) <= 78]),
    ("CACZ", [c for c in ALL_LETTERS if 79 <= _idx(c) <= 104]),
    ("DADH", [c for c in ALL_LETTERS if 105 <= _idx(c) <= 112]),
    ("EAEZ", [c for c in ALL_LETTERS if 131 <= _idx(c) <= 156]),
    ("FAFZ", [c for c in ALL_LETTERS if 157 <= _idx(c) <= 182]),
    ("GAGZ", [c for c in ALL_LETTERS if 183 <= _idx(c) <= 208]),
    ("HAHZ", [c for c in ALL_LETTERS if 209 <= _idx(c) <= 234]),
    ("IAJO", [c for c in ALL_LETTERS if 235 <= _idx(c) <= 275]),
)


def _fam_score(hexv):
    try:
        return classify_fill(str(hexv).lstrip("#"))
    except Exception:
        return "none", 0.0


def _num(v):
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").replace("%", "").strip()
    if s.upper() in ("TRUE", "FALSE"):
        return 1.0 if s.upper() == "TRUE" else 0.0
    try:
        return float(s)
    except ValueError:
        return None


def _legal_lag(kind, let, lag):
    if lag >= 1:
        return True
    if kind == "fill":
        return let in FILL_OPEN
    if kind == "value":
        return let in VALUE_OPEN_44 and let not in ("H", "I")
    return False


def _letter_state(days, t, let, lag):
    i = t - lag
    if i < 0 or i >= len(days):
        return None
    d = days[i]
    fams = d.get("fams") or {}
    vals = d.get("vals") or {}
    fills = d.get("fills") or {}
    texts = d.get("texts") or {}
    if isinstance(fams, list):
        letters = list("ABCDEFGHIJKLMNO")
        fams = {letters[k]: fams[k] for k in range(min(15, len(fams)))}
    if isinstance(fills, list):
        letters = list("ABCDEFGHIJKLMNO")
        fills = {letters[k]: fills[k] for k in range(min(15, len(fills)))}
    fam = fams.get(let)
    sc = 0.0
    if fam is None and let in fills:
        fam, sc = _fam_score(fills[let])
    elif fam == "green":
        sc = 2.0
        if let in fills:
            _, sc = _fam_score(fills[let])
    elif fam == "red":
        sc = -2.0
        if let in fills:
            _, sc = _fam_score(fills[let])
    raw = vals.get(let)
    x = _num(raw)
    txt = texts.get(let)
    if txt is None and isinstance(raw, str) and _num(raw) is None:
        txt = raw
    present = fam not in (None, "none", "") or x is not None or bool(txt)
    return {"fam": fam or "none", "score": sc, "x": x, "txt": txt, "present": present}


def _streak(days, t, let, kind, lag):
    first = _letter_state(days, t, let, lag)
    if not first:
        return 0
    if kind == "pos" and (first["x"] is None or first["x"] <= 0):
        return 0
    if kind == "neg" and (first["x"] is None or first["x"] >= 0):
        return 0
    if kind == "green" and first["fam"] != "green":
        return 0
    if kind == "red" and first["fam"] != "red":
        return 0
    n = 0
    for k in range(0, 40):
        st = _letter_state(days, t, let, lag + k)
        if not st:
            break
        if kind == "pos" and (st["x"] is None or st["x"] <= 0):
            break
        if kind == "neg" and (st["x"] is None or st["x"] >= 0):
            break
        if kind == "green" and st["fam"] != "green":
            break
        if kind == "red" and st["fam"] != "red":
            break
        n += 1
    return n


def _window(days, t, let, w, lag):
    xs, gs, rs = [], 0, 0
    for k in range(lag, lag + w):
        st = _letter_state(days, t, let, k)
        if not st:
            continue
        if st["fam"] == "green":
            gs += 1
        elif st["fam"] == "red":
            rs += 1
        if st["x"] is not None:
            xs.append(st["x"])
    return gs, rs, xs


def pixel_features(days, t, quant):
    d = {}
    for let in ALL_LETTERS:
        for lag in LAGS:
            fill_ok = _legal_lag("fill", let, lag)
            val_ok = _legal_lag("value", let, lag)
            if not fill_ok and not val_ok:
                continue
            st = _letter_state(days, t, let, lag)
            if not st:
                if val_ok:
                    d[f"{let}@L{lag}_blank"] = True
                continue
            tag = f"{let}@L{lag}"
            if fill_ok:
                d[f"{tag}_g"] = st["fam"] == "green"
                d[f"{tag}_r"] = st["fam"] == "red"
                d[f"{tag}_deepg"] = st["fam"] == "green" and st["score"] >= 2
                d[f"{tag}_deepr"] = st["fam"] == "red" and st["score"] <= -2
                d[f"{tag}_none"] = st["fam"] in ("none", "neutral")
            if val_ok:
                d[f"{tag}_present"] = st["present"]
                d[f"{tag}_blank"] = not st["present"]
                if st["x"] is not None:
                    d[f"{tag}_pos"] = st["x"] > 0
                    d[f"{tag}_neg"] = st["x"] < 0
                    d[f"{tag}_zero"] = st["x"] == 0
                    if let in quant:
                        p10, p25, p50, p75, p90 = quant[let]
                        d[f"{tag}>p90"] = st["x"] > p90
                        d[f"{tag}>p75"] = st["x"] > p75
                        d[f"{tag}>p50"] = st["x"] > p50
                        d[f"{tag}<p25"] = st["x"] < p25
                        d[f"{tag}<p10"] = st["x"] < p10
                if st["txt"]:
                    token = st["txt"].strip()[:28]
                    if token and token.lower() not in ("none", "nan", "#n/a"):
                        d[f"{tag}={token}"] = True
            if lag >= 1 or fill_ok:
                sg = _streak(days, t, let, "green", lag)
                sr = _streak(days, t, let, "red", lag)
                for g in STREAK_GATES:
                    d[f"{let}@L{lag}_gstreak>={g}"] = sg >= g
                    d[f"{let}@L{lag}_rstreak>={g}"] = sr >= g
            if lag >= 1 or val_ok:
                sp = _streak(days, t, let, "pos", lag)
                sn = _streak(days, t, let, "neg", lag)
                for g in STREAK_GATES:
                    d[f"{let}@L{lag}_pstreak>={g}"] = sp >= g
                    d[f"{let}@L{lag}_nstreak>={g}"] = sn >= g
            wlag = max(lag, 1)
            for w in WINDOWS:
                gs, rs, xs = _window(days, t, let, w, wlag)
                for g in COUNT_GATES:
                    if g > w:
                        continue
                    d[f"{let}_w{w}_g>={g}"] = gs >= g
                    d[f"{let}_w{w}_r>={g}"] = rs >= g
                d[f"{let}_w{w}_g>r"] = gs > rs
                if xs:
                    d[f"{let}_w{w}_max>0"] = max(xs) > 0
                    d[f"{let}_w{w}_min<0"] = min(xs) < 0
                    d[f"{let}_w{w}_max_gt_absmin"] = max(xs) > abs(min(xs))
                    d[f"{let}_w{w}_last_is_max"] = xs[-1] == max(xs)
                    d[f"{let}_w{w}_last_is_min"] = xs[-1] == min(xs)
    anchors = {}
    for a in RATIO_ANCHORS:
        lag_a = 0 if a in VALUE_OPEN_44 and a not in ("H", "I") else 1
        st = _letter_state(days, t, a, lag_a)
        if st and st["x"] not in (None, 0):
            anchors[a] = st["x"]
    h1 = _letter_state(days, t, "H", 1)
    if h1 and h1["x"] not in (None, 0):
        anchors["H"] = h1["x"]
    for let in ALL_LETTERS:
        st = _letter_state(days, t, let, 1)
        if not st or st["x"] is None:
            continue
        for a, av in anchors.items():
            if a == let or av == 0:
                continue
            ratio = st["x"] / av
            d[f"ratio_{let}/{a}>1"] = ratio > 1
            d[f"ratio_{let}/{a}>1.5"] = ratio > 1.5
            d[f"ratio_{let}/{a}<0.5"] = ratio < 0.5
            d[f"ratio_{let}/{a}<0"] = ratio < 0
    for bname, letters in BANDS:
        for lag in (1, 2):
            g = r = 0
            for let in letters:
                st = _letter_state(days, t, let, lag)
                if not st:
                    continue
                if st["fam"] == "green":
                    g += 1
                elif st["fam"] == "red":
                    r += 1
            n = len(letters) or 1
            d[f"band_{bname}@L{lag}_g>=3"] = g >= 3
            d[f"band_{bname}@L{lag}_g>=6"] = g >= 6
            d[f"band_{bname}@L{lag}_r>=3"] = r >= 3
            d[f"band_{bname}@L{lag}_r>=6"] = r >= 6
            d[f"band_{bname}@L{lag}_g>r"] = g > r
            d[f"band_{bname}@L{lag}_majority_g"] = g > n / 2
            d[f"band_{bname}@L{lag}_majority_r"] = r > n / 2
    return {k: True for k, v in d.items() if v}
