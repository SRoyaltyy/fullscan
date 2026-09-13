"""Dense per-letter descriptors read off Simple View--Calculation.xlsx.

The live file is Sheet1 A1:JO364 — 275 letters, 35,769 formulas, CF fills.
Screenshot row-1 headers (noodle strip the owner scrolls):

  A Date  B Close  C Open  D High  E Low  F Volume
  G Vol ratio  H Intraday  I Daily  J Range/open-to-open
  K Body  L Candl  M Boomer  N Yesterday  O Volume sls
  P-Y tally / flag / #VALUE! mix
  DF / DG / DH 1-bar / 2-bar / 3-bar candle TEXT
  AH FR FQ ER EP EN JB JC ... open-44 prior-print tallies

Clock (excel_clock_gate): lag 0 only for fill_mine_open / value_mine_open.
H and I are labels on row T and features on rows above T. Same-row
DF/DG/DH/BB/BQ/BU is a leak — lag 1+ only.

This factory is the high-definition paint: every letter at every legal
lag emits presence, fill family, sign, frozen quantiles, sheet-native
thresholds, exact streak length, window extrema, ratios, candle tokens,
and horizontal past-X-columns counts. FDR + holdout still sit on top.

Research only. flatten_robust untouched.
"""
from __future__ import annotations

from excel_clock_gate import FILL_OPEN, VALUE_OPEN_44, SKIP_VALUE_OPS
from signals import classify_fill

WINDOWS = (3, 5, 10, 20, 40)
STREAK_GATES = (2, 3, 4, 5, 8, 12)
COUNT_GATES = (2, 3, 4, 6, 8, 12, 16)
LAGS = (0, 1, 2, 3, 5)
RATIO_ANCHORS = ("C", "J", "H", "G", "I")
COL_WINDOWS = (3, 5, 8, 15)

CUTS = {
    "G": (1.0, 1.5, 2.0, 2.5, 3.0, 5.0),
    "H": (0.02, 0.03, 0.05, 0.08, 0.12),
    "I": (0.01, 0.02, 0.03, 0.05, 0.08),
    "J": (0.01, 0.02, 0.03, 0.05),
    "K": (0.01, 0.02, 0.04),
    "F": (200_000, 700_000, 1_000_000, 3_000_000, 12_000_000),
    "AH": (1, 2, 3),
    "FR": (1, 2),
    "EN": (0, 2, 3),
    "EP": (0.03, 0.05),
    "FQ": (1,),
    "JB": (1,),
    "JC": (1,),
    "ER": (1,),
    "FS": (1, 2),
    "N": (1, 2),
    "O": (0.0,),
    "BQ": (0.0,),
    "BU": (0.0,),
    "M": (0.0,),
}

CAMERA = tuple("ABCDEFGHIJKLMNO")
CANDLE_LETTERS = ("DF", "DG", "DH", "EQ", "BB")
TALLY_LETTERS = (
    "AH", "BT", "BV", "Q", "Z", "AC", "FR", "FQ", "FS", "FU",
    "EN", "EP", "ER", "ES", "ET", "EU", "EV", "JB", "JC", "JD", "JE", "JF",
    "GD", "GE", "GF", "HF", "HG", "HW", "II", "IY", "IZ", "JL",
    "CG", "CH", "DC", "DE", "EB", "EK",
)
SKIP_NUM = set(SKIP_VALUE_OPS) | {"A", "IR"}

CANDLE_TAGS = (
    ("bull", "bullish"),
    ("bear", "bearish"),
    ("doji", "doji"),
    ("hammer", "hammer"),
    ("hang", "hanging"),
    ("shoot", "shooting"),
    ("engulf", "engulf"),
    ("star", "star"),
    ("harami", "harami"),
    ("maru", "marubozu"),
    ("soldier", "soldier"),
    ("crow", "crow"),
    ("tweezer", "tweezer"),
    ("kicker", "kicker"),
    ("piercing", "piercing"),
    ("cloud", "cloud"),
    ("spin", "spinning"),
    ("dragon", "dragonfly"),
    ("grave", "gravestone"),
    ("umbrella", "umbrella"),
    ("belt", "belt"),
    ("wave", "high wave"),
)


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
    if s.upper() in ("#VALUE!", "#REF!", "#N/A", "#DIV/0!", "N/A", "NONE", "NAN"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _is_error(v):
    if v is None:
        return False
    s = str(v).strip().upper()
    return s.startswith("#") or s in ("N/A", "NONE", "NAN")


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
    if fam is None and let in fills:
        fam, sc = _fam_score(fills[let])
    else:
        sc = 2.0 if fam == "green" else -2.0 if fam == "red" else 0.0
        if let in fills and fam in ("green", "red"):
            _, sc = _fam_score(fills[let])
    raw = vals.get(let)
    x = _num(raw)
    txt = texts.get(let)
    if txt is None and isinstance(raw, str) and _num(raw) is None:
        txt = raw
    err = _is_error(raw) or _is_error(txt)
    present = (fam not in (None, "none", "") or x is not None or bool(txt)) and not err
    return {
        "fam": fam or "none",
        "score": sc,
        "x": x,
        "txt": txt,
        "present": present,
        "err": err,
        "raw": raw,
    }


class _Cache:
    def __init__(self, days, t):
        self.days = days
        self.t = t
        self.mem = {}

    def st(self, let, lag):
        key = (let, lag)
        if key not in self.mem:
            self.mem[key] = _letter_state(self.days, self.t, let, lag)
        return self.mem[key]


def _streak(cx, let, kind, lag, cap=24):
    first = cx.st(let, lag)
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
    for k in range(0, cap):
        st = cx.st(let, lag + k)
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


def _window(cx, let, w, lag):
    xs, gs, rs, deepg, deepr = [], 0, 0, 0, 0
    for k in range(lag, lag + w):
        st = cx.st(let, k)
        if not st:
            continue
        if st["fam"] == "green":
            gs += 1
            if st["score"] >= 2:
                deepg += 1
        elif st["fam"] == "red":
            rs += 1
            if st["score"] <= -2:
                deepr += 1
        if st["x"] is not None:
            xs.append(st["x"])
    return gs, rs, xs, deepg, deepr


def _emit_cuts(d, tag, let, x):
    cuts = CUTS.get(let)
    if not cuts or x is None:
        return
    for c in cuts:
        d[f"{tag}>{c:g}"] = x > c
        d[f"{tag}>={c:g}"] = x >= c
        d[f"{tag}<{c:g}"] = x < c
        d[f"{tag}<={-c:g}"] = x <= -c
        d[f"{tag}>={-c:g}"] = x >= -c


def _emit_text(d, tag, txt):
    if not txt:
        return
    token = str(txt).strip()[:40]
    if not token or token.lower() in ("none", "nan", "#n/a", "#value!"):
        return
    d[f"{tag}={token}"] = True
    low = token.lower()
    for short, needle in CANDLE_TAGS:
        if needle in low:
            d[f"{tag}_{short}"] = True
    if "bullish" in low:
        d[f"{tag}_side=bull"] = True
    elif "bearish" in low:
        d[f"{tag}_side=bear"] = True


def _sum_i(cx, n):
    s = 0.0
    for k in range(1, n + 1):
        st = cx.st("I", k)
        if st and st["x"] is not None:
            s += st["x"]
    return s


def _hi_region(cx, d):
    for lag in (1, 2, 3, 5):
        h = cx.st("H", lag)
        i = cx.st("I", lag)
        if h:
            d[f"H@L{lag}_g"] = h["fam"] == "green"
            d[f"H@L{lag}_r"] = h["fam"] == "red"
            if h["x"] is not None:
                d[f"H@L{lag}_wide"] = h["x"] >= 0.05
                d[f"H@L{lag}_quiet"] = h["x"] < 0.02
        if i:
            d[f"I@L{lag}_g"] = i["fam"] == "green"
            d[f"I@L{lag}_r"] = i["fam"] == "red"
            if i["x"] is not None:
                d[f"I@L{lag}_up"] = i["x"] > 0
                d[f"I@L{lag}_down"] = i["x"] < 0
        if h and i:
            d[f"HI@L{lag}_both_g"] = h["fam"] == "green" and i["fam"] == "green"
            d[f"HI@L{lag}_both_r"] = h["fam"] == "red" and i["fam"] == "red"
            d[f"HI@L{lag}_split"] = (h["fam"] == "green") != (i["fam"] == "green")
            d[f"HI@L{lag}_deepg"] = (
                h["fam"] == "green" and i["fam"] == "green"
                and h["score"] >= 2 and i["score"] >= 2
            )
    ig = _streak(cx, "I", "green", 1)
    ir = _streak(cx, "I", "red", 1)
    hg = _streak(cx, "H", "green", 1)
    hr = _streak(cx, "H", "red", 1)
    for g in STREAK_GATES:
        d[f"Iregion_g>={g}"] = ig >= g
        d[f"Iregion_r>={g}"] = ir >= g
        d[f"Hregion_g>={g}"] = hg >= g
        d[f"Hregion_r>={g}"] = hr >= g
    d["Iregion_deepg"] = ig >= 5 or (ig >= 3 and _sum_i(cx, ig) >= 0.08)
    d["Iregion_deepr"] = ir >= 5
    i1 = cx.st("I", 1)
    i2 = cx.st("I", 2)
    if i1 and i2:
        d["I_broke_green"] = i1["fam"] != "green" and _streak(cx, "I", "green", 2) >= 3
        d["I_broke_red"] = i1["fam"] != "red" and _streak(cx, "I", "red", 2) >= 3
        d["I_flipped_to_g"] = i1["fam"] == "green" and i2["fam"] == "red"
        d["I_flipped_to_r"] = i1["fam"] == "red" and i2["fam"] == "green"


def _row_paint(cx, letters, lag):
    g = r = deepg = deepr = present = 0
    xs = []
    for let in letters:
        st = cx.st(let, lag)
        if not st:
            continue
        present += int(bool(st["present"]))
        if st["fam"] == "green":
            g += 1
            if st["score"] >= 2:
                deepg += 1
        elif st["fam"] == "red":
            r += 1
            if st["score"] <= -2:
                deepr += 1
        if st["x"] is not None:
            xs.append(st["x"])
    return g, r, deepg, deepr, present, xs


def _emit_band_stats(d, prefix, g, r, deepg, deepr, present, xs, n):
    n = n or 1
    d[f"{prefix}_g>=3"] = g >= 3
    d[f"{prefix}_g>=6"] = g >= 6
    d[f"{prefix}_g>=8"] = g >= 8
    d[f"{prefix}_r>=3"] = r >= 3
    d[f"{prefix}_r>=6"] = r >= 6
    d[f"{prefix}_r>=8"] = r >= 8
    d[f"{prefix}_g>r"] = g > r
    d[f"{prefix}_r>g"] = r > g
    d[f"{prefix}_majority_g"] = g > n / 2
    d[f"{prefix}_majority_r"] = r > n / 2
    d[f"{prefix}_all_g"] = present >= 3 and g == present and r == 0
    d[f"{prefix}_all_r"] = present >= 3 and r == present and g == 0
    d[f"{prefix}_deepg>=3"] = deepg >= 3
    d[f"{prefix}_deepr>=3"] = deepr >= 3
    d[f"{prefix}_blank>=3"] = (n - present) >= 3
    if xs:
        mx, mn = max(xs), min(xs)
        d[f"{prefix}_max>0"] = mx > 0
        d[f"{prefix}_min<0"] = mn < 0
        d[f"{prefix}_max_gt_absmin"] = mx > abs(mn)
        d[f"{prefix}_absmin_gt_max"] = abs(mn) > mx
        d[f"{prefix}_both_signs"] = mx > 0 and mn < 0
        d[f"{prefix}_all_pos"] = mn > 0
        d[f"{prefix}_all_neg"] = mx < 0


def pixel_features(days, t, quant):
    """Boolean pixel map for session T. `days` already normalized."""
    d = {}
    cx = _Cache(days, t)

    for let in ALL_LETTERS:
        for lag in LAGS:
            fill_ok = _legal_lag("fill", let, lag)
            val_ok = _legal_lag("value", let, lag)
            if not fill_ok and not val_ok:
                continue
            st = cx.st(let, lag)
            tag = f"{let}@L{lag}"
            if not st:
                if val_ok:
                    d[f"{tag}_blank"] = True
                    d[f"{tag}_missing"] = True
                continue
            if fill_ok:
                d[f"{tag}_g"] = st["fam"] == "green"
                d[f"{tag}_r"] = st["fam"] == "red"
                d[f"{tag}_deepg"] = st["fam"] == "green" and st["score"] >= 2
                d[f"{tag}_deepr"] = st["fam"] == "red" and st["score"] <= -2
                d[f"{tag}_none"] = st["fam"] in ("none", "neutral")
            if val_ok:
                d[f"{tag}_present"] = st["present"]
                d[f"{tag}_blank"] = not st["present"]
                d[f"{tag}_err"] = st["err"]
                d[f"{tag}_exists"] = st["present"] and not st["err"]
                if st["x"] is not None and let not in SKIP_NUM:
                    d[f"{tag}_pos"] = st["x"] > 0
                    d[f"{tag}_neg"] = st["x"] < 0
                    d[f"{tag}_zero"] = st["x"] == 0
                    d[f"{tag}_nonzero"] = st["x"] != 0
                    if let in quant:
                        p10, p25, p50, p75, p90 = quant[let]
                        d[f"{tag}>p90"] = st["x"] > p90
                        d[f"{tag}>p75"] = st["x"] > p75
                        d[f"{tag}>p50"] = st["x"] > p50
                        d[f"{tag}<p25"] = st["x"] < p25
                        d[f"{tag}<p10"] = st["x"] < p10
                    _emit_cuts(d, tag, let, st["x"])
                    if let in TALLY_LETTERS and abs(st["x"]) <= 12 and float(st["x"]).is_integer():
                        d[f"{tag}=={int(st['x'])}"] = True
                _emit_text(d, tag, st["txt"])

            if lag >= 1 or fill_ok:
                sg = _streak(cx, let, "green", lag)
                sr = _streak(cx, let, "red", lag)
                for g in STREAK_GATES:
                    d[f"{let}@L{lag}_gstreak>={g}"] = sg >= g
                    d[f"{let}@L{lag}_rstreak>={g}"] = sr >= g
                d[f"{let}@L{lag}_gstreak==1"] = sg == 1
                d[f"{let}@L{lag}_rstreak==1"] = sr == 1
            if (lag >= 1 or val_ok) and let not in SKIP_NUM:
                sp = _streak(cx, let, "pos", lag)
                sn = _streak(cx, let, "neg", lag)
                for g in STREAK_GATES:
                    d[f"{let}@L{lag}_pstreak>={g}"] = sp >= g
                    d[f"{let}@L{lag}_nstreak>={g}"] = sn >= g
                d[f"{let}@L{lag}_pstreak==1"] = sp == 1
                d[f"{let}@L{lag}_nstreak==1"] = sn == 1

            wlag = max(lag, 1)
            for w in WINDOWS:
                gs, rs, xs, deepg, deepr = _window(cx, let, w, wlag)
                wtag = f"{let}@L{lag}_w{w}"
                for g in COUNT_GATES:
                    if g > w:
                        continue
                    d[f"{wtag}_g>={g}"] = gs >= g
                    d[f"{wtag}_r>={g}"] = rs >= g
                if lag in (0, 1):
                    for g in COUNT_GATES:
                        if g > w:
                            continue
                        d[f"{let}_w{w}_g>={g}"] = gs >= g
                        d[f"{let}_w{w}_r>={g}"] = rs >= g
                    d[f"{let}_w{w}_g>r"] = gs > rs
                d[f"{wtag}_g>r"] = gs > rs
                d[f"{wtag}_r>g"] = rs > gs
                d[f"{wtag}_deepg>=3"] = deepg >= 3
                d[f"{wtag}_deepr>=3"] = deepr >= 3
                if xs:
                    mx, mn = max(xs), min(xs)
                    last = xs[0]
                    d[f"{wtag}_max>0"] = mx > 0
                    d[f"{wtag}_min<0"] = mn < 0
                    d[f"{wtag}_max_gt_absmin"] = mx > abs(mn)
                    d[f"{wtag}_absmin_gt_max"] = abs(mn) > mx
                    d[f"{wtag}_last_is_max"] = last == mx
                    d[f"{wtag}_last_is_min"] = last == mn
                    d[f"{wtag}_both_signs"] = mx > 0 and mn < 0
                    if lag in (0, 1):
                        d[f"{let}_w{w}_max>0"] = mx > 0
                        d[f"{let}_w{w}_min<0"] = mn < 0
                        d[f"{let}_w{w}_max_gt_absmin"] = mx > abs(mn)
                        d[f"{let}_w{w}_last_is_max"] = last == mx
                        d[f"{let}_w{w}_last_is_min"] = last == mn

    anchors = {}
    for a in RATIO_ANCHORS:
        lag_a = 0 if a in VALUE_OPEN_44 and a not in ("H", "I") else 1
        st = cx.st(a, lag_a)
        if st and st["x"] not in (None, 0):
            anchors[a] = st["x"]
    h1 = cx.st("H", 1)
    if h1 and h1["x"] not in (None, 0):
        anchors["H"] = h1["x"]
    i1 = cx.st("I", 1)
    if i1 and i1["x"] not in (None, 0):
        anchors["I"] = i1["x"]

    ratio_letters = CAMERA + TALLY_LETTERS + CANDLE_LETTERS
    for let in ratio_letters:
        st = cx.st(let, 1)
        if not st or st["x"] is None:
            continue
        for a, av in anchors.items():
            if a == let or av == 0:
                continue
            ratio = st["x"] / av
            d[f"ratio_{let}/{a}>1"] = ratio > 1
            d[f"ratio_{let}/{a}>1.5"] = ratio > 1.5
            d[f"ratio_{let}/{a}>2"] = ratio > 2
            d[f"ratio_{let}/{a}<0.5"] = ratio < 0.5
            d[f"ratio_{let}/{a}<0"] = ratio < 0
            d[f"ratio_{let}/{a}_abs>1"] = abs(ratio) > 1

    for i, let in enumerate(CAMERA):
        if i == 0:
            continue
        a, b = CAMERA[i - 1], let
        sa, sb = cx.st(a, 1), cx.st(b, 1)
        if not sa or not sb:
            continue
        d[f"neigh_{a}{b}@L1_both_g"] = sa["fam"] == "green" and sb["fam"] == "green"
        d[f"neigh_{a}{b}@L1_both_r"] = sa["fam"] == "red" and sb["fam"] == "red"
        d[f"neigh_{a}{b}@L1_gr"] = sa["fam"] == "green" and sb["fam"] == "red"
        d[f"neigh_{a}{b}@L1_rg"] = sa["fam"] == "red" and sb["fam"] == "green"
        if sa["x"] not in (None, 0) and sb["x"] is not None:
            d[f"ratio_{b}/{a}>1"] = sb["x"] / sa["x"] > 1

    c0 = cx.st("C", 0)
    b1 = cx.st("B", 1)
    if c0 and b1 and c0["x"] is not None and b1["x"] not in (None, 0):
        gap = c0["x"] / b1["x"] - 1.0
        d["gap_C/B>0"] = gap > 0
        d["gap_C/B>0.01"] = gap > 0.01
        d["gap_C/B>0.03"] = gap > 0.03
        d["gap_C/B<-0.01"] = gap < -0.01
        d["gap_C/B<-0.03"] = gap < -0.03

    j0 = cx.st("J", 0)
    if j0 and j0["x"] is not None:
        d["J@L0_pos"] = j0["x"] > 0
        d["J@L0_neg"] = j0["x"] < 0
        _emit_cuts(d, "J@L0", "J", j0["x"])

    for bname, letters in BANDS:
        for lag in (1, 2, 3):
            g, r, deepg, deepr, present, xs = _row_paint(cx, letters, lag)
            _emit_band_stats(
                d, f"band_{bname}@L{lag}", g, r, deepg, deepr, present, xs, len(letters)
            )

    for lag in (1, 2):
        for width in COL_WINDOWS:
            for start in range(0, len(CAMERA) - width + 1):
                chunk = CAMERA[start:start + width]
                g, r, deepg, deepr, present, xs = _row_paint(cx, chunk, lag)
                name = f"cols_{chunk[0]}{chunk[-1]}@L{lag}"
                d[f"{name}_g>r"] = g > r
                d[f"{name}_r>g"] = r > g
                d[f"{name}_g>={max(2, width // 2)}"] = g >= max(2, width // 2)
                d[f"{name}_r>={max(2, width // 2)}"] = r >= max(2, width // 2)
                d[f"{name}_all_g"] = g == present and r == 0 and present == width
                d[f"{name}_all_r"] = r == present and g == 0 and present == width
                if xs:
                    d[f"{name}_max_gt_absmin"] = max(xs) > abs(min(xs))
                    d[f"{name}_absmin_gt_max"] = abs(min(xs)) > max(xs)

    for lag in (1, 2, 3, 5):
        g, r, deepg, deepr, present, xs = _row_paint(cx, CAMERA, lag)
        _emit_band_stats(d, f"camera@L{lag}", g, r, deepg, deepr, present, xs, 15)
        d[f"camera@L{lag}_g>=10"] = g >= 10
        d[f"camera@L{lag}_r>=10"] = r >= 10

    _hi_region(cx, d)

    for lag in (1, 2, 3):
        texts = []
        for let in ("DF", "DG", "DH"):
            st = cx.st(let, lag)
            if st and st["txt"]:
                texts.append(str(st["txt"]))
        blob = " ".join(texts).lower()
        if blob:
            d[f"candles@L{lag}_bull"] = "bullish" in blob
            d[f"candles@L{lag}_bear"] = "bearish" in blob
            d[f"candles@L{lag}_doji"] = "doji" in blob
            d[f"candles@L{lag}_engulf"] = "engulf" in blob
            d[f"candles@L{lag}_star"] = "star" in blob
            bull = blob.count("bullish")
            bear = blob.count("bearish")
            d[f"candles@L{lag}_net_bull"] = bull > bear
            d[f"candles@L{lag}_net_bear"] = bear > bull
            d[f"candles@L{lag}_any"] = True

    for let in VALUE_OPEN_44:
        if let in SKIP_NUM:
            continue
        st = cx.st(let, 0)
        if not st:
            d[f"{let}@L0_missing"] = True
            continue
        d[f"{let}@L0_exists"] = st["present"] and not st["err"]
        d[f"{let}@L0_blank"] = not st["present"]
        d[f"{let}@L0_err"] = st["err"]

    return {k: True for k, v in d.items() if v}
