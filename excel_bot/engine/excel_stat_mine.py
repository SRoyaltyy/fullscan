"""Statistical mine over the WHOLE Excel emulator sheet A..JL.

Every letter is a feature source. Same-row close atoms enter as lag-1.
Runtime is controlled by statistics, not by dropping columns:
  1. binary descriptors per letter (fill lag, value bins, 10-row counts, text)
  2. univariate 2x2 vs I>0: support, lift, chi-square, mutual info
  3. Benjamini-Hochberg FDR on discovery p-values
  4. holdout must keep lift > 1
  5. Apriori pairs among FDR survivors

Holdout is the last 30% of *session dates*. Discovery may use a row only
when the feature date AND the label window (I1 / I5 last close) are
strictly before that cutoff. A ticker-name split is not leak-free.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import FILL_OPEN, VALUE_OPEN_44, assert_excel_clock_gate, gate_payload  # noqa: E402
from excel_deep_corr_mine import (  # noqa: E402
    AVGVOL_MIN, HOLD_FRAC, MCAP_MIN_M, SPLIT_KIND, _f, _fwd,
    find_grids, iso_date, load_finviz_filter, load_grids, time_split_cutoff,
)
from signals import classify_fill  # noqa: E402

FDR_Q = 0.10
MIN_SUPPORT = 0.02
MIN_DISC_N = 200
MIN_HOLD_N = 80
PAIR_MAX_SEEDS = 80
# Re-export so ckpt / pixel hook / tests can keep importing from here.
HOLD_FRAC = HOLD_FRAC
SPLIT_KIND = SPLIT_KIND


def col_letters(end="JL"):
    out = []
    s = "A"
    n = 0
    while _col_idx(s) <= _col_idx(end):
        out.append(s)
        s = _next_col(s)
        n += 1
        if n > 400:
            break
    return out


def _col_idx(s):
    n = 0
    for ch in s:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n


def _next_col(s):
    chars = list(s)
    i = len(chars) - 1
    while i >= 0:
        if chars[i] != "Z":
            chars[i] = chr(ord(chars[i]) + 1)
            return "".join(chars)
        chars[i] = "A"
        i -= 1
    return "A" + "".join(chars)


ALL_LETTERS = tuple(col_letters("JL"))


def _fam(hexv):
    try:
        fam, _ = classify_fill(str(hexv).lstrip("#"))
        return fam
    except Exception:
        return "none"


def _fills_map(day):
    raw = day.get("fills")
    out = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            out[str(k).upper()] = v
        return out
    if isinstance(raw, list):
        letters = list("ABCDEFGHIJKLMNO")
        for i, v in enumerate(raw[:15]):
            out[letters[i]] = v
    return out


def _vals_map(day):
    raw = day.get("values") or day.get("cols") or {}
    if not isinstance(raw, dict):
        return {}
    return {str(k).upper(): v for k, v in raw.items()}


def _chi2_p(a, b, c, d):
    n = a + b + c + d
    if n <= 0:
        return 0.0, 1.0
    r1, r2 = a + b, c + d
    c1, c2 = a + c, b + d
    if r1 * r2 * c1 * c2 == 0:
        return 0.0, 1.0
    chi = 0.0
    for obs, ex in ((a, r1 * c1 / n), (b, r1 * c2 / n), (c, r2 * c1 / n), (d, r2 * c2 / n)):
        if ex <= 0:
            continue
        chi += (obs - ex) ** 2 / ex
    p = math.erfc(math.sqrt(max(chi, 0.0) / 2.0))
    return chi, max(min(p, 1.0), 0.0)


def _mi(a, b, c, d):
    n = a + b + c + d
    if n <= 0:
        return 0.0
    mi = 0.0
    for x, rx, cx in ((a, a + b, a + c), (b, a + b, b + d), (c, c + d, a + c), (d, c + d, b + d)):
        if x <= 0 or rx <= 0 or cx <= 0:
            continue
        mi += (x / n) * math.log((x * n) / (rx * cx) + 1e-15)
    return mi


def bh_keep(rows, q=FDR_Q):
    scored = [(i, r["p"]) for i, r in enumerate(rows) if r.get("p") is not None]
    m = len(scored)
    if m == 0:
        return set()
    scored.sort(key=lambda t: t[1])
    cutoff = -1
    for rank, (i, p) in enumerate(scored, 1):
        if p <= q * rank / m:
            cutoff = rank
    return {rows[scored[k][0]]["rule"] for k in range(cutoff)}


def _is_num(v):
    try:
        float(str(v).replace(",", ""))
        return True
    except (TypeError, ValueError):
        return False


def normalize_days(raw):
    out = []
    prev_c = None
    for d in raw:
        o = _f(d.get("open") if "open" in d else d.get("o"))
        h = _f(d.get("high") if "high" in d else d.get("h"))
        l = _f(d.get("low") if "low" in d else d.get("l"))
        c = _f(d.get("close") if "close" in d else d.get("c"))
        fills = _fills_map(d)
        vals = _vals_map(d)
        fams = {k: _fam(v) for k, v in fills.items()}
        H = ((h - l) / o) if o and h is not None and l is not None and o > 0 else None
        I = ((c / prev_c) - 1.0) if c and prev_c and prev_c > 0 else None
        out.append({
            "date": iso_date(d.get("date")),
            "o": o, "h": h, "l": l, "c": c, "H": H, "I": I,
            "fills": fills, "vals": vals, "fams": fams,
            "texts": {k: str(v) for k, v in vals.items()
                      if isinstance(v, str) and v and not _is_num(v)},
        })
        prev_c = c if c is not None else prev_c
    return out


def label_horizon(label_key):
    k = str(label_key or "I1_green")
    if "10" in k:
        return 10
    if "5" in k:
        return 5
    return 1


def label_end_date(days, t, horizon):
    sl = days[t:t + horizon]
    if len(sl) < horizon:
        return ""
    return iso_date(sl[-1].get("date"))


def disc_label_ok(feat_date, label_end, cutoff):
    """True only if feature date AND the label's last close are < cutoff."""
    return bool(cutoff and feat_date and label_end
                and feat_date < cutoff and label_end < cutoff)


def discovery_quantiles(ticker_days, cutoff):
    bags = defaultdict(list)
    for tkr, raw in ticker_days.items():
        days = normalize_days(raw)
        for t in range(1, len(days)):
            feat = days[t].get("date") or ""
            if not cutoff or not feat or feat >= cutoff:
                continue
            for let, v in days[t - 1]["vals"].items():
                x = _f(v)
                if x is not None and math.isfinite(x):
                    bags[let].append(x)
    q = {}
    for let, xs in bags.items():
        if len(xs) < 50:
            continue
        xs = sorted(xs)
        def pct(p, xs=xs):
            return xs[int(p * (len(xs) - 1))]
        q[let] = (pct(0.25), pct(0.50), pct(0.75))
    return q


def features_at(days, t, quant):
    d = {}
    prev = days[t - 1] if t else None
    now = days[t]
    for let in FILL_OPEN:
        fam = now["fams"].get(let, "none")
        d[f"{let}0_g"] = fam == "green"
        d[f"{let}0_r"] = fam == "red"
    for let in VALUE_OPEN_44:
        x = _f(now["vals"].get(let))
        if let not in quant or x is None:
            continue
        p25, p50, p75 = quant[let]
        d[f"{let}0>p75"] = x > p75
        d[f"{let}0<p25"] = x < p25
        d[f"{let}0>p50"] = x > p50
    if prev is None:
        return {k: bool(v) for k, v in d.items() if v}
    for let in ALL_LETTERS:
        fam = prev["fams"].get(let)
        if fam:
            d[f"{let}-1_g"] = fam == "green"
            d[f"{let}-1_r"] = fam == "red"
        x = _f(prev["vals"].get(let))
        if let in quant and x is not None:
            p25, p50, p75 = quant[let]
            d[f"{let}-1>p75"] = x > p75
            d[f"{let}-1<p25"] = x < p25
        txt = prev["texts"].get(let)
        if txt:
            token = txt.strip()[:24]
            if token:
                d[f"{let}-1={token}"] = True
        sl = days[max(0, t - 10):t]
        if sl and any(let in row["fams"] for row in sl):
            g = sum(1 for row in sl if row["fams"].get(let) == "green")
            r = sum(1 for row in sl if row["fams"].get(let) == "red")
            d[f"{let}_10g>=6"] = g >= 6
            d[f"{let}_10r>=6"] = r >= 6
            d[f"{let}_10g>r"] = g > r
    return {k: bool(v) for k, v in d.items() if v}


def _labels(days, t):
    sl1 = _fwd(days, t, 1)
    sl5 = _fwd(days, t, 5)
    out = {}
    if sl1:
        out["I1_green"] = sl1["green"] > 0.5
        out["I1"] = sl1["meanI"]
        out["absI1"] = sl1["absI"]
        out["H1"] = sl1["meanH"]
    if sl5:
        out["I5_green"] = sl5["green"] > 0.5
        out["I5"] = sl5["meanI"]
        out["absI5"] = sl5["absI"]
    return out


def collect(ticker_days, cutoff, quant, which, label_key="I1_green"):
    rows = []
    hzn = label_horizon(label_key)
    for tkr, raw in ticker_days.items():
        days = normalize_days(raw)
        if len(days) < 30:
            continue
        for t in range(20, len(days)):
            if not days[t]["o"]:
                continue
            feat = days[t].get("date") or ""
            end = label_end_date(days, t, hzn)
            if which == "disc":
                if not disc_label_ok(feat, end, cutoff):
                    continue
            elif not feat or feat < cutoff:
                continue
            labs = _labels(days, t)
            if label_key not in labs:
                continue
            rows.append((features_at(days, t, quant), labs))
    return rows


def score_univariate(rows, label_key="I1_green"):
    n = len(rows)
    if n == 0:
        return [], n, 0
    lab_n = sum(1 for _, lab in rows if lab.get(label_key))
    counts = defaultdict(lambda: [0, 0])
    for feats, lab in rows:
        y = 1 if lab.get(label_key) else 0
        for k in feats:
            counts[k][1] += 1
            counts[k][0] += y
    base = lab_n / n if n else 0
    out = []
    for rule, (hit, supp_n) in counts.items():
        if supp_n < max(MIN_DISC_N, MIN_SUPPORT * n):
            continue
        a, b = hit, supp_n - hit
        c = lab_n - hit
        d = n - supp_n - c
        chi, p = _chi2_p(a, b, c, d)
        conf = a / supp_n if supp_n else 0
        out.append({"rule": rule, "n": supp_n, "label_n": a, "base": base,
                    "conf": conf, "lift": (conf / base) if base else None,
                    "chi2": chi, "p": p, "mi": _mi(a, b, c, d)})
    out.sort(key=lambda r: (r["p"], -(r["mi"] or 0)))
    return out, n, lab_n


def score_pairs(rows, seeds, label_key="I1_green"):
    n = len(rows)
    lab_n = sum(1 for _, lab in rows if lab.get(label_key))
    counts = defaultdict(lambda: [0, 0])
    for feats, lab in rows:
        on = [s for s in seeds if s in feats]
        if len(on) < 2:
            continue
        y = 1 if lab.get(label_key) else 0
        for i, a in enumerate(on):
            for b in on[i + 1:]:
                key = f"{a}|{b}"
                counts[key][1] += 1
                counts[key][0] += y
    base = lab_n / n if n else 0
    out = []
    for rule, (hit, supp_n) in counts.items():
        if supp_n < max(MIN_HOLD_N, MIN_SUPPORT * n / 2):
            continue
        a, b = hit, supp_n - hit
        c = lab_n - hit
        d = n - supp_n - c
        chi, p = _chi2_p(a, b, c, d)
        conf = a / supp_n if supp_n else 0
        out.append({"rule": rule, "n": supp_n, "label_n": a, "base": base,
                    "conf": conf, "lift": (conf / base) if base else None,
                    "chi2": chi, "p": p, "mi": _mi(a, b, c, d), "kind": "pair"})
    out.sort(key=lambda r: (r["p"], -(r["mi"] or 0)))
    return out


def confirm(hold_rows, rules, label_key="I1_green"):
    n = len(hold_rows)
    lab_n = sum(1 for _, lab in hold_rows if lab.get(label_key))
    want = set(rules)
    counts = defaultdict(lambda: [0, 0])
    for feats, lab in hold_rows:
        y = 1 if lab.get(label_key) else 0
        present = set(feats)
        for rule in want:
            if "|" in rule:
                a, b = rule.split("|", 1)
                ok = a in present and b in present
            else:
                ok = rule in present
            if not ok:
                continue
            counts[rule][1] += 1
            counts[rule][0] += y
    base = lab_n / n if n else 0
    out = {}
    for rule, (hit, supp_n) in counts.items():
        conf = hit / supp_n if supp_n else 0
        out[rule] = {"hold_n": supp_n, "hold_conf": conf,
                     "hold_lift": (conf / base) if base else None, "hold_base": base}
    return out


def write_board(path, meta, singles, pairs, base_disc, base_hold):
    lines = [
        "# Excel statistical mine — whole sheet A..JL", "", meta, "",
        "Method: every letter as lag-1 fill / value / 10-row paint + same-row "
        "open-12 fills and open-44 numbers. chi2 + mutual info on discovery, "
        f"Benjamini-Hochberg FDR q={FDR_Q}, holdout must keep lift>1. "
        "TIME-SPLIT: last 30% of session dates are holdout. Discovery feature "
        "date AND the full label window must be strictly before the cutoff. "
        "Pairs = Apriori AND of FDR survivors.", "",
        f"Discovery base P(I1 green)={base_disc:.3f} · holdout base={base_hold:.3f}", "",
        "## Holdout-confirmed singles (lowest p, lift>1 both sides)", "",
        "| rule | disc n | disc lift | disc p | MI | hold n | hold lift |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in singles[:60]:
        lines.append(
            f"| `{r['rule']}` | {r['n']} | {r.get('lift') or 0:.3f} | "
            f"{r['p']:.2e} | {r['mi']:.4f} | {r.get('hold_n') or 0} | "
            f"{r.get('hold_lift') or 0:.3f} |"
        )
    lines += ["", "## Holdout-confirmed pairs", "",
              "| rule | disc n | disc lift | disc p | hold n | hold lift |",
              "|---|---:|---:|---:|---:|---:|"]
    for r in pairs[:40]:
        lines.append(
            f"| `{r['rule']}` | {r['n']} | {r.get('lift') or 0:.3f} | "
            f"{r['p']:.2e} | {r.get('hold_n') or 0} | {r.get('hold_lift') or 0:.3f} |"
        )
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w", encoding="utf-8").write("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grids", default="")
    ap.add_argument("--finviz", default="")
    ap.add_argument("--split", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--label", default="I1_green")
    ap.add_argument("--out-md", default="03_scoreboard/EXCEL_STAT_MINE.md")
    ap.add_argument("--out-json", default="03_scoreboard/excel_stat_mine.json")
    args = ap.parse_args()
    assert_excel_clock_gate()
    print(f"[letters] {len(ALL_LETTERS)} columns A..JL", flush=True)
    finviz_path = args.finviz or next(
        (p for p in ("excel_bot/data/finviz_with_descriptions.csv",
                     "data/finviz_with_descriptions.csv") if os.path.isfile(p)), "")
    allow = load_finviz_filter(finviz_path) if finviz_path else {}
    print(f"[univ] {len(allow)} names mcap>${MCAP_MIN_M:.0f}M vol>{AVGVOL_MIN:.0f}", flush=True)
    files = find_grids(args.grids)
    if args.limit:
        files = files[: args.limit]
    ticker_days = load_grids(files, allow if allow else None)
    print(f"[grids] {len(ticker_days)} tickers", flush=True)
    cutoff = time_split_cutoff(ticker_days)
    print(f"[split] kind=time cutoff={cutoff} hold_frac={HOLD_FRAC} "
          f"(ticker split ignored)", flush=True)
    if not cutoff:
        raise SystemExit("no session dates — cannot time-split")
    quant = discovery_quantiles(ticker_days, cutoff)
    print(f"[quant] {len(quant)} letters with discovery numeric bins", flush=True)
    disc_rows = collect(ticker_days, cutoff, quant, "disc", args.label)
    hold_rows = collect(ticker_days, cutoff, quant, "hold", args.label)
    print(f"[rows] disc={len(disc_rows)} hold={len(hold_rows)}", flush=True)
    uni, n_d, lab_d = score_univariate(disc_rows, args.label)
    keep = bh_keep(uni, FDR_Q)
    uni_fdr = [r for r in uni if r["rule"] in keep and (r.get("lift") or 0) > 1]
    print(f"[fdr] {len(uni)} tested, {len(keep)} pass q={FDR_Q}, {len(uni_fdr)} lift>1", flush=True)
    hold_stats = confirm(hold_rows, [r["rule"] for r in uni_fdr], args.label)
    confirmed = []
    for r in uni_fdr:
        h = hold_stats.get(r["rule"]) or {}
        if (h.get("hold_n") or 0) < MIN_HOLD_N or (h.get("hold_lift") or 0) <= 1:
            continue
        confirmed.append(dict(r, **h))
    confirmed.sort(key=lambda r: (r["p"], -(r.get("hold_lift") or 0)))
    print(f"[hold] {len(confirmed)} singles confirmed", flush=True)
    seeds = [r["rule"] for r in confirmed[:PAIR_MAX_SEEDS]]
    pairs_d = score_pairs(disc_rows, seeds, args.label)
    pair_keep = bh_keep(pairs_d, FDR_Q)
    pairs_fdr = [r for r in pairs_d if r["rule"] in pair_keep and (r.get("lift") or 0) > 1]
    pair_hold = confirm(hold_rows, [r["rule"] for r in pairs_fdr], args.label)
    pairs_ok = []
    for r in pairs_fdr:
        h = pair_hold.get(r["rule"]) or {}
        if (h.get("hold_n") or 0) < MIN_HOLD_N // 2 or (h.get("hold_lift") or 0) <= 1:
            continue
        pairs_ok.append(dict(r, **h))
    pairs_ok.sort(key=lambda r: (r["p"], -(r.get("hold_lift") or 0)))
    print(f"[pairs] {len(pairs_ok)} confirmed", flush=True)
    base_h = 0
    if pairs_ok:
        base_h = pair_hold.get(pairs_ok[0]["rule"], {}).get("hold_base") or 0
    elif confirmed:
        base_h = hold_stats.get(confirmed[0]["rule"], {}).get("hold_base") or 0
    write_board(args.out_md,
                f"TIME-SPLIT cutoff={cutoff} Tickers={len(ticker_days)} "
                f"letters={len(ALL_LETTERS)} "
                f"univ=mcap>${MCAP_MIN_M:.0f}M & vol>{AVGVOL_MIN:.0f} "
                f"gate={gate_payload()['gate']}",
                confirmed, pairs_ok, (lab_d / n_d if n_d else 0), base_h)
    slim = {"n_tickers": len(ticker_days), "n_letters": len(ALL_LETTERS),
            "n_disc_rows": n_d, "n_hold_rows": len(hold_rows),
            "n_univariate_tested": len(uni), "n_fdr": len(uni_fdr),
            "n_confirmed": len(confirmed), "n_pairs_confirmed": len(pairs_ok),
            "top_singles": confirmed[:30], "top_pairs": pairs_ok[:30],
            "gate": gate_payload(), "split_kind": SPLIT_KIND, "cutoff": cutoff}
    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    json.dump(slim, open(args.out_json, "w"), indent=1, default=str)
    print(f"[out] {args.out_md}", flush=True)


if __name__ == "__main__":
    main()
