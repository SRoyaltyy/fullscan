"""Excel-emulator deep correlation mine. See REGION_CORR_MINE.md."""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    FILL_OPEN,
    VALUE_OPEN_44,
    assert_excel_clock_gate,
    assert_feature_legal,
    gate_payload,
)
from excel_open_features import feature_flags, open_features  # noqa: E402
from signals import classify_fill  # noqa: E402

HORIZONS = (1, 5, 10)
FILL_WINDOWS = (5, 10, 20)
COUNT_GATES = (3, 6, 8)
MIN_N_DISC = 200
MIN_N_HOLD = 80
MCAP_MIN_M = 50.0
AVGVOL_MIN = 100_000.0
COMBO_TOP = 40
COMBO_PARTNERS = 25
FILL_LETTERS = list("ABCDEFGHIJKLMNO")
OPEN_FILL_IDX = {let: FILL_LETTERS.index(let) for let in FILL_OPEN if let in FILL_LETTERS}


def _f(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _hi(days):
    out = []
    prev_c = None
    for d in days:
        o = _f(d.get("open") if "open" in d else d.get("o"))
        h = _f(d.get("high") if "high" in d else d.get("h"))
        l = _f(d.get("low") if "low" in d else d.get("l"))
        c = _f(d.get("close") if "close" in d else d.get("c"))
        v = _f(d.get("volume") if "volume" in d else d.get("v"))
        H = ((h - l) / o) if o and h is not None and l is not None and o > 0 else None
        I = ((c / prev_c) - 1.0) if c and prev_c and prev_c > 0 else None
        fills = list(d.get("fills") or [])
        while len(fills) < 15:
            fills.append("")
        fams = []
        for hexv in fills[:15]:
            try:
                fam, _sc = classify_fill(str(hexv).lstrip("#"))
            except Exception:
                fam = "none"
            fams.append(fam)
        vals = d.get("values") or d.get("cols") or {}
        out.append({
            "date": d.get("date"), "o": o, "h": h, "l": l, "c": c, "v": v,
            "H": H, "I": I, "fills": fills[:15], "fams": fams, "values": vals,
        })
        prev_c = c if c is not None else prev_c
    return out


def load_finviz_filter(path):
    keep = {}
    if not os.path.isfile(path):
        return keep
    with open(path, encoding="utf-8", errors="replace") as fh:
        for rec in csv.DictReader(fh):
            t = (rec.get("Ticker") or "").strip()
            if not t:
                continue

            def num(k):
                v = (rec.get(k) or "").replace("%", "").replace(",", "").strip()
                mult = 1.0
                if v.endswith(("K", "M", "B")):
                    mult = {"K": 1e3, "M": 1e6, "B": 1e9}[v[-1]]
                    v = v[:-1]
                try:
                    return float(v) * mult
                except ValueError:
                    return None

            mcap = num("Market Cap")
            avg = num("Average Volume")
            if avg is not None:
                avg *= 1e3
            if mcap is None or avg is None:
                continue
            if mcap > MCAP_MIN_M and avg > AVGVOL_MIN:
                keep[t] = {"mcap": mcap, "avgvol": avg}
    return keep


def _fwd(days, t, hzn):
    sl = days[t:t + hzn]
    if len(sl) < hzn:
        return None
    Is = [d["I"] for d in sl if d["I"] is not None]
    Hs = [d["H"] for d in sl if d["H"] is not None]
    if not Is:
        return None
    n = len(Is)
    o0, cN = days[t]["o"], sl[-1]["c"]
    hold = ((cN / o0) - 1.0) if o0 and cN and o0 > 0 else None
    return {"green": sum(1 for x in Is if x > 0) / n,
            "red": sum(1 for x in Is if x < 0) / n,
            "meanI": sum(Is) / n, "absI": sum(abs(x) for x in Is) / n,
            "meanH": (sum(Hs) / len(Hs)) if Hs else None, "hold": hold}


def _hold_region(days, t, side):
    if t >= len(days) or not days[t]["o"]:
        return None
    end = t
    for k in range(t, len(days)):
        I = days[k]["I"]
        if I is None:
            end = k
            break
        if side > 0 and I <= 0:
            end = k
            break
        if side < 0 and I >= 0:
            end = k
            break
        end = k
    sl = days[t:end + 1]
    Is = [d["I"] for d in sl if d["I"] is not None]
    Hs = [d["H"] for d in sl if d["H"] is not None]
    if not Is or not days[end]["c"]:
        return None
    n = len(Is)
    return {"green": sum(1 for x in Is if x > 0) / n,
            "red": sum(1 for x in Is if x < 0) / n,
            "meanI": sum(Is) / n, "absI": sum(abs(x) for x in Is) / n,
            "meanH": (sum(Hs) / len(Hs)) if Hs else None,
            "hold": (days[end]["c"] / days[t]["o"]) - 1.0,
            "days": end - t + 1}


def _count_fam(days, t, idx, window, fam):
    sl = days[max(0, t - window):t]
    if not sl:
        return 0
    return sum(1 for d in sl if len(d["fams"]) > idx and d["fams"][idx] == fam)


def descriptors_at(days, t, flags, xl):
    d = {"uncond": True}
    fams = days[t]["fams"]
    for let, idx in OPEN_FILL_IDX.items():
        try:
            assert_feature_legal("fill", let, 0)
        except ValueError:
            continue
        fam = fams[idx] if idx < len(fams) else "none"
        d[f"fill0_{let}_green"] = fam == "green"
        d[f"fill0_{let}_red"] = fam == "red"
    for let in FILL_LETTERS:
        idx = FILL_LETTERS.index(let)
        for w in FILL_WINDOWS:
            g = _count_fam(days, t, idx, w, "green")
            r = _count_fam(days, t, idx, w, "red")
            for gate in COUNT_GATES:
                if gate > w:
                    continue
                d[f"{let}_last{w}_g>={gate}"] = g >= gate
                d[f"{let}_last{w}_r>={gate}"] = r >= gate
            d[f"{let}_last{w}_g>r"] = g > r
    for k in (
        "prior_hammer", "prior_hanging", "prior_shooting",
        "prior_bull_engulf", "prior_bear_engulf",
        "prior_morning", "prior_evening", "prior_doji",
        "prior_bullish", "prior_bearish",
        "FR_ge1", "AH_ge1", "FQ", "JC", "JB",
        "J_ge0", "J_lt0", "J_le-1", "EP_ge03", "ER_p1", "ER_m1",
        "el_pos", "el_neg",
    ):
        d[k] = bool(flags.get(k))
    vals_now = days[t].get("values") or {}
    vals_prev = days[t - 1].get("values") if t else {}
    for a, b in (("C", "B"), ("J", "C"), ("FR", "AH"), ("EP", "J"), ("FQ", "FR")):
        va = _f((vals_now if a in VALUE_OPEN_44 else vals_prev).get(a))
        vb = _f((vals_now if b in VALUE_OPEN_44 else vals_prev).get(b))
        if va is None or vb in (None, 0):
            continue
        ratio = va / vb
        d[f"ratio_{a}/{b}>1"] = ratio > 1
        d[f"ratio_{a}/{b}>1.5"] = ratio > 1.5
        d[f"ratio_{a}/{b}<0.7"] = ratio < 0.7
    a_idx = OPEN_FILL_IDX.get("A")
    if a_idx is not None:
        streak_g = streak_r = 0
        for k in range(t - 1, -1, -1):
            fam = days[k]["fams"][a_idx] if a_idx < len(days[k]["fams"]) else "none"
            if fam == "green" and streak_r == 0:
                streak_g += 1
            elif fam == "red" and streak_g == 0:
                streak_r += 1
            else:
                break
        if fams[a_idx] == "green":
            streak_g += 1
        elif fams[a_idx] == "red":
            streak_r += 1
        d["A_region_g>=3"] = streak_g >= 3
        d["A_region_g>=6"] = streak_g >= 6
        d["A_region_r>=3"] = streak_r >= 3
        d["A_region_r>=6"] = streak_r >= 6
        d["in_A_green_region"] = streak_g >= 2
        d["in_A_red_region"] = streak_r >= 2
    return d


def _acc():
    return dict(n=0, green=0.0, red=0.0, meanI=0.0, absI=0.0, meanH=0.0, nH=0, hold=0.0, nhold=0)


def _add(cell, o):
    if not o:
        return
    cell["n"] += 1
    cell["green"] += o.get("green") or 0.0
    cell["red"] += o.get("red") or 0.0
    cell["meanI"] += o.get("meanI") or 0.0
    cell["absI"] += o.get("absI") or 0.0
    if o.get("meanH") is not None:
        cell["meanH"] += o["meanH"]
        cell["nH"] += 1
    if o.get("hold") is not None:
        cell["hold"] += o["hold"]
        cell["nhold"] += 1


def _fin(cell):
    n = cell["n"]
    if n <= 0:
        return None
    return {"n": n, "green": cell["green"] / n, "red": cell["red"] / n,
            "meanI": cell["meanI"] / n, "absI": cell["absI"] / n,
            "meanH": (cell["meanH"] / cell["nH"]) if cell["nH"] else None,
            "hold": (cell["hold"] / cell["nhold"]) if cell["nhold"] else None}


def _lift(a, b, k):
    if not a or not b or not b.get(k) or a.get(k) is None:
        return None
    return a[k] / b[k]


def scan_ticker(days_raw):
    days = _hi(days_raw)
    if len(days) < 30:
        return []
    rows = []
    for t in range(20, len(days)):
        if not days[t]["o"]:
            continue
        prior = [{"o": d["o"], "h": d["h"], "l": d["l"], "c": d["c"], "v": d["v"]}
                 for d in days[:t]]
        try:
            xl = open_features(prior, days[t]["o"])
            flags = feature_flags(xl)
        except Exception:
            flags, xl = {}, {}
        desc = descriptors_at(days, t, flags, xl)
        fw = {f"h{h}": _fwd(days, t, h) for h in HORIZONS}
        fw["holdG"] = _hold_region(days, t, +1)
        fw["holdR"] = _hold_region(days, t, -1)
        rows.append((desc, fw))
    return rows


def mine(ticker_days, split_map, min_disc=MIN_N_DISC, min_hold=MIN_N_HOLD):
    cells = defaultdict(lambda: {"disc": _acc(), "hold": _acc()})
    n_tk = 0
    for tkr, raw in ticker_days.items():
        slot = "disc" if split_map.get(tkr) == "discovery" else "hold"
        for desc, fw in scan_ticker(raw):
            for name in [k for k, v in desc.items() if v]:
                for b, o in fw.items():
                    _add(cells[(name, b)][slot], o)
        n_tk += 1
        if n_tk % 200 == 0:
            print(f"  scanned {n_tk}/{len(ticker_days)}", flush=True)
    bases = {}
    for b in [f"h{h}" for h in HORIZONS] + ["holdG", "holdR"]:
        bases[b] = {"disc": _fin(cells[("uncond", b)]["disc"]),
                    "hold": _fin(cells[("uncond", b)]["hold"])}
    singles = []
    for (name, b), acc in cells.items():
        if name == "uncond":
            continue
        d, h = _fin(acc["disc"]), _fin(acc["hold"])
        if not d or d["n"] < min_disc:
            continue
        bd, bh = bases[b]["disc"], bases[b]["hold"]
        row = {"rule": name, "bucket": b, "kind": "single",
               "disc_n": d["n"], "disc_green": d["green"], "disc_meanI": d["meanI"],
               "disc_absI": d["absI"], "disc_H": d["meanH"], "disc_hold": d["hold"],
               "disc_lift_g": _lift(d, bd, "green"), "disc_lift_abs": _lift(d, bd, "absI"),
               "disc_lift_H": _lift(d, bd, "meanH"),
               "disc_dI": (d["meanI"] - bd["meanI"]) if bd else None}
        if h and h["n"] >= min_hold and bh:
            row.update({"hold_n": h["n"], "hold_green": h["green"], "hold_meanI": h["meanI"],
                        "hold_absI": h["absI"], "hold_H": h["meanH"], "hold_hold": h["hold"],
                        "hold_lift_g": _lift(h, bh, "green"), "hold_lift_abs": _lift(h, bh, "absI"),
                        "hold_lift_H": _lift(h, bh, "meanH"),
                        "hold_dI": (h["meanI"] - bh["meanI"]) if bh else None,
                        "sign_ok": ((row.get("disc_lift_g") or 1) > 1 and (_lift(h, bh, "green") or 1) > 1)
                        or ((row.get("disc_dI") or 0) * ((h["meanI"] - bh["meanI"]) if bh else 0) > 0)})
        else:
            row["sign_ok"] = False
        singles.append(row)
    focus = [r for r in singles if r.get("sign_ok") and r["bucket"] in ("h5", "holdG")
             and (r.get("hold_lift_g") or 0) >= 1.02]
    focus.sort(key=lambda r: (-(r.get("hold_lift_g") or 0), -r["disc_n"]))
    seeds, seen = [], set()
    for r in focus:
        if r["rule"] in seen:
            continue
        seen.add(r["rule"])
        seeds.append(r["rule"])
        if len(seeds) >= COMBO_TOP:
            break
    partners = seeds[:COMBO_PARTNERS]
    print(f"[combo] {len(seeds)} seeds x {len(partners)} partners", flush=True)
    combo_cells = defaultdict(lambda: {"disc": _acc(), "hold": _acc()})
    if seeds:
        for tkr, raw in ticker_days.items():
            slot = "disc" if split_map.get(tkr) == "discovery" else "hold"
            for desc, fw in scan_ticker(raw):
                on = [s for s in seeds if desc.get(s)]
                if len(on) < 2:
                    continue
                for i, a in enumerate(on):
                    for bname in on[i + 1:]:
                        if bname not in partners:
                            continue
                        key = f"{a}|{bname}"
                        for buck, o in fw.items():
                            _add(combo_cells[(key, buck)][slot], o)
    combos = []
    for (name, b), acc in combo_cells.items():
        d, h = _fin(acc["disc"]), _fin(acc["hold"])
        if not d or d["n"] < min_disc // 2:
            continue
        bd, bh = bases[b]["disc"], bases[b]["hold"]
        row = {"rule": name, "bucket": b, "kind": "combo",
               "disc_n": d["n"], "disc_green": d["green"], "disc_meanI": d["meanI"],
               "disc_absI": d["absI"], "disc_H": d["meanH"], "disc_hold": d["hold"],
               "disc_lift_g": _lift(d, bd, "green"), "disc_lift_abs": _lift(d, bd, "absI"),
               "disc_dI": (d["meanI"] - bd["meanI"]) if bd else None}
        if h and h["n"] >= min_hold // 2 and bh:
            row.update({"hold_n": h["n"], "hold_green": h["green"],
                        "hold_lift_g": _lift(h, bh, "green"),
                        "hold_lift_abs": _lift(h, bh, "absI"),
                        "hold_dI": (h["meanI"] - bh["meanI"]) if bh else None,
                        "hold_hold": h["hold"],
                        "sign_ok": (row.get("disc_lift_g") or 1) > 1 and (_lift(h, bh, "green") or 1) > 1})
        else:
            row["sign_ok"] = False
        combos.append(row)
    singles.sort(key=lambda r: (-(r.get("hold_lift_g") or 0), -r["disc_n"]))
    combos.sort(key=lambda r: (-(r.get("hold_lift_g") or 0), -r["disc_n"]))
    return {"base": bases, "singles": singles, "combos": combos,
            "gate": gate_payload(), "n_tickers": len(ticker_days)}


def load_grids(paths, allow):
    out = {}
    for p in paths:
        t = os.path.basename(p)[:-5]
        if allow and t not in allow:
            continue
        try:
            g = json.load(open(p, encoding="utf-8"))
        except Exception:
            continue
        days = g.get("days") or []
        t = g.get("ticker") or t
        if allow and t not in allow:
            continue
        if len(days) >= 30:
            out[t] = days
    return out


def write_md(result, path, meta):
    lines = ["# Excel deep correlation mine", "",
             f"Universe: mcap > ${MCAP_MIN_M:.0f}M and avg vol > {AVGVOL_MIN:.0f} shares. "
             f"Tickers mined: {result['n_tickers']}. {meta}", "",
             "Clock: rows above T + 09:30-knowable atoms. H/I are labels. "
             "A combo only counts if discovery and holdout lift green the same way.", "",
             "## Base", ""]
    for split in ("disc", "hold"):
        lines.append(f"### {split}")
        for b, s in (result["base"] or {}).items():
            st = (s or {}).get(split)
            if not st:
                continue
            lines.append(f"- `{b}` n={st['n']} green={st['green']:.3f} "
                         f"I={st['meanI']:+.4f} |I|={st['absI']:.4f}")
        lines.append("")
    lines += ["## Holdout-surviving singles (top 40)", "",
              "| rule | bucket | disc n | disc lift g | hold n | hold lift g | hold dI | hold |I| lift |",
              "|---|---|---:|---:|---:|---:|---:|---:|"]
    for r in [x for x in result["singles"] if x.get("sign_ok")][:40]:
        lines.append(f"| `{r['rule']}` | {r['bucket']} | {r['disc_n']} | "
                     f"{r.get('disc_lift_g') or 0:.3f} | {r.get('hold_n') or 0} | "
                     f"{r.get('hold_lift_g') or 0:.3f} | {r.get('hold_dI') or 0:+.4f} | "
                     f"{r.get('hold_lift_abs') or 0:.3f} |")
    lines += ["", "## Holdout-surviving combos (top 40)", "",
              "| rule | bucket | disc n | disc lift g | hold n | hold lift g | hold dI |",
              "|---|---|---:|---:|---:|---:|---:|"]
    for r in [x for x in result["combos"] if x.get("sign_ok")][:40]:
        lines.append(f"| `{r['rule']}` | {r['bucket']} | {r['disc_n']} | "
                     f"{r.get('disc_lift_g') or 0:.3f} | {r.get('hold_n') or 0} | "
                     f"{r.get('hold_lift_g') or 0:.3f} | {r.get('hold_dI') or 0:+.4f} |")
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w", encoding="utf-8").write("\n".join(lines) + "\n")


def write_csv(rows, path):
    if not rows:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    keys = sorted({k for r in rows for k in r})
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def find_grids(explicit):
    if explicit:
        return sorted(f for f in glob.glob(os.path.join(explicit, "*.json"))
                      if not os.path.basename(f).startswith("_"))
    for folder in ("grids_deep", "excel_bot/grids_deep", "grids", "excel_bot/grids"):
        files = sorted(f for f in glob.glob(os.path.join(folder, "*.json"))
                       if not os.path.basename(f).startswith("_"))
        if files:
            return files
    return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grids", default="")
    ap.add_argument("--finviz", default="")
    ap.add_argument("--split", default="")
    ap.add_argument("--min-disc", type=int, default=MIN_N_DISC)
    ap.add_argument("--min-hold", type=int, default=MIN_N_HOLD)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out-md", default="03_scoreboard/EXCEL_DEEP_CORR_MINE.md")
    ap.add_argument("--out-csv", default="03_scoreboard/excel_deep_corr_mine.csv")
    ap.add_argument("--out-json", default="03_scoreboard/excel_deep_corr_mine.json")
    args = ap.parse_args()
    assert_excel_clock_gate()
    finviz_path = args.finviz or next(
        (p for p in ("excel_bot/data/finviz_with_descriptions.csv",
                     "data/finviz_with_descriptions.csv") if os.path.isfile(p)), "")
    allow = load_finviz_filter(finviz_path) if finviz_path else {}
    print(f"[univ] finviz filter {len(allow)} names from {finviz_path}", flush=True)
    files = find_grids(args.grids)
    if args.limit:
        files = files[: args.limit]
    print(f"[grids] {len(files)} files", flush=True)
    ticker_days = load_grids(files, allow if allow else None)
    print(f"[grids] {len(ticker_days)} tickers after 50M/100k filter", flush=True)
    split_path = args.split or next(
        (p for p in ("excel_bot/engine/holdout_split.json",
                     "engine/holdout_split.json") if os.path.isfile(p)), "")
    split_map = {}
    if split_path:
        raw = json.load(open(split_path, encoding="utf-8"))
        split_map = {t: "discovery" for t in raw.get("discovery", [])}
        split_map.update({t: "holdout" for t in raw.get("holdout", [])})
    result = mine(ticker_days, split_map, args.min_disc, args.min_hold)
    meta = f"grids={len(files)} gate={result['gate']['gate']}"
    write_md(result, args.out_md, meta)
    write_csv(result["singles"] + result["combos"], args.out_csv)
    slim = {"n_tickers": result["n_tickers"], "base": result["base"],
            "n_singles": len(result["singles"]), "n_combos": len(result["combos"]),
            "n_sign_ok_singles": sum(1 for r in result["singles"] if r.get("sign_ok")),
            "n_sign_ok_combos": sum(1 for r in result["combos"] if r.get("sign_ok")),
            "top_singles": [r for r in result["singles"] if r.get("sign_ok")][:25],
            "top_combos": [r for r in result["combos"] if r.get("sign_ok")][:25],
            "gate": result["gate"]}
    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    json.dump(slim, open(args.out_json, "w"), indent=1, default=str)
    print(f"[out] {args.out_md}")
    print(f"[out] singles={len(result['singles'])} ok={slim['n_sign_ok_singles']} "
          f"combos_ok={slim['n_sign_ok_combos']}")


if __name__ == "__main__":
    main()
