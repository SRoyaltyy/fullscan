"""Camera combo mine — which 09:30 lights actually pay.

Walks the liquid universe the same way as ticker lookback (no same-day
Finviz / same-day book leak). For every printed name-day it grades:

  1d / 2d / 3d / 1w / 2w   close-to-close session hops
  win rate                 raw % of names with ret > 0
  average return           arithmetic mean of those rets
  excess                   vs that session's universe median

Then it sifts:

  * single camera tones
  * 2-way and 3-way AND combos (green / not-red)
  * zero-red vs red-count buckets
  * D0 × D-1 × D-2 lag interactions on the same camera

  python -m src.camera_combo_mine
  python -m src.camera_combo_mine --max-names 80 --from-date 2026-08-13
"""
from __future__ import annotations

import argparse
import itertools
import json
import statistics
from datetime import datetime

from . import ticker_lookback as tl
from . import ticker_lookback_cli as scan

SCORE = tl.SCORE
DAILY = tl.DAILY
OUT_MD = SCORE / "CAMERA_COMBO_MINE.md"
OUT_JSON = SCORE / "camera_combo_mine.json"

HORIZONS = ("1d", "2d", "3d", "1w", "2w")
HOPS = {"1d": 1, "2d": 2, "3d": 3, "1w": 5, "2w": 10}
MIN_PRINT = 3
MIN_N = 80
MIN_N_LAG = 40
MIN_N_RARE = 40
TOP_N = 20

# Stock-ish cameras. `gen` is the morning market essay (same stamp on
# almost every name that day) — still mined, but labeled session-level.
COMBO_LIGHTS = ("join", "sector", "gen", "ab", "peer", "vol", "heat")
LAG_LIGHTS = ("join", "sector", "gen", "ab", "vol")
SESSION_LIGHTS = frozenset({"gen"})
PRINTED = frozenset(tl.TONE_POINTS)


def _tone(boxes, key) -> str:
    return str((boxes or {}).get(key) or "missing").lower()


def _n_red(boxes) -> int:
    return sum(1 for k, _ in tl.BOX_COLS if _tone(boxes, k) == "bad")


def _n_print(boxes) -> int:
    return sum(1 for k, _ in tl.BOX_COLS if _tone(boxes, k) in PRINTED)


def _fwd_2w(ticker: str, date: str) -> float | None:
    """Close → close 10 trading sessions later from the OHLC panel."""
    panel = tl._price_panel()
    t = tl._tick(ticker)
    if panel is None or panel.empty or t not in panel.columns:
        return None
    import pandas as pd
    idx = panel.index.searchsorted(pd.Timestamp(date))
    n = HOPS["2w"]
    if idx >= len(panel.index) or panel.index[idx].date().isoformat() != date:
        return None
    if idx + n >= len(panel.index):
        return None
    entry = tl._num(panel[t].iloc[idx])
    exitp = tl._num(panel[t].iloc[idx + n])
    if not entry or not exitp:
        return None
    return round(100.0 * (exitp / entry - 1.0), 3)


def slim_row(ticker: str, day: dict, prev1: dict | None, prev2: dict | None):
    boxes = day.get("boxes") or {}
    if _n_print(boxes) < MIN_PRINT:
        return None
    fwd = day.get("forward_returns") or {}
    if all(fwd.get(h) is None for h in ("1d", "2d", "3d", "1w")):
        return None
    return {
        "ticker": ticker,
        "date": day.get("date"),
        "boxes": {k: _tone(boxes, k) for k, _ in tl.BOX_COLS},
        "boxes_d1": {k: _tone((prev1 or {}).get("boxes") or {}, k) for k, _ in tl.BOX_COLS},
        "boxes_d2": {k: _tone((prev2 or {}).get("boxes") or {}, k) for k, _ in tl.BOX_COLS},
        "n_print": _n_print(boxes),
        "n_red": _n_red(boxes),
        "n_red_d1": _n_red((prev1 or {}).get("boxes") or {}),
        "zero_red": bool(day.get("zero_red") or tl.zero_red(boxes)),
        "zero_red_d1": bool((prev1 or {}).get("zero_red")),
        "blue": bool(day.get("signal_improved")),
        "alarm": bool(day.get("signal_alarm")),
        "ret_1d": fwd.get("1d"),
        "ret_2d": fwd.get("2d"),
        "ret_3d": fwd.get("3d"),
        "ret_1w": fwd.get("1w"),
        "ret_2w": fwd.get("2w") if fwd.get("2w") is not None else _fwd_2w(ticker, day.get("date") or ""),
    }


def collect(names, from_date=None, to_date=None) -> list[dict]:
    idx = tl.build_index()
    sessions = idx["sessions"]
    window = [
        s for s in sessions
        if (not from_date or s["date"] >= from_date)
        and (not to_date or s["date"] <= to_date)
    ]
    rows: list[dict] = []
    for i, ticker in enumerate(names, 1):
        days = []
        for sess in window:
            card = scan._scan_session(sess, ticker)
            if card is None:
                card = {
                    "date": sess["date"], "ticker": ticker,
                    "boxes": {k: "missing" for k, _ in tl.BOX_COLS},
                    "class": "no_data",
                }
            else:
                fv = (sess.get("finviz") or {}).get(ticker)
                card["forward_returns"] = tl.forward_returns(
                    ticker, sess["date"], sessions=sessions, current_finviz=fv)
            days.append(card)
        tl.annotate_signal_improved(days)
        for j, day in enumerate(days):
            prev1 = days[j - 1] if j >= 1 else None
            prev2 = days[j - 2] if j >= 2 else None
            row = slim_row(ticker, day, prev1, prev2)
            if row:
                rows.append(row)
        if i % 250 == 0:
            print(f"[combo] scanned {i}/{len(names)} names, {len(rows)} rows",
                  flush=True)
    return rows


def day_medians(rows) -> dict:
    by: dict[str, dict[str, list]] = {}
    for row in rows:
        d = row.get("date")
        if not d:
            continue
        bucket = by.setdefault(d, {h: [] for h in HORIZONS})
        for h in HORIZONS:
            v = row.get(f"ret_{h}")
            if v is not None:
                bucket[h].append(float(v))
    out = {}
    for d, hs in by.items():
        out[d] = {h: (statistics.median(vs) if vs else None) for h, vs in hs.items()}
    return out


def attach_excess(rows, medians=None) -> list[dict]:
    medians = medians if medians is not None else day_medians(rows)
    for row in rows:
        med = medians.get(row.get("date") or "", {})
        for h in HORIZONS:
            raw = row.get(f"ret_{h}")
            mid = med.get(h)
            row[f"xs_{h}"] = None if raw is None or mid is None else float(raw) - float(mid)
    return rows


def summarize(rows, horizon="1d") -> dict:
    raw, xs = [], []
    hits = xs_hits = 0
    key_r, key_x = f"ret_{horizon}", f"xs_{horizon}"
    for row in rows:
        r = row.get(key_r)
        if r is not None:
            raw.append(float(r))
            if r > 0:
                hits += 1
        x = row.get(key_x)
        if x is not None:
            xs.append(float(x))
            if x > 0:
                xs_hits += 1
    if not raw:
        return {"n": 0, "horizon": horizon}
    return {
        "horizon": horizon,
        "n": len(raw),
        "n_xs": len(xs),
        "hit": round(hits / len(raw), 4),
        "hit_xs": None if not xs else round(xs_hits / len(xs), 4),
        "mean": round(statistics.fmean(raw), 3),
        "median": round(statistics.median(raw), 3),
        "mean_xs": None if not xs else round(statistics.fmean(xs), 3),
        "median_xs": None if not xs else round(statistics.median(xs), 3),
    }


def summarize_all(rows) -> dict:
    out = {}
    for h in HORIZONS:
        out[h] = summarize(rows, h)
    return out


def _ok(row, light: str, want: str, lag: int) -> bool:
    src = "boxes" if lag == 0 else ("boxes_d1" if lag == -1 else "boxes_d2")
    tone = _tone(row.get(src) or {}, light)
    if want == "not_red":
        return tone in {"good", "neutral"}
    if want == "printed":
        return tone in PRINTED
    return tone == want


def _filter(rows, clauses) -> list[dict]:
    keep = []
    for row in rows:
        if all(_ok(row, light, want, lag) for light, want, lag in clauses):
            keep.append(row)
    return keep


def _label(clauses) -> str:
    bits = []
    for light, want, lag in clauses:
        tag = {"0": "D0", "-1": "D-1", "-2": "D-2"}[str(lag)]
        short = dict(tl.BOX_COLS).get(light, light)
        bits.append(f"{short}@{tag}={want}")
    return " & ".join(bits)


def pack_group(key: str, family: str, rows, base: dict, min_n=MIN_N) -> dict | None:
    stats = summarize_all(rows)
    d1 = stats.get("1d") or {}
    if d1.get("n", 0) < min_n:
        return None
    item = {
        "key": key,
        "family": family,
        "n": d1.get("n", 0),
        "stats": stats,
    }
    for h in HORIZONS:
        s = stats.get(h) or {}
        b = (base.get(h) or {}) if base else {}
        item[f"{h}_n"] = s.get("n", 0)
        item[f"{h}_hit"] = s.get("hit")
        item[f"{h}_mean"] = s.get("mean")
        item[f"{h}_median"] = s.get("median")
        item[f"{h}_mean_xs"] = s.get("mean_xs")
        bh = b.get("hit")
        bm = b.get("mean")
        item[f"{h}_hit_lift"] = None if s.get("hit") is None or bh is None else round(s["hit"] - bh, 4)
        item[f"{h}_mean_lift"] = None if s.get("mean") is None or bm is None else round(s["mean"] - bm, 3)
    return item


def _singles(rows, base) -> list[dict]:
    out = []
    for light, _ in tl.BOX_COLS:
        for want in ("good", "neutral", "bad"):
            got = _filter(rows, [(light, want, 0)])
            item = pack_group(f"{dict(tl.BOX_COLS).get(light, light)}={want}", "single", got, base)
            if item:
                item["session_level"] = light in SESSION_LIGHTS
                out.append(item)
    return out


def _combos(rows, base, size: int) -> list[dict]:
    out = []
    wants = ("good", "not_red")
    for lights in itertools.combinations(COMBO_LIGHTS, size):
        for want_set in itertools.product(wants, repeat=size):
            clauses = list(zip(lights, want_set, [0] * size))
            got = _filter(rows, clauses)
            item = pack_group(_label(clauses), f"combo{size}", got, base, min_n=MIN_N)
            if item:
                item["session_level"] = any(L in SESSION_LIGHTS for L in lights)
                out.append(item)
    return out


def _zero_red(rows, base) -> list[dict]:
    out = []
    buckets = {
        "zero_red": [r for r in rows if r.get("zero_red")],
        "has_red": [r for r in rows if not r.get("zero_red")],
        "n_red=0": [r for r in rows if r.get("n_red") == 0],
        "n_red=1": [r for r in rows if r.get("n_red") == 1],
        "n_red=2": [r for r in rows if r.get("n_red") == 2],
        "n_red=3+": [r for r in rows if (r.get("n_red") or 0) >= 3],
        "zero_red@D0 & zero_red@D-1": [
            r for r in rows if r.get("zero_red") and r.get("zero_red_d1")
        ],
        "zero_red@D0 & join@D0=good": _filter(
            [r for r in rows if r.get("zero_red")], [("join", "good", 0)]),
        "has_red@D0 & join@D0=good": _filter(
            [r for r in rows if not r.get("zero_red")], [("join", "good", 0)]),
    }
    for key, got in buckets.items():
        item = pack_group(key, "zero_red", got, base, min_n=MIN_N_RARE)
        if item:
            out.append(item)
    return out


def _lags(rows, base) -> list[dict]:
    out = []
    patterns = (
        (("good", 0), ("good", -1)),
        (("good", 0), ("bad", -1)),
        (("bad", 0), ("good", -1)),
        (("good", 0), ("good", -1), ("good", -2)),
        (("good", 0), ("not_red", -1)),
        (("not_red", 0), ("good", -1)),
    )
    for light in LAG_LIGHTS:
        for pat in patterns:
            clauses = [(light, want, lag) for want, lag in pat]
            got = _filter(rows, clauses)
            item = pack_group(_label(clauses), "lag", got, base, min_n=MIN_N_LAG)
            if item:
                item["session_level"] = light in SESSION_LIGHTS
                out.append(item)
    extra = (
        [("join", "good", 0), ("gen", "good", -1)],
        [("join", "good", 0), ("gen", "bad", -1)],
        [("join", "good", 0), ("vol", "good", -1)],
        [("ab", "good", 0), ("join", "good", -1)],
        [("peer", "good", 0), ("gen", "good", -1)],
    )
    for clauses in extra:
        got = _filter(rows, clauses)
        item = pack_group(_label(clauses), "lag_cross", got, base, min_n=MIN_N_LAG)
        if item:
            item["session_level"] = True
            out.append(item)
    return out


def mine(rows) -> dict:
    base_stats = summarize_all(rows)
    base_item = pack_group("ALL", "base", rows, base_stats, min_n=1) or {
        "key": "ALL", "family": "base", "n": 0, "stats": base_stats,
    }
    return {
        "base": [base_item],
        "single": _singles(rows, base_stats),
        "combo2": _combos(rows, base_stats, 2),
        "combo3": _combos(rows, base_stats, 3),
        "zero_red": _zero_red(rows, base_stats),
        "lag": _lags(rows, base_stats),
    }


def _rank(items, horizon: str, field: str) -> list[dict]:
    key = f"{horizon}_{field}"
    rows = [r for r in items if r.get(key) is not None and r.get("family") != "base"]
    rows.sort(key=lambda r: (-(r.get(key) or -999), -r.get("n", 0)))
    return rows


def _pct(x):
    return "—" if x is None else f"{100 * float(x):.1f}%"


def _num(x, nd=2):
    return "—" if x is None else f"{float(x):+.{nd}f}"


def _table(rows, horizon: str, limit=TOP_N) -> str:
    lines = [
        f"| pattern | n | {horizon} hit | hit lift | {horizon} mean | mean lift | {horizon} xs | family |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows[:limit]:
        mark = " · sess" if row.get("session_level") else ""
        lines.append(
            f"| `{row['key']}`{mark} | {row.get(f'{horizon}_n', row.get('n', 0))} | "
            f"{_pct(row.get(f'{horizon}_hit'))} | {_pct(row.get(f'{horizon}_hit_lift'))} | "
            f"{_num(row.get(f'{horizon}_mean'))} | {_num(row.get(f'{horizon}_mean_lift'))} | "
            f"{_num(row.get(f'{horizon}_mean_xs'))} | {row.get('family')} |"
        )
    return "\n".join(lines) if len(lines) > 2 else "_nothing cleared min_n._"


def render_md(payload) -> str:
    meta = payload.get("meta") or {}
    buckets = payload.get("buckets") or {}
    base = (buckets.get("base") or [{}])[0]
    pool = []
    for fam in ("single", "combo2", "combo3", "zero_red", "lag"):
        pool.extend(buckets.get(fam) or [])
    L = [
        "# Camera combo mine",
        "",
        f"_Generated {payload.get('generated_at')} · as-of 09:30 ET_",
        "",
        f"Liquid universe **{meta.get('n_names', 0)}** names "
        f"(mcap > $100M, adv > 500k, ATR% ≥ {tl.MIN_ATR_PCT}). **{meta.get('n_rows', 0)}** printed "
        f"name-days (≥{MIN_PRINT} cameras + a forward). "
        f"Sessions {meta.get('from_date') or '—'} → {meta.get('to_date') or '—'}.",
        "",
        "Win = close-to-close return > 0. Mean = arithmetic average of those "
        "returns. Lift = combo minus the all-name base rate on the same "
        "horizon. Excess = name minus that session's universe median. "
        "`sess` means the light is mostly the morning market essay, not a "
        "stock-specific camera — treat those as weather filters.",
        "",
        f"Min n = {MIN_N} (lag / rare buckets {MIN_N_LAG}). Combos are AND "
        f"of green or not-red on {', '.join(COMBO_LIGHTS)}. Same-day Finviz "
        "and same-day book never color a cell.",
        "",
        "## Base rate",
        "",
        "| horizon | n | hit | mean | median | mean xs |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for h in HORIZONS:
        L.append(
            f"| {h} | {base.get(f'{h}_n', 0)} | {_pct(base.get(f'{h}_hit'))} | "
            f"{_num(base.get(f'{h}_mean'))} | {_num(base.get(f'{h}_median'))} | "
            f"{_num(base.get(f'{h}_mean_xs'))} |"
        )
    L += ["", "## Zero reds — is a clean card better?", "",
          _table(buckets.get("zero_red") or [], "1d", limit=20), ""]

    for h in HORIZONS:
        L += [
            f"## {h} — highest win rate",
            "",
            _table(_rank(pool, h, "hit"), h),
            "",
            f"## {h} — highest average return",
            "",
            _table(_rank(pool, h, "mean"), h),
            "",
        ]

    L += [
        "## Lag interactions (D0 × D-1 × D-2)",
        "",
        "Same camera yesterday vs today. `good@D0 & bad@D-1` is a turn. "
        "`good@D0 & good@D-1` is persistence. Cross-light rows mix join/vol "
        "with yesterday's gen (weather).",
        "",
        _table(buckets.get("lag") or [], "1d", limit=30),
        "",
        "## Singles (every printed camera)",
        "",
        _table(buckets.get("single") or [], "1d", limit=40),
        "",
    ]
    return "\n".join(L)


def run(names=None, from_date=None, to_date=None, max_names=None, write=True):
    names = list(names or tl.liquid_universe())
    names = [tl._tick(t) for t in names if tl._tick(t)]
    if max_names:
        names = names[: int(max_names)]
    print(f"[combo] {len(names)} names", flush=True)
    rows = collect(names, from_date=from_date, to_date=to_date)
    attach_excess(rows)
    buckets = mine(rows)
    dates = sorted({r["date"] for r in rows if r.get("date")})
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "meta": {
            "n_names": len(names),
            "n_rows": len(rows),
            "from_date": from_date or (dates[0] if dates else None),
            "to_date": to_date or (dates[-1] if dates else None),
            "horizons": list(HORIZONS),
            "min_print": MIN_PRINT,
            "min_n": MIN_N,
            "min_n_lag": MIN_N_LAG,
            "combo_lights": list(COMBO_LIGHTS),
            "universe": "liquid_mcap100_adv500k",
        },
        "buckets": buckets,
    }
    if write:
        SCORE.mkdir(parents=True, exist_ok=True)
        DAILY.mkdir(parents=True, exist_ok=True)
        md = render_md(payload)
        OUT_MD.write_text(md, encoding="utf-8")
        day = datetime.now(tl.ET).date().isoformat()
        (DAILY / f"{day}_camera_combo_mine.md").write_text(md, encoding="utf-8")
        slim = dict(payload)
        OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
        print(md[:12000])
        print(f"[combo] wrote {OUT_MD}")
        print(f"[combo] wrote {OUT_JSON}")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--max-names", type=int, default=0)
    ap.add_argument("--tickers", default="")
    args = ap.parse_args()
    names = None
    if args.tickers.strip():
        names = [t.strip() for t in args.tickers.split(",") if t.strip()]
    run(names=names,
        from_date=args.from_date or None,
        to_date=args.to_date or None,
        max_names=args.max_names or None)


if __name__ == "__main__":
    main()
