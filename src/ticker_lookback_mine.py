"""Market-wide 09:30 lookback pattern mine.

Scans the liquid universe (mcap > $100M, avg vol > 500K) and grades
tag × region, tag × stretch, factor tones, and small factor pairs
against +1d / +3d / +1w — raw and vs the same-day universe median.

Days need at least MIN_PRINT colored factor boxes. Same-day Finviz /
book still never color the 09:30 boxes.

  python -m src.ticker_lookback_mine
  python -m src.ticker_lookback_mine --max-names 50
"""
from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime

from . import ticker_lookback as tl
from . import ticker_lookback_cli as scan

SCORE = tl.SCORE
DAILY = tl.DAILY
OUT_MD = SCORE / "TICKER_LOOKBACK_MINE.md"
OUT_JSON = SCORE / "ticker_lookback_mine.json"

MIN_PRINT = 3
MIN_N = 80
MIN_N_RARE = 40
XS_CUT = 0.15
HIT_CUT = 0.06
HORIZONS = ("1d", "3d", "1w")
PAIRS = (
    ("join", "vol"), ("join", "ab"), ("join", "gen"),
    ("ab", "peer"), ("gen", "vol"), ("heat", "join"),
    ("buy", "join"), ("vol", "ab"),
)
TAG_BOXES = ("join", "vol", "ab", "gen", "heat")


def _n_print(boxes) -> int:
    return sum(
        1 for k, _ in tl.BOX_COLS
        if str((boxes or {}).get(k) or "").lower() in tl.TONE_POINTS
    )


def _tag_name(row) -> str:
    if row.get("blue"):
        return "blue"
    if row.get("alarm"):
        return "alarm"
    if row.get("white"):
        return "white"
    return "none"


def slim_row(ticker: str, day: dict) -> dict | None:
    boxes = day.get("boxes") or {}
    n = _n_print(boxes)
    if n < MIN_PRINT:
        return None
    fwd = day.get("forward_returns") or {}
    if fwd.get("1d") is None and fwd.get("3d") is None and fwd.get("1w") is None:
        return None
    return {
        "ticker": ticker,
        "date": day.get("date"),
        "n_print": n,
        "boxes": {k: str(boxes.get(k) or "missing") for k, _ in tl.BOX_COLS},
        "blue": bool(day.get("signal_improved")),
        "alarm": bool(day.get("signal_alarm")),
        "white": bool(day.get("zero_red")),
        "region": str((day.get("region") or {}).get("tone") or ""),
        "stretch": str((day.get("stretch") or {}).get("tone") or ""),
        "cond": str((day.get("condition") or {}).get("tone") or ""),
        "tag_context": list(day.get("tag_context") or []),
        "cls": str(day.get("class") or ""),
        "ret_1d": fwd.get("1d"),
        "ret_3d": fwd.get("3d"),
        "ret_1w": fwd.get("1w"),
    }


def collect(names, from_date=None, to_date=None) -> list[dict]:
    """Scan names on the 09:30 recipe and keep printed days with a forward."""
    idx = tl.build_index()
    sessions = idx["sessions"]
    window = [
        s for s in sessions
        if (not from_date or s["date"] >= from_date)
        and (not to_date or s["date"] <= to_date)
    ]
    rows = []
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
        for day in days:
            row = slim_row(ticker, day)
            if row:
                rows.append(row)
        if i % 250 == 0:
            print(f"[mine] scanned {i}/{len(names)} names, {len(rows)} rows",
                  flush=True)
    return rows


def day_medians(rows) -> dict[str, dict[str, float]]:
    by_date: dict[str, dict[str, list[float]]] = {}
    for row in rows:
        d = row.get("date")
        if not d:
            continue
        bucket = by_date.setdefault(d, {h: [] for h in HORIZONS})
        for h in HORIZONS:
            v = row.get(f"ret_{h}")
            if v is not None:
                bucket[h].append(float(v))
    out = {}
    for d, hs in by_date.items():
        out[d] = {
            h: statistics.median(vs) if vs else None for h, vs in hs.items()
        }
    return out


def attach_excess(rows, medians=None) -> list[dict]:
    medians = medians if medians is not None else day_medians(rows)
    for row in rows:
        med = medians.get(row.get("date") or "", {})
        for h in HORIZONS:
            raw = row.get(f"ret_{h}")
            mid = med.get(h)
            row[f"xs_{h}"] = (
                None if raw is None or mid is None else float(raw) - float(mid))
    return rows


def summarize(rows, horizon="1d") -> dict:
    raw, xs, hits, xs_hits = [], [], 0, 0
    key_r, key_x = f"ret_{horizon}", f"xs_{horizon}"
    n_raw = n_xs = 0
    for row in rows:
        r, x = row.get(key_r), row.get(key_x)
        if r is not None:
            n_raw += 1
            raw.append(float(r))
            if r > 0:
                hits += 1
        if x is not None:
            n_xs += 1
            xs.append(float(x))
            if x > 0:
                xs_hits += 1
    if n_raw == 0:
        return {"n": 0}
    return {
        "n": n_raw,
        "n_xs": n_xs,
        "hit": round(hits / n_raw, 4),
        "hit_xs": None if not n_xs else round(xs_hits / n_xs, 4),
        "mean": round(statistics.fmean(raw), 3),
        "median": round(statistics.median(raw), 3),
        "mean_xs": None if not xs else round(statistics.fmean(xs), 3),
        "median_xs": None if not xs else round(statistics.median(xs), 3),
    }


def verdict(stat, min_n=MIN_N) -> str:
    """long / fade / noise from 1d excess."""
    if not stat or stat.get("n", 0) < min_n:
        return "thin"
    xs = stat.get("mean_xs")
    hit = stat.get("hit_xs")
    if xs is None:
        return "noise"
    hit_edge = hit is not None and abs(hit - 0.5) >= HIT_CUT
    if xs >= XS_CUT and (hit is None or hit >= 0.5 or hit_edge):
        return "long"
    if xs <= -XS_CUT and (hit is None or hit <= 0.5 or hit_edge):
        return "fade"
    return "noise"


def _group(rows, key_fn) -> dict[str, list]:
    out: dict[str, list] = {}
    for row in rows:
        key = key_fn(row)
        if not key:
            continue
        out.setdefault(key, []).append(row)
    return out


def _pack(groups, min_n=MIN_N) -> list[dict]:
    packed = []
    for key, group in groups.items():
        stat = summarize(group, "1d")
        if not stat.get("n"):
            continue
        item = {"key": key, **stat, "verdict": verdict(stat, min_n=min_n)}
        for h in ("3d", "1w"):
            extra = summarize(group, h)
            item[f"{h}_n"] = extra.get("n", 0)
            item[f"{h}_mean_xs"] = extra.get("mean_xs")
            item[f"{h}_hit_xs"] = extra.get("hit_xs")
        packed.append(item)
    packed.sort(key=lambda r: (
        0 if r["verdict"] in {"long", "fade"} else 1,
        -abs(r.get("mean_xs") or 0),
        -r["n"],
    ))
    return packed


def mine_buckets(rows) -> dict[str, list[dict]]:
    """Interpretable configs only — no kitchen-sink cross."""
    return {
        "base": _pack({"all": rows}, min_n=1),
        "tags": _pack(_group(rows, _tag_name)),
        "tag_context": _pack(
            _group(rows, lambda r: ",".join(r.get("tag_context") or []) or None),
            min_n=MIN_N_RARE),
        "tag_region": _pack(_group(
            rows,
            lambda r: f"{_tag_name(r)}|{r.get('region') or '?'}"
            if _tag_name(r) != "none" else None),
            min_n=MIN_N_RARE),
        "tag_stretch": _pack(_group(
            rows,
            lambda r: f"{_tag_name(r)}|{r.get('stretch') or '?'}"
            if _tag_name(r) != "none" else None),
            min_n=MIN_N_RARE),
        "region": _pack(_group(rows, lambda r: r.get("region") or None)),
        "stretch": _pack(_group(rows, lambda r: r.get("stretch") or None)),
        "cond": _pack(_group(rows, lambda r: r.get("cond") or None)),
        "class": _pack(_group(rows, lambda r: r.get("cls") or None),
                       min_n=MIN_N_RARE),
        "factor": _pack(_factor_groups(rows)),
        "pair": _pack(_pair_groups(rows), min_n=MIN_N),
        "tag_factor": _pack(_tag_factor_groups(rows), min_n=MIN_N_RARE),
    }


def _factor_groups(rows) -> dict[str, list]:
    out: dict[str, list] = {}
    for row in rows:
        boxes = row.get("boxes") or {}
        for key, _ in tl.BOX_COLS:
            tone = str(boxes.get(key) or "").lower()
            if tone not in tl.TONE_POINTS:
                continue
            out.setdefault(f"{key}={tone}", []).append(row)
    return out


def _pair_groups(rows) -> dict[str, list]:
    out: dict[str, list] = {}
    for row in rows:
        boxes = row.get("boxes") or {}
        for a, b in PAIRS:
            ta = str(boxes.get(a) or "").lower()
            tb = str(boxes.get(b) or "").lower()
            if ta not in tl.TONE_POINTS or tb not in tl.TONE_POINTS:
                continue
            out.setdefault(f"{a}={ta}|{b}={tb}", []).append(row)
    return out


def _tag_factor_groups(rows) -> dict[str, list]:
    out: dict[str, list] = {}
    for row in rows:
        tag = _tag_name(row)
        if tag == "none":
            continue
        boxes = row.get("boxes") or {}
        for key in TAG_BOXES:
            tone = str(boxes.get(key) or "").lower()
            if tone not in tl.TONE_POINTS:
                continue
            out.setdefault(f"{tag}|{key}={tone}", []).append(row)
    return out


def _pct(x):
    if x is None:
        return "—"
    return f"{100 * float(x):.1f}%"


def _num(x, nd=2):
    if x is None:
        return "—"
    return f"{float(x):+.{nd}f}"


def _usable(rows):
    return [r for r in rows if r.get("verdict") in {"long", "fade"}]


def render_md(payload) -> str:
    meta = payload.get("meta") or {}
    buckets = payload.get("buckets") or {}
    base = (buckets.get("base") or [{}])[0]
    L = [
        "# Ticker lookback mine — market-wide 09:30",
        "",
        f"_Generated {payload.get('generated_at')}_",
        "",
        f"Liquid universe **{meta.get('n_names', 0)}** names "
        f"(mcap > $100M, avg vol > 500K). "
        f"**{meta.get('n_rows', 0)}** printed days "
        f"(≥{MIN_PRINT} factor boxes + a forward). "
        f"Sessions {meta.get('from_date') or '—'} → {meta.get('to_date') or '—'}.",
        "",
        "Excess = name return minus that session's universe median. "
        "Long / fade need n≥80 (n≥40 for rarer tags) and |1d excess|≥0.15. "
        "Same-day Finviz and same-day book do not color the factors.",
        "",
        "## Base rate",
        "",
        f"| n | 1d hit | 1d xs-hit | 1d mean | 1d xs | 3d xs | 1w xs |",
        f"|---|---|---|---|---|---|---|",
        f"| {base.get('n', 0)} | {_pct(base.get('hit'))} | "
        f"{_pct(base.get('hit_xs'))} | {_num(base.get('mean'))} | "
        f"{_num(base.get('mean_xs'))} | {_num(base.get('3d_mean_xs'))} | "
        f"{_num(base.get('1w_mean_xs'))} |",
        "",
    ]

    sections = (
        ("Usable tags × region", "tag_region",
         "The same 🔵 / 🚨 / ⚪ flips meaning on green vs red mass."),
        ("Usable tag context", "tag_context",
         "turn / late / first_crack / continuation / crowded / clean_chop."),
        ("Usable tags × stretch", "tag_stretch",
         "Trailing 3-session green/red balance."),
        ("Usable tags alone", "tags",
         "Bare tag without the row color. Usually weaker."),
        ("Usable factor tones", "factor",
         "Single box, printed cells only."),
        ("Usable factor pairs", "pair",
         "Two printed cores on the same 09:30 row."),
        ("Usable tag × factor", "tag_factor",
         "Tag plus one printed box."),
        ("Region / stretch / cond / class", "region",
         "Row color without a tag."),
    )
    shown = set()
    for title, key, note in sections:
        rows = buckets.get(key) or []
        if key == "region":
            extra = []
            for alt in ("stretch", "cond", "class"):
                extra.extend(buckets.get(alt) or [])
            rows = rows + extra
        keep = _usable(rows)
        L += [f"## {title}", "", f"_{note}_", ""]
        if not keep:
            L += ["_Nothing cleared the usable bar._", ""]
            continue
        L += [_table(keep, limit=24), ""]
        shown.add(key)

    L += [
        "## Also-ran (biggest |excess|, still noise or thin)",
        "",
        "_These printed but did not clear long/fade. Useful as 'do not trade this bare.'_",
        "",
    ]
    also = []
    for key in ("tag_region", "tag_context", "tags", "factor", "pair"):
        for row in buckets.get(key) or []:
            if row.get("verdict") in {"long", "fade"}:
                continue
            also.append({**row, "key": f"{key}:{row['key']}"})
    also.sort(key=lambda r: -abs(r.get("mean_xs") or 0))
    L += [_table(also[:20], limit=20), ""]
    return "\n".join(L)


def _table(rows, limit=24) -> str:
    lines = [
        "| pattern | n | read | 1d hit | 1d xs-hit | 1d xs | 3d xs | 1w xs |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in rows[:limit]:
        lines.append(
            f"| `{row['key']}` | {row['n']} | {row.get('verdict')} | "
            f"{_pct(row.get('hit'))} | {_pct(row.get('hit_xs'))} | "
            f"{_num(row.get('mean_xs'))} | {_num(row.get('3d_mean_xs'))} | "
            f"{_num(row.get('1w_mean_xs'))} |"
        )
    return "\n".join(lines)


def run(names=None, from_date=None, to_date=None, max_names=None, write=True):
    names = list(names or tl.liquid_universe())
    names = [tl._tick(t) for t in names if tl._tick(t)]
    if max_names:
        names = names[: int(max_names)]
    print(f"[mine] {len(names)} names", flush=True)
    rows = collect(names, from_date=from_date, to_date=to_date)
    attach_excess(rows)
    buckets = mine_buckets(rows)
    dates = sorted({r["date"] for r in rows if r.get("date")})
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "meta": {
            "n_names": len(names),
            "n_rows": len(rows),
            "from_date": from_date or (dates[0] if dates else None),
            "to_date": to_date or (dates[-1] if dates else None),
            "min_print": MIN_PRINT,
            "min_n": MIN_N,
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
        (DAILY / f"{day}_ticker_lookback_mine.md").write_text(md, encoding="utf-8")
        slim = dict(payload)
        OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
        print(md[:14000])
        print(f"[mine] wrote {OUT_MD}")
        print(f"[mine] wrote {OUT_JSON}")
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default="", help="optional YYYY-MM-DD")
    ap.add_argument("--to-date", default="", help="optional YYYY-MM-DD")
    ap.add_argument("--max-names", type=int, default=0,
                    help="cap the liquid universe (0 = all)")
    ap.add_argument("--tickers", default="",
                    help="optional comma list instead of the liquid universe")
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
