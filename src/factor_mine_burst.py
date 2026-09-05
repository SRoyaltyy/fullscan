"""Research sweep: −N red cameras, green headlines, parabolic tape.

Does not remine the 161 official books. Scores a small grid of overlays
on the current cash-book leaders. Keep a variant only if it beats its
base on the $10k book and is not a one-name pothole.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import gainer_asof as ga
from . import paper_trade as pt

ROOT = Path(__file__).resolve().parent.parent
OUT_JSON = ROOT / "03_scoreboard" / "factor_mine_burst.json"

BASES = (
    "flatten_h5", "flatten_h3", "union_h5", "union_h3",
    "union_e_green_h3", "yday_gainer_h3",
)

OVERLAYS = (
    ("base", {}),
    ("nneg2", {"n_neg_max": 2}),
    ("nneg1", {"n_neg_max": 1}),
    ("nneg3", {"n_neg_min": 3}),
    ("news_g", {"news": "good"}),
    ("burst", {"burst": True}),
    ("burst_nneg2", {"burst": True, "n_neg_max": 2}),
    ("burst_news", {"burst": True, "news": "good"}),
    ("ext15", {"ret_5_min": 15.0, "last_green": True}),
)


def _clone_panel(panel: dict, *, upgrade_news: bool) -> dict:
    """Copy rows; optionally lift news from an insider/director headline."""
    rows = []
    title_cache: dict[str, dict] = {}
    for r in panel.get("rows") or []:
        row = dict(r)
        boxes = dict(row.get("boxes") or {})
        if upgrade_news:
            prior = row.get("news_export_date") or row.get("prior_date")
            if prior not in title_cache:
                title_cache[prior] = _title_map(prior)
            title = (title_cache.get(prior) or {}).get(fm._tick(row.get("ticker"))) or ""
            boxes["news"] = fm.input_news_tone(row.get("news_box") or boxes.get("news"), title)
            row["headline"] = title
            row["headline_tone"] = fm.prior_news_tone(title)
        row["boxes"] = boxes
        row["n_neg"] = fm.n_neg(row)
        row["burst"] = fm.is_burst(row)
        rows.append(row)
    by_date: dict[str, list] = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(r)
    out = dict(panel)
    out["rows"] = rows
    out["by_date"] = by_date
    return out


def _title_map(date: str | None) -> dict[str, str]:
    if not date:
        return {}
    df = ga.load_finviz(date)
    if df is None or getattr(df, "empty", True) or "Ticker" not in df.columns:
        return {}
    if "News Title" not in df.columns:
        return {}
    out = {}
    for rec in df[["Ticker", "News Title"]].to_dict("records"):
        t = fm._tick(rec.get("Ticker"))
        if t:
            out[t] = str(rec.get("News Title") or "")
    return out


def _base_rec(name: str) -> dict:
    return next(r for r in fm.build_recipes() if r["name"] == name)


def _variant(base: dict, suffix: str, extra: dict) -> dict:
    req = dict(base.get("require") or {})
    req.update(extra)
    forb = dict(base.get("forbid") or {})
    name = base["name"] if suffix == "base" else f"{base['name']}_{suffix}"
    return fm.make_recipe(
        name, universe=base["universe"], hold=base["hold"],
        side=base.get("side") or "long", top_n=base.get("top_n") or fm.TOP_N_DEFAULT,
        require=req or None, forbid=forb or None, rank=base.get("rank"),
        note=f"{base.get('note') or 'base'} · {suffix}",
    )


def _nneg_on_buys(panel: dict, rec: dict) -> dict:
    from . import factor_mine_sim as fms
    lo, hi = [], []
    for d in panel.get("session_dates") or []:
        for x in fms.look_day(panel, rec, d):
            if not x.get("buy") or x.get("ret") is None:
                continue
            (hi if int(x.get("n_neg") or 0) >= 3 else lo).append(x["ret"])
    return {"le2": fms.pack_rets(lo), "ge3": fms.pack_rets(hi)}


def run_sweep(panel: dict | None = None) -> dict:
    panel = fm.rehydrate_panel(panel or fm.load_or_build_panel(fm.START))
    fees = pt.load_fees()
    regime = fmb.load_regime()
    plain = _clone_panel(panel, upgrade_news=False)
    lifted = _clone_panel(panel, upgrade_news=True)
    rows = []
    for bname in BASES:
        base = _base_rec(bname)
        for suffix, extra in OVERLAYS:
            rec = _variant(base, suffix, extra)
            use = lifted if suffix in ("news_g", "burst_news") else plain
            book = fmb.simulate_book(use, rec, fees=fees, regime=regime)
            split = _nneg_on_buys(use, rec)
            rows.append({
                "name": rec["name"],
                "base": bname,
                "overlay": suffix,
                "book_pct": book.get("total_ret_pct"),
                "n_trades": book.get("n_trades"),
                "n_skips": book.get("n_skips"),
                "buy_nneg_le2": split["le2"],
                "buy_nneg_ge3": split["ge3"],
                "require": rec.get("require") or {},
            })
    by_base: dict[str, list] = {}
    for r in rows:
        by_base.setdefault(r["base"], []).append(r)
    floor = next((float(x["book_pct"]) for x in rows
                  if x["name"] == "flatten_h5"), 22.84)
    keepers = []
    for bname, group in by_base.items():
        base_row = next(x for x in group if x["overlay"] == "base")
        base_pct = float(base_row["book_pct"] or 0)
        for r in group:
            if r["overlay"] == "base":
                continue
            pct = float(r["book_pct"] or 0)
            # Must beat its own base *and* the current flatten_h5 phone book.
            if (pct >= base_pct + 1.0 and pct >= floor + 1.0
                    and (r.get("n_trades") or 0) >= 4):
                keepers.append(r["name"])
    # ASST case study
    asst = [r for r in lifted.get("rows") or [] if r.get("ticker") == "ASST"]
    asst_case = [{
        "date": r["date"], "ret_5": r.get("ohlc_ret_5"),
        "rvol": r.get("ohlc_rvol"), "break_10": r.get("ohlc_break_10"),
        "n_neg": r.get("n_neg"), "burst": r.get("burst"),
        "news_box": r.get("news_box"), "headline_tone": r.get("headline_tone"),
        "headline": (r.get("headline") or "")[:140],
        "news": (r.get("boxes") or {}).get("news"),
    } for r in asst]
    report = {
        "generated_at": fm.datetime.now().isoformat(timespec="seconds"),
        "rows": rows,
        "keepers": keepers,
        "asst": asst_case,
        "note": (
            "Keepers beat their own base by ≥1 book-point with ≥4 fills. "
            "If keepers is empty, current methods stay."
        ),
    }
    return report


def slim_for_dash(report: dict) -> dict:
    return {
        "verdict": "keep current" if not (report.get("keepers")) else "new overlays",
        "keepers": list(report.get("keepers") or []),
        "floor": 22.843,
        "asst": report.get("asst") or [],
        "rows": [
            {k: r.get(k) for k in
             ("name", "base", "overlay", "book_pct", "n_trades")}
            for r in (report.get("rows") or [])
        ],
        "buy_nneg": {
            r["name"]: {"le2": r.get("buy_nneg_le2"), "ge3": r.get("buy_nneg_ge3")}
            for r in (report.get("rows") or []) if r.get("overlay") == "base"
        },
        "note": report.get("note") or "",
    }


def write_outputs(report: dict) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")


def main() -> int:
    report = run_sweep()
    write_outputs(report)
    print(f"{'name':40} {'book':>8} {'fills':>5}  overlay")
    for r in sorted(report["rows"], key=lambda x: -(x.get("book_pct") or -999)):
        print(f"{r['name']:40} {r.get('book_pct'):+8.2f} {r.get('n_trades') or 0:5d}  {r['overlay']}")
    print("keepers", report["keepers"] or "(none — keep current methods)")
    print("ASST", report["asst"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
