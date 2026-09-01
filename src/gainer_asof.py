"""Top-gainer as-of walk — what the golden inputs said before the open.

For every session since the dashboard start (2026-08-13):

  * realized liquid gainers at 2% and 5% (same-day Finviz Change%)
  * the names the 1d BUY list actually printed that morning
  * the 12 lookback boxes plus yΔ (yesterday's Change%, prior tape)

Boxes come from the 09:30 ET packet. Same-day RelVol and same-day
stock book never color a cell. Change% / BUY realized Δ are outcomes.

Writes:
  03_scoreboard/TOP_GAINER_ASOF.md
  03_scoreboard/top_gainer_asof.json

CLI: python -m src.gainer_asof --floors 2,5 --all --buys --write
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import book_era, ticker_lookback as tl
from . import ticker_lookback_cli as scan

ROOT = Path(__file__).resolve().parent.parent
OUT_MD = ROOT / "03_scoreboard" / "TOP_GAINER_ASOF.md"
OUT_JSON = ROOT / "03_scoreboard" / "top_gainer_asof.json"
EXPORT_DIR = ROOT / "data" / "exports"
BOOK_DIR = ROOT / "data" / "stock_book"

START = book_era.DASHBOARD_START
TOP_N = 15
MIN_CHANGE = 5.0
FLOORS = (2.0, 5.0)
MIN_MCAP_M = 100.0
MIN_AVG_VOL_K = 500.0
COVERAGE_OK = 0.45
REGIME_EDGE = 15.0
STABLE_EDGE = 8.0

GAINER_BOX_COLS = tl.BOX_COLS + (("yday", "yΔ"),)
BOX_ERA = {
    "join": "join",
    "sector": "sector_predict",
    "gen": "general_predict",
    "news": "news_actions",
    "digest": "finviz_digest",
    "judge": "news_judge",
    "ab": "ab_enriched",
    "peer": "peer_rs",
    "heat": "map_heat",
    "vol": None,
    "catal": "catalyst",
    "buy": "stock_book",
    "yday": None,
}

_WALK = None


def _num(x, default=None):
    return tl._num(x, default)


def _pct(x):
    return _num(x)


def _legend() -> str:
    return " · ".join(lab for _, lab in GAINER_BOX_COLS)


def _labeled(boxes: dict | None) -> str:
    return " ".join(
        f"{lab}{tl.BOX_ICON.get((boxes or {}).get(key), '⬛')}"
        for key, lab in GAINER_BOX_COLS
    )


def _chg_tone(chg) -> str:
    v = _pct(chg)
    if v is None:
        return "missing"
    if v > 0:
        return "good"
    if v < 0:
        return "bad"
    return "neutral"


def same_day_buy_rows(date: str) -> list[dict]:
    path = BOOK_DIR / f"{date}_stock_book.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    rows = ((data.get("books") or {}).get("1d") or {}).get("buy") or []
    out = []
    for i, r in enumerate(rows, 1):
        if not isinstance(r, dict):
            continue
        t = tl._tick(r.get("ticker"))
        if not t:
            continue
        out.append({
            "ticker": t,
            "company": r.get("company") or "",
            "sector": r.get("sector") or "",
            "industry": r.get("industry") or r.get("group_label") or "",
            "rank": i,
            "score": _num(r.get("score")),
            "size": r.get("size") or "",
        })
    return out


def same_day_buy_set(date: str) -> set[str]:
    return {r["ticker"] for r in same_day_buy_rows(date)}


def load_finviz(date: str) -> pd.DataFrame:
    path = EXPORT_DIR / f"finviz_{date}.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (OSError, ValueError, pd.errors.ParserError):
        return pd.DataFrame()


def tape_coverage(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {"status": "missing", "n": 0, "n_change": 0, "frac": 0.0}
    chg = df["Change"].map(_pct) if "Change" in df.columns else pd.Series(dtype=float)
    n = int(len(df))
    n_change = int((chg.fillna(0.0) != 0.0).sum()) if n else 0
    frac = (n_change / n) if n else 0.0
    status = "full" if frac >= COVERAGE_OK else "partial"
    if n_change == 0:
        status = "empty"
    return {"status": status, "n": n, "n_change": n_change, "frac": round(frac, 3)}


def liquid_gainers(df: pd.DataFrame, top_n: int = TOP_N,
                   min_change: float = 0.0, liquid: bool = True,
                   min_mcap_m: float | None = None) -> list[dict]:
    if df is None or df.empty:
        return []
    work = df.copy()
    if "Ticker" not in work.columns or "Change" not in work.columns:
        return []
    floor = MIN_MCAP_M if min_mcap_m is None else float(min_mcap_m)
    work["ticker"] = work["Ticker"].map(tl._tick)
    work["chg"] = work["Change"].map(_pct)
    work["mcap"] = pd.to_numeric(work["Market Cap"], errors="coerce") if "Market Cap" in work.columns else float("nan")
    work["adv"] = pd.to_numeric(work["Average Volume"], errors="coerce") if "Average Volume" in work.columns else float("nan")
    work["volume"] = pd.to_numeric(work["Volume"], errors="coerce") if "Volume" in work.columns else float("nan")
    if "Industry" in work.columns:
        not_etf = ~work["Industry"].astype(str).eq("Exchange Traded Fund")
    else:
        not_etf = True
    keep = (
        work["ticker"].astype(bool)
        & work["chg"].notna()
        & (work["chg"] >= float(min_change))
        & (work["volume"].fillna(0) > 0)
        & not_etf
    )
    if liquid:
        keep = keep & (work["mcap"] >= floor) & (work["adv"] >= MIN_AVG_VOL_K)
    ranked = work.loc[keep].sort_values("chg", ascending=False)
    if top_n and int(top_n) > 0:
        ranked = ranked.head(int(top_n))
    out = []
    for rec in ranked.to_dict(orient="records"):
        out.append({
            "ticker": rec["ticker"],
            "company": rec.get("Company") or "",
            "sector": rec.get("Sector") or "",
            "industry": rec.get("Industry") or "",
            "change_pct": round(float(rec["chg"]), 2),
            "mcap_m": _num(rec.get("mcap")),
            "avg_vol_k": _num(rec.get("adv")),
        })
    return out


def _era_skip(date: str) -> list[str]:
    skip = []
    for key, feature in BOX_ERA.items():
        if feature and not book_era.live(date, feature):
            skip.append(key)
    return skip


def _yday_from_sess(sess: dict | None, ticker: str) -> tuple[str, str | None, float | None]:
    prior = (sess or {}).get("prior")
    fv = ((prior or {}).get("finviz") or {}).get(ticker)
    vintage = (prior or {}).get("date")
    if not fv:
        return "missing", vintage, None
    chg = _pct(fv.get("Change") if fv.get("Change") is not None else fv.get("Change %"))
    return _chg_tone(chg), vintage, None if chg is None else round(float(chg), 2)


def color_name(sess: dict | None, row: dict, buy_today: set[str],
               realized: float | None = None) -> dict:
    t = row["ticker"]
    card = scan._scan_session(sess, t) if sess else {}
    card = card or {}
    boxes = dict(card.get("boxes") or {k: "missing" for k, _ in tl.BOX_COLS})
    vintage = dict(card.get("factor_vintage") or {})
    yday, yv, ychg = _yday_from_sess(sess, t)
    boxes["yday"] = yday
    if yv:
        vintage["yday"] = yv
    cond = tl.general_condition({k: boxes.get(k) for k, _ in tl.BOX_COLS})
    region = tl.color_region({k: boxes.get(k) for k, _ in tl.BOX_COLS})
    chg = row.get("change_pct") if row.get("change_pct") is not None else realized
    return {
        **row,
        "change_pct": chg,
        "boxes": boxes,
        "labeled": _labeled(boxes),
        "factor_vintage": vintage,
        "sources": card.get("sources") or [],
        "class": card.get("class") or ("no_session" if not sess else "no_data"),
        "condition": cond,
        "region": region,
        "yday_change": ychg,
        "overnight_buy": boxes.get("buy") == "good",
        "on_1d_buy": t in buy_today,
        "asof": "09:30_et",
        "prior_date": (sess or {}).get("prior_date"),
    }


def color_gainer(sess: dict, row: dict, buy_today: set[str]) -> dict:
    return color_name(sess, row, buy_today)


def _spy_change(fv: pd.DataFrame):
    if fv is None or fv.empty or "Ticker" not in fv.columns:
        return None
    hit = fv[fv["Ticker"].astype(str).str.upper() == "SPY"]
    if hit.empty:
        return None
    return _pct(hit.iloc[0].get("Change"))


def _finviz_change_map(fv: pd.DataFrame) -> dict[str, float]:
    if fv is None or fv.empty or "Ticker" not in fv.columns:
        return {}
    out = {}
    for rec in fv.to_dict(orient="records"):
        t = tl._tick(rec.get("Ticker"))
        chg = _pct(rec.get("Change"))
        if t and chg is not None:
            out[t] = chg
    return out


def day_walk(date: str, *, idx=None, top_n: int = TOP_N,
             min_change: float = MIN_CHANGE, liquid: bool = True,
             include_buys: bool = True,
             min_mcap_m: float | None = None) -> dict:
    idx = idx or tl.build_index()
    sess = next((s for s in idx["sessions"] if s["date"] == date), None)
    fv = load_finviz(date)
    cov = tape_coverage(fv)
    names = (
        liquid_gainers(
            fv, top_n=top_n, min_change=min_change, liquid=liquid,
            min_mcap_m=min_mcap_m,
        )
        if cov["status"] != "missing" else []
    )
    buy_meta = same_day_buy_rows(date)
    buy_today = {r["ticker"] for r in buy_meta}
    realized = _finviz_change_map(fv)
    cache: dict[str, dict] = {}

    def paint(row, chg=None):
        t = row["ticker"]
        if t not in cache:
            cache[t] = color_name(sess, row, buy_today, realized=chg)
        return cache[t]

    rows = [paint(row) for row in names]
    buys = []
    if include_buys:
        for raw in buy_meta:
            chg = realized.get(raw["ticker"])
            painted = paint({**raw, "change_pct": chg}, chg=chg)
            buys.append(painted)
    return {
        "date": date,
        "coverage": cov,
        "spy_change": _spy_change(fv),
        "era_skip": _era_skip(date),
        "n_gainers": len(rows),
        "n_overnight_buy": sum(1 for r in rows if r.get("overnight_buy")),
        "n_on_1d_buy": sum(1 for r in rows if r.get("on_1d_buy")),
        "min_change": float(min_change),
        "liquid": bool(liquid),
        "top_n": top_n,
        "rows": rows,
        "buys": buys,
    }


def _tally(rows: list[dict], era_skip: list[str] | None = None,
           skip_by_date: dict[str, list[str]] | None = None) -> dict:
    counts = {key: Counter() for key, _ in GAINER_BOX_COLS}
    n = n_over = n_today = 0
    hit2 = hit5 = 0
    chgs = []
    for row in rows:
        n += 1
        if row.get("overnight_buy"):
            n_over += 1
        if row.get("on_1d_buy"):
            n_today += 1
        chg = _pct(row.get("change_pct"))
        if chg is not None:
            chgs.append(chg)
            if chg >= 2:
                hit2 += 1
            if chg >= 5:
                hit5 += 1
        skip = set(era_skip or [])
        if skip_by_date is not None:
            skip = set(skip_by_date.get(row.get("date") or "", []) or skip)
        boxes = row.get("boxes") or {}
        for key, _ in GAINER_BOX_COLS:
            tone = boxes.get(key) or "missing"
            if key in skip and tone == "missing":
                counts[key]["era"] += 1
            else:
                counts[key][tone] += 1
    out = {}
    for key, _ in GAINER_BOX_COLS:
        c = counts[key]
        total = sum(c.values()) or 1
        out[key] = {
            "good": c.get("good", 0),
            "neutral": c.get("neutral", 0),
            "bad": c.get("bad", 0),
            "missing": c.get("missing", 0),
            "era": c.get("era", 0),
            "good_pct": round(100.0 * c.get("good", 0) / total, 1),
            "bad_pct": round(100.0 * c.get("bad", 0) / total, 1),
            "printed_pct": round(
                100.0 * (total - c.get("missing", 0) - c.get("era", 0)) / total, 1
            ),
        }
    chgs_sorted = sorted(chgs)
    mid = chgs_sorted[len(chgs_sorted) // 2] if chgs_sorted else None
    return {
        "n_names": n,
        "n_overnight_buy": n_over,
        "n_on_1d_buy": n_today,
        "overnight_buy_pct": round(100.0 * n_over / n, 1) if n else 0.0,
        "on_1d_buy_pct": round(100.0 * n_today / n, 1) if n else 0.0,
        "n_with_change": len(chgs),
        "median_change": None if mid is None else round(float(mid), 2),
        "hit_2_pct": round(100.0 * hit2 / len(chgs), 1) if chgs else 0.0,
        "hit_5_pct": round(100.0 * hit5 / len(chgs), 1) if chgs else 0.0,
        "boxes": out,
    }


def _attach_dates(days: list[dict], key: str) -> list[dict]:
    out = []
    skip = {}
    for day in days:
        skip[day["date"]] = day.get("era_skip") or []
        for row in day.get(key) or []:
            rec = dict(row)
            rec["date"] = day["date"]
            out.append(rec)
    return out, skip


def _split_regime(days: list[dict], key: str) -> tuple[list[dict], list[dict]]:
    up, down = [], []
    for day in days:
        spy = day.get("spy_change")
        if spy is None:
            continue
        bucket = up if spy > 0 else down
        for row in day.get(key) or []:
            rec = dict(row)
            rec["date"] = day["date"]
            bucket.append(rec)
    return up, down


def _insights(floors: dict, buys: dict, regime: dict) -> list[str]:
    """Plain-language read of the job — robust vs regime-sensitive boxes."""
    lines = ["## What the boxes actually said", ""]
    g5 = (floors.get("5") or {}).get("summary") or {}
    g2 = (floors.get("2") or {}).get("summary") or {}
    bsum = buys.get("summary") or {}
    lines.append(
        f"At ≥5% the book almost never held the rip: overnight BUY "
        f"{g5.get('overnight_buy_pct') or 0:.1f}% · today's 1d BUY "
        f"{g5.get('on_1d_buy_pct') or 0:.1f}% of "
        f"{g5.get('n_names') or 0} liquid winners. "
        f"The 1d BUY sleeve itself "
        f"({bsum.get('n_names') or 0} names) realized a median "
        f"{bsum.get('median_change') if bsum.get('median_change') is not None else '—'}% "
        f"and hit ≥2% / ≥5% on "
        f"{bsum.get('hit_2_pct') or 0:.1f}% / {bsum.get('hit_5_pct') or 0:.1f}% "
        f"of names with a printed Change%."
    )
    lines.append("")

    g5b = g5.get("boxes") or {}
    upb = ((regime.get("spy_up") or {}).get("boxes") or {})
    dnb = ((regime.get("spy_down") or {}).get("boxes") or {})
    stable, sensitive, holes = [], [], []
    for key, lab in GAINER_BOX_COLS:
        rec = g5b.get(key) or {}
        printed = rec.get("printed_pct") or 0
        if printed < 10:
            holes.append(
                f"**{lab}** printed on {printed:.1f}% of ≥5% winners "
                f"(green {rec.get('good_pct') or 0:.1f}%) — a coverage hole, "
                f"not a failed tone"
            )
            continue
        ug = (upb.get(key) or {}).get("good_pct")
        dg = (dnb.get(key) or {}).get("good_pct")
        if ug is None or dg is None:
            continue
        delta = ug - dg
        bit = (
            f"**{lab}** green {rec.get('good_pct') or 0:.1f}% overall "
            f"(SPY-up {ug:.1f}% / SPY-down {dg:.1f}%, Δ {delta:+.1f})"
        )
        if abs(delta) >= REGIME_EDGE:
            sensitive.append(bit)
        elif abs(delta) <= STABLE_EDGE:
            stable.append(bit)
    if stable:
        lines += [
            "Stable across the tape (green% barely moves when SPY closes up vs down):",
            "",
        ]
        for bit in stable:
            lines.append(f"- {bit}")
        lines.append("")
    if sensitive:
        lines += [
            "Moves with market conditions (green% on winners flips with SPY close):",
            "",
        ]
        for bit in sensitive:
            lines.append(f"- {bit}")
        lines.append("")
    if holes:
        lines += ["Almost never printed on the names that ripped:", ""]
        for bit in holes:
            lines.append(f"- {bit}")
        lines.append("")
    lines += [
        "`gen` is the market-condition box. It is the morning essay, not the "
        "close, so a SPY-down session can still show gen🟢 on the names that "
        "ripped if the pre-open write was constructive. `yΔ` is yesterday's "
        "Change% from the last completed tape — a continuation tell that does "
        "not use today's close.",
        "",
        f"The ≥2% cut is the wider net ({g2.get('n_names') or 0} names vs "
        f"{g5.get('n_names') or 0} at ≥5%). Use it to see whether a box still "
        "prints when the move is ordinary, not only on the spike tail.",
        "",
    ]
    return lines


def walk(from_date: str = START, to_date: str | None = None,
         top_n: int = TOP_N, min_change: float = MIN_CHANGE,
         liquid: bool = True, *, force: bool = False,
         floors: list[float] | None = None,
         include_buys: bool = True,
         min_mcap_m: float | None = None) -> dict:
    global _WALK
    floor_list = [float(x) for x in (floors or [min_change])]
    floor_list = sorted({x for x in floor_list})
    primary = min(floor_list) if floor_list else float(min_change)
    key = (
        from_date, to_date, top_n, primary, tuple(floor_list),
        liquid, include_buys, min_mcap_m,
    )
    if _WALK is not None and not force and _WALK.get("_key") == list(key):
        return _WALK
    idx = tl.build_index()
    dates = [
        s["date"] for s in idx["sessions"]
        if s["date"] >= from_date and (not to_date or s["date"] <= to_date)
    ]
    raw_days = [
        day_walk(
            d, idx=idx, top_n=top_n, min_change=primary,
            liquid=liquid, include_buys=include_buys, min_mcap_m=min_mcap_m,
        )
        for d in dates
    ]
    # Slice higher floors from the primary (widest) walk.
    days_by_floor = {}
    for fl in floor_list:
        sliced = []
        for day in raw_days:
            rows = [
                r for r in (day.get("rows") or [])
                if (_pct(r.get("change_pct")) or 0) >= fl
            ]
            rec = dict(day)
            rec["rows"] = rows
            rec["min_change"] = fl
            rec["n_gainers"] = len(rows)
            rec["n_overnight_buy"] = sum(1 for r in rows if r.get("overnight_buy"))
            rec["n_on_1d_buy"] = sum(1 for r in rows if r.get("on_1d_buy"))
            sliced.append(rec)
        rows, skip = _attach_dates(sliced, "rows")
        days_by_floor[str(int(fl)) if fl == int(fl) else str(fl)] = {
            "min_change": fl,
            "days": sliced,
            "summary": _tally(rows, skip_by_date=skip),
        }
    buy_rows, buy_skip = _attach_dates(raw_days, "buys")
    buy_block = {
        "days": raw_days,
        "summary": _tally(buy_rows, skip_by_date=buy_skip),
    }
    primary_key = str(int(primary)) if primary == int(primary) else str(primary)
    primary_days = days_by_floor.get(primary_key, {}).get("days") or raw_days
    up, down = _split_regime(primary_days, "rows")
    regime = {
        "spy_up": _tally(up),
        "spy_down": _tally(down),
    }
    mcap = MIN_MCAP_M if min_mcap_m is None else float(min_mcap_m)
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "from_date": from_date,
        "to_date": to_date,
        "top_n": top_n,
        "min_change": primary,
        "floors": floor_list,
        "liquid": bool(liquid),
        "include_buys": bool(include_buys),
        "min_mcap_m": mcap if liquid else 0.0,
        "min_avg_vol_k": MIN_AVG_VOL_K if liquid else 0.0,
        "legend": _legend(),
        "days": primary_days,
        "summary": days_by_floor.get(primary_key, {}).get("summary") or _tally([]),
        "floors_detail": days_by_floor,
        "buys": buy_block,
        "regime": regime,
        "_key": list(key),
    }
    payload["insights"] = _insights(days_by_floor, buy_block, regime)
    _WALK = payload
    return payload


def _box_table(boxes: dict) -> list[str]:
    lines = [
        "| Box | Green | Yellow | Red | Missing | Era-skip | Green% | Printed% |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, lab in GAINER_BOX_COLS:
        rec = (boxes or {}).get(key) or {}
        lines.append(
            f"| {lab} | {rec.get('good', 0)} | {rec.get('neutral', 0)} | "
            f"{rec.get('bad', 0)} | {rec.get('missing', 0)} | "
            f"{rec.get('era', 0)} | {rec.get('good_pct', 0):.1f}% | "
            f"{rec.get('printed_pct', 0):.1f}% |"
        )
    return lines


def _name_table(rows: list[dict], *, realized_label: str = "Δ") -> list[str]:
    lines = [
        f"| # | Ticker | {realized_label} | Sector | As-of boxes | Cond | Overnight BUY | On today's 1d BUY |",
        "|---:|---|---:|---|---|---|---|---|",
    ]
    for i, row in enumerate(rows, 1):
        cond = row.get("condition") or {}
        cond_s = (
            f"{tl.BOX_ICON.get(cond.get('tone'), '⬛')} "
            f"{cond.get('good', 0)}/{cond.get('neutral', 0)}/{cond.get('bad', 0)}"
            if cond.get("n") else "—"
        )
        chg = row.get("change_pct")
        chg_s = f"{chg:+.2f}%" if chg is not None else "—"
        lines.append(
            f"| {i} | `{row['ticker']}` | {chg_s} | "
            f"{row.get('sector') or '—'} | {row.get('labeled') or _labeled(row.get('boxes'))} | "
            f"{cond_s} | "
            f"{'yes' if row.get('overnight_buy') else '—'} | "
            f"{'yes' if row.get('on_1d_buy') else '—'} |"
        )
    return lines


def render_day_markdown(date: str, day: dict | None = None,
                        min_mcap_m: float | None = None) -> list[str]:
    day = day or day_walk(date, min_mcap_m=min_mcap_m)
    cov = day.get("coverage") or {}
    spy = day.get("spy_change")
    spy_s = f"{spy:+.2f}%" if spy is not None else "—"
    mcap = MIN_MCAP_M if min_mcap_m is None else float(min_mcap_m)
    lines = [
        f"### Top gainers — as-of 09:30 on {date}",
        "",
        "Realized winners (Finviz Change%"
        + (
            f" ≥ {day.get('min_change'):.0f}%"
            if (day.get("min_change") or 0) > 0 else ""
        )
        + (
            f", mcap ≥ ${mcap:.0f}M, adv ≥ 500k, not ETF"
            if day.get("liquid", True) else ", any name, not ETF"
        )
        + "). Boxes are the 09:30 ET packet. `yΔ` is yesterday's Change% "
        "from the last completed tape. Same-day RelVol / same-day book do "
        "not color a cell.",
        "",
        f"_Boxes {_legend()}_",
        "",
        f"Coverage **{cov.get('status') or '?'}** "
        f"({cov.get('n_change') or 0}/{cov.get('n') or 0} printed a Change%) "
        f"· SPY {spy_s}"
        + (
            f" · era-skip {', '.join(day.get('era_skip') or [])}"
            if day.get("era_skip") else ""
        ),
        "",
    ]
    rows = day.get("rows") or []
    if not rows:
        lines += ["_No liquid gainer tape for this session._", ""]
    else:
        lines += _name_table(rows)
        lines += [
            "",
            f"Overnight BUY caught {day.get('n_overnight_buy') or 0}/{len(rows)}; "
            f"today's 1d BUY list caught {day.get('n_on_1d_buy') or 0}/{len(rows)}.",
            "",
        ]
    buys = day.get("buys") or []
    if buys:
        hit2 = sum(1 for r in buys if (_pct(r.get("change_pct")) or 0) >= 2)
        hit5 = sum(1 for r in buys if (_pct(r.get("change_pct")) or 0) >= 5)
        lines += [
            f"#### Today's 1d BUY — realized vs as-of boxes on {date}",
            "",
            f"{len(buys)} names the book printed. "
            f"{hit2}/{len(buys)} closed ≥2% · {hit5}/{len(buys)} closed ≥5%.",
            "",
        ]
        lines += _name_table(buys, realized_label="Realized Δ")
        lines.append("")
    return lines


def render_markdown(payload: dict) -> str:
    floors = payload.get("floors_detail") or {}
    buys = payload.get("buys") or {}
    regime = payload.get("regime") or {}
    g5 = floors.get("5") or {
        "summary": payload.get("summary") or {},
        "days": payload.get("days") or [],
        "min_change": payload.get("min_change") or 5,
    }
    lines = [
        "# Top gainers — as-of 09:30",
        "",
        f"_Generated {payload.get('generated_at')} · as-of 09:30 ET · "
        f"{payload.get('from_date')} → {payload.get('to_date') or 'latest'}"
        + (
            " · floors "
            + ", ".join(f"≥{x:g}%" for x in (payload.get("floors") or []))
            if payload.get("floors") else ""
        )
        + (
            " · all names"
            if not payload.get("top_n") else f" · top {payload.get('top_n')}"
        )
        + "_",
        "",
        "Each session's realized gainers plus the 1d BUY sleeve. "
        "Boxes are the 09:30 ET packet. `yΔ` is yesterday's Change% from "
        "the prior tape (never today's close). Same-day RelVol and same-day "
        "stock book never color a box."
        + (
            f" Liquidity: mcap ≥ ${payload.get('min_mcap_m') or 0:.0f}M, "
            f"adv ≥ {payload.get('min_avg_vol_k') or 0:.0f}k, not ETF."
            if payload.get("liquid", True) else " Any printed name, not ETF."
        ),
        "",
        f"_Boxes {payload.get('legend') or _legend()}_",
        "",
    ]
    lines += payload.get("insights") or _insights(floors, buys, regime)

    for label, block, blurb in (
        ("≥5% winners", g5, "names that closed ≥5%"),
        ("≥2% winners", floors.get("2"), "names that closed ≥2%"),
        ("today's 1d BUY", buys, "what the book actually printed"),
    ):
        if not block:
            continue
        summ = block.get("summary") or {}
        lines += [
            f"## Hit rate — {label}",
            "",
            blurb[0].upper() + blurb[1:] + f": {summ.get('n_names') or 0} names. ",
        ]
        extra = []
        if summ.get("median_change") is not None:
            extra.append(f"median realized Δ {summ['median_change']:+.2f}%")
        if label.startswith("today"):
            extra.append(f"hit ≥2% {summ.get('hit_2_pct') or 0:.1f}%")
            extra.append(f"hit ≥5% {summ.get('hit_5_pct') or 0:.1f}%")
        else:
            extra.append(f"overnight BUY {summ.get('overnight_buy_pct') or 0:.1f}%")
            extra.append(f"today's 1d BUY {summ.get('on_1d_buy_pct') or 0:.1f}%")
        lines[-1] += " · ".join(extra) + "."
        lines += ["", *_box_table(summ.get("boxes") or {}), ""]

    up, down = regime.get("spy_up") or {}, regime.get("spy_down") or {}
    if up or down:
        lines += [
            "## Regime — ≥ lowest floor, SPY up vs down",
            "",
            f"SPY-up days: {up.get('n_names') or 0} winners. "
            f"SPY-down days: {down.get('n_names') or 0} winners. "
            "`gen` is the morning essay; a down close does not rewrite it.",
            "",
            "| Box | Up green% | Down green% | Δ | Up printed% | Down printed% |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for key, lab in GAINER_BOX_COLS:
            a = (up.get("boxes") or {}).get(key) or {}
            b = (down.get("boxes") or {}).get(key) or {}
            delta = (a.get("good_pct") or 0) - (b.get("good_pct") or 0)
            lines.append(
                f"| {lab} | {a.get('good_pct') or 0:.1f}% | "
                f"{b.get('good_pct') or 0:.1f}% | {delta:+.1f} | "
                f"{a.get('printed_pct') or 0:.1f}% | "
                f"{b.get('printed_pct') or 0:.1f}% |"
            )
        lines.append("")

    lines += ["## Per session", ""]
    show_days = (g5.get("days") or payload.get("days") or [])
    buy_by_date = {d["date"]: d for d in (buys.get("days") or [])}
    for day in show_days:
        merged = dict(day)
        src = buy_by_date.get(day["date"]) or {}
        if src.get("buys") and not merged.get("buys"):
            merged["buys"] = src["buys"]
        lines += render_day_markdown(day["date"], day=merged,
                                     min_mcap_m=payload.get("min_mcap_m"))
    lines.append("")
    return "\n".join(lines)


def write_scoreboard(payload: dict | None = None) -> tuple[Path, Path]:
    payload = payload or walk(force=True)
    slim = {k: v for k, v in payload.items() if k != "_key"}
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(render_markdown(payload), encoding="utf-8")
    OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    return OUT_MD, OUT_JSON


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="from_date", default=START)
    ap.add_argument("--to", dest="to_date", default="")
    ap.add_argument("--top", type=int, default=TOP_N,
                    help="Cap per session (0 or --all = every name over the floor)")
    ap.add_argument("--all", action="store_true",
                    help="Do not cap — every name at/above the floor")
    ap.add_argument("--min-change", type=float, default=MIN_CHANGE,
                    help="Single floor when --floors is omitted (default 5)")
    ap.add_argument("--floors", default="",
                    help="Comma floors, e.g. 2,5 (walks the lowest, slices the rest)")
    ap.add_argument("--buys", action="store_true", default=True,
                    help="Also color today's 1d BUY sleeve (default on)")
    ap.add_argument("--no-buys", dest="buys", action="store_false")
    ap.add_argument("--no-liquid", dest="liquid", action="store_false",
                    help="Include names below the mcap / adv floor")
    ap.add_argument("--min-mcap", type=float, default=MIN_MCAP_M,
                    help="Market-cap floor in $ millions (default 100)")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    top_n = 0 if args.all else args.top
    floors = None
    if args.floors.strip():
        floors = [float(x) for x in args.floors.split(",") if x.strip()]
    payload = walk(
        from_date=args.from_date,
        to_date=args.to_date or None,
        top_n=top_n,
        min_change=args.min_change,
        floors=floors,
        liquid=args.liquid,
        include_buys=args.buys,
        min_mcap_m=args.min_mcap,
        force=True,
    )
    text = render_markdown(payload)
    print(text)
    if args.write:
        md, js = write_scoreboard(payload)
        print(f"[gainer-asof] wrote {md}")
        print(f"[gainer-asof] wrote {js}")


if __name__ == "__main__":
    main()
