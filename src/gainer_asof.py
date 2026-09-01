"""Top-gainer as-of walk — what the 12 golden inputs said before the open.

For every session since the dashboard start (2026-08-13), take that day's
realized liquid top gainers (same-day Finviz Change%) and color the
lookback boxes from the 09:30 ET information set:

  join / sect / gen / news / dig / jdg  — D's morning packet
  AB / peer / vol / overnight buy       — last completed tape before D
  heat / cat                            — morning captains / pre-open dossiers

Same-day Finviz RelVol and same-day stock book never color a box.
Change% is the outcome only. A separate column asks whether today's
1d BUY list caught the name after the ranker ran.

Writes:
  03_scoreboard/TOP_GAINER_ASOF.md
  03_scoreboard/top_gainer_asof.json

CLI: python -m src.gainer_asof [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--write]
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
MIN_MCAP_M = 500.0
MIN_AVG_VOL_K = 500.0
# Same-day Change tape is usable when at least this share of rows printed.
COVERAGE_OK = 0.45

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
}

_WALK = None


def _num(x, default=None):
    return tl._num(x, default)


def _pct(x):
    return _num(x)


def _labeled(boxes: dict | None) -> str:
    return " ".join(
        f"{lab}{tl.BOX_ICON.get((boxes or {}).get(key), '⬛')}"
        for key, lab in tl.BOX_COLS
    )


def _legend() -> str:
    return " · ".join(lab for _, lab in tl.BOX_COLS)


def same_day_buy_set(date: str) -> set[str]:
    data = {}
    path = BOOK_DIR / f"{date}_stock_book.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return set()
    rows = ((data.get("books") or {}).get("1d") or {}).get("buy") or []
    return {tl._tick(r.get("ticker")) for r in rows if isinstance(r, dict)}


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


def liquid_gainers(df: pd.DataFrame, top_n: int = TOP_N) -> list[dict]:
    if df is None or df.empty:
        return []
    work = df.copy()
    if "Ticker" not in work.columns or "Change" not in work.columns:
        return []
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
        & (work["chg"] > 0)
        & (work["mcap"] >= MIN_MCAP_M)
        & (work["adv"] >= MIN_AVG_VOL_K)
        & (work["volume"].fillna(0) > 0)
        & not_etf
    )
    ranked = work.loc[keep].sort_values("chg", ascending=False).head(int(top_n))
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


def color_gainer(sess: dict, row: dict, buy_today: set[str]) -> dict:
    card = scan._scan_session(sess, row["ticker"]) or {}
    boxes = card.get("boxes") or {k: "missing" for k, _ in tl.BOX_COLS}
    vintage = card.get("factor_vintage") or {}
    cond = tl.general_condition(boxes)
    region = tl.color_region(boxes)
    t = row["ticker"]
    return {
        **row,
        "boxes": boxes,
        "labeled": _labeled(boxes),
        "factor_vintage": vintage,
        "sources": card.get("sources") or [],
        "class": card.get("class") or "no_data",
        "condition": cond,
        "region": region,
        "overnight_buy": boxes.get("buy") == "good",
        "on_1d_buy": t in buy_today,
        "asof": "09:30_et",
        "prior_date": sess.get("prior_date"),
    }


def day_walk(date: str, *, idx=None, top_n: int = TOP_N) -> dict:
    idx = idx or tl.build_index()
    sess = next((s for s in idx["sessions"] if s["date"] == date), None)
    fv = load_finviz(date)
    cov = tape_coverage(fv)
    names = liquid_gainers(fv, top_n=top_n) if cov["status"] != "missing" else []
    buy_today = same_day_buy_set(date)
    rows = []
    if sess is not None:
        for row in names:
            rows.append(color_gainer(sess, row, buy_today))
    else:
        for row in names:
            rows.append({
                **row,
                "boxes": {k: "missing" for k, _ in tl.BOX_COLS},
                "labeled": _labeled({}),
                "factor_vintage": {},
                "sources": [],
                "class": "no_session",
                "condition": {"tone": "missing", "good": 0, "neutral": 0, "bad": 0, "n": 0},
                "region": {"tone": "missing", "good": 0, "bad": 0},
                "overnight_buy": False,
                "on_1d_buy": row["ticker"] in buy_today,
                "asof": "09:30_et",
                "prior_date": None,
            })
    spy = None
    if not fv.empty and "Ticker" in fv.columns:
        hit = fv[fv["Ticker"].astype(str).str.upper() == "SPY"]
        if not hit.empty:
            spy = _pct(hit.iloc[0].get("Change"))
    return {
        "date": date,
        "coverage": cov,
        "spy_change": spy,
        "era_skip": _era_skip(date),
        "n_gainers": len(rows),
        "n_overnight_buy": sum(1 for r in rows if r.get("overnight_buy")),
        "n_on_1d_buy": sum(1 for r in rows if r.get("on_1d_buy")),
        "rows": rows,
    }


def _tally(days: list[dict]) -> dict:
    counts = {key: Counter() for key, _ in tl.BOX_COLS}
    n = 0
    n_over = n_today = 0
    for day in days:
        for row in day.get("rows") or []:
            n += 1
            if row.get("overnight_buy"):
                n_over += 1
            if row.get("on_1d_buy"):
                n_today += 1
            boxes = row.get("boxes") or {}
            skip = set(day.get("era_skip") or [])
            for key, _ in tl.BOX_COLS:
                tone = boxes.get(key) or "missing"
                if key in skip and tone == "missing":
                    counts[key]["era"] += 1
                else:
                    counts[key][tone] += 1
    out = {}
    for key, _ in tl.BOX_COLS:
        c = counts[key]
        total = sum(c.values()) or 1
        out[key] = {
            "good": c.get("good", 0),
            "neutral": c.get("neutral", 0),
            "bad": c.get("bad", 0),
            "missing": c.get("missing", 0),
            "era": c.get("era", 0),
            "good_pct": round(100.0 * c.get("good", 0) / total, 1),
            "printed_pct": round(
                100.0 * (total - c.get("missing", 0) - c.get("era", 0)) / total, 1
            ),
        }
    return {
        "n_names": n,
        "n_overnight_buy": n_over,
        "n_on_1d_buy": n_today,
        "overnight_buy_pct": round(100.0 * n_over / n, 1) if n else 0.0,
        "on_1d_buy_pct": round(100.0 * n_today / n, 1) if n else 0.0,
        "boxes": out,
    }


def walk(from_date: str = START, to_date: str | None = None,
         top_n: int = TOP_N, *, force: bool = False) -> dict:
    global _WALK
    if _WALK is not None and not force:
        if (_WALK.get("from_date") == from_date
                and _WALK.get("to_date") == to_date
                and _WALK.get("top_n") == top_n):
            return _WALK
    idx = tl.build_index()
    dates = [
        s["date"] for s in idx["sessions"]
        if s["date"] >= from_date and (not to_date or s["date"] <= to_date)
    ]
    days = [day_walk(d, idx=idx, top_n=top_n) for d in dates]
    payload = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "asof": "09:30_et",
        "from_date": from_date,
        "to_date": to_date,
        "top_n": top_n,
        "min_mcap_m": MIN_MCAP_M,
        "min_avg_vol_k": MIN_AVG_VOL_K,
        "legend": _legend(),
        "days": days,
        "summary": _tally(days),
    }
    _WALK = payload
    return payload


def render_day_markdown(date: str, day: dict | None = None) -> list[str]:
    day = day or day_walk(date)
    cov = day.get("coverage") or {}
    spy = day.get("spy_change")
    spy_s = f"{spy:+.2f}%" if spy is not None else "—"
    lines = [
        f"### Top gainers — as-of 09:30 on {date}",
        "",
        "Realized liquid winners (Finviz Change%, mcap ≥ $500M, adv ≥ 500k, "
        "not ETF). Boxes are the 09:30 ET packet — last completed tape + "
        "that morning's pre-open files. Same-day RelVol / same-day book do "
        "not color a cell. `buy` = overnight 1d BUY; `on 1d BUY` = today's "
        "book after the ranker ran.",
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
        lines += ["_No liquid top-gainer tape for this session._", ""]
        return lines
    lines += [
        "| # | Ticker | Δ | Sector | As-of boxes | Cond | Overnight BUY | On today's 1d BUY |",
        "|---:|---|---:|---|---|---|---|---|",
    ]
    for i, row in enumerate(rows, 1):
        cond = row.get("condition") or {}
        cond_s = (
            f"{tl.BOX_ICON.get(cond.get('tone'), '⬛')} "
            f"{cond.get('good', 0)}/{cond.get('neutral', 0)}/{cond.get('bad', 0)}"
            if cond.get("n") else "—"
        )
        lines.append(
            f"| {i} | `{row['ticker']}` | {row.get('change_pct') or 0:+.2f}% | "
            f"{row.get('sector') or '—'} | {row.get('labeled') or _labeled(row.get('boxes'))} | "
            f"{cond_s} | "
            f"{'yes' if row.get('overnight_buy') else '—'} | "
            f"{'yes' if row.get('on_1d_buy') else '—'} |"
        )
    caught = day.get("n_on_1d_buy") or 0
    over = day.get("n_overnight_buy") or 0
    lines += [
        "",
        f"Overnight BUY caught {over}/{len(rows)}; today's 1d BUY list "
        f"caught {caught}/{len(rows)}.",
        "",
    ]
    return lines


def render_markdown(payload: dict) -> str:
    summ = payload.get("summary") or {}
    boxes = summ.get("boxes") or {}
    lines = [
        "# Top gainers — as-of 09:30",
        "",
        f"_Generated {payload.get('generated_at')} · as-of 09:30 ET · "
        f"{payload.get('from_date')} → {payload.get('to_date') or 'latest'}_",
        "",
        "Each session's liquid top gainers (Finviz Change%, "
        f"mcap ≥ ${payload.get('min_mcap_m'):.0f}M, "
        f"adv ≥ {payload.get('min_avg_vol_k'):.0f}k, not ETF). "
        "Boxes are filled from the information set knowable before the open. "
        "Same-day Finviz RelVol and same-day stock book never color a box.",
        "",
        f"_Boxes {payload.get('legend') or _legend()}_",
        "",
        "## Hit rate on names that then ripped",
        "",
        f"{summ.get('n_names') or 0} liquid winners across "
        f"{len(payload.get('days') or [])} sessions. "
        f"Overnight BUY {summ.get('overnight_buy_pct') or 0:.1f}% · "
        f"today's 1d BUY {summ.get('on_1d_buy_pct') or 0:.1f}%.",
        "",
        "| Box | Green | Yellow | Red | Missing | Era-skip | Green% | Printed% |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, lab in tl.BOX_COLS:
        rec = boxes.get(key) or {}
        lines.append(
            f"| {lab} | {rec.get('good', 0)} | {rec.get('neutral', 0)} | "
            f"{rec.get('bad', 0)} | {rec.get('missing', 0)} | "
            f"{rec.get('era', 0)} | {rec.get('good_pct', 0):.1f}% | "
            f"{rec.get('printed_pct', 0):.1f}% |"
        )
    lines += ["", "## Per session", ""]
    for day in payload.get("days") or []:
        lines += render_day_markdown(day["date"], day=day)
    lines.append("")
    return "\n".join(lines)


def write_scoreboard(payload: dict | None = None) -> tuple[Path, Path]:
    payload = payload or walk(force=True)
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(render_markdown(payload), encoding="utf-8")
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return OUT_MD, OUT_JSON


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="from_date", default=START)
    ap.add_argument("--to", dest="to_date", default="")
    ap.add_argument("--top", type=int, default=TOP_N)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    payload = walk(
        from_date=args.from_date,
        to_date=args.to_date or None,
        top_n=args.top,
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
