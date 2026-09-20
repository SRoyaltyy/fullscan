"""Fee-aware OOS board for the frozen ≤9/9 Clock-B / oppset remine.

Excel lane. Does **not** re-select on 9/10–9/18. Names come from
``factor_mine_blind_0909_oppset/holdout.json`` (Cyrus ∪ formal KEEP ∪
hot4 / Clock-B contamination checks). Scoring adds OOS start-day YES
and after-fee H beside continued / fresh books.

  python -m src.factor_mine_blind_oppset_oos
"""
from __future__ import annotations

import json
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_blind as fmbld
from . import factor_mine_blind_oos as fmoos
from . import factor_mine_blind_oppset as fmbopp

FEE_RT = fmoos.FEE_RT
OOS_START = fmbopp.OOS_START
OOS_END = fmoos.OOS_END
CUTOFF = fmbopp.CUTOFF

HOLDOUT_JSON = (
    fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909_oppset" / "holdout.json"
)
IS_JSON = (
    fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909_oppset" / "factor_mine.json"
)
LIVE_JSON = fm.ROOT / "03_scoreboard" / "factor_mine.json"
NO_OPPSET_HOLDOUT = (
    fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909" / "holdout.json"
)
BOARD_MD = fm.ROOT / "03_scoreboard" / "FACTOR_MINE_BLIND_0909_OPPSET_OOS.md"
BOARD_JSON = fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909_oppset_oos.json"


def should_replay(name: str, cyrus: set[str]) -> bool:
    return (
        name in cyrus
        or name == "union_hot_n4_h1"
        or name in fmbopp.CLOCK_B_REPORT
    )


def contrast_286(holdout: dict, no_oppset: dict | None) -> dict:
    """What changed vs the no-oppset #286 freeze. Not a re-pick."""
    here = set(holdout.get("cyrus_featured") or [])
    there = set((no_oppset or {}).get("cyrus_featured") or [])
    return {
        "n_cyrus_oppset": len(here),
        "n_cyrus_no_oppset": len(there),
        "new_vs_286": sorted(here - there),
        "dropped_vs_286": sorted(there - here),
        "shared": sorted(here & there),
        "clock_b_in_cyrus": sorted(
            n for n in here
            if n in fmbopp.CLOCK_B_MENU or "clk_" in n or "oppset" in n
        ),
    }


def live_contamination(live: dict | None, holdout: dict) -> dict:
    base = fmoos.live_contamination(live, holdout)
    featured = list((live or {}).get("featured") or [])
    clk_live = [n for n in featured if n in fmbopp.CLOCK_B_MENU or n.startswith("union_clk_")
                or n.startswith("short_clk_") or "oppset" in n]
    base["clock_b_live_pins"] = clk_live
    base["clock_b_in_0909_menu"] = True
    base["holdup_in_0909_menu"] = False
    return base


def render_oos_md(holdout: dict, rows: list[dict], *,
                  contamination: dict | None = None,
                  contrast: dict | None = None,
                  oos_start: str = OOS_START, oos_end: str = OOS_END) -> str:
    keep = [r for r in rows if r.get("verdict") == "KEEP"]
    fail = [r for r in rows if r.get("verdict") != "KEEP"]
    cyrus = [r for r in rows if r.get("cyrus_is")]
    book_only = [r for r in cyrus if r.get("book_only")]
    wr_trap = [r for r in rows if r.get("wr_only") and r.get("verdict") != "KEEP"]
    headline = f"**KEEP** {len(keep)}" if keep else "**FAIL** 0 KEEP"
    cont = contamination or {}
    vs = contrast or {}
    clk_rows = [
        r for r in rows
        if r["name"] in fmbopp.CLOCK_B_REPORT
        or any("clk_" in str(m) or "oppset" in str(m)
               for m in (r.get("members") or []))
        or "clk_" in r["name"] or "oppset" in r["name"]
    ]
    lines = [
        "# Factor mine blind 9/9 + Clock-B/oppset — fee-aware OOS",
        "",
        f"status=DONE verdict={headline} cutoff={CUTOFF} "
        f"OOS={oos_start}→{oos_end} FEE_RT={FEE_RT} "
        f"KEEP={len(keep)} FAIL={len(fail)} freeze="
        f"{holdout.get('recipe_freeze') or fmbopp.RECIPE_FREEZE_SHA}",
        "",
        "Research only. Live `flatten_robust` / `dashboard/factor-mine/` / "
        "`03_scoreboard/factor_mine.json` were not written. Recipe names "
        "are the IS freeze — OOS dates did not re-select.",
        "",
        "## KEEP bar",
        "",
        "Cyrus OOS KEEP: **IS Cyrus featured** (Starts YES + Book% on "
        f"8/13–9/9) **and** continued OOS Book% > 0 **and** OOS "
        f"start-day YES ≥{fmbld.CYRUS_START_RATE:.0%} "
        f"(7 sessions → ≥6/7). Win% > 55% is not enough. "
        f"`FEE_RT={FEE_RT}`.",
        "",
        "## Headline",
        "",
    ]
    if keep:
        lines.append("KEEP: " + ", ".join(f"`{r['name']}`" for r in keep) + ".")
    else:
        lines.append(
            "FAIL. No frozen ≤9/9+oppset Cyrus name keeps positive "
            f"continued Book% **and** enough OOS start-day wins "
            f"({oos_start}–{oos_end})."
        )
    lines += [
        "",
        f"Book-only survivors (positive continued Book%, not a KEEP bar): "
        f"**{len(book_only)}** of {len(cyrus)} Cyrus IS names. "
        f"Win%>55% traps: **{len(wr_trap)}**.",
        "",
        "## Contrast vs #286 (no-oppset)",
        "",
        f"- #286 Cyrus featured: **{vs.get('n_cyrus_no_oppset', '—')}**. "
        f"This remine: **{vs.get('n_cyrus_oppset', '—')}**.",
        f"- New vs #286: "
        + (", ".join(f"`{n}`" for n in (vs.get("new_vs_286") or [])) or "none")
        + ".",
        f"- Dropped vs #286: "
        + (", ".join(f"`{n}`" for n in (vs.get("dropped_vs_286") or [])) or "none")
        + ".",
        f"- Shared: "
        + (", ".join(f"`{n}`" for n in (vs.get("shared") or [])) or "none")
        + ".",
        f"- Clock-B / oppset names in Cyrus: "
        + (", ".join(f"`{n}`" for n in (vs.get("clock_b_in_cyrus") or [])) or "none")
        + ".",
        "",
        "## Clock-B / oppset catalogue — IS + OOS",
        "",
        "| Strategy | Cyrus IS | OOS starts | Cont book% | Fresh $10k | "
        "After-fee H WR | Verdict |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    by = {r["name"]: r for r in rows}
    for name in fmbopp.CLOCK_B_REPORT:
        r = by.get(name) or {"name": name, "verdict": "FAIL",
                             "why": "not scored / not in freeze"}
        lines.append(
            f"| `{r['name']}` | "
            f"{'YES' if r.get('cyrus_is') else 'no'} | "
            f"{fmoos._md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
            f"{fmoos._md_pp(r.get('oos_book_pct_continued'))} | "
            f"{fmoos._md_pp(r.get('fresh_book_pct'))} | "
            f"{fmoos._md_pct(r.get('oos_fee_h_wr'))} | "
            f"**{r.get('verdict') or 'FAIL'}** |"
        )
    extra_clk = [r for r in clk_rows if r["name"] not in fmbopp.CLOCK_B_REPORT]
    if extra_clk:
        lines += [
            "",
            "Clock-B / oppset-touched mixes (formed or 50/50 overlay):",
            "",
            "| Strategy | Cyrus IS | Members | OOS starts | Cont book% | "
            "Fresh $10k | Verdict |",
            "|---|---|---|---:|---:|---:|---|",
        ]
        for r in extra_clk:
            kids = " + ".join(f"`{m}`" for m in (r.get("members") or []))
            lines.append(
                f"| `{r['name']}` | {'YES' if r.get('cyrus_is') else 'no'} | "
                f"{kids} | "
                f"{fmoos._md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
                f"{fmoos._md_pp(r.get('oos_book_pct_continued'))} | "
                f"{fmoos._md_pp(r.get('fresh_book_pct'))} | "
                f"**{r.get('verdict')}** |"
            )
    lines += [
        "",
        "## Frozen Cyrus featured — KEEP / FAIL",
        "",
        "| Strategy | Side | IS start | IS book% | IS win% | "
        "OOS starts | Cont book% | Fresh $10k | After-fee H WR | "
        "WR-only | Verdict |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in cyrus:
        lines.append(
            f"| `{r['name']}` | {r.get('side') or '—'} | "
            f"{fmoos._md_start(r.get('is_start_green'), r.get('is_start_n'))} | "
            f"{fmoos._md_pp(r.get('is_book_pct'))} | "
            f"{fmoos._md_pct(r.get('is_win_rate'))} | "
            f"{fmoos._md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
            f"{fmoos._md_pp(r.get('oos_book_pct_continued'))} | "
            f"{fmoos._md_pp(r.get('fresh_book_pct'))} | "
            f"{fmoos._md_pct(r.get('oos_fee_h_wr'))} | "
            f"{'yes' if r.get('wr_only') else ''} | **{r.get('verdict')}** |"
        )
    pins = ", ".join(f"`{n}`" for n in (cont.get("post_0909_live_pins") or [])) or "none"
    clk_live = ", ".join(f"`{n}`" for n in (cont.get("clock_b_live_pins") or [])) or "none"
    lines += [
        "",
        "## Contamination vs today's full-sample board",
        "",
        f"Live board window **{cont.get('live_from') or '8/13'} → "
        f"{cont.get('live_to') or '9/18'}** "
        f"({cont.get('live_n_featured') or 0} featured pins). "
        "That pack saw 9/10–9/18 while ranking / pinning.",
        "",
        f"- **hot4 / `union_hot_n4_h1`:** Live featured: "
        f"{'yes' if cont.get('hot4_live_featured') else 'no'}. "
        f"IS starts {cont.get('hot4_is_starts') or '—'}.",
        f"- **holdup / `union_hot_n4_holdup`:** Still not in the 9/9 "
        f"seed set. Live featured: "
        f"{'yes' if cont.get('holdup_live_featured') else 'no'}.",
        f"- **Post-9/9 live pins:** {pins}.",
        f"- **Clock-B / oppset on the live featured strip:** {clk_live}.",
        "",
        "Recipe-definition freeze: "
        f"`{holdout.get('recipe_freeze') or fmbopp.RECIPE_FREEZE_SHA}`. "
        "Side paths only.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_board(holdout: dict, rows: list[dict], *,
                contamination: dict | None = None,
                contrast: dict | None = None,
                oos_start: str = OOS_START, oos_end: str = OOS_END,
                dest_md: Path | None = None,
                dest_json: Path | None = None) -> dict:
    dest_md = Path(dest_md or BOARD_MD)
    dest_json = Path(dest_json or BOARD_JSON)
    md = render_oos_md(
        holdout, rows, contamination=contamination, contrast=contrast,
        oos_start=oos_start, oos_end=oos_end)
    keep = [r["name"] for r in rows if r.get("verdict") == "KEEP"]
    blob = {
        "generated_at": holdout.get("generated_at"),
        "kind": "blind_0909_oppset_fee_aware_oos",
        "cutoff": CUTOFF,
        "oos_start": oos_start,
        "oos_end": oos_end,
        "fee_rt": FEE_RT,
        "recipe_freeze": holdout.get("recipe_freeze") or fmbopp.RECIPE_FREEZE_SHA,
        "cyrus_featured": list(holdout.get("cyrus_featured") or []),
        "formal_keep": list(holdout.get("formal_keep") or []),
        "keep": keep,
        "n_keep": len(keep),
        "n_fail": sum(1 for r in rows if r.get("verdict") != "KEEP"),
        "contamination": contamination or {},
        "contrast_286": contrast or {},
        "live_untouched": ["flatten_robust", "dashboard/factor-mine/",
                           "03_scoreboard/factor_mine.json"],
        "frozen_names": [r["name"] for r in rows],
        "rows": rows,
    }
    dest_md.parent.mkdir(parents=True, exist_ok=True)
    dest_json.parent.mkdir(parents=True, exist_ok=True)
    dest_md.write_text(md, encoding="utf-8")
    dest_json.write_text(json.dumps(blob, indent=2), encoding="utf-8")
    print(f"[blind-oppset-oos] wrote {dest_md}", flush=True)
    print(f"[blind-oppset-oos] wrote {dest_json}", flush=True)
    return blob


def run(*, holdout_path: Path | None = None, payload_path: Path | None = None,
        live_path: Path | None = None, dest_md: Path | None = None,
        dest_json: Path | None = None, panel: dict | None = None,
        bars=None, fees=None, regime=None, replay: bool = True,
        workers: int = 4) -> dict:
    holdout = fmoos.load_json(holdout_path or HOLDOUT_JSON)
    payload = fmoos.load_json(payload_path or IS_JSON)
    live = None
    live_p = Path(live_path or LIVE_JSON)
    if live_p.is_file():
        live = fmoos.load_json(live_p)
    no_oppset = None
    if NO_OPPSET_HOLDOUT.is_file():
        no_oppset = fmoos.load_json(NO_OPPSET_HOLDOUT)
    if panel is None and replay:
        # Reuse the in-memory aisle builder so OOS Clock-B/oppset
        # recipes see the same union as IS (OOS mornings included).
        _is_panel, panel, _aisle = fmbopp.prepare_panels(
            from_date=fmbld.FROM_DATE, to_date=CUTOFF, pull=False)
        del _is_panel, _aisle
    orig_should = fmoos.should_replay
    fmoos.should_replay = should_replay
    try:
        rows = fmoos.score_frozen(
            holdout, payload, panel or {},
            bars=bars, fees=fees, regime=regime, replay=replay,
            workers=workers)
    finally:
        fmoos.should_replay = orig_should
    contam = live_contamination(live, holdout)
    contra = contrast_286(holdout, no_oppset)
    return write_board(
        holdout, rows, contamination=contam, contrast=contra,
        dest_md=dest_md, dest_json=dest_json)


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--holdout", default=str(HOLDOUT_JSON))
    ap.add_argument("--payload", default=str(IS_JSON))
    ap.add_argument("--live", default=str(LIVE_JSON))
    ap.add_argument("--md", default=str(BOARD_MD))
    ap.add_argument("--json", dest="json_path", default=str(BOARD_JSON))
    ap.add_argument("--no-replay", action="store_true")
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args(argv)
    md = Path(args.md)
    js = Path(args.json_path)
    for p in (md, js):
        if p.resolve() in {fm.OUT_MD.resolve(), fm.OUT_JSON.resolve()}:
            raise SystemExit("refusing to write live factor-mine board")
    blob = run(
        holdout_path=Path(args.holdout),
        payload_path=Path(args.payload),
        live_path=Path(args.live),
        dest_md=md, dest_json=js,
        replay=not args.no_replay, workers=args.workers)
    print(f"[blind-oppset-oos] KEEP={blob['n_keep']} FAIL={blob['n_fail']}",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
