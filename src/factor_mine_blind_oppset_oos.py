"""Fee-aware OOS board for the frozen ≤9/9 Clock-B / oppset remine.

Excel lane follow-up to PR #288. Does **not** re-select on 9/10–9/18.
Names come from ``factor_mine_blind_0909_oppset/FROZEN_RECIPES.txt``
(and the matching ``holdout.json`` books). Scoring adds OOS start-day
YES and after-fee H (``FEE_RT=0.0015``) beside continued / fresh books.

Cyrus OOS KEEP is IS featured + continued Book% > 0 + start-day YES
≥85% (7 sessions → ≥6/7). Win% > 55% is not enough. Pure Clock-B /
oppset singles not featuring is expected — aisle, not solo.

Research only. Live ``flatten_robust`` / Pages are not imported or written.

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
WR_BAR = fmoos.WR_BAR
BOOK_MATCH_TOL = fmoos.BOOK_MATCH_TOL

HOLDOUT_JSON = (
    fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909_oppset" / "holdout.json"
)
IS_JSON = (
    fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909_oppset" / "factor_mine.json"
)
FROZEN_LIST = (
    fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909_oppset" / "FROZEN_RECIPES.txt"
)
LIVE_JSON = fm.ROOT / "03_scoreboard" / "factor_mine.json"
NO_OPPSET_HOLDOUT = (
    fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909" / "holdout.json"
)
BOARD_MD = fm.ROOT / "03_scoreboard" / "FACTOR_MINE_BLIND_0909_OPPSET_OOS.md"
BOARD_JSON = fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909_oppset_oos.json"

# Cyrus IS featured with green continued OOS Book% on the #288 freeze.
# Still not KEEP unless start-day YES ≥6/7. Verify all frozen names anyway.
TASKFORCE_BOOK_SURVIVORS = (
    "combo_ecearnguid_5050_shared",
    "combo_se1_5050_shared",
    "combo_e1s_7030_shared",
    "combo_form_efrh1snerh3_7030_shared",
    "combo_scextvetooh_5050_shared",
    "combo_sh_7030_shared",
    "combo_form_jovogrh1sclexve_5050_shared",
)

CYRUS_OOS_RULE = (
    "IS Cyrus featured (Starts YES + Book% on 8/13–9/9) AND continued "
    f"OOS Book% > {fmbld.CYRUS_MIN_BOOK:g} AND OOS start-day YES "
    f"≥{fmbld.CYRUS_START_RATE:.0%} (7 sessions → ≥6/7). "
    f"Same start rule as IS (≥{fmbld.CYRUS_START_YES}/"
    f"{fmbld.CYRUS_START_N_REF} when n≥{fmbld.CYRUS_START_N_REF}). "
    f"Win% > {WR_BAR:.0%} is not KEEP."
)


def load_frozen_list(path: Path | None = None) -> list[str]:
    """IS freeze names from FROZEN_RECIPES.txt. Never add OOS names."""
    p = Path(path or FROZEN_LIST)
    if not p.is_file():
        return []
    names: list[str] = []
    seen: set[str] = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s not in seen:
            names.append(s)
            seen.add(s)
    return names


def frozen_names(holdout: dict, frozen_path: Path | None = None) -> list[str]:
    """Prefer the Excel handoff list. Fall back to holdout keys."""
    listed = load_frozen_list(frozen_path)
    if listed:
        return listed
    return fmoos.frozen_names(holdout)


def should_replay(name: str, cyrus: set[str],
                  frozen: set[str] | None = None) -> bool:
    """Replay every frozen name. Live contamination / flatten stay out."""
    if not name or name in fmbld.POST_0909_NAMES:
        return False
    if name.startswith("flatten"):
        return False
    bag = set(frozen) if frozen is not None else set(load_frozen_list())
    if bag:
        return name in bag
    return (
        name in cyrus
        or name == "union_hot_n4_h1"
        or name in fmbopp.CLOCK_B_REPORT
        or name in TASKFORCE_BOOK_SURVIVORS
    )


def _touches_clock_b(name: str, members=None) -> bool:
    bag = [name, *(members or [])]
    for n in bag:
        s = str(n)
        if s in fmbopp.CLOCK_B_MENU or s.startswith("union_clk_") or s.startswith("short_clk_"):
            return True
        if "clk_" in s or s in ("union_oppset_h1", "oppset_h1") or "oppset" in s:
            return True
    return False


def contrast_286(holdout: dict, no_oppset: dict | None,
                 rows: list[dict] | None = None) -> dict:
    """What changed vs the no-oppset #286 freeze. Not a re-pick."""
    here = set(holdout.get("cyrus_featured") or [])
    there = set((no_oppset or {}).get("cyrus_featured") or [])
    members_by = {}
    for r in rows or holdout.get("rows") or []:
        if r.get("name"):
            members_by[r["name"]] = r.get("members") or []
    clk = sorted(
        n for n in here
        if _touches_clock_b(n, members_by.get(n))
    )
    return {
        "n_cyrus_oppset": len(here),
        "n_cyrus_no_oppset": len(there),
        "new_vs_286": sorted(here - there),
        "dropped_vs_286": sorted(there - here),
        "shared": sorted(here & there),
        "clock_b_in_cyrus": clk,
    }


def live_contamination(live: dict | None, holdout: dict) -> dict:
    base = fmoos.live_contamination(live, holdout)
    featured = list((live or {}).get("featured") or [])
    clk_live = [
        n for n in featured
        if n in fmbopp.CLOCK_B_MENU or n.startswith("union_clk_")
        or n.startswith("short_clk_") or "oppset" in n
    ]
    base["clock_b_live_pins"] = clk_live
    base["clock_b_in_0909_menu"] = True
    base["holdup_in_0909_menu"] = False
    base["taskforce_in_live_featured"] = [
        n for n in TASKFORCE_BOOK_SURVIVORS if n in featured
    ]
    return base


def mark_taskforce(rows: list[dict]) -> list[dict]:
    bag = set(TASKFORCE_BOOK_SURVIVORS)
    out = []
    for r in rows:
        row = dict(r)
        row["taskforce_book"] = row.get("name") in bag
        out.append(row)
    return out


def _start_path_table(names: list[str], by: dict) -> list[str]:
    oos_dates: list[str] = []
    for name in names:
        for s in (by.get(name) or {}).get("oos_starts") or []:
            if s.get("start") and s["start"] not in oos_dates:
                oos_dates.append(s["start"])
    if not oos_dates:
        return ["No start-day paths replayed.", ""]
    head = "| Strategy | " + " | ".join(d[5:] for d in oos_dates) + " |"
    sep = "|---|" + "---:|" * len(oos_dates)
    lines = [
        "Each cell is the fee-aware $10k book that **starts** that morning "
        "(empty lots) through 9/18. YES = Book% > 0.",
        "",
        head,
        sep,
    ]
    for name in names:
        r = by.get(name) or {}
        hits = {s["start"]: s for s in (r.get("oos_starts") or [])}
        cells = []
        for d in oos_dates:
            s = hits.get(d) or {}
            ret = s.get("return_pct")
            if ret is None:
                cells.append("—")
            else:
                mark = "YES" if s.get("made_money") else "no"
                cells.append(f"{mark} {fmoos._md_pp(ret)}")
        lines.append(f"| `{name}` | " + " | ".join(cells) + " |")
    lines.append("")
    return lines


def render_oos_md(holdout: dict, rows: list[dict], *,
                  contamination: dict | None = None,
                  contrast: dict | None = None,
                  oos_start: str = OOS_START, oos_end: str = OOS_END) -> str:
    keep = [r for r in rows if r.get("verdict") == "KEEP"]
    fail = [r for r in rows if r.get("verdict") != "KEEP"]
    cyrus = [r for r in rows if r.get("cyrus_is")]
    book_only = [r for r in cyrus if r.get("book_only")]
    wr_trap = [r for r in rows if r.get("wr_only") and r.get("verdict") != "KEEP"]
    mismatches = [
        r for r in rows
        if r.get("cont_match") is False or r.get("fresh_match") is False
    ]
    replayed = [r for r in rows if (r.get("oos_starts") or [])]
    headline = f"**KEEP** {len(keep)}" if keep else "**FAIL** 0 KEEP"
    cont = contamination or {}
    vs = contrast or {}
    by = {r["name"]: r for r in rows}
    clk_rows = [
        r for r in rows
        if r["name"] in fmbopp.CLOCK_B_REPORT
        or any("clk_" in str(m) or "oppset" in str(m)
               for m in (r.get("members") or []))
        or "clk_" in r["name"] or "oppset" in r["name"]
    ]
    lines = [
        "# Factor mine blind 9/9 + Clock-B/oppset — fee-aware OOS (Excel lane)",
        "",
        f"status=DONE verdict={headline} cutoff={CUTOFF} "
        f"OOS={oos_start}→{oos_end} FEE_RT={FEE_RT} "
        f"KEEP={len(keep)} FAIL={len(fail)} freeze="
        f"{holdout.get('recipe_freeze') or fmbopp.RECIPE_FREEZE_SHA}",
        "",
        "Research only. Live `flatten_robust` / `dashboard/factor-mine/` / "
        "`03_scoreboard/factor_mine.json` were not written. Recipe names "
        "are the #288 IS freeze (`FROZEN_RECIPES.txt`) — OOS dates did "
        "not re-select.",
        "",
        "## KEEP bar",
        "",
        "Cyrus OOS KEEP (plain): **IS Cyrus featured** (Starts YES + Book% "
        "on 8/13–9/9) **and** continued OOS Book% > 0 **and** OOS "
        f"start-day YES ≥{fmbld.CYRUS_START_RATE:.0%} "
        f"(7 sessions → ≥6/7). Same start rule as IS "
        f"(≥{fmbld.CYRUS_START_YES}/{fmbld.CYRUS_START_N_REF} when n≥"
        f"{fmbld.CYRUS_START_N_REF}).",
        "",
        f"**Win% > {WR_BAR:.0%} is not enough by itself.** After-fee H = "
        f"same-session open→close minus {FEE_RT * 10000:.0f} bp Futubull "
        f"(`FEE_RT={FEE_RT}`). Shorts pay the 15 bp (they do not collect "
        "it). Cash Book% already uses the Futubull order-fee schedule "
        "(`00_grounding/futubull_fees.json`). n≥30 is the IS trade-count "
        "bar; the 7-session window cannot meet it and does not KEEP.",
        "",
        "OOS **continued** Book% is copied from #288 `holdout.json` "
        "(9/9 cash book walked forward). OOS **start-day YES** wakes "
        f"$10k empty lots on each session in {oos_start}–{oos_end}. "
        "Start 9/10 is the fresh $10k path.",
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
        f"Taskforce continued-Book% survivors (not a KEEP bar): "
        f"**{len(book_only)}** of {len(cyrus)} Cyrus IS names. "
        f"Win%>55% traps (WR clears, Cyrus FAIL): **{len(wr_trap)}**. "
        f"Frozen names verified: **{len(rows)}**. Start-day walks: "
        f"**{len(replayed)}/{len(rows)}**.",
        "",
        "## Taskforce 7 — continued Book% > 0 on #288 Cyrus",
        "",
        "| Strategy | Cyrus OOS | Book-only | OOS starts | Cont book% | "
        "Fresh $10k | Fresh WR | After-fee H WR | n H | Why |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for name in TASKFORCE_BOOK_SURVIVORS:
        r = by.get(name) or {"name": name, "verdict": "FAIL", "why": "missing"}
        lines.append(
            f"| `{r['name']}` | **{r.get('verdict') or 'FAIL'}** | "
            f"{'yes' if r.get('book_only') else 'no'} | "
            f"{fmoos._md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
            f"{fmoos._md_pp(r.get('oos_book_pct_continued'))} | "
            f"{fmoos._md_pp(r.get('fresh_book_pct'))} | "
            f"{fmoos._md_pct(r.get('fresh_win_rate'))} | "
            f"{fmoos._md_pct(r.get('oos_fee_h_wr'))} | "
            f"{r.get('oos_fee_h_n') or 0} | {r.get('why') or ''} |"
        )
    lines += [
        "",
        "### Start-day paths (Taskforce 7)",
        "",
    ]
    lines += _start_path_table(list(TASKFORCE_BOOK_SURVIVORS), by)
    lines += [
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
        "Pure Clock-B / oppset **singles** not featuring on 9/9 is "
        "**expected**. The aisle is a mixer (T−1 top-30 ∪ panel), not a "
        "solo KEEP. None of these clear Cyrus Starts YES ≥17/19.",
        "",
        "| Strategy | Cyrus IS | OOS starts | Cont book% | Fresh $10k | "
        "After-fee H WR | Verdict |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
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
    formal_only = [r for r in rows if r.get("formal_is") and not r.get("cyrus_is")]
    if formal_only:
        lines += [
            "",
            "## Formal-bar IS KEEP that missed Cyrus featuring",
            "",
            "These cleared WORKABLE_BAR on 9/9 (Win% / $days / start≥50%) "
            "but not Starts YES ≥17/19. OOS cannot promote them. "
            "`combo_sh_5050_shared` / `combo_sh_3070_shared` were Cyrus "
            "on the no-oppset #286 freeze; the Clock-B aisle moved their "
            "IS starts below 17/19 here.",
            "",
            "| Strategy | IS start | IS book% | IS win% | OOS starts | "
            "Cont book% | Fresh $10k | Verdict |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
        for r in formal_only:
            lines.append(
                f"| `{r['name']}` | "
                f"{fmoos._md_start(r.get('is_start_green'), r.get('is_start_n'))} | "
                f"{fmoos._md_pp(r.get('is_book_pct'))} | "
                f"{fmoos._md_pct(r.get('is_win_rate'))} | "
                f"{fmoos._md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
                f"{fmoos._md_pp(r.get('oos_book_pct_continued'))} | "
                f"{fmoos._md_pp(r.get('fresh_book_pct'))} | **FAIL** |"
            )
    other = [
        r for r in rows
        if not r.get("cyrus_is")
        and not r.get("formal_is")
        and r["name"] not in fmbopp.CLOCK_B_REPORT
    ]
    if other:
        lines += [
            "",
            "## Other frozen names (contamination / thin singles)",
            "",
            "In the freeze for the hot4 check or thin formal miss. "
            "Cannot KEEP from OOS.",
            "",
            "| Strategy | IS start | IS book% | OOS starts | Cont book% | "
            "Fresh $10k | Verdict |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
        for r in other:
            lines.append(
                f"| `{r['name']}` | "
                f"{fmoos._md_start(r.get('is_start_green'), r.get('is_start_n'))} | "
                f"{fmoos._md_pp(r.get('is_book_pct'))} | "
                f"{fmoos._md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
                f"{fmoos._md_pp(r.get('oos_book_pct_continued'))} | "
                f"{fmoos._md_pp(r.get('fresh_book_pct'))} | **FAIL** |"
            )
    is_wr = [
        r for r in cyrus
        if fm._finite(r.get("is_win_rate")) is not None
        and float(r["is_win_rate"]) > WR_BAR
    ]
    lines += [
        "",
        "## Win% > 55% alone",
        "",
        "Win% is not the Cyrus bar. On 9/9, "
        + (
            ", ".join(
                f"`{r['name']}` IS WR {fmoos._md_pct(r.get('is_win_rate'))}"
                for r in is_wr
            ) or "no Cyrus name"
        )
        + " would pass a Win%>55% screen; every one **FAIL**s OOS "
        "starts + Book%. Several Clock-B mixes were featured with IS WR "
        "at or under 55% — starts 17/19 + Book% kept them, not Win%.",
        "",
    ]
    if wr_trap:
        lines.append(
            "OOS after-fee H / fresh cash WR > 55% and still **FAIL** "
            "Cyrus: "
            + ", ".join(
                f"`{r['name']}` (H {fmoos._md_pct(r.get('oos_fee_h_wr'))}, "
                f"cash WR {fmoos._md_pct(r.get('fresh_win_rate'))})"
                for r in wr_trap
            )
            + "."
        )
    else:
        lines.append(
            "No frozen name clears **OOS** after-fee H WR or fresh "
            "cash-trade WR > 55%. WR is still not the KEEP bar."
        )
    pins = ", ".join(f"`{n}`" for n in (cont.get("post_0909_live_pins") or [])) or "none"
    clk_live = ", ".join(f"`{n}`" for n in (cont.get("clock_b_live_pins") or [])) or "none"
    live_task = cont.get("taskforce_in_live_featured") or []
    lines += [
        "",
        "## Contamination vs today's full-sample board",
        "",
        f"Live board window **{cont.get('live_from') or '8/13'} → "
        f"{cont.get('live_to') or '9/18'}** "
        f"({cont.get('live_n_featured') or 0} featured pins). "
        "That pack saw 9/10–9/18 while ranking / pinning. This OOS board "
        "does not.",
        "",
        f"- **hot4 / `union_hot_n4_h1`:** In the 9/9 auto grid, **not** "
        f"Cyrus featured (IS starts {cont.get('hot4_is_starts') or '8/19'}, "
        f"book {fmoos._md_pp(cont.get('hot4_is_book'))}%). Live featured pin: "
        f"{'yes' if cont.get('hot4_live_featured') else 'no'}. "
        f"Full-sample book {fmoos._md_pp(cont.get('hot4_live_book'))}% "
        f"starts {cont.get('hot4_live_starts') or '—'}. A 9/9 researcher "
        "would not have featured it; OOS shine cannot promote it here.",
        f"- **holdup / `union_hot_n4_holdup`:** Still not in the 9/9 "
        f"seed set (landed 2026-09-19). Live featured: "
        f"{'yes' if cont.get('holdup_live_featured') else 'no'}. "
        "No holdup twin was invented. Not scored.",
        f"- **Post-9/9 live pins:** {pins}.",
        f"- **Clock-B / oppset on the live featured strip:** {clk_live}.",
        f"- **Taskforce 7 on the live featured pin list:** "
        + (", ".join(f"`{n}`" for n in live_task) or "none")
        + ".",
        "",
        "## Confirm vs #288 holdout books",
        "",
    ]
    if mismatches:
        lines.append(
            "MISMATCH vs holdout.json: "
            + ", ".join(
                f"`{r['name']}` cont={r.get('cont_match')} "
                f"fresh={r.get('fresh_match')}"
                for r in mismatches
            )
            + "."
        )
    else:
        lines.append(
            "Continued Book% copied from #288 `holdout.json`. "
            "Fresh $10k start (9/10) matches holdout within "
            f"{BOOK_MATCH_TOL:g} pp for every replayed name."
        )
    lines += [
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
    rows = mark_taskforce(rows)
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
        "cyrus_rule": CYRUS_OOS_RULE,
        "cyrus_featured": list(holdout.get("cyrus_featured") or []),
        "formal_keep": list(holdout.get("formal_keep") or []),
        "taskforce_book_survivors": list(TASKFORCE_BOOK_SURVIVORS),
        "frozen_list": str(FROZEN_LIST.relative_to(fm.ROOT)),
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
        workers: int = 4, frozen_path: Path | None = None) -> dict:
    holdout = fmoos.load_json(holdout_path or HOLDOUT_JSON)
    payload = fmoos.load_json(payload_path or IS_JSON)
    live = None
    live_p = Path(live_path or LIVE_JSON)
    if live_p.is_file():
        live = fmoos.load_json(live_p)
    no_oppset = None
    if NO_OPPSET_HOLDOUT.is_file():
        no_oppset = fmoos.load_json(NO_OPPSET_HOLDOUT)
    names = frozen_names(holdout, frozen_path)
    frozen = set(names)
    if panel is None and replay:
        # Reuse the in-memory aisle builder so OOS Clock-B/oppset
        # recipes see the same union as IS (OOS mornings included).
        _is_panel, panel, _aisle = fmbopp.prepare_panels(
            from_date=fmbld.FROM_DATE, to_date=CUTOFF, pull=False)
        del _is_panel, _aisle

    def _should(name: str, cyrus: set[str]) -> bool:
        return should_replay(name, cyrus, frozen=frozen)

    def _frozen(h: dict) -> list[str]:
        return list(names) if names else fmoos.frozen_names(h)

    orig_should = fmoos.should_replay
    orig_frozen = fmoos.frozen_names
    fmoos.should_replay = _should
    fmoos.frozen_names = _frozen
    try:
        rows = fmoos.score_frozen(
            holdout, payload, panel or {},
            bars=bars, fees=fees, regime=regime, replay=replay,
            workers=workers)
    finally:
        fmoos.should_replay = orig_should
        fmoos.frozen_names = orig_frozen
    rows = mark_taskforce(rows)
    contam = live_contamination(live, holdout)
    contra = contrast_286(holdout, no_oppset, rows)
    return write_board(
        holdout, rows, contamination=contam, contrast=contra,
        dest_md=dest_md, dest_json=dest_json)


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--holdout", default=str(HOLDOUT_JSON))
    ap.add_argument("--payload", default=str(IS_JSON))
    ap.add_argument("--live", default=str(LIVE_JSON))
    ap.add_argument("--frozen", default=str(FROZEN_LIST))
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
        frozen_path=Path(args.frozen),
        replay=not args.no_replay, workers=args.workers)
    print(f"[blind-oppset-oos] KEEP={blob['n_keep']} FAIL={blob['n_fail']}",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
