#!/usr/bin/env python3
"""Patch ab_backfill.py MD output: colored scores/labels, P01-P04 legend, sector footnotes."""
from pathlib import Path

p = Path("src/ab_backfill.py")
t = p.read_text(encoding="utf-8")
if "Sector source footnote" in t and "_label_colored" in t:
    print("already patched")
    raise SystemExit(0)

old_helpers = '''def _sec_dot(direction) -> str:
    if direction == "up":
        return "🟢"
    if direction == "down":
        return "🔴"
    return "⚪"


def _ensure_price_coverage'''

new_helpers = '''def _sec_dot(direction) -> str:
    if direction == "up":
        return "🟢"
    if direction == "down":
        return "🔴"
    return "⚪"


def _score_cell(score) -> str:
    try:
        s = int(score)
    except Exception:
        return "⚪—"
    if s > 0:
        return f"🟢 **{s:+d}**"
    if s < 0:
        return f"🔴 **{s:+d}**"
    return f"⚪ **{s:+d}**"


def _p_chip(val, pos_label: str, neg_label: str) -> str:
    try:
        v = int(val)
    except Exception:
        return "⚪"
    if v > 0:
        return f"🟢{pos_label}"
    if v < 0:
        return f"🔴{neg_label}"
    return "⚪"


def _label_colored(r) -> str:
    parts = [
        _p_chip(r.get("P01"), "LEAD", "LAG"),
        _p_chip(r.get("P02"), "peers↑", "peers↓"),
        _p_chip(r.get("P03"), "ind↑", "ind↓"),
        _p_chip(r.get("P04"), "sec↑", "sec↓"),
    ]
    return " ".join(parts)


def _ensure_price_coverage'''

if old_helpers not in t:
    raise SystemExit("helpers anchor missing — wrong base file")
t = t.replace(old_helpers, new_helpers, 1)

# Replace from "## Legend" construction through daily trail loop
start = t.find('        "## Legend",
')
if start < 0:
    start = t.find('        "## Legend"')
if start < 0:
    raise SystemExit("Legend section not found")

# Find the pattern audit attach after trail
marker = "    if len(out):\n        lines.append(\"\")\n        lines.extend(_pattern_audit(out))"
end = t.find(marker, start)
if end < 0:
    raise SystemExit("pattern audit marker not found")

replacement = r'''        "## Legend — scores",
        "",
        "| col | meaning |",
        "|-----|---------|",
        "| score | Sum of AB Part A + B1 feature flags **as of that day** |",
        "| ctx | Sum of context flags P01+P02+P03+P04 that day |",
        "| enr | score + ctx |",
        "| color on score/enr | 🟢 if >0, 🔴 if <0, ⚪ if 0 |",
        "| 1d 3d 1w 2m | forward max-upside vs |max-downside| from that day's close |",
        "",
        "## Legend — P01…P04 (context flags)",
        "",
        "| Flag | Name | +1 (🟢) | −1 (🔴) | Data source |",
        "|------|------|---------|---------|-------------|",
        "| **P01** | Peer lead / lag | stock 5d − peer-median 5d > 0 and beats ≥50% peers | lags peers | Correlations peers + price_store OHLC ≤ asof |",
        "| **P02** | Peers advancing | peer-basket median 5d > 0 | median 5d < 0 | same peer set |",
        "| **P03** | Industry advancing | industry median 5d > 0 | median 5d < 0 | Finviz Industry roster + price_store |",
        "| **P04** | Sector supportive | sector board Dir=up | Dir=down | nearest `01_daily/sectors/<board_date>/_BOARD.md` with board_date ≤ asof |",
        "",
        "Label chips: `🟢LEAD`/`🔴LAG` · `🟢peers↑`/`🔴peers↓` · `🟢ind↑`/`🔴ind↓` · `🟢sec↑`/`🔴sec↓` (⚪ = neutral/no data).",
        "",
    ]

    if len(out) and ticker:
        lines += [
            f"## {ticker} — daily trail (every session)",
            "",
            "| date | score | ctx | enr | context | rs5d | sec | board_date | 1d | 3d | 1w | 2m |",
            "|------|-------|----:|-----|---------|------|-----|------------|:--:|:--:|:--:|:--:|",
        ]
        for _, r in out.sort_values("asof_date").iterrows():
            rs = r.get("rs_5d")
            lines.append(
                f"| {r['asof_date']} | {_score_cell(r['score'])} | {int(r['score_context']):+d} | "
                f"{_score_cell(r['score_enriched'])} | {_label_colored(r)} | "
                f"{(f'{rs:+.1%}' if np.isfinite(rs) else '—')} | "
                f"{_sec_dot(r.get('sector_dir'))} | {r.get('sector_board_date') or '—'} | "
                f"{_dot(r.get('fav_1d'))} | {_dot(r.get('fav_3d'))} | "
                f"{_dot(r.get('fav_1w'))} | {_dot(r.get('fav_2m'))} |"
            )

        lines += [
            "",
            "### Sector source footnote (P04)",
            "",
            f"- Ticker **{ticker}** sector **`{sector_name or '—'}`**, industry **`{industry_name or '—'}`** "
            f"(from latest Finviz export roster).",
            "- Rule: each asof day uses the **latest** `board_date ≤ asof`. If none, sec=⚪ and P04=0.",
            "",
            "| board_date | file | sector row | Dir | Score |",
            "|------------|------|------------|-----|------:|",
        ]
        used = out.dropna(subset=["sector_board_date"]) if "sector_board_date" in out.columns else out.iloc[0:0]
        if len(used):
            seen = set()
            for _, r in used.sort_values("sector_board_date").iterrows():
                bd = r.get("sector_board_date")
                if not bd or bd in seen:
                    continue
                seen.add(bd)
                lines.append(
                    f"| {bd} | `01_daily/sectors/{bd}/_BOARD.md` | **{r.get('sector') or sector_name or '—'}** | "
                    f"{r.get('sector_dir') or '—'} | "
                    f"{r.get('sector_score') if np.isfinite(r.get('sector_score', float('nan'))) else '—'} |"
                )
        else:
            lines.append("| — | _(no sector board ≤ any asof)_ | — | — | — |")

        lines += ["", "All sector board files on disk at run time:"]
        if board_dates:
            for bd in board_dates:
                lines.append(f"- `01_daily/sectors/{bd}/_BOARD.md`")
        else:
            lines.append("- _(none found)_")

'''

# Need to include the beginning of lines list - the replacement starts mid-list
# Find the lines = [ block end of intro bullets before Legend
intro = t.rfind('f"- Industry:', 0, start)
if intro < 0:
    raise SystemExit('industry bullet not found')
# find end of that string line
line_end = t.find('\n', intro)
# after industry bullet there should be "", then ## Legend
# Keep everything before ## Legend, replace from ## Legend to pattern audit

new_t = t[:start] + replacement + "\n" + t[end:]
p.write_text(new_t, encoding="utf-8")
print("patched", p, "bytes", p.stat().st_size)
assert "_label_colored" in new_t
assert "Sector source footnote" in new_t
assert "P01" in new_t and "Peer lead" in new_t
'}, {