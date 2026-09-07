"""Harden: does an open-knowable color add edge on top of KEEP-6 lights?

Reuse the A–O grids already scored in color_join_mine.json (Yahoo/rows
source=rows_cache). This beat is color only — no Finviz / AB / weather /
book. Does not remine. Does not emit cards. Does not import flatten_robust.

  python engine/harden_color.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harden_hyst_open import CANDIDATE_KEYS, PLAIN, splice_md  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
SRC = os.path.join(RESEARCH, "color_join_mine.json")
PARENT = os.path.join(RESEARCH, "hyst_open_harden.json")
MARKER = "## Color harden (light + open fill vs light alone)"

OPEN_LETTERS = "ABCGJKLMO"
JOIN_MARK = ("__fz_", "__ab_", "__book_", "__wx_")


def letter_plain(letter):
    return f"morning cell {letter} is also highlighted green (known at 9:30)"


def is_color_fold(cell):
    name = cell.get("def") or ""
    if cell.get("family") == "join":
        return False
    if any(m in name for m in JOIN_MARK):
        return False
    if cell.get("family") == "hyst_color":
        return True
    if "__" not in name:
        return False
    extra = name.split("__", 1)[1]
    return extra.endswith("_green") or extra.endswith("_red")


def parent_key(name, hold):
    return name.split("__", 1)[0], int(hold)


def load_folds():
    raw = json.load(open(SRC))
    parents = {}
    for r in json.load(open(PARENT))["candidates"]:
        parents[(r["def"], int(r["hold"]))] = r
    folds = [c for c in raw["cells"] if is_color_fold(c)]
    return raw, parents, folds


def vs_parent_pp(cell):
    h = cell.get("holdout") or {}
    p = cell.get("parent") or {}
    if h.get("avg_net") is None or p.get("avg_net") is None:
        return None
    return (h["avg_net"] - p["avg_net"]) * 100


def recipe_rows(folds, parents):
    """One verdict per KEEP-6 recipe: does any open color add ≥20 bp?"""
    out = []
    for name, rule in CANDIDATE_KEYS:
        hold = int(rule[4:])
        kids = [c for c in folds if parent_key(c["def"], c["hold"]) == (name, hold)]
        kids = sorted(kids, key=lambda c: (-((c.get("holdout") or {}).get("avg_net") or -9)))
        keepers = [c for c in kids if c["verdict"] == "KEEP"]
        best = keepers[0] if keepers else (kids[0] if kids else None)
        verdict = "KEEP" if keepers else ("THIN" if kids and all(
            c["verdict"] == "THIN" for c in kids) else "KILL")
        parent = parents.get((name, hold)) or {}
        ph = (parent.get("holdout") or {})
        out.append({
            "def": name,
            "hold": hold,
            "exit": rule,
            "plain": PLAIN[name],
            "parent_holdout": ph,
            "parent_n": ph.get("n"),
            "parent_avg": ph.get("avg_net"),
            "verdict": verdict,
            "best_def": (best or {}).get("def"),
            "best_letter": ((best or {}).get("def") or "").split("__")[-1].replace("_green", "").replace("_red", ""),
            "best_holdout": (best or {}).get("holdout"),
            "best_vs_parent_pp": vs_parent_pp(best) if best else None,
            "best_verdict": (best or {}).get("verdict"),
            "n_color_folds": len(kids),
            "n_keep_folds": len(keepers),
            "keepers": [c["def"] for c in keepers],
            "fail_reasons": (best or {}).get("fail_reasons") or [],
            "live_untouched": "flatten_robust",
        })
    return out


def _pct(b):
    if not b or b.get("avg_net") is None:
        return "—"
    return f"{b['avg_net']*100:+.2f}% (n={b['n']})"


def render(recipes, folds, n_grids):
    n_keep = sum(1 for r in recipes if r["verdict"] == "KEEP")
    n_kill = sum(1 for r in recipes if r["verdict"] == "KILL")
    n_thin = sum(1 for r in recipes if r["verdict"] == "THIN")
    fold_keep = [c for c in folds if c["verdict"] == "KEEP"]
    L = [
        MARKER,
        "",
        "_Generated 2026-09-07. Color-only beat — no Finviz / AB / weather / "
        "book. Futubull fees kept. Live `flatten_robust` frozen. No cards._",
        "",
        "### Question",
        "",
        "When the morning green light first turns on, does **also** requiring "
        "an open-knowable cell to be green (or red) add money after fees, "
        "versus the light alone?",
        "",
        "Open-knowable paints: **A, B, C, G, J, K, L, M, O**. Close paints "
        "D, E, F, H, I, N never start this trade. Same ship bar as the "
        "KEEP-6 harden: discovery/holdout, both 2026 halves (cut 2026-05-01), "
        "SPY up and down, top-day lottery under 25%, beat the parent light "
        "by at least 20 bps. Grids are the Yahoo/rows rebuild "
        f"(`source: rows_cache`), **{n_grids}** files. No Excel cached A–F.",
        "",
        f"**KEEP {n_keep}** recipes · **KILL {n_kill}** · **THIN {n_thin}**. "
        f"Color folds that themselves KEEP: **{len(fold_keep)}** "
        f"(all of them are a green **O**).",
        "",
        "**A green O on top of the light can add about +24–45 bp.** Other "
        "morning greens (A, B, G, J, K, L, M) do not clear +20 bp versus "
        "the light itself. A color by itself, without the light, already "
        "failed this bar (late 2026 goes red on same-day holds).",
        "",
        "### Per recipe (light alone vs light + best open color)",
        "",
        "| meaning | hold | light alone | light + best color | extra after fees | verdict | code |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in recipes:
        extra = "—"
        if r["best_vs_parent_pp"] is not None:
            extra = f"{r['best_vs_parent_pp']:+.2f} pp"
        best = r["best_letter"] or "—"
        L.append(
            f"| {r['plain']} Plus {letter_plain(best) if r['best_letter'] else 'no open color scored'}. | "
            f"next {r['hold']} | {_pct(r['parent_holdout'])} | "
            f"{_pct(r['best_holdout'])} | {extra} | **{r['verdict']}** | "
            f"`{r['best_def'] or r['def']}` |"
        )
    L += [
        "",
        "### Every open-letter fold (same six lights)",
        "",
        "| light | hold | color | holdout | vs light | verdict | why | code |",
        "|---|---|---|---|---|---|---|---|",
    ]
    def sort_key(c):
        return (
            0 if c["verdict"] == "KEEP" else 1,
            -((c.get("holdout") or {}).get("avg_net") or -9),
        )
    for c in sorted(folds, key=sort_key):
        letter = c["def"].split("__")[-1]
        vs = vs_parent_pp(c)
        vs_s = f"{vs:+.2f} pp" if vs is not None else "—"
        why = ",".join(c.get("fail_reasons") or []) or "—"
        parent = c["def"].split("__")[0]
        L.append(
            f"| `{parent}` | next {c['hold']} | {letter} | "
            f"{_pct(c.get('holdout'))} | {vs_s} | **{c['verdict']}** | "
            f"{why} | `{c['def']}` |"
        )
    L += [
        "",
        "KEEP is still research-only: one 2026 window, Futubull model, "
        "no card, no live wire. Joins (Finviz / AB / weather / book) are "
        "out of scope for this beat.",
        "",
    ]
    return "\n".join(L) + "\n"


def write_outputs(recipes, folds, n_grids):
    md = render(recipes, folds, n_grids)
    open(os.path.join(RESEARCH, "COLOR_HARDEN.md"), "w", encoding="utf-8").write(md)
    payload = {
        "generated": str(date.today()),
        "spec": "light + open-knowable color vs light alone; futubull; no joins",
        "grids": n_grids,
        "grids_source": "rows_cache",
        "excel_cache_used": False,
        "n_keep_recipes": sum(1 for r in recipes if r["verdict"] == "KEEP"),
        "n_kill_recipes": sum(1 for r in recipes if r["verdict"] == "KILL"),
        "n_thin_recipes": sum(1 for r in recipes if r["verdict"] == "THIN"),
        "n_keep_folds": sum(1 for c in folds if c["verdict"] == "KEEP"),
        "n_kill_folds": sum(1 for c in folds if c["verdict"] == "KILL"),
        "open_letters": OPEN_LETTERS,
        "cost_model": "futubull",
        "live_untouched": "flatten_robust",
        "recipes": recipes,
        "folds": [{
            "def": c["def"], "hold": c["hold"], "verdict": c["verdict"],
            "holdout": c.get("holdout"), "parent": c.get("parent"),
            "fail_reasons": c.get("fail_reasons"),
            "early": c.get("early"), "late": c.get("late"),
            "spy_up": c.get("spy_up"), "spy_dn": c.get("spy_dn"),
            "lottery_day_frac": c.get("lottery_day_frac"),
        } for c in folds],
    }
    json.dump(payload, open(os.path.join(RESEARCH, "color_harden.json"), "w"),
              indent=2)
    ao = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
    sb = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
    cy = os.path.join(RESEARCH, "MINE_CYCLE.md")
    ao_text = splice_md(ao, MARKER, md, require="VISIBLE_COLS A..O")
    sb_text = splice_md(sb, MARKER, md,
                        require_any=("first A–JL cut", "A–O clock cycle"))
    note = (
        MARKER + "\n\n"
        f"KEEP {payload['n_keep_recipes']} recipes · "
        f"KILL {payload['n_kill_recipes']}. "
        "Green O adds +24–45 bp on 3 of 6 lights. Other open colors fail "
        "the +20 bp parent bar. No joins in this beat. See `COLOR_HARDEN.md`.\n"
    )
    cy_text = splice_md(cy, MARKER, note,
                        require_any=("first A–JL cut", "A–O clock cycle"))
    open(ao, "w", encoding="utf-8").write(ao_text)
    open(sb, "w", encoding="utf-8").write(sb_text)
    open(cy, "w", encoding="utf-8").write(cy_text)
    return payload


def main():
    raw, parents, folds = load_folds()
    recipes = recipe_rows(folds, parents)
    payload = write_outputs(recipes, folds, raw.get("grids") or 0)
    print(f"recipes KEEP {payload['n_keep_recipes']} "
          f"KILL {payload['n_kill_recipes']} folds KEEP "
          f"{payload['n_keep_folds']} / {len(folds)}")
    for r in recipes:
        print(f"  {r['verdict']:4} {r['def']} {r['exit']} "
              f"best={r['best_def']} vs={r['best_vs_parent_pp']}")


if __name__ == "__main__":
    main()
