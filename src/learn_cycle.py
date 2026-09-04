"""Closed learning cycle across ALL workflows.

Sources mined:
  - 03_scoreboard/scoreboard.json     general + every sector:* topic
  - 03_scoreboard/news_actions_scoreboard.json
  - 02_lessons/candidate/*            general + sector_* lessons

Writes:
  - 02_lessons/hypotheses/<scope>_*.md
  - 02_lessons/active/ (promote complete candidates)
  - 00_grounding/mutable_policy.md
  - 00_grounding/weather_rules_proposals.json
  - 03_scoreboard/LEARNINGS.md              (human digest, always latest)
  - 01_daily/<date>_learnings.md            (dated copy)

CLI: python -m src.learn_cycle [--lookback 15]
"""
from __future__ import annotations

import argparse
import glob
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, scoreboard

ROOT = Path(__file__).resolve().parent.parent
MUTABLE = ROOT / "00_grounding" / "mutable_policy.md"
PROPOSALS = ROOT / "00_grounding" / "weather_rules_proposals.json"
HYPO_DIR = ROOT / "02_lessons" / "hypotheses"
CAND_DIR = Path(config.LESSONS_CANDIDATE)
ACTIVE_DIR = Path(config.LESSONS_ACTIVE)
NEWS_SB = ROOT / "03_scoreboard" / "news_actions_scoreboard.json"
LEARNINGS = ROOT / "03_scoreboard" / "LEARNINGS.md"
DAILY = ROOT / "01_daily"


def _read(p: Path | str) -> str:
    try:
        return Path(p).read_text(encoding="utf-8")
    except OSError:
        return ""


def _all_graded(lookback_per_topic: int = 15) -> list[dict]:
    board = scoreboard.load()
    by_topic: dict[str, list] = defaultdict(list)
    for r in board.get("runs", []):
        if r.get("direction_hit") is None:
            continue
        if r.get("ops_fail"):
            continue
        if not r.get("predicted_direction"):
            continue
        topic = r.get("topic") or "general"
        by_topic[topic].append(r)
    out = []
    for topic, runs in by_topic.items():
        runs = sorted(runs, key=lambda x: x.get("date", ""))
        out.extend(runs[-lookback_per_topic:])
    return out


def _scope_of(run: dict) -> str:
    t = run.get("topic") or "general"
    if t == "general":
        return "general"
    if t.startswith("sector:"):
        return "sector_" + t.split(":", 1)[1].lower().replace(" ", "_")
    return re.sub(r"[^a-z0-9]+", "_", t.lower())


def _hypotheses_from_runs(runs: list[dict]) -> list[dict]:
    hypos = []
    for r in runs:
        d = r.get("date")
        scope = _scope_of(r)
        hit = r.get("direction_hit") is True
        pred = r.get("predicted_direction")
        act = r.get("actual_direction")
        pct = r.get("actual_pct_change")
        score = r.get("total_score")
        sector = r.get("sector") or ("" if scope == "general" else scope)

        if hit:
            hypos.append({
                "date": d,
                "scope": scope,
                "kind": "win",
                "title": f"{scope}_win_{d}_{pred}",
                "when": (
                    f"[{scope}] Predicted {pred}, market/sector went {act} "
                    f"(pct={pct}, score={score}, sector={sector})."
                ),
                "ask": (
                    "Could magnitude/conviction have been better? "
                    "Double-count in factors? Missing confirming source?"
                ),
                "experiment": (
                    f"[{scope}] On similar setups, test milder bands when |score|<4; "
                    "log whether lagging tape factors overrode leading ones."
                ),
                "do_instead": (
                    f"[{scope}] Keep direction; shrink confidence on modest |score| "
                    "when magnitude historically misses."
                ),
                "wrong_if": (
                    f"[{scope}] Wrong if milder bands hurt direction accuracy over 10 runs."
                ),
            })
        else:
            hypos.append({
                "date": d,
                "scope": scope,
                "kind": "loss",
                "title": f"{scope}_loss_{d}_{pred}_vs_{act}",
                "when": (
                    f"[{scope}] Predicted {pred} but went {act} "
                    f"(pct={pct}, score={score}, sector={sector})."
                ),
                "ask": (
                    "Dominant factor family? Regime misread vs sector-specific shock? "
                    "Shared macro S0 wrong or sector factors S1 wrong?"
                ),
                "experiment": (
                    f"[{scope}] Require one extra confirming source in the dominant "
                    "bucket before full weight when score sign matches this fail pattern."
                ),
                "do_instead": (
                    f"[{scope}] When score sign conflicts with sector ETF tape / breadth, "
                    "cut conviction; prefer flat/mild."
                ),
                "wrong_if": (
                    f"[{scope}] Wrong if this hedge reduces direction accuracy over 10 runs."
                ),
            })
    return hypos


def _hypotheses_from_news() -> list[dict]:
    if not NEWS_SB.exists():
        return []
    try:
        data = json.loads(NEWS_SB.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    hypos = []
    summary = data.get("summary") or {}
    close_1d = summary.get("close_1d") or {}
    suggestions = data.get("suggestions") or []

    by_event: dict[str, list] = defaultdict(list)
    for s in suggestions:
        events = s.get("events") or ["unknown"]
        if isinstance(events, str):
            events = [events]
        for ev in events:
            by_event[str(ev)].append(s)

    for ev, rows in sorted(by_event.items(), key=lambda x: -len(x[1])):
        n = len(rows)
        rets = []
        for s in rows:
            r = s.get("ret_1d")
            if r is None:
                r = s.get("now_pct") or s.get("now%")
            if r is not None:
                try:
                    rets.append(float(r))
                except (TypeError, ValueError):
                    pass
        avg = sum(rets) / len(rets) if rets else None
        buys = [s for s in rows if str(s.get("side", "")).lower() == "buy"]

        if avg is not None and avg < 0 and len(buys) >= 3:
            hypos.append({
                "date": "news",
                "scope": "news",
                "kind": "loss",
                "title": f"news_event_{ev}_buy_drag",
                "when": (
                    f"[news] Event family '{ev}' buy book avg ret~{avg:.2f}% "
                    f"(n_buys={len(buys)}, n_all={n})."
                ),
                "ask": "Is the edge inverted, overfit to one headline day, or wrong tickers?",
                "experiment": (
                    f"[news] Next '{ev}' signal: lower buy weight or require second "
                    f"confirming headline family before mapping to tickers."
                ),
                "do_instead": (
                    f"[news] Treat '{ev}' buys as lower conviction until 1d close win rate >55%."
                ),
                "wrong_if": (
                    f"[news] Wrong if '{ev}' buys start beating 55% on 1d close for 2 weeks."
                ),
            })
        elif avg is not None and avg > 1.0 and len(rows) >= 5:
            hypos.append({
                "date": "news",
                "scope": "news",
                "kind": "win",
                "title": f"news_event_{ev}_working",
                "when": (
                    f"[news] Event family '{ev}' working avg ret~{avg:.2f}% (n={n})."
                ),
                "ask": "Can we size up without spreading into unrelated tickers?",
                "experiment": (
                    f"[news] Keep '{ev}' mapping; avoid diluting into low-liquidity names."
                ),
                "do_instead": (
                    f"[news] Prefer liquid primary bucket for '{ev}' only."
                ),
                "wrong_if": (
                    f"[news] Wrong if expanding ticker list improves risk-adjusted results."
                ),
            })

    wr = close_1d.get("win_rate")
    if wr is not None and wr < 55 and close_1d.get("n", 0) >= 20:
        hypos.append({
            "date": "news",
            "scope": "news",
            "kind": "loss",
            "title": "news_global_1d_weak",
            "when": f"[news] Global 1d close win rate {wr}% (n={close_1d.get('n')}).",
            "ask": "Entry timing, side mix, or event taxonomy noise?",
            "experiment": "[news] Raise min net weight to map a ticker; drop weak edges.",
            "do_instead": "[news] Only emit actions with |net| above a higher floor.",
            "wrong_if": "[news] Wrong if higher floor reduces 1d win rate further.",
        })

    if not hypos and summary:
        hypos.append({
            "date": "news",
            "scope": "news",
            "kind": "win",
            "title": "news_summary_pulse",
            "when": f"[news] summary={json.dumps(summary)[:300]}",
            "ask": "Which event families drive ever-profitable vs 1d close?",
            "experiment": "[news] Track event-level 1d close win rate daily in learn_cycle.",
            "do_instead": "[news] Rank event families by 1d close, not ever-touch MFE.",
            "wrong_if": "[news] Wrong if ever-touch is the better trading objective for you.",
        })
    return hypos


def _write_hypotheses(hypos: list[dict]) -> list[Path]:
    HYPO_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for h in hypos:
        p = HYPO_DIR / f"{h['title']}.md"
        body = (
            f"---\n"
            f"kind: {h['kind']}\n"
            f"scope: {h['scope']}\n"
            f"date: {h['date']}\n"
            f"status: open\n"
            f"---\n\n"
            f"# Hypothesis — {h['scope']} / {h['kind'].upper()} {h['date']}\n\n"
            f"## WHEN\n{h['when']}\n\n"
            f"## ASK (counterfactual)\n{h['ask']}\n\n"
            f"## EXPERIMENT\n{h['experiment']}\n\n"
            f"## DO INSTEAD (policy candidate)\n{h['do_instead']}\n\n"
            f"## WRONG IF (falsifier)\n{h['wrong_if']}\n"
        )
        p.write_text(body, encoding="utf-8")
        paths.append(p)
    return paths


def _promote_complete_candidates(min_market: int = 1) -> list[str]:
    from .promote_lessons import _parse_candidate, _write_active, _cluster

    paths = sorted(glob.glob(str(CAND_DIR / "*.md")))
    cands = [_parse_candidate(p) for p in paths]
    cands = [
        c for c in cands
        if c.get("status", "candidate") in ("candidate", "candidate_incomplete")
        and c.get("_complete")
    ]
    promoted = []
    for cl in _cluster(cands):
        complete = [c for c in cl if c["_complete"]]
        if not complete:
            continue
        cat = complete[0]["_norm"].get("error_category", "E")
        need = 1 if cat == "D" else min_market
        if len(complete) < need:
            continue
        apath = _write_active(complete, merged_body="(learn_cycle promote)")
        promoted.append(apath)
    return promoted


def _topic_accuracy(runs: list[dict]) -> list[tuple[str, int, int, str]]:
    by: dict[str, list] = defaultdict(list)
    for r in runs:
        by[r.get("topic") or "general"].append(r)
    rows = []
    for topic, rs in sorted(by.items()):
        n = len(rs)
        hits = sum(1 for r in rs if r.get("direction_hit") is True)
        acc = f"{100 * hits / n:.0f}%" if n else "n/a"
        rows.append((topic, hits, n, acc))
    return rows


def _workflow_impact_map() -> str:
    """How learnings touch each daily workflow."""
    return (
        "### General market predict (`run_predict` / daily pipeline)\n"
        "- Loads `mutable_policy.md` + `02_lessons/active/*` via `memory.prediction_context()`.\n"
        "- Must answer methodology checklist in MEMORY_CONFIRM.\n"
        "- Ops lessons (missing predict file) change grading, not B0–B7 math.\n"
        "- Macro / geo / regime lessons change how Channel-2 evidence is weighted in narrative.\n\n"
        "### Per-sector predict (`run_sector_predict` / sector daily)\n"
        "- Loads the **same** `mutable_policy.md` via `sector_memory` (filter lines for this sector + general).\n"
        "- Sector-specific active lessons (XLB temper, XLK geo, XLE Hormuz, staples/CPI) apply to S0–S4 scoring judgment.\n"
        "- Weak sectors in accuracy table → extra caution, milder bands, demand confirming tape.\n\n"
        "### Sector / general outcome + reflect\n"
        "- Outcomes grade hits; reflect writes new candidates.\n"
        "- Next learn_cycle mines those candidates again (promote if complete).\n\n"
        "### News parse + news actions\n"
        "- Hypotheses under scope `news` steer event-family conviction and ticker mapping.\n"
        "- Prefer 1d close quality over ever-touch MFE when ranking event edges.\n"
        "- Does not change SCORES format; changes which edges deserve size.\n\n"
        "### Label + Weather + Join\n"
        "- `weather_rules_proposals.json` may suggest threshold nudges (not auto-applied).\n"
        "- Label membership unchanged; weather stances may tighten if proposals are accepted later.\n"
        "- Join/match inherits weather; better weather → cleaner favorable/hostile books.\n\n"
        "### HIT board / report card\n"
        "- Still pure arithmetic over scoreboard; learn_cycle does not rewrite history.\n"
        "- Accuracy-by-topic in this file should match HIT board trends over time.\n"
    )


def _write_learnings_report(
    runs: list[dict],
    hypos: list[dict],
    promoted: list[str],
    news_n: int,
    date_str: str | None = None,
) -> Path:
    today = date_str or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    now = datetime.now(ZoneInfo(config.TZ)).isoformat()

    by_scope: dict[str, list] = defaultdict(list)
    for h in hypos:
        by_scope[h["scope"]].append(h)

    wins = sum(1 for h in hypos if h["kind"] == "win")
    losses = sum(1 for h in hypos if h["kind"] == "loss")

    L = [
        f"# Learnings report — {today}",
        "",
        f"Generated: **{now}** by `src/learn_cycle.py`.",
        "",
        "This is the human-readable digest of what the bot **actually learned** this cycle: "
        "graded evidence, hypotheses (wins and losses), promoted standing rules, and "
        "**how that changes every daily workflow**.",
        "",
        "Machine policy file (injected into predicts): `00_grounding/mutable_policy.md`.",
        "",
        "---",
        "",
        "## 1. Snapshot",
        "",
        f"| Item | Value |",
        f"|------|-------|",
        f"| Graded runs mined | {len(runs)} |",
        f"| Hypotheses written | {len(hypos)} (wins={wins}, losses={losses}) |",
        f"| News hypotheses | {news_n} |",
        f"| Lessons promoted to active | {len(promoted)} |",
        f"| Active lesson files now | {len(list(ACTIVE_DIR.glob('*.md')))} |",
        "",
        "## 2. Accuracy by topic (evidence this cycle learned from)",
        "",
        "| Topic | Direction HIT% | hits/n | Read |",
        "|-------|----------------|--------|------|",
    ]
    for topic, hits, n, acc in _topic_accuracy(runs):
        rate = hits / n if n else 0
        read = "ok" if rate >= 0.6 else ("thin/weak" if n < 5 else "weak — priority")
        L.append(f"| {topic} | {acc} | {hits}/{n} | {read} |")

    L += [
        "",
        "## 3. What we learned (by scope)",
        "",
        "Each scope lists recent win and loss hypotheses: the **counterfactual ask**, "
        "the **experiment** to run next, and the **policy candidate** (do instead).",
        "",
    ]
    for scope, hs in sorted(by_scope.items()):
        w = sum(1 for h in hs if h["kind"] == "win")
        lo = sum(1 for h in hs if h["kind"] == "loss")
        L.append(f"### `{scope}` — {w} wins, {lo} losses")
        L.append("")
        for h in hs[-5:]:
            L.append(f"#### {h['kind'].upper()} — {h['date']}")
            L.append(f"- **When:** {h['when']}")
            L.append(f"- **Ask:** {h['ask']}")
            L.append(f"- **Experiment:** {h['experiment']}")
            L.append(f"- **Do instead:** {h['do_instead']}")
            L.append(f"- **Wrong if:** {h['wrong_if']}")
            L.append("")

    L += [
        "## 4. Promoted standing rules (this cycle)",
        "",
    ]
    if promoted:
        for p in promoted[:40]:
            L.append(f"- `{Path(p).name}`")
        if len(promoted) > 40:
            L.append(f"- … and {len(promoted) - 40} more")
    else:
        L.append("_No new promotions this cycle (candidates incomplete or already active)._")

    L += [
        "",
        "Full text lives in `02_lessons/active/`. Summaries also feed `mutable_policy.md`.",
        "",
        "## 5. How these learnings affect daily workflows",
        "",
        _workflow_impact_map(),
        "## 6. Concrete operating rules for tomorrow",
        "",
        "1. **General:** Follow active ops + macro lessons; apply open experiments when setup matches.\n"
        "2. **Weak sectors** (HIT% soft in §2): default to milder magnitude; demand ETF tape confirmation.\n"
        "3. **Strong general / solid sectors:** Do not loosen risk controls only because recent hits look good.\n"
        "4. **News:** Prefer event families with clean 1d close evidence; do not size on MFE alone.\n"
        "5. **Weather/join:** Review `weather_rules_proposals.json` before accepting threshold changes.\n"
        "6. **All predicts:** Core output blocks stay fixed; only judgment/weights/search emphasis change.\n",
        "",
        "## 7. Files touched",
        "",
        "| File | Role |",
        "|------|------|",
        "| `03_scoreboard/LEARNINGS.md` | This digest (latest) |",
        f"| `01_daily/{today}_learnings.md` | Dated copy |",
        "| `00_grounding/mutable_policy.md` | Injected into general + sector predict |",
        "| `02_lessons/hypotheses/*` | Per-event experiments |",
        "| `02_lessons/active/*` | Standing rules |",
        "| `00_grounding/weather_rules_proposals.json` | Optional weather threshold deltas |",
        "",
    ]
    text = "\n".join(L)
    LEARNINGS.parent.mkdir(parents=True, exist_ok=True)
    LEARNINGS.write_text(text, encoding="utf-8")
    DAILY.mkdir(parents=True, exist_ok=True)
    dated = DAILY / f"{today}_learnings.md"
    dated.write_text(text, encoding="utf-8")
    print(f"[learn] wrote {LEARNINGS}")
    print(f"[learn] wrote {dated}")
    return LEARNINGS


def _rebuild_mutable_policy(
    runs: list[dict], hypos: list[dict], promoted: list[str],
    date_str: str | None = None,
) -> None:
    today = date_str or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    active_parts = []
    for p in sorted(ACTIVE_DIR.glob("*.md")):
        if p.name.startswith("."):
            continue
        text = _read(p).strip()
        if text:
            # compress for prompt: keep front matter + first rule chunk only
            active_parts.append(f"### {p.name}\n{text[:800]}")

    by_scope: dict[str, list] = defaultdict(list)
    for h in hypos:
        by_scope[h["scope"]].append(h)

    scope_blocks = []
    for scope, hs in sorted(by_scope.items()):
        wins = sum(1 for h in hs if h["kind"] == "win")
        losses = sum(1 for h in hs if h["kind"] == "loss")
        lines = [f"### scope `{scope}` — wins={wins} losses={losses}"]
        for h in hs[-3:]:
            lines.append(f"- **{h['kind']} {h['date']}:** {h['do_instead']}")
        scope_blocks.append("\n".join(lines))

    acc_lines = []
    for topic, hits, n, acc in _topic_accuracy(runs):
        acc_lines.append(f"- **{topic}**: {acc} ({hits}/{n})")

    open_exp = [
        f"- **{h['scope']}/{h['kind']} {h['date']}:** {h['experiment']}"
        for h in hypos[-12:]
    ]

    active_block = "\n\n".join(active_parts) if active_parts else "_(no active lessons)_"
    scope_block = "\n\n".join(scope_blocks) if scope_blocks else "_(no hypotheses)_"
    exp_block = "\n".join(open_exp) if open_exp else "_(none)_"
    acc_block = "\n".join(acc_lines) if acc_lines else "_(no graded runs)_"

    body = (
        f"---\n"
        f"status: living_policy\n"
        f"updated: {today}\n"
        f"source: src/learn_cycle.py\n"
        f"covers: general, sectors, news\n"
        f"note: Injected into general + sector PREDICT. Core output formats unchanged.\n"
        f"see_also: 03_scoreboard/LEARNINGS.md\n"
        f"---\n\n"
        f"# Mutable policy (all workflows)\n\n"
        f"Last learn_cycle: **{today}**. Promoted: {len(promoted)}. "
        f"Human digest: `03_scoreboard/LEARNINGS.md`.\n\n"
        f"## Accuracy by topic (graded window)\n\n"
        f"{acc_block}\n\n"
        f"## Active adjustments (promoted lessons, truncated)\n\n"
        f"{active_block}\n\n"
        f"## Per-scope DO-INSTEAD\n\n"
        f"{scope_block}\n\n"
        f"## Open experiments\n\n"
        f"{exp_block}\n\n"
        f"## Methodology checklist (MEMORY_CONFIRM)\n\n"
        f"1. Did any open experiment for THIS scope apply today?\n"
        f"2. Missing factor that would have flipped a recent loss?\n"
        f"3. Overweighting one bucket / double-counting one headline?\n"
        f"4. Sectors: S0 macro vs S1 sector factors — which failed?\n"
        f"5. News: event family still earning weight on 1d close?\n\n"
        f"## Retired / falsified\n\n"
        f"_(append when a falsifier triggers)_\n"
    )
    MUTABLE.write_text(body, encoding="utf-8")
    print(f"[learn] wrote {MUTABLE}")


def _weather_proposals(runs: list[dict]) -> None:
    gen = [r for r in runs if (r.get("topic") or "general") == "general"]
    if len(gen) < 5:
        return
    hits = sum(1 for r in gen if r.get("direction_hit") is True)
    acc = hits / len(gen)
    proposals = {
        "updated": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "window_n": len(gen),
        "direction_acc": acc,
        "notes": [],
        "threshold_deltas": {},
        "sector_notes": [],
    }
    if acc < 0.5:
        proposals["notes"].append(
            "General accuracy <50%: propose wider neutral band on risk thresholds."
        )
        proposals["threshold_deltas"] = {
            "risk_on_score": 1.0,
            "risk_off_score": -1.0,
        }
    for topic, hits_i, n, acc_s in _topic_accuracy(runs):
        if topic.startswith("sector:") and n >= 3:
            rate = hits_i / n
            if rate < 0.4:
                proposals["sector_notes"].append(
                    f"{topic} weak ({acc_s}): review sector taxonomy weights / search templates."
                )
    PROPOSALS.write_text(json.dumps(proposals, indent=2), encoding="utf-8")
    print(f"[learn] weather proposals -> {PROPOSALS}")


def run(lookback: int = 15, date: str | None = None) -> None:
    runs = _all_graded(lookback_per_topic=lookback)
    print(f"[learn] graded runs (all topics): {len(runs)}")
    for topic, hits, n, acc in _topic_accuracy(runs):
        print(f"  {topic}: {acc} ({hits}/{n})")

    hypos = _hypotheses_from_runs(runs)
    news_hypos = _hypotheses_from_news()
    hypos.extend(news_hypos)
    print(f"[learn] hypotheses: {len(hypos)} (news={len(news_hypos)})")

    paths = _write_hypotheses(hypos)
    print(f"[learn] hypothesis files: {len(paths)}")

    promoted = _promote_complete_candidates(min_market=1)
    print(f"[learn] promoted: {len(promoted)}")
    for p in promoted:
        print(f"  -> {p}")

    _rebuild_mutable_policy(runs, hypos, promoted, date_str=date)
    _weather_proposals(runs)
    _write_learnings_report(runs, hypos, promoted, news_n=len(news_hypos),
                            date_str=date)
    print("[learn] done — see 03_scoreboard/LEARNINGS.md")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=15)
    ap.add_argument("--date", default=None,
                    help="Session YYYY-MM-DD for the dated learnings file")
    args = ap.parse_args()
    run(lookback=args.lookback, date=args.date)


if __name__ == "__main__":
    main()
