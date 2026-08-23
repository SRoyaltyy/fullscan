"""Book-level reflection: what did the ranker miss, and what can it not see?

Two layers, run after the daily backtest:

1. DETERMINISTIC GAP SCAN (always runs, no LLM):
   For the latest signal date whose 5-session window is fully realized,
   find the biggest liquid-universe movers that were NOT in any buy book,
   and classify each miss:
     - blind      every component signal was ~0 — the inputs saw nothing.
                  These are the system's known unknown-unknowns: names that
                  moved on information no current input carries.
     - outweighed some signals were positive but the weighted rank buried
                  them — weight-learning territory (book_learn).
     - gated_out  excluded by hard gates (micro / <$400M) — a policy
                  choice, priced and recorded, not a mistake.
   Plus the worst realized buys with their full component vector.
   Output: 03_scoreboard/BOOK_GAPS.md + data/stock_book/{date}_book_gaps.json

2. LLM REFLECTION (deepseek-reasoner, skipped without an API key):
   Reads the gap scan, the backtest aggregate, the weight-tuner ledger, the
   input-health record, and its own previous hypotheses, then must:
     - write ≤2 schema-validated LESSON blocks (scope: book) — same
       candidate → promote → mutable-policy rails as every other lesson
     - maintain a MISSING_INPUTS list: concrete data sources the ranker
       demonstrably lacks (each with a falsifier), updating or retiring
       previous hypotheses instead of re-stating them.
   Output: 02_lessons/candidate/{date}_book_lesson.md
           02_lessons/hypotheses/book_missing_inputs.md

CLI: python -m src.book_reflect [--date YYYY-MM-DD] [--skip-llm]
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from . import config, lesson_schema
from .book_learn import COMPONENTS, HORIZON_DAYS, _fwd_returns, _load_panel, load_frame

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
GAPS_MD = ROOT / "03_scoreboard" / "BOOK_GAPS.md"
LEDGER_MD = ROOT / "03_scoreboard" / "BOOK_LEARN.md"
BACKTEST_JSON = ROOT / "03_scoreboard" / "stock_book_backtest.json"
HYPO_PATH = ROOT / "02_lessons" / "hypotheses" / "book_missing_inputs.md"
CAND_DIR = ROOT / "02_lessons" / "candidate"

GAP_HORIZON = "1w"          # 5 sessions — the mid sleeve
N_MISSED = 15
N_WORST_BUYS = 10
BLIND_EPS = 0.05


# ------------------------------------------------------------ gap scan

def _latest_realized_date(panel) -> str | None:
    n_td = HORIZON_DAYS[GAP_HORIZON]
    dates = sorted({p.name[:10] for p in BOOK_DIR.glob("????-??-??_stock_book.json")})
    for d in reversed(dates):
        if _fwd_returns(panel, d, n_td) is not None:
            return d
    return None


def _book_buys(date: str) -> set[str]:
    p = BOOK_DIR / f"{date}_stock_book.json"
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    out: set[str] = set()
    for entry in (data.get("books") or {}).values():
        for r in entry.get("buy") or []:
            out.add(str(r.get("ticker") or "").upper())
    return out


def _components_row(row: pd.Series) -> dict[str, float]:
    return {c: round(float(row.get(c) or 0.0), 3) for c in (*COMPONENTS, "s_opp")}


def _classify(row: pd.Series, live_components: tuple[str, ...]) -> str:
    mcap = float(pd.to_numeric(pd.Series([row.get("market_cap_m")]),
                               errors="coerce").fillna(0).iloc[0])
    if str(row.get("size") or "").lower() == "micro" or mcap < 400.0:
        return "gated_out"
    # only judge blindness on the stock-specific inputs that were actually
    # live that day (an absent input file must not count as "silent")
    specific = [c for c in ("s_news", "s_ab", "s_peer") if c in live_components]
    if not specific:
        return "outweighed"
    sig = max(abs(float(row.get(c) or 0.0)) for c in specific)
    return "blind" if sig < BLIND_EPS else "outweighed"


def gap_scan(panel) -> dict | None:
    date = _latest_realized_date(panel)
    if not date:
        print("[book-reflect] no realized signal date yet — skipping gap scan")
        return None
    frame = load_frame(date)
    if frame is None:
        print(f"[book-reflect] no frame for {date}")
        return None
    rets = _fwd_returns(panel, date, HORIZON_DAYS[GAP_HORIZON])
    frame = frame.set_index("Ticker", drop=False)
    frame["fwd"] = rets.reindex(frame.index)
    frame = frame.dropna(subset=["fwd"])
    buys = _book_buys(date)

    # inputs that were absent for the whole date (all-zero column) must be
    # reported as ABSENT, not counted as per-ticker silence
    live_components = tuple(
        c for c in COMPONENTS if c in frame.columns and frame[c].abs().gt(0).any()
    )
    absent_inputs = [c for c in COMPONENTS if c not in live_components]

    movers = frame.sort_values("fwd", ascending=False).head(60)
    missed = movers[~movers["Ticker"].isin(buys)].head(N_MISSED)
    missed_rows = []
    blind_n = 0
    for _, r in missed.iterrows():
        cls = _classify(r, live_components)
        blind_n += int(cls == "blind")
        missed_rows.append({
            "ticker": r["Ticker"],
            "fwd_pct": round(float(r["fwd"]) * 100, 2),
            "class": cls,
            "sector": r.get("sector"),
            "size": r.get("size"),
            "signals": _components_row(r),
        })

    held = frame[frame["Ticker"].isin(buys)].sort_values("fwd").head(N_WORST_BUYS)
    worst_rows = [{
        "ticker": r["Ticker"],
        "fwd_pct": round(float(r["fwd"]) * 100, 2),
        "sector": r.get("sector"),
        "size": r.get("size"),
        "signals": _components_row(r),
    } for _, r in held.iterrows()]

    # blind-spot aggregate: among ALL top-60 movers, how often was each LIVE
    # input silent? This is the evidence base for "what we cannot see".
    silence = {
        c: round(float((movers[c].abs() < BLIND_EPS).mean()), 3)
        for c in live_components
    }

    gaps = {
        "signal_date": date,
        "horizon": GAP_HORIZON,
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "missed_movers": missed_rows,
        "n_blind": blind_n,
        "worst_buys": worst_rows,
        "input_silence_on_movers": silence,
        "absent_inputs": absent_inputs,
        "universe_median_fwd_pct": round(float(frame["fwd"].median()) * 100, 2),
    }
    (BOOK_DIR / f"{date}_book_gaps.json").write_text(
        json.dumps(gaps, indent=2, default=str), encoding="utf-8")
    _write_gaps_md(gaps)
    return gaps


def _write_gaps_md(g: dict) -> None:
    L = [
        f"# Book gaps — what the ranker missed (signal {g['signal_date']}, "
        f"{g['horizon']} = {HORIZON_DAYS[g['horizon']]} sessions)",
        "",
        f"_Generated {g['generated_at']}_ · universe median fwd: "
        f"{g['universe_median_fwd_pct']:+.2f}%",
        "",
        "Classes: **blind** = every input was silent (unknown-unknown evidence) · "
        "**outweighed** = signals existed but the rank buried them (weight-tuner "
        "territory) · **gated_out** = excluded by hard micro/mcap gates on purpose.",
        "",
        "## Top movers NOT in any buy book",
        "",
        "| Ticker | fwd% | class | size | sector | join | sect | gen | news | ab | peer | opp |",
        "|--------|------|-------|------|--------|------|------|-----|------|----|------|-----|",
    ]
    for r in g["missed_movers"]:
        s = r["signals"]
        L.append(
            f"| {r['ticker']} | {r['fwd_pct']:+.1f} | {r['class']} | {r['size']} | "
            f"{r['sector']} | {s['s_join']:+.2f} | {s['s_sector']:+.2f} | "
            f"{s['s_general']:+.2f} | {s['s_news']:+.2f} | {s['s_ab']:+.2f} | "
            f"{s['s_peer']:+.2f} | {s['s_opp']:+.2f} |"
        )
    L += [
        "",
        f"**{g['n_blind']}/{len(g['missed_movers'])} missed movers were blind** — "
        "no current input carried any signal on them.",
        "",
        "## How often each live input was silent across the top-60 movers",
        "",
        "| input | silent share |",
        "|-------|--------------|",
    ]
    for c, v in (g.get("input_silence_on_movers") or {}).items():
        L.append(f"| {c} | {v:.0%} |")
    if g.get("absent_inputs"):
        L.append("")
        L.append(f"**Absent for this whole date (file missing — not silence):** "
                 f"{', '.join(g['absent_inputs'])}")
    L += [
        "",
        "## Worst realized buys",
        "",
        "| Ticker | fwd% | size | sector | join | sect | gen | news | ab | peer | opp |",
        "|--------|------|------|--------|------|------|-----|------|----|------|-----|",
    ]
    for r in g["worst_buys"]:
        s = r["signals"]
        L.append(
            f"| {r['ticker']} | {r['fwd_pct']:+.1f} | {r['size']} | {r['sector']} | "
            f"{s['s_join']:+.2f} | {s['s_sector']:+.2f} | {s['s_general']:+.2f} | "
            f"{s['s_news']:+.2f} | {s['s_ab']:+.2f} | {s['s_peer']:+.2f} | "
            f"{s['s_opp']:+.2f} |"
        )
    GAPS_MD.parent.mkdir(parents=True, exist_ok=True)
    GAPS_MD.write_text("\n".join(L), encoding="utf-8")
    print(f"[book-reflect] wrote {GAPS_MD}")


# ------------------------------------------------------------ LLM layer

def _read(p: Path, limit: int = 6000) -> str:
    try:
        return p.read_text(encoding="utf-8")[:limit]
    except OSError:
        return "(missing)"


def _backtest_aggregate() -> str:
    try:
        data = json.loads(BACKTEST_JSON.read_text(encoding="utf-8"))
        return json.dumps(data.get("aggregate") or {}, indent=1)
    except (OSError, json.JSONDecodeError):
        return "(missing)"


def _active_book_lessons(limit: int = 6) -> str:
    out = []
    for p in sorted((ROOT / "02_lessons" / "active").glob("*.md")):
        head = _read(p, 700)
        if 'scope: "book"' in head or "scope: book" in head:
            out.append(f"--- {p.name}\n{head}")
        if len(out) >= limit:
            break
    return "\n".join(out) or "(none yet)"


SYSTEM = """You are the reflection engine for a daily stock-ranking book.
Your ONLY goal is improving the paper-trading dashboard, which compounds the
forward returns of the daily top-10 buy books.

You receive hard evidence: movers the ranker missed (with each input's signal
at pick time), the worst realized buys, the rolling backtest, the weight
tuner's ledger, and input-health status. Numeric weights are tuned by a
separate deterministic process — do NOT propose weight values. Your job is
the part arithmetic cannot do:

1. LESSONS about ranker behavior — selection gates, signal construction,
   input hygiene — grounded in the evidence.
2. Reasoning about what the system CANNOT SEE. 'blind' missed movers moved
   on information no current input carries. Infer WHICH information that was
   (look the tickers up in the evidence, reason about what likely moved
   them: earnings dates? analyst notes? sector flows? options activity?
   small-cap news coverage gaps?) and maintain the MISSING_INPUTS list.
   Review previous hypotheses first: strengthen, revise, or retire them —
   never restate them unchanged.

Output EXACTLY this structure:

LESSON_BEGIN
ERROR_CATEGORY: A|B|C|E|NONE
TRIGGER_PATTERN: <when this failure mode appears, max 2 sentences>
CURRENT_BEHAVIOR: <what the book does today>
CORRECTED_BEHAVIOR: <do_instead — must name a concrete gate, score, weight or input change>
FALSIFIER: <wrong_if — one sentence>
EVIDENCE: <tickers + dates from the provided data>
SCOPE: book
LESSON_END

(0, 1 or 2 LESSON blocks. If nothing is warranted, emit one with
ERROR_CATEGORY: NONE.)

MISSING_INPUTS_BEGIN
[
  {"input": "<data source>", "why": "<evidence-linked reason>",
   "expected_effect": "<which misses it would have caught>",
   "falsifier": "<what would prove this input useless>",
   "status": "new|strengthened|revised|retired"}
]
MISSING_INPUTS_END
"""


def llm_reflect(gaps: dict, date: str) -> None:
    if not config.has_llm():
        print("[book-reflect] no OPENCLAW_GATEWAY_URL or DEEPSEEK_API_KEY — "
              "LLM layer skipped")
        return
    from . import deepseek_client

    user = (
        f"DATE: {date}\n\n"
        f"=== GAP SCAN (signal {gaps['signal_date']}) ===\n"
        f"{json.dumps(gaps, indent=1, default=str)[:7000]}\n\n"
        f"=== BACKTEST AGGREGATE ===\n{_backtest_aggregate()}\n\n"
        f"=== WEIGHT TUNER LEDGER ===\n{_read(LEDGER_MD, 3000)}\n\n"
        f"=== INPUT HEALTH (latest) ===\n"
        f"{_read(BOOK_DIR / f'{date}_input_health.json', 2500)}\n\n"
        f"=== ACTIVE BOOK LESSONS ===\n{_active_book_lessons()}\n\n"
        f"=== PREVIOUS MISSING-INPUT HYPOTHESES ===\n{_read(HYPO_PATH, 4000)}\n"
    )
    try:
        text = deepseek_client.chat(
            [{"role": "system", "content": SYSTEM},
             {"role": "user", "content": user}],
            model=config.MODEL_REFLECT, tools=False, max_tokens=4000,
        )
    except Exception as e:  # noqa: BLE001 — reflection must never fail the run
        print(f"[book-reflect] LLM call failed: {e}")
        return
    if not text:
        print("[book-reflect] empty LLM response")
        return

    # --- lessons → candidate pool (same rails as run_reflect) ---
    blocks = re.findall(r"LESSON_BEGIN(.*?)LESSON_END", text, re.S)
    written = 0
    for i, block in enumerate(blocks[:2]):
        raw = {}
        for line in block.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                raw[k.strip()] = v.strip()
        norm = lesson_schema.normalize(raw, date)
        norm["scope"] = "book"
        if norm.get("error_category") in ("NONE", ""):
            continue
        errs = lesson_schema.validation_errors(norm)
        norm["status"] = "candidate" if not errs else "candidate_incomplete"
        CAND_DIR.mkdir(parents=True, exist_ok=True)
        suffix = "" if i == 0 else f"_{i+1}"
        path = CAND_DIR / f"{date}_book_lesson{suffix}.md"
        path.write_text(
            lesson_schema.frontmatter(
                norm, extra={"validation_errors": "; ".join(errs) if errs else ""})
            + f"\n# Book reflection — {date}\n\n"
            + f"Gap scan: `data/stock_book/{gaps['signal_date']}_book_gaps.json`\n",
            encoding="utf-8",
        )
        written += 1
        print(f"[book-reflect] lesson → {path} (errs: {errs or 'none'})")
    if not written:
        print("[book-reflect] no corrective lesson this run")

    # --- missing-inputs hypotheses file (replaced wholesale each run;
    #     the LLM is instructed to carry forward what still stands) ---
    m = re.search(r"MISSING_INPUTS_BEGIN(.*?)MISSING_INPUTS_END", text, re.S)
    if m:
        body = m.group(1).strip()
        try:
            items = json.loads(body)
        except json.JSONDecodeError:
            items = None
        HYPO_PATH.parent.mkdir(parents=True, exist_ok=True)
        L = [
            "---",
            f'scope: "book"',
            f'kind: "missing_inputs"',
            f'updated: "{date}"',
            "---",
            "",
            "# Suspected missing inputs — the book's known unknowns",
            "",
            "Maintained by book_reflect (deepseek-reasoner) from hard evidence of",
            "movers that had zero signal coverage. Each entry names a falsifier;",
            "retired entries are dropped by the model when falsified.",
            "",
        ]
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                L += [
                    f"## {it.get('input', '?')}  _({it.get('status', 'new')})_",
                    "",
                    f"- **Why:** {it.get('why', '')}",
                    f"- **Expected effect:** {it.get('expected_effect', '')}",
                    f"- **Falsifier:** {it.get('falsifier', '')}",
                    "",
                ]
        else:
            L += ["```", body[:4000], "```"]
        HYPO_PATH.write_text("\n".join(L), encoding="utf-8")
        print(f"[book-reflect] hypotheses → {HYPO_PATH}")


# ------------------------------------------------------------ main

def run(date: str | None = None, skip_llm: bool = False) -> None:
    date = date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    panel = _load_panel()
    if panel is None or panel.empty:
        print("[book-reflect] no price store — skipping")
        return
    gaps = gap_scan(panel)
    if gaps is None:
        return
    if skip_llm:
        print("[book-reflect] --skip-llm — deterministic scan only")
        return
    llm_reflect(gaps, date)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--skip-llm", action="store_true")
    args = ap.parse_args()
    run(date=args.date, skip_llm=args.skip_llm)


if __name__ == "__main__":
    main()
