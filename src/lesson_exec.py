"""Lesson executor — applies promoted book lessons to the live ranker, then
grades them against their own falsifiers.

The learning loop (learn_cycle -> 02_lessons/active/*.md) writes lessons with
RULE / WHEN IT FIRES / WRONG IF frontmatter, but nothing applied them to the
stock-book ranker: lesson_select only feeds general/sector predict prompts
(it explicitly drops scope=book/news lessons), and book_learn only drifts the
six family weights. This module is the missing apply + grade half.

apply_to_frame(df, date)
    Called by stock_book.build right before the score loop. For every
    registered override (00_grounding/book_lesson_overrides.json) whose
    lesson file is still active and not suspended, it adds traded copies of
    the families it can touch (s_join_lx / s_news_lx / s_ab_lx / s_peer_lx)
    plus eligibility flags (lesson_admit / lesson_admit_micro) and a
    lesson_fires audit tag. The ranker scores on the *_lx columns; the raw
    columns stay untouched for audit. Never raises — a lesson bug must not
    kill the book.

grade(date)
    Replays the last ~40 calendar days of committed stock-book CSVs,
    recomputes each override's trigger mask on the RAW columns (shadow
    grading — the counterfactual trigger set), and measures the fired names'
    realized 1w excess vs the universe median via book_learn's price panel.
    Once >= MIN_REALIZED_DATES books are fully realized, mean excess <= 0
    fires the lesson's own falsifier: the override is suspended (state in
    03_scoreboard/lesson_check.json) until a human clears the flag.

Outputs: 03_scoreboard/LESSON_CHECK.md, 03_scoreboard/lesson_check.json,
dashboard/lesson-check/index.html — the lesson check surface.

CLI: python -m src.lesson_exec --date YYYY-MM-DD
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
LESSON_DIR = ROOT / "02_lessons" / "active"
REGISTRY_PATH = ROOT / "00_grounding" / "book_lesson_overrides.json"
STATE_PATH = ROOT / "03_scoreboard" / "lesson_check.json"
CHECK_MD_PATH = ROOT / "03_scoreboard" / "LESSON_CHECK.md"
DASH_DIR = ROOT / "dashboard" / "lesson-check"
BOOK_DIR = ROOT / "data" / "stock_book"
EXPORTS_DIR = ROOT / "data" / "exports"

GRADE_LOOKBACK_DAYS = 40
GRADE_HORIZON_TD = 5        # 1w forward window — the lessons' own claim horizon
MIN_REALIZED_DATES = 5      # lessons ask for ">=5 fully-realized books"
MAX_TICKERS_LISTED = 12

# Which masks of each override carry an eligibility admit (waive LAG-peer
# veto / lattice bull_eligible) vs a pure score adjustment.
_ADMIT_MASKS = {
    "crash_rebound_admit": ("rescue", "micro"),
    "join_floor_pass": ("peer_maxed", "earnings"),
    "earnings_anti_fade": ("anti_fade",),
    "news_attach": (),
}


# ---------------------------------------------------------------- lessons

def _frontmatter(text: str) -> dict[str, str]:
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end == -1:
        return {}
    out: dict[str, str] = {}
    for line in text[3:end].splitlines():
        if ":" not in line:
            continue
        k, _, v = line.partition(":")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _section(text: str, name: str) -> str:
    m = re.search(
        rf"^##\s+{re.escape(name)}\s*\n(.*?)(?=^##\s|\Z)", text, re.M | re.S)
    body = (m.group(1).strip() if m else "")
    return re.sub(r"\(learn_cycle promote\)\s*$", "", body).strip()


def _active_lessons() -> dict[str, dict]:
    """filename -> {fm, rule} for active book/news-scope lessons."""
    out: dict[str, dict] = {}
    try:
        files = sorted(LESSON_DIR.glob("*.md"))
    except OSError:
        return out
    for p in files:
        try:
            text = p.read_text(encoding="utf-8")
        except OSError:
            continue
        fm = _frontmatter(text)
        if str(fm.get("scope", "")).strip() not in ("book", "news"):
            continue
        if str(fm.get("status", "active")).strip() != "active":
            continue
        out[p.name] = {"fm": fm, "rule": _section(text, "RULE")}
    return out


def load_registry() -> dict:
    try:
        data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        return data.get("overrides") or {}
    except (OSError, json.JSONDecodeError, AttributeError):
        return {}


def load_state() -> dict:
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("overrides", {})
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {"overrides": {}}


def _is_suspended(state: dict, oid: str) -> bool:
    return bool(((state.get("overrides") or {}).get(oid) or {}).get("suspended"))


# ---------------------------------------------------------------- lookups

def _digest_map(date: str) -> dict[str, float]:
    """ticker -> signed headline polarity from the day's finviz digest."""
    try:
        from . import stock_book
        book = stock_book._load_finviz_digest(date)
        return {t: float(rec.get("net") or 0.0) for t, rec in book.items()}
    except Exception:  # noqa: BLE001 — digest is optional
        return {}


def _earnings_map(date: str) -> dict[str, int]:
    """ticker -> days until earnings (0..7). Forward-looking dates only."""
    out: dict[str, int] = {}
    try:
        base = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        return out
    csv_path = EXPORTS_DIR / f"finviz_{date}.csv"
    try:
        import pandas as pd
        from .finviz_events import parse_finviz_datetime
        if csv_path.exists():
            df = pd.read_csv(csv_path, low_memory=False)
            tcol = "Ticker" if "Ticker" in df.columns else None
            ecol = "Earnings Date" if "Earnings Date" in df.columns else None
            if tcol and ecol:
                for t, raw in zip(df[tcol], df[ecol]):
                    tk = str(t or "").strip().upper()
                    if not tk:
                        continue
                    iso, _hm = parse_finviz_datetime(raw)
                    if not iso:
                        continue
                    try:
                        d = (datetime.strptime(iso, "%Y-%m-%d").date() - base).days
                    except ValueError:
                        continue
                    if 0 <= d <= 7:
                        out[tk] = d
    except Exception:  # noqa: BLE001
        pass
    if out:
        return out
    # Fallback: map-heat research keeps the mega-caps reporting today.
    try:
        from .map_heat_research import earnings_entry_tickers
        for t in earnings_entry_tickers(date) or []:
            out.setdefault(str(t).upper(), 0)
    except Exception:  # noqa: BLE001
        pass
    return out


def _earn_series(df, date: str):
    import pandas as pd
    earn = _earnings_map(date)
    if not earn:
        return pd.Series(-1, index=df.index)
    return (
        df["Ticker"].astype(str).str.upper().map(earn).fillna(-1).astype(int)
    )


# ---------------------------------------------------------------- masks

def _compute_masks(oid: str, p: dict, df, date: str,
                   earn_days=None, digest=None) -> dict:
    """Trigger masks for one override, computed on the RAW s_* columns.
    Single source of truth shared by apply (live) and grade (shadow)."""
    import pandas as pd

    def num(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index)

    zero = pd.Series(False, index=df.index)
    s_join, s_news = num("s_join"), num("s_news")
    s_ab, s_peer, s_sec = num("s_ab"), num("s_peer"), num("s_sector")
    if earn_days is None:
        earn_days = _earn_series(df, date)

    if oid == "crash_rebound_admit":
        rescue = (
            (s_join > float(p.get("join_min", 0.5)))
            & (s_sec > float(p.get("sector_min", 0.5)))
            & (s_ab < float(p.get("ab_max", -0.5)))
            & (s_peer < float(p.get("peer_max", -0.5)))
        )
        size = (
            df["size"].astype(str).str.lower()
            if "size" in df.columns else pd.Series("", index=df.index)
        )
        mcap = num("market_cap_m")
        micro = (
            (size == "micro") | (mcap < 400.0)
        ) & (
            s_peer >= float(p.get("micro_peer_min", 0.99))
        ) & (
            s_ab > float(p.get("micro_ab_min", 0.5))
        )
        return {"rescue": rescue, "micro": micro}

    if oid == "join_floor_pass":
        peer_maxed = (
            (s_peer >= float(p.get("peer_min", 0.99)))
            & (s_ab > float(p.get("ab_min", 0.0)))
        )
        w = int(p.get("earnings_window_days", 7))
        earnings = (
            (earn_days >= 0) & (earn_days <= w)
            & (s_ab > float(p.get("ab_min", 0.0)))
        )
        return {"peer_maxed": peer_maxed, "earnings": earnings}

    if oid == "earnings_anti_fade":
        w = int(p.get("earnings_window_days", 7))
        anti_fade = (
            (earn_days >= 0) & (earn_days <= w)
            & (s_ab >= float(p.get("ab_min", 0.6)))
        )
        return {"anti_fade": anti_fade}

    if oid == "news_attach":
        trig = float(p.get("trigger_abs", 0.5))
        if digest is None:
            digest = _digest_map(date)
        dser = (
            df["Ticker"].astype(str).str.upper().map(digest)
            if digest else pd.Series(float("nan"), index=df.index)
        )
        attach = (
            (s_news == 0.0)
            & ((s_join.abs() > trig) | (s_ab.abs() > trig))
            & dser.notna()
        )
        w = int(p.get("earnings_window_days", 7))
        neutralize = (
            (s_news < 0)
            & (s_ab > float(p.get("neutralize_ab_min", 0.8)))
            & (earn_days >= 0) & (earn_days <= w)
        )
        return {"attach": attach, "neutralize": neutralize}

    return {"_unknown": zero}


# ---------------------------------------------------------------- apply

def apply_to_frame(df, date: str):
    """Return (df, meta). Adds *_lx traded copies + eligibility flags.
    Never raises; on any failure the frame is returned with neutral columns."""
    import numpy as np
    import pandas as pd

    meta: dict = {"date": date, "overrides": {}, "prose_only": []}
    if df is None or not len(df):
        return df, meta

    # Traded copies — the ranker scores on these; raw columns stay for audit.
    for base in ("s_join", "s_news", "s_ab", "s_peer"):
        col = f"{base}_lx"
        if col not in df.columns:
            if base in df.columns:
                df[col] = pd.to_numeric(df[base], errors="coerce").fillna(0.0)
            else:
                df[col] = 0.0
    for flag in ("lesson_admit", "lesson_admit_micro"):
        if flag not in df.columns:
            df[flag] = False
    if "lesson_fires" not in df.columns:
        df["lesson_fires"] = ""

    registry = load_registry()
    lessons = _active_lessons()
    state = load_state()
    earn_days = _earn_series(df, date)
    digest: dict | None = None

    def mark(mask, oid, admit: bool) -> int:
        if mask is None or not mask.any():
            return 0
        if admit:
            df.loc[mask, "lesson_admit"] = True
        prev = df.loc[mask, "lesson_fires"].astype(str)
        df.loc[mask, "lesson_fires"] = [
            (p_ + ";" if p_ else "") + oid for p_ in prev
        ]
        return int(mask.sum())

    for oid, spec in registry.items():
        spec = spec or {}
        lesson_files = list(spec.get("lessons") or [])
        active_files = [f for f in lesson_files if f in lessons]
        entry = {
            "lessons": lesson_files,
            "lessons_active": active_files,
            "enabled": bool(spec.get("enabled")),
            "suspended": _is_suspended(state, oid),
            "fired": 0,
            "tickers": [],
        }
        meta["overrides"][oid] = entry
        if not spec.get("enabled") or entry["suspended"] or not active_files:
            continue
        p = spec.get("params") or {}
        try:
            if oid == "news_attach" and digest is None:
                digest = _digest_map(date)
            masks = _compute_masks(
                oid, p, df, date, earn_days=earn_days, digest=digest)
            admits = set(_ADMIT_MASKS.get(oid, ()))
            fired_total = 0
            for name, m in masks.items():
                if m is None or not m.any():
                    continue
                admit = name in admits
                if oid == "crash_rebound_admit" and name == "rescue":
                    df.loc[m, "s_ab_lx"] = df.loc[m, "s_ab_lx"].clip(lower=0.0)
                    df.loc[m, "s_peer_lx"] = df.loc[m, "s_peer_lx"].clip(lower=0.0)
                elif oid == "crash_rebound_admit" and name == "micro":
                    df.loc[m, "lesson_admit_micro"] = True
                elif oid == "join_floor_pass":
                    df.loc[m, "s_join_lx"] = df.loc[m, "s_join_lx"].clip(lower=0.0)
                elif oid == "earnings_anti_fade":
                    df.loc[m, "s_join_lx"] = df.loc[m, "s_join_lx"].clip(lower=0.0)
                    df.loc[m, "s_news_lx"] = df.loc[m, "s_news_lx"].clip(lower=0.0)
                elif oid == "news_attach" and name == "attach":
                    amp = float(p.get("attach_abs", 0.2))
                    dser = df["Ticker"].astype(str).str.upper().map(digest or {})
                    df.loc[m, "s_news_lx"] = np.sign(
                        pd.to_numeric(dser[m], errors="coerce").fillna(0.0)
                    ) * amp
                elif oid == "news_attach" and name == "neutralize":
                    df.loc[m, "s_news_lx"] = 0.0
                fired_total += mark(m, oid, admit)
            entry["fired"] = fired_total
            if fired_total:
                all_m = None
                for m in masks.values():
                    all_m = m if all_m is None else (all_m | m)
                entry["tickers"] = sorted(
                    df["Ticker"].astype(str).str.upper()[all_m].tolist()
                )[:MAX_TICKERS_LISTED]
        except Exception as e:  # noqa: BLE001 — one bad override must not kill the book
            entry["error"] = str(e)
            print(f"[lesson-exec] WARN: override {oid} failed: {e}")

    registered = {f for spec in registry.values() for f in (spec or {}).get("lessons") or []}
    meta["prose_only"] = sorted(f for f in lessons if f not in registered)
    n_admit = int(pd.Series(df["lesson_admit"]).astype(bool).sum())
    n_micro = int(pd.Series(df["lesson_admit_micro"]).astype(bool).sum())
    bits = " ".join(
        f"{k}:{v.get('fired', 0)}" for k, v in meta["overrides"].items())
    print(f"[lesson-exec] {date}: admit={n_admit} micro={n_micro} {bits}")
    return df, meta


# ---------------------------------------------------------------- grade

def _verdict(n_realized: int, mean_excess: float, suspended: bool) -> str:
    if suspended:
        return "suspended"
    if n_realized >= MIN_REALIZED_DATES:
        return "working" if mean_excess > 0 else "falsified"
    if n_realized:
        return "collecting"
    return "no triggers realized yet"


def _book_dates(lookback_days: int, upto: str) -> list[str]:
    out: list[str] = []
    try:
        end = datetime.strptime(upto, "%Y-%m-%d").date()
    except ValueError:
        return out
    try:
        files = sorted(BOOK_DIR.glob("*_stock_book.csv"))
    except OSError:
        return out
    for p in files:
        d = p.name[:10]
        try:
            dd = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        if 0 <= (end - dd).days <= lookback_days:
            out.append(d)
    return sorted(out)


def grade(date: str, lookback_days: int = GRADE_LOOKBACK_DAYS) -> dict:
    """Grade each override against its falsifier; persist state. Never raises."""
    registry = load_registry()
    lessons = _active_lessons()
    state = load_state()
    state.setdefault("overrides", {})
    dates = _book_dates(lookback_days, date)

    panel = None
    frames: dict[str, object] = {}
    try:
        from . import book_learn
        panel = book_learn._load_panel()
        if panel is not None:
            for d in dates:
                try:
                    fr = book_learn.load_frame(d)
                    if fr is not None and len(fr):
                        frames[d] = fr
                except Exception:  # noqa: BLE001
                    continue
    except Exception as e:  # noqa: BLE001
        print(f"[lesson-exec] grade: book_learn unavailable: {e}")

    report: dict = {
        "date": date,
        "generated_at": datetime.now(ZoneInfo("America/New_York")).isoformat(),
        "n_book_dates": len(dates),
        "n_frames": len(frames),
        "horizon_td": GRADE_HORIZON_TD,
        "min_realized": MIN_REALIZED_DATES,
        "overrides": {},
        "prose_only": [],
    }

    for oid, spec in registry.items():
        spec = spec or {}
        lesson_files = list(spec.get("lessons") or [])
        active_files = [f for f in lesson_files if f in lessons]
        ostate = state["overrides"].setdefault(oid, {})
        suspended = bool(ostate.get("suspended"))
        first = lessons.get(active_files[0]) if active_files else None
        entry = {
            "lessons": lesson_files,
            "lessons_active": active_files,
            "enabled": bool(spec.get("enabled")),
            "suspended": suspended,
            "summary": spec.get("summary") or "",
            "rule": (first or {}).get("rule", ""),
            "falsifier": (first or {}).get("fm", {}).get("falsifier", ""),
            "per_date": [],
            "n_realized": int(ostate.get("n_realized") or 0),
            "mean_excess": ostate.get("mean_excess"),
            "verdict": "suspended" if suspended else "not graded",
        }
        if not spec.get("enabled"):
            entry["verdict"] = "disabled in registry"
        elif not active_files:
            entry["verdict"] = "lesson retired — override inert"
        elif panel is None or not frames:
            entry["verdict"] = "waiting on price panel / book frames"
        else:
            per_date: list[dict] = []
            for d, fr in frames.items():
                try:
                    masks = _compute_masks(oid, spec.get("params") or {}, fr, d)
                    fired = None
                    for m in masks.values():
                        fired = m if fired is None else (fired | m)
                    if fired is None or not fired.any():
                        continue
                    rets = book_learn._fwd_returns(panel, d, GRADE_HORIZON_TD)
                    if rets is None or not len(rets):
                        continue
                    tks = [
                        t for t in fr["Ticker"].astype(str).str.upper()[fired]
                        if t in rets.index
                    ]
                    if not tks:
                        continue
                    vals = [float(rets[t]) for t in tks]
                    med = float(rets.median())
                    per_date.append({
                        "date": d,
                        "n": len(vals),
                        "mean": round(sum(vals) / len(vals), 5),
                        "universe_median": round(med, 5),
                        "excess": round(sum(vals) / len(vals) - med, 5),
                        "tickers": tks[:MAX_TICKERS_LISTED],
                    })
                except Exception:  # noqa: BLE001 — skip that book date
                    continue
            n = len(per_date)
            mean_excess = (
                round(sum(x["excess"] for x in per_date) / n, 5) if n else 0.0
            )
            verdict = _verdict(n, mean_excess, suspended)
            if verdict == "falsified":
                ostate["suspended"] = True
                ostate["suspended_on"] = date
                entry["suspended"] = True
                verdict = "falsified — suspended"
            ostate["n_realized"] = n
            ostate["mean_excess"] = mean_excess
            ostate["last_graded"] = date
            hist = list(ostate.get("history") or [])
            hist.append({
                "date": date, "n_realized": n,
                "mean_excess": mean_excess, "verdict": verdict,
            })
            ostate["history"] = hist[-90:]
            entry.update({
                "per_date": per_date,
                "n_realized": n,
                "mean_excess": mean_excess,
                "verdict": verdict,
            })
        report["overrides"][oid] = entry

    registered = {f for spec in registry.values() for f in (spec or {}).get("lessons") or []}
    report["prose_only"] = [
        {"file": f, "rule": lessons[f]["rule"]}
        for f in sorted(lessons)
        if f not in registered
    ]
    report["state"] = state

    _write_outputs(date, report)
    return report


# ---------------------------------------------------------------- outputs

def _pill(verdict: str) -> str:
    v = verdict.lower()
    if v.startswith("working"):
        return "🟢 working"
    if v.startswith("falsified") or v == "suspended":
        return "⛔ " + verdict
    if v.startswith("collecting"):
        return "🟡 " + verdict
    return "⚪ " + verdict


def _write_outputs(date: str, report: dict) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        state = report.get("state") or {"overrides": {}}
        state_out = {
            "date": date,
            "generated_at": report.get("generated_at"),
            "overrides": state.get("overrides") or {},
            "report": {
                oid: {
                    k: v for k, v in entry.items()
                    if k in ("verdict", "n_realized", "mean_excess",
                             "suspended", "enabled", "lessons_active")
                }
                for oid, entry in (report.get("overrides") or {}).items()
            },
        }
        STATE_PATH.write_text(
            json.dumps(state_out, indent=2, default=str), encoding="utf-8")
    except OSError as e:
        print(f"[lesson-exec] WARN: state write failed: {e}")
    try:
        CHECK_MD_PATH.write_text(_render_md(date, report), encoding="utf-8")
    except OSError as e:
        print(f"[lesson-exec] WARN: md write failed: {e}")
    try:
        DASH_DIR.mkdir(parents=True, exist_ok=True)
        (DASH_DIR / "index.html").write_text(
            _render_html(date, report), encoding="utf-8")
    except OSError as e:
        print(f"[lesson-exec] WARN: dashboard write failed: {e}")


def _render_md(date: str, report: dict) -> str:
    lines = [
        f"# Lesson check — {date}",
        "",
        "Do promoted lessons actually reach the book, and do they survive",
        "their own falsifiers? Overrides are code in `src/lesson_exec.py`,",
        "registered in `00_grounding/book_lesson_overrides.json`, graded on",
        f"the last {GRADE_LOOKBACK_DAYS} days of books "
        f"({report.get('n_frames', 0)} frames, 1w excess vs universe median).",
        "",
        "## Overrides (code-active)",
        "",
    ]
    for oid, e in (report.get("overrides") or {}).items():
        lines.append(f"### `{oid}` — {_pill(e.get('verdict') or '')}")
        if e.get("summary"):
            lines.append(f"- What it does: {e['summary']}")
        if e.get("rule"):
            lines.append(f"- Rule: {e['rule']}")
        if e.get("falsifier"):
            lines.append(f"- Falsifier: {e['falsifier']}")
        n = e.get("n_realized")
        me = e.get("mean_excess")
        if n is not None and me is not None:
            lines.append(
                f"- Graded: {n}/{MIN_REALIZED_DATES} realized books, "
                f"mean 1w excess {me:+.2%}" if isinstance(me, float)
                else f"- Graded: {n} realized books")
        if e.get("suspended"):
            lines.append("- **Suspended** — apply() is a no-op until a human "
                         "clears the flag in `03_scoreboard/lesson_check.json`.")
        rows = e.get("per_date") or []
        if rows:
            lines.append("")
            lines.append("| book date | fired | mean 1w | univ median | excess | tickers |")
            lines.append("|---|---|---|---|---|---|")
            for r in rows[-10:]:
                lines.append(
                    f"| {r['date']} | {r['n']} | {r['mean']:+.2%} "
                    f"| {r['universe_median']:+.2%} | {r['excess']:+.2%} "
                    f"| {', '.join(r['tickers'][:6])} |")
        lines.append("")
    prose = report.get("prose_only") or []
    lines += [
        "## Prose-only book lessons (not wired to code yet)",
        "",
    ]
    if prose:
        for row in prose:
            rule = (row.get("rule") or "").replace("\n", " ")[:160]
            lines.append(f"- `{row['file']}` — {rule}")
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def _render_html(date: str, report: dict) -> str:
    def esc(s: object) -> str:
        return (
            str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    cards = []
    for oid, e in (report.get("overrides") or {}).items():
        verdict = e.get("verdict") or ""
        v = verdict.lower()
        color = ("#4ade80" if v.startswith("working")
                 else "#f87171" if v.startswith("falsified") or v == "suspended"
                 else "#facc15" if v.startswith("collecting")
                 else "#94a3b8")
        n = e.get("n_realized")
        me = e.get("mean_excess")
        graded = ""
        if isinstance(n, int) and isinstance(me, (int, float)):
            graded = (f"<div class='stat'>graded <b>{n}</b>/"
                      f"{MIN_REALIZED_DATES} realized books · mean 1w excess "
                      f"<b>{me:+.2%}</b></div>")
        rows = []
        for r in (e.get("per_date") or [])[-10:]:
            ex = r["excess"]
            cls = "pos" if ex > 0 else "neg"
            rows.append(
                f"<tr><td>{esc(r['date'])}</td><td>{r['n']}</td>"
                f"<td>{r['mean']:+.2%}</td><td>{r['universe_median']:+.2%}</td>"
                f"<td class='{cls}'>{ex:+.2%}</td>"
                f"<td class='tk'>{esc(', '.join(r['tickers'][:6]))}</td></tr>")
        table = (
            "<table><tr><th>book date</th><th>fired</th><th>mean 1w</th>"
            "<th>univ median</th><th>excess</th><th>tickers</th></tr>"
            + "".join(rows) + "</table>"
        ) if rows else "<div class='muted'>no realized triggers yet</div>"
        susp = (
            "<div class='susp'>SUSPENDED — no-op until re-enabled in "
            "03_scoreboard/lesson_check.json</div>"
        ) if e.get("suspended") else ""
        cards.append(f"""
<div class="card">
 <div class="oid">{esc(oid)} <span class="pill" style="background:{color}22;color:{color};border:1px solid {color}">{esc(_pill(verdict))}</span></div>
 <div class="rule">{esc(e.get('summary') or e.get('rule'))}</div>
 {f"<div class='fals'>WRONG IF: {esc(e.get('falsifier'))}</div>" if e.get('falsifier') else ""}
 {graded}{susp}{table}
</div>""")
    prose = report.get("prose_only") or []
    prose_html = "".join(
        f"<li><code>{esc(r['file'])}</code> — {esc((r.get('rule') or '')[:180])}</li>"
        for r in prose
    ) or "<li class='muted'>none</li>"
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lesson check — {esc(date)}</title>
<style>
body{{background:#0b1220;color:#cbd5e1;font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;margin:0;padding:24px}}
.wrap{{max-width:1000px;margin:0 auto}}
h1{{color:#e2e8f0;font-size:22px;margin:0 0 4px}}
.sub{{color:#64748b;margin-bottom:18px}}
.sub a{{color:#93c5fd}}
.card{{background:#111a2e;border:1px solid #1e293b;border-radius:10px;padding:16px 18px;margin-bottom:16px}}
.oid{{font-weight:700;color:#e2e8f0;font-size:15px;margin-bottom:6px}}
.pill{{font-size:12px;font-weight:600;border-radius:999px;padding:2px 10px;margin-left:8px;vertical-align:middle}}
.rule{{margin:4px 0}}
.fals{{color:#fbbf24;font-size:13px;margin:6px 0}}
.stat{{color:#94a3b8;font-size:13px;margin:6px 0}}
.stat b{{color:#e2e8f0}}
.susp{{color:#f87171;font-weight:600;font-size:13px;margin:6px 0}}
table{{width:100%;border-collapse:collapse;margin-top:8px;font-size:13px}}
th,td{{text-align:left;padding:4px 8px;border-bottom:1px solid #1e293b}}
th{{color:#64748b;font-weight:600}}
.pos{{color:#4ade80}}.neg{{color:#f87171}}
.tk{{color:#93c5fd;font-size:12px}}
.muted{{color:#475569}}
h2{{color:#e2e8f0;font-size:17px;margin-top:26px}}
li{{margin:4px 0}}code{{color:#93c5fd;font-size:12px}}
</style></head><body><div class="wrap">
<h1>Lesson check</h1>
<div class="sub">{esc(date)} · do promoted lessons reach the book, and do they survive their own falsifiers ·
<a href="/fullscan/dashboard/">paper</a> ·
<a href="/fullscan/dashboard/factor-mine/">factor mine</a> ·
<a href="/fullscan/dashboard/strategy-board/">strategy board</a></div>
{''.join(cards)}
<h2>Prose-only book lessons (not wired to code)</h2>
<ul>{prose_html}</ul>
</div></body></html>"""


# ---------------------------------------------------------------- cli

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Trading day YYYY-MM-DD")
    ap.add_argument("--lookback", type=int, default=GRADE_LOOKBACK_DAYS)
    args = ap.parse_args()
    rep = grade(args.date, lookback_days=args.lookback)
    for oid, e in (rep.get("overrides") or {}).items():
        print(f"[lesson-exec] grade {oid}: {e.get('verdict')} "
              f"(n={e.get('n_realized')}, excess={e.get('mean_excess')})")
    print(f"[lesson-exec] wrote {CHECK_MD_PATH.relative_to(ROOT)}, "
          f"{STATE_PATH.relative_to(ROOT)}, "
          f"{(DASH_DIR / 'index.html').relative_to(ROOT)}")


if __name__ == "__main__":
    main()
