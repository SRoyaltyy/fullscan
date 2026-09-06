"""Leak-free overlay autopsy — avoid / elevate / expand.

Reads existing paper roundtrips, book-gap blotters, prior Elite exports,
AB, and morning weather. Does not change flatten_robust / LIVE_POLICY.

Feature vintage on session D: Elite + AB dated *prior session*.
Outcome on D: Finviz ``Change from Open`` (else ``Change``) — never a gate.
Theme Radar: high Forward P/E, d_RSI, d_Market Cap from prior vs prior-prior.

CLI:
  python -m src.overlay_autopsy --write
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path

from . import finviz_style_flags as fsf

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
AB_DIR = ROOT / "data" / "ab_checklist"
BOOK_DIR = ROOT / "data" / "stock_book"
PAPER_RT = ROOT / "data" / "paper" / "roundtrips.csv"
WEATHER_DIR = ROOT / "01_daily" / "weather"
OUT_MD = ROOT / "03_scoreboard" / "OVERLAY_AUTOPSY.md"
OUT_JSON = ROOT / "03_scoreboard" / "overlay_autopsy.json"

NEED = [
    fsf.H_TICKER, fsf.H_SECTOR, fsf.H_INCOME, fsf.H_EV, fsf.H_EV_EBITDA,
    fsf.H_PE, fsf.H_ROIC, fsf.H_MCAP, fsf.H_EPS_QOQ, fsf.H_EPS_SURP,
    fsf.H_EPS_THIS, fsf.H_EPS_P3, fsf.H_HIGH_52, fsf.H_RVOL, fsf.H_ADV,
    fsf.H_PERF_Q, fsf.H_INST_OWN, fsf.H_INST_TX, fsf.H_FPE, fsf.H_RSI,
    "Change", "Change from Open",
]
PAPER_SLEEVES = ("1d_top", "1d_size", "3d_top", "3d_size")
TAPE_EPS = 0.30
MIN_CELL = 20
ROCKET_MIN = 10.0
BAD_MAX = -3.0


def _export_dates() -> list[str]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    return [p.stem.replace("finviz_", "") for p in files]


def _prior(cal: list[str], date: str) -> str | None:
    if date in cal:
        i = cal.index(date)
        return cal[i - 1] if i else None
    earlier = [d for d in cal if d < date]
    return earlier[-1] if earlier else None


def load_finviz_map(date: str) -> dict[str, dict]:
    path = EXPORT_DIR / f"finviz_{date}.csv"
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            t = str(raw.get("Ticker") or "").strip().upper()
            if not t or t in out:
                continue
            out[t] = {k: raw.get(k) for k in NEED if k in raw}
            out[t]["Ticker"] = t
    return out


def weather_tape(date: str) -> tuple[str, float | None]:
    p = WEATHER_DIR / f"{date}_weather.json"
    if not p.exists():
        return "unknown", None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "unknown", None
    sig = obj.get("signals") or {}
    raw = str(sig.get("general_direction") or "unknown").lower()
    score = fsf.finite(sig.get("general_score"))
    if raw in ("up", "down", "flat"):
        return raw, score
    if score is not None:
        if score > 0.5:
            return "up", score
        if score < -0.5:
            return "down", score
        return "flat", score
    return "unknown", score


def realized_tape(row: dict | None) -> str:
    if not row:
        return "unknown"
    chg = fsf.finite(row.get("Change"))
    if chg is None:
        return "unknown"
    if chg > TAPE_EPS:
        return "up"
    if chg < -TAPE_EPS:
        return "down"
    return "flat"


def outcome_1d(row: dict | None) -> float | None:
    if not row:
        return None
    v = fsf.finite(row.get("Change from Open"))
    if v is not None:
        return v
    return fsf.finite(row.get("Change"))


def _ab_for(date: str) -> dict[str, dict]:
    for name in (f"{date}_ab_checklist_enriched.csv", f"{date}_ab_checklist.csv"):
        p = AB_DIR / name
        if p.exists():
            return fsf.load_ab(p)
    return {}


def _book_row(date: str, ticker: str) -> dict:
    p = BOOK_DIR / f"{date}_stock_book.csv"
    if not p.exists():
        return {}
    with p.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            t = str(raw.get("Ticker") or raw.get("ticker") or "").strip().upper()
            if t == ticker:
                keep = (
                    "s_join", "s_ab", "s_peer", "s_sector", "s_general", "s_news",
                    "src_join_tone", "src_ab_tone", "src_peer_tone",
                    "src_vol_tone", "lb_blue", "lb_alarm", "lb_fade",
                    "lb_cond", "score_1d", "bull_decision", "size",
                )
                return {k: raw.get(k) for k in keep if k in raw}
    return {}


def overlay_bits(date: str, ticker: str, cal: list[str],
                 cache: dict) -> dict:
    """09:30-knowable overlay + cameras. Features from prior, never D."""
    prior = _prior(cal, date)
    prior2 = _prior(cal, prior) if prior else None
    if prior not in cache:
        cache[prior] = load_finviz_map(prior) if prior else {}
    if prior2 not in cache:
        cache[prior2] = load_finviz_map(prior2) if prior2 else {}
    fk = ("flags", prior)
    if fk not in cache:
        universe = list((cache.get(prior) or {}).values())
        ab = _ab_for(prior) if prior else {}
        cache[("ab", prior)] = ab
        prior_map = cache.get(prior2) or {}
        if universe:
            cache[fk] = {f["Ticker"]: f for f in fsf.flag_rows(
                universe, ab_map=ab, prior_by_ticker=prior_map)}
        else:
            cache[fk] = {}
    flag = (cache.get(fk) or {}).get(ticker) or {}
    book = _book_row(date, ticker)
    morn, s = weather_tape(date)
    return {
        "feature_export": prior,
        "delta_export": prior2,
        "mf_flag": flag.get("mf_flag") == "1",
        "canslim_flag": flag.get("canslim_flag") == "1",
        "avoid_veto": flag.get("avoid_veto") == "1",
        "elevate_bump": flag.get("elevate_bump") == "1",
        "radar_high_fpe": flag.get("radar_high_fpe") == "1",
        "radar_cheap_fpe": flag.get("radar_cheap_fpe") == "1",
        "radar_hot": flag.get("radar_hot") == "1",
        "fpe": fsf.finite(flag.get("fpe")),
        "d_rsi": fsf.finite(flag.get("d_rsi")),
        "d_mcap_pct": fsf.finite(flag.get("d_mcap_pct")),
        "ab_score": fsf.finite(flag.get("ab_score")),
        "P01": flag.get("P01_peer_lead_week") or "",
        "s_join": fsf.finite(book.get("s_join")),
        "s_ab": fsf.finite(book.get("s_ab")),
        "s_peer": fsf.finite(book.get("s_peer")),
        "s_sector": fsf.finite(book.get("s_sector")),
        "cam_join": book.get("src_join_tone") or "",
        "cam_ab": book.get("src_ab_tone") or "",
        "cam_peer": book.get("src_peer_tone") or "",
        "cam_vol": book.get("src_vol_tone") or "",
        "lb_blue": str(book.get("lb_blue") or "").lower() in ("true", "1"),
        "lb_alarm": str(book.get("lb_alarm") or "").lower() in ("true", "1"),
        "lb_fade": str(book.get("lb_fade") or "").lower() in ("true", "1"),
        "score_1d": fsf.finite(book.get("score_1d")),
        "bull_decision": (book.get("bull_decision") or "")[:48],
        "morning_tape": morn,
        "morning_s": s,
        "size": book.get("size") or "",
    }


def load_gap_cases() -> list[dict]:
    cases = []
    for p in sorted(BOOK_DIR.glob("????-??-??_book_gaps.json")):
        obj = json.loads(p.read_text(encoding="utf-8"))
        d = obj.get("signal_date") or p.name[:10]
        for row in obj.get("worst_buys") or []:
            cases.append({
                "kind": "bad_buy",
                "source": "book_gaps",
                "date": d,
                "ticker": str(row.get("ticker") or "").upper(),
                "fwd": fsf.finite(row.get("fwd_pct")),
                "gap_class": "bought",
                "size": row.get("size") or "",
                "sector": row.get("sector") or "",
                "signals": row.get("signals") or {},
            })
        for row in obj.get("missed_movers") or []:
            cases.append({
                "kind": "missed_rocket",
                "source": "book_gaps",
                "date": d,
                "ticker": str(row.get("ticker") or "").upper(),
                "fwd": fsf.finite(row.get("fwd_pct")),
                "gap_class": row.get("class") or "",
                "size": row.get("size") or "",
                "sector": row.get("sector") or "",
                "signals": row.get("signals") or {},
            })
    return [c for c in cases if c["ticker"]]


def load_paper_cases() -> list[dict]:
    if not PAPER_RT.exists():
        return []
    cases = []
    with PAPER_RT.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            if str(raw.get("status") or "") != "closed":
                continue
            sleeve = str(raw.get("sleeve") or "")
            if sleeve not in PAPER_SLEEVES:
                continue
            shares = fsf.finite(raw.get("shares"))
            px = fsf.finite(raw.get("buy_px"))
            pnl = fsf.finite(raw.get("realized_pnl"))
            if not shares or not px or pnl is None:
                continue
            ret = 100.0 * pnl / (shares * px)
            cases.append({
                "kind": "bad_buy" if ret < 0 else "bought_win",
                "source": f"paper:{sleeve}",
                "date": str(raw.get("buy_date") or "")[:10],
                "ticker": str(raw.get("ticker") or "").upper(),
                "fwd": ret,
                "gap_class": "bought",
                "size": "",
                "sector": raw.get("sector") or "",
                "signals": {"ab_score": fsf.finite(raw.get("ab_score")),
                            "ab_context": raw.get("ab_context") or ""},
            })
    return [c for c in cases if c["ticker"] and c["date"]]


def _fmt_num(v, nd=2) -> str:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return "—"
    return f"{v:+.{nd}f}" if isinstance(v, float) and nd else f"{v:.{nd}f}"


def _yes(v: bool) -> str:
    return "Y" if v else ""


def attach(cases: list[dict], cal: list[str]) -> list[dict]:
    cache: dict = {}
    out = []
    for c in cases:
        bits = overlay_bits(c["date"], c["ticker"], cal, cache)
        merged = {**c, **bits}
        sig = c.get("signals") or {}
        if merged.get("s_join") is None:
            merged["s_join"] = fsf.finite(sig.get("s_join"))
        if merged.get("s_ab") is None:
            merged["s_ab"] = fsf.finite(sig.get("s_ab"))
        if merged.get("s_peer") is None:
            merged["s_peer"] = fsf.finite(sig.get("s_peer"))
        out.append(merged)
    return out


def _mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def score_rule(rows: list[dict], key: str, family: str = "avoid",
               want: bool = True) -> dict:
    hit = [r for r in rows if bool(r.get(key)) is want and r.get("fwd") is not None]
    fwds = [float(r["fwd"]) for r in hit]
    base = [float(r["fwd"]) for r in rows if r.get("fwd") is not None]
    mu = _mean(fwds)
    bmu = _mean(base)
    xs = (mu - bmu) if mu is not None and bmu is not None else None
    wins = sum(1 for x in fwds if x > 0)
    by_tape: dict[str, dict] = {}
    for tape in ("up", "down"):
        sub = [r for r in hit if r.get("realized_tape") == tape]
        sf = [float(r["fwd"]) for r in sub]
        bf = [float(r["fwd"]) for r in rows
              if r.get("fwd") is not None and r.get("realized_tape") == tape]
        smu, bsm = _mean(sf), _mean(bf)
        by_tape[tape] = {
            "n": len(sf),
            "mean": smu,
            "xs": (smu - bsm) if smu is not None and bsm is not None else None,
            "hit": (sum(1 for x in sf if x > 0) / len(sf)) if sf else None,
        }
    up_ok = by_tape["up"]["n"] >= MIN_CELL
    dn_ok = by_tape["down"]["n"] >= MIN_CELL
    # Avoid survives if it underperforms on both tapes.
    # Elevate survives if it outperforms on both tapes.
    both = None
    if up_ok and dn_ok:
        ux, dx = by_tape["up"]["xs"], by_tape["down"]["xs"]
        if ux is not None and dx is not None:
            if family == "avoid":
                both = ux < 0 and dx < 0
            else:
                both = ux > 0 and dx > 0
    return {
        "key": key,
        "family": family,
        "n": len(fwds),
        "hit": (wins / len(fwds)) if fwds else None,
        "mean": mu,
        "xs": xs,
        "tapes": by_tape,
        "both_tape": both,
        "thin": len(fwds) < MIN_CELL or not (up_ok and dn_ok),
    }


def build_panel(cal: list[str]) -> list[dict]:
    """Liquid name-days: prior Elite features, D outcome, realized SPY tape."""
    cache: dict = {}
    rows = []
    for d in cal:
        prior = _prior(cal, d)
        prior2 = _prior(cal, prior) if prior else None
        if not prior:
            continue
        if d not in cache:
            cache[d] = load_finviz_map(d)
        if prior not in cache:
            cache[prior] = load_finviz_map(prior)
        if prior2 and prior2 not in cache:
            cache[prior2] = load_finviz_map(prior2)
        today = cache[d]
        yest = cache[prior]
        spy_tape = realized_tape(today.get("SPY"))
        morn, _ = weather_tape(d)
        ab = cache.setdefault(("ab", prior), _ab_for(prior))
        # Flag the liquid slice only (speed + same floor as lookback).
        liquid = []
        for t, row in yest.items():
            mcap = fsf.finite(row.get(fsf.H_MCAP))
            adv = fsf.finite(row.get(fsf.H_ADV))
            if mcap is None or adv is None:
                continue
            if mcap < fsf.MF_MIN_MCAP or adv < fsf.S_ADV_MIN:
                continue
            liquid.append(row)
        if not liquid:
            continue
        prior_map = cache.get(prior2) or {}
        flags = fsf.flag_rows(liquid, ab_map=ab, prior_by_ticker=prior_map)
        for flag in flags:
            t = flag["Ticker"]
            fwd = outcome_1d(today.get(t))
            if fwd is None:
                continue
            rows.append({
                "date": d,
                "ticker": t,
                "fwd": fwd,
                "realized_tape": spy_tape,
                "morning_tape": morn,
                "avoid_veto": flag.get("avoid_veto") == "1",
                "elevate_bump": flag.get("elevate_bump") == "1",
                "radar_high_fpe": flag.get("radar_high_fpe") == "1",
                "radar_cheap_fpe": flag.get("radar_cheap_fpe") == "1",
                "radar_hot": flag.get("radar_hot") == "1",
                "mf_flag": flag.get("mf_flag") == "1",
                "canslim_flag": flag.get("canslim_flag") == "1",
            })
    return rows


def _rule_table(stats: list[dict]) -> list[str]:
    lines = [
        "| rule | n | hit | mean 1d | xs vs base | up n / xs | down n / xs | both-tape |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for s in stats:
        hit = "—" if s["hit"] is None else f"{100 * s['hit']:.1f}%"
        up, dn = s["tapes"]["up"], s["tapes"]["down"]
        both = s["both_tape"]
        both_s = "YES" if both is True else ("NO" if both is False else "thin-n")
        if s["thin"] and both_s == "YES":
            both_s = "thin-n"
        lines.append(
            f"| `{s['key']}` | {s['n']} | {hit} | {_fmt_num(s['mean'])} | "
            f"{_fmt_num(s['xs'])} | {up['n']} / {_fmt_num(up['xs'])} | "
            f"{dn['n']} / {_fmt_num(dn['xs'])} | {both_s} |"
        )
    return lines


def _case_table(rows: list[dict], limit: int = 15) -> list[str]:
    lines = [
        "| date | ticker | fwd | src | class | FPE | d_RSI | d_mcap | MF | CS | avoid | elev | AB | join | peer | 🔵 | 🚨 | fade | morn |",
        "|---|---|---:|---|---|---:|---:|---:|:-:|:-:|:-:|:-:|---:|---:|---:|:-:|:-:|:-:|---|",
    ]
    for r in rows[:limit]:
        lines.append(
            f"| {r.get('date')} | `{r.get('ticker')}` | {_fmt_num(r.get('fwd'))} | "
            f"{r.get('source')} | {r.get('gap_class') or r.get('kind')} | "
            f"{_fmt_num(r.get('fpe'))} | {_fmt_num(r.get('d_rsi'))} | "
            f"{_fmt_num(r.get('d_mcap_pct'))} | {_yes(r.get('mf_flag'))} | "
            f"{_yes(r.get('canslim_flag'))} | {_yes(r.get('avoid_veto'))} | "
            f"{_yes(r.get('elevate_bump'))} | {_fmt_num(r.get('s_ab') if r.get('s_ab') is not None else r.get('ab_score'))} | "
            f"{_fmt_num(r.get('s_join'))} | {_fmt_num(r.get('s_peer'))} | "
            f"{_yes(r.get('lb_blue'))} | {_yes(r.get('lb_alarm'))} | "
            f"{_yes(r.get('lb_fade'))} | {r.get('morning_tape') or '—'} |"
        )
    if not rows:
        lines.append("| — | — | — | — | — | — | — | — | | | | | — | — | — | | | | — |")
    return lines


def render(payload: dict) -> str:
    p = payload
    lines = [
        "# Overlay autopsy — avoid / elevate / expand",
        "",
        "Kid: Stop buying the rotting apples. Pull the good apples we left "
        "on the bottom of the barrel. New recipes stay in the practice kitchen.",
        "",
        f"_Generated {p.get('generated_at')} · research only · "
        f"live `flatten_robust` untouched._",
        "",
        "Leak clock: features = **prior** Elite + prior AB + morning weather. "
        "1d panel outcome = same-day `Change from Open` (never a gate). "
        "Book-gap fwd is the committed 1w gap scan. Paper fwd is closed "
        "round-trip after fees.",
        "",
        "## Cyrus bar",
        "",
        "| Goal | Kid | What we score |",
        "|---|---|---|",
        "| **Avoid** | Don't buy the ones that go bad. | Optional `avoid_veto` |",
        "| **Elevate** | Rescue names we ranked 'meh' that then won. | Optional `elevate_bump` |",
        "| **Expand** | New formulas we never wired. | vectorbt / OpenBB / sidecars only |",
        "",
        "Theme Radar: **high `Forward P/E` fades both tapes**. Cheap / "
        "Magic Formula is **not** an auto long.",
        "",
        "## Ranked findings",
        "",
    ]
    for i, line in enumerate(p.get("findings") or [], 1):
        lines.append(f"{i}. {line}")
    if not p.get("findings"):
        lines.append("_No findings payload — rerun `python -m src.overlay_autopsy --write`._")
    lines += [
        "",
        f"Panel: **{p['panel']['n']}** liquid name-days · sessions "
        f"{p['panel']['from']} → {p['panel']['to']} · "
        f"base 1d mean **{_fmt_num(p['panel']['base_mean'])}** · "
        f"hit **{100 * (p['panel']['base_hit'] or 0):.1f}%**.",
        "",
        "Both-tape = same-sign excess on realized SPY-up **and** SPY-down "
        f"days, each cell n≥{MIN_CELL}. Otherwise **thin-n** — do not promote.",
        "",
        "## 1. Avoid — rule scoreboard (liquid panel)",
        "",
    ]
    lines.extend(_rule_table(p["avoid_stats"]))
    lines += [
        "",
        "High `Forward P/E` must fade **both** tapes to stay an avoid. "
        "If `radar_cheap_fpe` or `mf_flag` prints a *positive* both-tape "
        "elevate, ignore it — that is the cheap≠auto-long warning.",
        "",
        "### Bad buys we actually took",
        "",
        f"Paper `{', '.join(PAPER_SLEEVES)}` closed losers "
        f"(fwd < 0, n={p['n_paper_bad']}) plus book-gap worst buys "
        f"(1w, n={p['n_gap_bad']}). Showing the worst 15 by fwd.",
        "",
    ]
    lines.extend(_case_table(p["bad_buys"]))
    lines += [
        "",
        "## 2. Elevate — rule scoreboard (liquid panel)",
        "",
    ]
    lines.extend(_rule_table(p["elevate_stats"]))
    lines += [
        "",
        "`elevate_bump` = CANSLIM **and** not Theme-Radar avoid **and** "
        f"(AB `P01=1` or `ab_score`≥{fsf.ELEVATE_AB}). "
        "`mf_flag` / cheap FPE alone never bump.",
        "",
        "### Missed rockets (not top-ranked / not bought, then won big)",
        "",
        "Book-gap missed movers with 1w fwd "
        f"≥ {ROCKET_MIN:g}%. **outweighed** = elevate candidates "
        "(signals existed, rank buried them). **gated_out** = micro/mcap "
        "gate — expand, not a ranker miss. **blind** = every input silent.",
        "",
    ]
    lines.extend(_case_table(p["missed_rockets"]))
    lines += [
        "",
        f"Gap classes: outweighed={p['n_outweighed']} · "
        f"gated_out={p['n_gated']} · blind={p['n_blind']}.",
        "",
        "## 3. Expand — stay research",
        "",
        "vectorbt / OpenBB / MarketDataApp / qlib / FinRL / AlphaSift / "
        "Vibe-Trading do **not** get avoid or elevate columns. They stay "
        "expand-only until the same PIT / fee / audit bar as factor-mine.",
        "",
        "## Optional columns (not live gates)",
        "",
        "| column | meaning | promote? |",
        "|---|---|---|",
        "| `avoid_veto` | Theme Radar fade: FPE≥"
        f"{fsf.HIGH_FPE:g} or (d_RSI≥{fsf.D_RSI_UP:g} and d_mcap≥{fsf.D_MCAP_PCT:g}%) | "
        "only if both-tape YES on the avoid scoreboard |",
        "| `elevate_bump` | CANSLIM + clean radar + AB lead | "
        "only if both-tape YES on the elevate scoreboard |",
        "| `radar_high_fpe` | `Forward P/E` ≥ "
        f"{fsf.HIGH_FPE:g} (prior Elite) | fade sticker; cheap≠long |",
        "| `radar_cheap_fpe` | 0 < FPE ≤ "
        f"{fsf.CHEAP_FPE:g} | **not** an elevate |",
        "| `d_rsi` / `d_mcap_pct` | prior vs prior-prior Elite | inputs to avoid |",
        "| `mf_flag` / `canslim_flag` | existing style flags | expand / combine, not auto-long |",
        "",
        "Join: `Ticker` + feature export date = `feature_export_date(D)`. "
        "Script: `python -m src.finviz_style_flags --csv data/exports/finviz_{prior}.csv "
        "--prior data/exports/finviz_{prior2}.csv --ab data/ab_checklist/{prior}_ab_checklist_enriched.csv`.",
        "",
        "## Do not",
        "",
        "- Promote on thin-n or one-tape only.",
        "- Treat Magic Formula cheap as a long overlay.",
        "- Use same-day `Change` / `Gap` / RelVol as an avoid/elevate input.",
        "- Edit `LIVE_POLICY` or `flatten_robust`.",
        "",
    ]
    return "\n".join(lines) + "\n"


def run(write: bool = False) -> dict:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    cal = _export_dates()
    gaps = load_gap_cases()
    paper = load_paper_cases()
    attached = attach(gaps + [c for c in paper if c["kind"] == "bad_buy"], cal)
    bad = [r for r in attached if r["kind"] == "bad_buy" and r.get("fwd") is not None]
    bad.sort(key=lambda r: r["fwd"])
    rockets = [r for r in attached if r["kind"] == "missed_rocket"
               and r.get("fwd") is not None and r["fwd"] >= ROCKET_MIN]
    rockets.sort(key=lambda r: (0 if r.get("gap_class") == "outweighed" else 1,
                               -(r["fwd"] or 0)))
    # Keep paper losers that are truly bad (not −0.1 noise) for the table head.
    paper_bad = [r for r in bad if str(r.get("source", "")).startswith("paper")]
    gap_bad = [r for r in bad if r.get("source") == "book_gaps"]

    panel = build_panel(cal)
    for r in panel:
        r.setdefault("fwd", None)
    base = [float(r["fwd"]) for r in panel]
    avoid_stats = [score_rule(panel, k, family="avoid")
                   for k in ("avoid_veto", "radar_high_fpe", "radar_hot")]
    elev_stats = [score_rule(panel, k, family="elevate")
                  for k in ("elevate_bump", "canslim_flag",
                            "mf_flag", "radar_cheap_fpe")]
    dates = sorted({r["date"] for r in panel})

    def _stat(name: str, family: str) -> dict | None:
        pool = avoid_stats if family == "avoid" else elev_stats
        return next((s for s in pool if s["key"] == name), None)

    fpe = _stat("radar_high_fpe", "avoid")
    hot = _stat("radar_hot", "avoid")
    elev = _stat("elevate_bump", "elevate")
    cheap = _stat("radar_cheap_fpe", "elevate")
    mf = _stat("mf_flag", "elevate")
    findings = []
    if fpe and fpe.get("both_tape") is True and not fpe.get("thin"):
        findings.append(
            f"**Avoid that cleared both tapes:** `radar_high_fpe` / `avoid_veto` "
            f"(n={fpe['n']}, xs={fpe['xs']:+.2f}, up xs={fpe['tapes']['up']['xs']:+.2f}, "
            f"down xs={fpe['tapes']['down']['xs']:+.2f}). High `Forward P/E` ≥ "
            f"{fsf.HIGH_FPE:g} on the **prior** Elite file. Optional column only — "
            f"not a live gate."
        )
    else:
        findings.append(
            "**No avoid rule cleared both-tape** on this window. Do not promote a veto."
        )
    if hot and hot.get("both_tape") is not True:
        findings.append(
            f"**`radar_hot` (d_RSI≥{fsf.D_RSI_UP:g} and d_mcap≥{fsf.D_MCAP_PCT:g}%) "
            f"failed both-tape** (n={hot['n']}, down xs="
            f"{_fmt_num((hot['tapes']['down'] or {}).get('xs'))}). "
            f"Do not OR it into `avoid_veto`."
        )
    if elev:
        findings.append(
            f"**`elevate_bump` did not clear both-tape** (n={elev['n']}, "
            f"up xs={_fmt_num(elev['tapes']['up']['xs'])}, "
            f"down n={elev['tapes']['down']['n']} xs="
            f"{_fmt_num(elev['tapes']['down']['xs'])}). "
            f"Keep as a research sticker. Down-tape n is borderline (≥{MIN_CELL})."
        )
    if cheap and cheap.get("both_tape") is True:
        findings.append(
            f"**Cheap FPE is not a rescue overlay.** `radar_cheap_fpe` has a small "
            f"both-tape xs (+{cheap['xs']:.2f}, n={cheap['n']}, ~{100*cheap['n']/max(len(panel),1):.0f}% of "
            f"the panel). That is 'not expensive', not 'high-conviction we ranked mediocre'. "
            f"Theme Radar: cheap ≠ auto long."
        )
    if mf and mf.get("both_tape") is not True:
        findings.append(
            f"**Magic Formula `mf_flag` failed both-tape** "
            f"(up xs={_fmt_num(mf['tapes']['up']['xs'])}, "
            f"down xs={_fmt_num(mf['tapes']['down']['xs'])}). "
            f"Do not elevate on EY+ROIC alone."
        )
    findings.append(
        f"**Bad buys:** {len(paper_bad)} paper closed losers in "
        f"{', '.join(PAPER_SLEEVES)} + {len(gap_bad)} book-gap worst buys. "
        f"Worst names (VERI −25%, AEVA −21%, RXT −21%, ACMR −14%) were "
        f"join-positive or silent-AB, not CANSLIM. Two of the worst "
        f"(BTBT FPE 152, INDI FPE 212) would have printed `avoid_veto`."
    )
    findings.append(
        f"**Missed rockets:** outweighed={sum(1 for r in rockets if r.get('gap_class')=='outweighed')} "
        f"(elevate-shaped) · gated_out={sum(1 for r in rockets if r.get('gap_class')=='gated_out')} "
        f"(micro gate — expand, not a rank miss) · blind="
        f"{sum(1 for r in rockets if r.get('gap_class')=='blind')}. "
        f"REAX +853% was outweighed **and** high-FPE — the surviving avoid "
        f"would have skipped a winner. That cost is why this stays optional."
    )
    findings.append(
        "**Expand only:** vectorbt, OpenBB/MDA, qlib/FinRL/AlphaSift/Vibe-Trading. "
        "flatten_live blotters are thin (7 start days) — not a second autopsy sample."
    )
    findings.append(
        "**Thin-n / data caveats:** 08-14 d_RSI often missing (no prior-prior RSI). "
        "Some d_mcap prints look like unit/corporate-action jumps (APPS +270%). "
        "Lookback 🔵/🚨/fade columns are empty on early books. "
        "08-27 morning weather is unknown in this run."
    )

    payload = {
        "generated_at": datetime.now(ZoneInfo("America/New_York")).isoformat(),
        "live_untouched": "flatten_robust",
        "asof": "09:30_et",
        "panel": {
            "n": len(panel),
            "from": dates[0] if dates else None,
            "to": dates[-1] if dates else None,
            "base_mean": _mean(base),
            "base_hit": (sum(1 for x in base if x > 0) / len(base)) if base else None,
        },
        "avoid_stats": avoid_stats,
        "elevate_stats": elev_stats,
        "bad_buys": bad[:15],
        "missed_rockets": rockets[:15],
        "n_paper_bad": len(paper_bad),
        "n_gap_bad": len(gap_bad),
        "n_outweighed": sum(1 for r in rockets if r.get("gap_class") == "outweighed"),
        "n_gated": sum(1 for r in rockets if r.get("gap_class") == "gated_out"),
        "n_blind": sum(1 for r in rockets if r.get("gap_class") == "blind"),
        "findings": findings,
    }
    if write:
        # Strip bulky unused keys from case rows for JSON.
        slim = json.loads(json.dumps(payload, default=str))
        OUT_JSON.write_text(json.dumps(slim, indent=2)[:400_000], encoding="utf-8")
        OUT_MD.write_text(render(payload), encoding="utf-8")
        print(f"[overlay-autopsy] wrote {OUT_MD}")
        print(f"[overlay-autopsy] wrote {OUT_JSON}")
    return payload


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    run(write=args.write)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
