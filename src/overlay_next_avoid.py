"""Next Avoid after FPE null: join-hot ∧ AB-silent micros, then soft 🚨∧fade.

Research only. Same leftover / unit / fee bar as overlay_horizon_bt.
Does not remine FPE / d_RSI / d_mcap on flatten_h5.
Does not import sleeve_merge / LIVE_POLICY.
Does not OR these Avoids with FPE.

CLI:
  python3 -m src.overlay_next_avoid --write
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from . import overlay_horizon_bt as oh
from .theme_radar_baskets import TS_TOP_Q

ROOT = Path(__file__).resolve().parent.parent
JOIN_DIR = ROOT / "data" / "join"
AB_DIR = ROOT / "data" / "ab_checklist"
BOOK_DIR = ROOT / "data" / "stock_book"

# ticker_lookback.EPS — polarity floor on normalized s_ab (raw/12).
AB_EPS = 0.05
# Autopsy VERI = micro; AEVA/RXT = small; ACMR = mid (exclude).
SIZE_OK = frozenset({"micro", "small"})
JOIN_MIN_N = 20
JOIN_MIN_CUT = 0.0
AB_SCORE_KEYS = ("score_enriched", "score_merged", "score", "ab_raw")


def _num(v):
    return oh._num(v)


def load_join_map(date: str) -> dict[str, dict]:
    path = JOIN_DIR / f"{date}_ranked.csv"
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            t = str(raw.get("Ticker") or "").strip().upper()
            if not t or t in out:
                continue
            out[t] = {
                "total_score": _num(raw.get("total_score")),
                "score_norm": _num(raw.get("score_norm")),
                "size": str(raw.get("size") or "").strip().lower(),
            }
    return out


def join_high_cut(join_map: dict[str, dict]) -> float | None:
    """Session p80 of total_score. None if thin or degenerate (cut < 0)."""
    xs = [r["total_score"] for r in join_map.values()
          if r.get("total_score") is not None]
    if len(xs) < JOIN_MIN_N:
        return None
    xs.sort()
    idx = min(len(xs) - 1, max(0, int(len(xs) * TS_TOP_Q)))
    cut = xs[idx]
    if cut < JOIN_MIN_CUT:
        return None
    return cut


def is_join_hot(row: dict | None, cut: float | None) -> bool:
    if not row or cut is None:
        return False
    ts = row.get("total_score")
    return ts is not None and ts >= cut


def ab_session_dates() -> list[str]:
    dates = set()
    for p in AB_DIR.glob("*_ab_checklist_enriched.csv"):
        d = p.name[:10]
        if len(d) == 10 and d[4] == "-" and d[7] == "-":
            dates.add(d)
    for p in AB_DIR.glob("*_ab_checklist.csv"):
        stem = p.name.replace("_ab_checklist.csv", "")
        if len(stem) == 10 and stem[4] == "-" and stem[7] == "-":
            dates.add(stem)
    return sorted(dates)


def prior_ab_date(date: str, cal: list[str] | None = None) -> str | None:
    """Last AB session strictly before D. Never D."""
    dates = cal if cal is not None else ab_session_dates()
    earlier = [d for d in dates if d < date]
    return earlier[-1] if earlier else None


def _ab_path(date: str) -> Path | None:
    enriched = AB_DIR / f"{date}_ab_checklist_enriched.csv"
    if enriched.exists():
        return enriched
    plain = AB_DIR / f"{date}_ab_checklist.csv"
    if plain.exists():
        return plain
    return None


def load_ab_map(date: str) -> dict[str, dict]:
    path = _ab_path(date)
    if path is None:
        return {}
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        for raw in csv.DictReader(fh):
            t = str(raw.get("Ticker") or "").strip().upper()
            if not t or t in out:
                continue
            raw_score = None
            for k in AB_SCORE_KEYS:
                raw_score = _num(raw.get(k))
                if raw_score is not None:
                    break
            if raw_score is None:
                s_ab = None
            else:
                s_ab = max(-1.0, min(1.0, raw_score / 12.0))
            out[t] = {
                "raw_score": raw_score,
                "s_ab": s_ab,
                "file": path.name,
            }
    return out


def is_ab_silent(ab_row: dict | None, prior: str | None) -> bool:
    """No prior AB file, no row, or |s_ab| < EPS. Honest on 08-13/14."""
    if prior is None:
        return True
    if not ab_row:
        return True
    s = ab_row.get("s_ab")
    if s is None:
        return True
    return abs(float(s)) < AB_EPS


def size_ok(*sizes: str) -> bool:
    for s in sizes:
        bit = str(s or "").strip().lower()
        if bit in SIZE_OK:
            return True
        if bit in {"mid", "large", "mega"}:
            return False
    return False


def is_jam(join_row: dict | None, cut: float | None,
           ab_row: dict | None, prior_ab: str | None,
           extra_size: str = "") -> bool:
    if not is_join_hot(join_row, cut):
        return False
    if not is_ab_silent(ab_row, prior_ab):
        return False
    sz = (join_row or {}).get("size") or extra_size
    return size_ok(sz)


def load_book_marks(date: str) -> dict[str, dict]:
    """D's morning stock-book CSV — lb_alarm / lb_fade / size / s_ab."""
    path = BOOK_DIR / f"{date}_stock_book.csv"
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        cols = set(reader.fieldnames or [])
        has_alarm = "lb_alarm" in cols
        has_fade = "lb_fade" in cols
        for raw in reader:
            t = str(raw.get("Ticker") or raw.get("ticker") or "").strip().upper()
            if not t or t in out:
                continue
            out[t] = {
                "lb_alarm": has_alarm and str(raw.get("lb_alarm") or "").lower()
                in ("true", "1", "yes"),
                "lb_fade": has_fade and str(raw.get("lb_fade") or "").lower()
                in ("true", "1", "yes"),
                "has_alarm_col": has_alarm,
                "has_fade_col": has_fade,
                "size": str(raw.get("size") or "").strip().lower(),
                "s_ab": _num(raw.get("s_ab")),
            }
    return out


def is_alarm_fade(marks: dict | None) -> bool:
    """Soft 🚨∧fade. Not OR'd with FPE. Missing columns = not flagged."""
    if not marks:
        return False
    return bool(marks.get("lb_alarm")) and bool(marks.get("lb_fade"))


def load_book_1d_buys(date: str) -> list[str]:
    path = BOOK_DIR / f"{date}_stock_book.json"
    if not path.exists():
        return []
    obj = json.loads(path.read_text(encoding="utf-8"))
    buys = ((obj.get("books") or {}).get("1d") or {}).get("buy") or []
    out = []
    seen = set()
    for rec in buys:
        t = str(rec.get("ticker") or rec.get("Ticker") or "").strip().upper()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def book_1d_dates() -> list[str]:
    return sorted(p.name[:10] for p in BOOK_DIR.glob("*_stock_book.json"))


class FeatureCache:
    """Session-D join (Theme Radar join_high) + prior AB + D book marks."""

    def __init__(self):
        self._join: dict[str, dict] = {}
        self._cut: dict[str, float | None] = {}
        self._ab: dict[str, dict] = {}
        self._ab_cal = ab_session_dates()
        self._book: dict[str, dict] = {}
        self._buys: dict[str, list[str]] = {}

    def join(self, date: str) -> dict[str, dict]:
        if date not in self._join:
            self._join[date] = load_join_map(date)
        return self._join[date]

    def cut(self, date: str) -> float | None:
        if date not in self._cut:
            self._cut[date] = join_high_cut(self.join(date))
        return self._cut[date]

    def ab(self, date: str) -> dict[str, dict]:
        if date not in self._ab:
            self._ab[date] = load_ab_map(date)
        return self._ab[date]

    def book(self, date: str) -> dict[str, dict]:
        if date not in self._book:
            self._book[date] = load_book_marks(date)
        return self._book[date]

    def buys(self, date: str) -> list[str]:
        if date not in self._buys:
            self._buys[date] = load_book_1d_buys(date)
        return self._buys[date]

    def jam(self, ticker: str, date: str) -> bool:
        prior = prior_ab_date(date, self._ab_cal)
        j = self.join(date).get(ticker)
        a = self.ab(prior).get(ticker) if prior else None
        bk = self.book(date).get(ticker) or {}
        return is_jam(j, self.cut(date), a, prior, extra_size=bk.get("size") or "")

    def jam_bits(self, ticker: str, date: str) -> dict:
        prior = prior_ab_date(date, self._ab_cal)
        j = self.join(date).get(ticker) or {}
        a = (self.ab(prior).get(ticker) if prior else None) or {}
        bk = self.book(date).get(ticker) or {}
        cut = self.cut(date)
        return {
            "join_hot": is_join_hot(j, cut),
            "ab_silent": is_ab_silent(a or None, prior),
            "size": j.get("size") or bk.get("size") or "",
            "size_ok": size_ok(j.get("size") or "", bk.get("size") or ""),
            "total_score": j.get("total_score"),
            "join_cut": cut,
            "prior_ab": prior,
            "s_ab": a.get("s_ab"),
        }

    def alarm_fade(self, ticker: str, date: str) -> bool:
        return is_alarm_fade(self.book(date).get(ticker))


def unit_rows(dates: list[str], picks: dict[str, list[str]],
              cal: list[str], bars: dict, fees: dict, hold: int,
              avoid_fn) -> list[dict]:
    rows = []
    for d in dates:
        today = bars.get(d) or {}
        tape = oh.spy_tape(today)
        win = oh.hold_window(cal, d, hold)
        if not win:
            continue
        exit_d = win[-1]
        for t in picks.get(d) or []:
            entry = oh.bar_at(bars, d, t, "open", None)
            exit_px = oh.bar_at(bars, exit_d, t, "close", None)
            tr = oh.unit_trade(entry, exit_px, fees)
            if tr is None:
                continue
            rows.append({
                "date": d,
                "ticker": t,
                "tape": tape,
                "avoid": bool(avoid_fn(t, d)),
                "pnl": tr["pnl"],
                "hit": tr["hit"],
                "ret_pct": tr["ret_pct"],
                "exit_date": exit_d,
            })
    return rows


def leftover_pair(cal: list[str], picks_base: dict[str, list[str]],
                  bars: dict, fees: dict, hold: int,
                  scores: dict[str, float | None],
                  avoid_fn) -> tuple[dict, dict, list[float], int, list[dict]]:
    picks_avoid: dict[str, list[str]] = {}
    skipped: list[dict] = []
    n_avoided = 0
    for d, raw in picks_base.items():
        kept = []
        for t in raw:
            if avoid_fn(t, d):
                n_avoided += 1
                skipped.append({"date": d, "ticker": t})
                continue
            kept.append(t)
        picks_avoid[d] = kept
    base = oh.leftover_book(cal, picks_base, bars, fees, hold, scores)
    avoid = oh.leftover_book(cal, picks_avoid, bars, fees, hold, scores)
    by_b = {x["date"]: x["pnl"] for x in base["daily"]}
    delta = [x["pnl"] - by_b.get(x["date"], 0.0) for x in avoid["daily"]]
    return base, avoid, delta, n_avoided, skipped


def _slim_book(book: dict) -> dict:
    return {k: book[k] for k in book if k != "trades"}


def score_sleeve(name: str, hold: int, universe: str, score_clock: str,
                 dates: list[str], picks: dict[str, list[str]],
                 cal: list[str], bars: dict, fees: dict,
                 scores: dict[str, float | None],
                 avoid_fn, rule: str) -> dict:
    rows = unit_rows(dates, picks, cal, bars, fees, hold, avoid_fn)
    ic = oh.ic_from_rows(rows)
    book_base, book_avoid, delta, n_avoided, skipped = leftover_pair(
        cal, picks, bars, fees, hold, scores, avoid_fn)
    gate = oh.decide(ic, book_base, book_avoid, delta, n_avoided,
                     book_kind="leftover")
    dd = oh.max_drawdown([x["equity"] for x in book_avoid["daily"]])
    return {
        "rule": rule,
        "sleeve": name,
        "hold_sessions": hold,
        "universe": universe,
        "score_clock": score_clock,
        "ic": ic,
        "book_base": _slim_book(book_base),
        "book_avoid": _slim_book(book_avoid),
        "max_dd": dd,
        "n_avoided_picks": n_avoided,
        "skipped": skipped,
        "gate": gate,
        "n_unit": len(rows),
    }


def _flatten_picks(flatten_days: list[dict]) -> dict[str, list[str]]:
    return {
        d["date"]: [t for t in (d.get("tickers") or [])[:oh.TOP_N]]
        for d in flatten_days
    }


def _book_picks(cal: list[str], feat: FeatureCache) -> dict[str, list[str]]:
    picks = {d: [] for d in cal}
    for d in book_1d_dates():
        picks[d] = feat.buys(d)
    return picks


def _any_pass(rows: list[dict]) -> bool:
    return any((r.get("gate") or {}).get("verdict") == "PASS" for r in rows)


def run_next_avoids() -> dict:
    """JAM first. Soft 🚨∧fade only if JAM does not PASS. No flatten_h5."""
    flatten_days = oh.load_flatten_days()
    book_dates = book_1d_dates()
    flat_cal = oh.trading_cal(flatten_days)
    book_cal = oh.trading_cal(flatten_days, book_dates)
    export_cal = oh.export_dates()
    need = sorted(set(export_cal + flat_cal + book_cal))
    bars = oh.load_bars(need)
    fees = oh.load_fees()
    feat = FeatureCache()
    scores = {d["date"]: d.get("score") for d in flatten_days}
    flatten_picks = _flatten_picks(flatten_days)
    book_picks = _book_picks(book_cal, feat)
    flatten_dates = [d["date"] for d in flatten_days]

    sleeves = (
        {
            "name": "flatten_h1",
            "hold": 1,
            "universe": "flatten",
            "score_clock": "09:30 leftover · min-hold 1 + fees",
            "dates": flatten_dates,
            "picks": flatten_picks,
            "cal": flat_cal,
        },
        {
            "name": "flatten_h3",
            "hold": 3,
            "universe": "flatten",
            "score_clock": "09:30 leftover · min-hold 3 + fees",
            "dates": flatten_dates,
            "picks": flatten_picks,
            "cal": flat_cal,
        },
        {
            "name": "book_1d",
            "hold": 1,
            "universe": "book_1d",
            "score_clock": "book 1d BUY leftover · min-hold 1 + fees",
            "dates": book_dates,
            "picks": book_picks,
            "cal": book_cal,
        },
    )

    jam_rows = []
    for sl in sleeves:
        jam_rows.append(score_sleeve(
            sl["name"], sl["hold"], sl["universe"], sl["score_clock"],
            sl["dates"], sl["picks"], sl["cal"], bars, fees, scores,
            feat.jam, "jam"))

    fade_rows = []
    jam_pass = _any_pass(jam_rows)
    if not jam_pass:
        for sl in sleeves:
            fade_rows.append(score_sleeve(
                sl["name"], sl["hold"], sl["universe"], sl["score_clock"],
                sl["dates"], sl["picks"], sl["cal"], bars, fees, scores,
                feat.alarm_fade, "alarm_fade"))

    fade_pass = _any_pass(fade_rows)
    any_pass = jam_pass or fade_pass

    jam_skips = []
    for r in jam_rows:
        for s in r.get("skipped") or []:
            bits = feat.jam_bits(s["ticker"], s["date"])
            jam_skips.append({**s, **bits, "sleeve": r["sleeve"]})

    fade_skips = []
    for r in fade_rows:
        for s in r.get("skipped") or []:
            fade_skips.append({**s, "sleeve": r["sleeve"]})

    n_micro = sum(1 for s in jam_skips if s.get("size") == "micro")
    n_small = sum(1 for s in jam_skips if s.get("size") == "small")

    stop = (not any_pass)
    return {
        "generated_note": "research only · live flatten_robust untouched",
        "rule_jam": (
            "join-hot (session D total_score ≥ p80, n≥20, cut≥0) "
            "∧ AB-silent (prior AB date < D missing/rowless/|s_ab|<0.05) "
            "∧ size micro|small"
        ),
        "rule_fade": "lb_alarm ∧ lb_fade on D morning stock book; not OR with FPE",
        "not_or_with_fpe": True,
        "fpe_clocks_closed": ["flatten_h5"],
        "elevate_closed": True,
        "sleeves_scored": [s["name"] for s in sleeves],
        "jam": jam_rows,
        "alarm_fade": fade_rows,
        "jam_ran_fade": not jam_pass,
        "jam_skips": jam_skips,
        "fade_skips": fade_skips,
        "jam_size_split": {"micro": n_micro, "small": n_small},
        "any_pass": any_pass,
        "jam_pass": jam_pass,
        "fade_pass": fade_pass,
        "clean_stop": stop,
        "named_autopsy": {
            "VERI": "micro · join-hot on 08-14 · not flatten top-8 / book 1d BUY",
            "AEVA": "small · join-hot on 08-14 · not flatten top-8 / book 1d BUY",
            "RXT": "small · join-hot on 08-14 · not flatten top-8 / book 1d BUY",
            "ACMR": "mid · size gate excludes (not a cherrypick drop)",
        },
    }


def _fmt(v, pct=False) -> str:
    return oh._fmt(v, pct=pct)


def _row_line(r: dict) -> str:
    ic, g = r["ic"], r["gate"]
    conc = g.get("concentration") or {}
    hit = f"{_fmt(ic.get('hit'), True)} / {_fmt(ic.get('hit_peer'), True)}"
    tape = ic.get("both_tape")
    tape_s = "YES" if tape is True else ("NO" if tape is False else "thin-n")
    return (
        f"| `{r['sleeve']}` | {r['hold_sessions']} | {r['score_clock']} | "
        f"{ic.get('n')} | {hit} | {_fmt(ic.get('xs'))} | "
        f"{_fmt(g.get('excess_usd'))} | {_fmt(r.get('max_dd'), True)} | "
        f"{_fmt(conc.get('top2'), True)} | {tape_s} | "
        f"**{g.get('verdict')}** |"
    )


def render_section(payload: dict) -> list[str]:
    jam = payload.get("jam") or []
    fade = payload.get("alarm_fade") or []
    split = payload.get("jam_size_split") or {}
    lines = [
        "## Next experiment — join-hot ∧ AB-silent micros (not FPE)",
        "",
        "Kid: The expensive-sticker skip is done. Next we skip the tiny "
        "names the crowd is yelling about while the report card is blank. "
        "If that is just a handful of days, we stop.",
        "",
        "FPE / d_RSI / d_mcap stay **closed** on `flatten_h5`. This Avoid "
        "is **not** OR'd with FPE. Elevate stays closed. No live wire.",
        "",
        "Pre-register (09:30):",
        "",
        f"- **join-hot:** session-D `data/join/{{D}}_ranked.csv` "
        f"`total_score` ≥ p{int(TS_TOP_Q * 100)} "
        f"(n≥{JOIN_MIN_N} scores and cut ≥ {JOIN_MIN_CUT:g}; "
        "08-18 p80 = −1 is discarded, not a fire).",
        "- **AB-silent:** last `ab_checklist_enriched.csv` (else plain) "
        "with date **< D** is missing, has no row, or `|s_ab|` < "
        f"{AB_EPS} (`s_ab` = clip(raw/12). `ticker_lookback.EPS`). "
        "08-13/14 have no AB — everyone silent (honest).",
        "- **size:** join/book `micro` **or** `small` (VERI + AEVA/RXT). "
        "`mid` (ACMR) excluded. Micro-only is a footnote, not a promote.",
        "- **sleeves:** `flatten_h1` / `flatten_h3` leftover + unit; "
        "`book_1d` BUY leftover + unit. **Not** `flatten_h5`.",
        "- Same bar: 09:30 open, Futubull, peer-excess both-tape, "
        "leftover Δ$, top-2 |ΔP&L| ≥ 65% → FAIL.",
        "",
        "| sleeve | hold | clock | n avoided | hit vs peer | xs $ | leftover Δ$ | max DD | top-2 day | both-tape | verdict |",
        "|---|---:|---|---:|---|---:|---:|---:|---:|---|---|",
    ]
    for r in jam:
        lines.append(_row_line(r))
    lines += ["", "### Why JAM", ""]
    for r in jam:
        why = "; ".join(r["gate"].get("reasons") or [])
        lines.append(f"- `{r['sleeve']}` **{r['gate']['verdict']}** — {why}.")

    skips = payload.get("jam_skips") or []
    by_sleeve: dict[str, list[str]] = {}
    for s in skips:
        by_sleeve.setdefault(s["sleeve"], []).append(
            f"{s['ticker']} ({s['date']})"
        )
    lines += [
        "",
        "### JAM skips (wish-list / book BUY — not live tickets)",
        "",
        f"Size split among skipped picks: micro={split.get('micro', 0)} "
        f"small={split.get('small', 0)}. Named autopsy VERI/AEVA/RXT were "
        "book-gap worst buys, **not** flatten top-8 or book 1d BUY — the "
        "mechanism is tested on the sleeves we can trade, not harvested "
        "as a name list.",
        "",
    ]
    if by_sleeve:
        for sl, names in by_sleeve.items():
            uniq = []
            seen = set()
            for n in names:
                if n not in seen:
                    seen.add(n)
                    uniq.append(n)
            lines.append(f"- `{sl}`: {', '.join(uniq) if uniq else 'none'}.")
    else:
        lines.append("No JAM fires on these sleeves.")

    lines += [
        "",
        "## Soft 🚨∧fade (only because JAM did not PASS)",
        "",
        "`lb_alarm` **and** `lb_fade` on **D's morning stock book**. "
        "Early books have no columns (honest empty). **Not** OR'd with FPE. "
        "Matching hold only.",
        "",
    ]
    if not fade:
        lines.append("Not run — a JAM sleeve PASSed.")
    else:
        lines += [
            "| sleeve | hold | clock | n avoided | hit vs peer | xs $ | leftover Δ$ | max DD | top-2 day | both-tape | verdict |",
            "|---|---:|---|---:|---|---:|---:|---:|---:|---|---|",
        ]
        for r in fade:
            lines.append(_row_line(r))
        lines += ["", "### Why 🚨∧fade", ""]
        for r in fade:
            why = "; ".join(r["gate"].get("reasons") or [])
            lines.append(f"- `{r['sleeve']}` **{r['gate']['verdict']}** — {why}.")
        fade_skips = payload.get("fade_skips") or []
        if fade_skips:
            bits = ", ".join(f"{s['ticker']} ({s['date']})" for s in fade_skips)
            lines += ["", f"Fires: {bits}."]
        else:
            lines += ["", "Veto never fired on flatten top-8 or book 1d BUY."]

    lines += [
        "",
        "## Clean stop / recommended pause",
        "",
    ]
    if payload.get("any_pass"):
        lines.append(
            "An Avoid cleared. Elevate stays closed until Cyrus opens it. "
            "Still no live wire."
        )
    else:
        lines.append(
            "**Clean stop.** Join-hot ∧ AB-silent (micro/small) is honest "
            "thin-n / concentration / null on `flatten_h1`, `flatten_h3`, "
            "and `book_1d`. Soft 🚨∧fade never fired on those sleeves "
            "(columns empty early; later 🚨∧fade names are not on the "
            "BUY / flatten wish-list). Neither Avoid clears the ship bar."
        )
        lines.append("")
        lines.append(
            "**Recommended pause:** do not keep mining join p80 / AB-silent "
            "/ 🚨∧fade knobs on these clocks, and do **not** reopen FPE / "
            "d_RSI / d_mcap on `flatten_h5`. Elevate stays closed. No live "
            "wire. No merge. Wait for a new 09:30 camera or a longer AB "
            "vintage — not another cut on the same thin fires."
        )
    lines.append("")
    return lines


def render(payload: dict) -> str:
    return "\n".join(render_section(payload)) + "\n"


def attach_to_horizon(horizon: dict | None = None) -> dict:
    """Stamp existing FPE JSON with next Avoids. Does not remine FPE."""
    path = oh.OUT_JSON
    if horizon is None:
        if path.exists():
            horizon = json.loads(path.read_text(encoding="utf-8"))
        else:
            horizon = oh.run()
    oh.stamp_local_5d_board(horizon.get("picked") or [])
    oh.stamp_local_5d_board(horizon.get("results") or [])
    horizon["local_5d_fpe_board"] = oh.LOCAL_5D_FPE_BOARD
    horizon["fpe_clocks_open"] = ["theme_radar_1d"]
    horizon["fpe_clocks_closed"] = ["flatten_h5"]
    nxt = run_next_avoids()
    horizon["next_avoids"] = nxt
    horizon["any_next_pass"] = bool(nxt.get("any_pass"))
    horizon["clean_stop"] = bool(nxt.get("clean_stop"))
    return horizon


def write(horizon: dict | None = None) -> dict:
    payload = attach_to_horizon(horizon)
    oh.OUT_JSON.write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8")
    oh.OUT_MD.write_text(oh.render(payload), encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    if args.write:
        payload = write()
        print(f"wrote {oh.OUT_MD} · {oh.OUT_JSON}")
    else:
        payload = run_next_avoids()
        print(render(payload))
    for r in (payload.get("next_avoids") or payload).get("jam") or payload.get("jam") or []:
        print(f"JAM {r['sleeve']}: {r['gate']['verdict']} "
              f"n={r['ic']['n']} Δ$={r['gate'].get('excess_usd')}")
    fade = (payload.get("next_avoids") or payload).get("alarm_fade") or payload.get("alarm_fade") or []
    for r in fade:
        print(f"FADE {r['sleeve']}: {r['gate']['verdict']} "
              f"n={r['ic']['n']} Δ$={r['gate'].get('excess_usd')}")


if __name__ == "__main__":
    main()
