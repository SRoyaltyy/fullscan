"""Research-only CLEAR letter panel for Theme Radar’s join mine.

Clock-clean CSV: date,ticker,FQ,ER,EP,AH,FR,DF_lag1

FQ/ER/EP/AH/FR are same-row open value_mine_open tallies (prior-print),
knowable at 09:30 on `date`. DF_lag1 is yesterday’s DF pattern text
(close same-row; open-legal only as lag t−1+).

Never peeks same-row DF/BB/BQ, H/I, M number, core_score, or H paint.
Does not import or write flatten_robust / the cash book. Not a live wire.

  python3 excel_bot/engine/excel_clear_letter_panel.py
"""
from __future__ import annotations

import csv
import json
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from glob import glob

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    OPEN_44_TALLY_SUBS, SAME_ROW_LEAK_ABORT, assert_excel_clock_gate,
    assert_feature_legal, gate_payload,
)
from excel_open_features import (  # noqa: E402
    assert_lag_atoms, assert_recipes_legal, df_pattern, open_features,
)
from excel_open_gates import load_ohlc_bars  # noqa: E402
from fastfetch import fetch_daily_fast  # noqa: E402
from join_post_813 import (  # noqa: E402
    BOOK_DIR, is_session, load_book_1d, load_finviz, load_join_days,
)
from j_universe_prove import PRIOR_VOL_LIQ, _try_pyarrow  # noqa: E402

RESEARCH = os.path.join(ROOT, "research")
CSV_PATH = os.path.join(RESEARCH, "excel_clear_letter_panel.csv")
MD_PATH = os.path.join(RESEARCH, "EXCEL_CLEAR_LETTER_PANEL.md")
JSON_PATH = os.path.join(RESEARCH, "excel_clear_letter_panel.json")
OHLC_PATH = os.path.join(REPO, "data", "prices", "ohlc.parquet")

HEADER = ("date", "ticker", "FQ", "ER", "EP", "AH", "FR", "DF_lag1")
OPEN_TALLIES = ("FQ", "ER", "EP", "AH", "FR")
# Theme Radar overlap mornings (skip weekends as *sessions*; still emit
# listed Sunday dumps 08-30 / 09-06 as Theme Radar join dates).
PANEL_DATES = (
    "2026-08-13", "2026-08-14",
    "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21",
    "2026-08-27",
    "2026-08-30", "2026-08-31",
    "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04",
    "2026-09-06",
    "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11",
)
HOLIDAYS = frozenset({"2026-09-07"})  # Labor Day
MIN_PRIOR = 6  # AH window; FR needs 5; EP needs 2
YAHOO_WORKERS = 16
# Yahoo bars needed as prior (missing from parquet + weekday Finviz).
YAHOO_GAP_DATES = ("2026-08-26", "2026-09-08", "2026-09-09", "2026-09-10")
# 08-27 may lack Yahoo 08-26; last true session 08-25 is documented fallback.
PRIOR_FALLBACK = {"2026-08-27": "2026-08-25"}


def is_true_session(iso):
    """US equity session: weekday and not Labor Day."""
    return is_session(iso) and iso not in HOLIDAYS


def expected_prior_session(iso):
    """Last true session strictly before iso (walks calendar days)."""
    d = date.fromisoformat(iso)
    for _ in range(12):
        d -= timedelta(days=1)
        s = d.isoformat()
        if is_true_session(s):
            return s
    return None


def prior_is_fresh(iso, prior_date):
    """Refuse a DF_lag1 whose last bar is older than the last true session.

    08-27 may fall back to 08-25 when Yahoo 08-26 is missing (documented gap).
    """
    if not prior_date:
        return False
    exp = expected_prior_session(iso)
    if prior_date == exp:
        return True
    return PRIOR_FALLBACK.get(iso) == prior_date


def _num_cell(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != v:  # NaN
            return ""
        if v == int(v) and abs(v) < 1e12:
            return str(int(v))
        return f"{v:.8g}"
    return str(v)


def leak_check():
    """Abort if same-row DF/BB/BQ is treated as a morning feature."""
    assert_excel_clock_gate()
    assert_lag_atoms()
    assert_recipes_legal()
    for col in SAME_ROW_LEAK_ABORT:
        try:
            assert_feature_legal("value", col, 0)
        except ValueError:
            pass
        else:
            raise ValueError(f"LEAK abort: same-row {col} did not abort")
        assert_feature_legal("value", col, 1)
    for col in OPEN_TALLIES:
        if col not in OPEN_44_TALLY_SUBS:
            raise ValueError(f"{col} is not an open-44 tally sub")
        assert_feature_legal("value", col, 0)
    assert_feature_legal("value", "DF", 1)
    return "PASS"


def load_green_tickers():
    by = {}
    for path in sorted(glob(os.path.join(BOOK_DIR, "????-??-??_green.json"))):
        iso = os.path.basename(path)[:10]
        try:
            raw = json.load(open(path, encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        names = []
        for t in raw.get("tickers") or []:
            t = str(t).strip().upper()
            if t:
                names.append(t)
        if not names:
            for rec in raw.get("live_buy") or []:
                if isinstance(rec, dict):
                    t = (rec.get("ticker") or "").strip().upper()
                    if t:
                        names.append(t)
        if names:
            by[iso] = names
    return by


def _prior_stamp(keys, iso):
    """PIT: last file strictly before iso (any dump date)."""
    prev = [k for k in keys if k < iso]
    return prev[-1] if prev else None


def book_overlap_by_date(joins, books, greens):
    """Morning date -> tickers Theme Radar can cross (join / book / green)."""
    j_keys, b_keys, g_keys = sorted(joins), sorted(books), sorted(greens)
    out = {}
    for iso in PANEL_DATES:
        names = set()
        if iso in joins:
            names.update(r["ticker"] for r in joins[iso])
        else:
            pk = _prior_stamp(j_keys, iso)
            if pk:
                names.update(r["ticker"] for r in joins[pk])
        pk = _prior_stamp(b_keys, iso)
        if pk:
            names.update(r["ticker"] for r in books[pk])
        pk = _prior_stamp(g_keys, iso)
        if pk:
            names.update(greens[pk])
        out[iso] = {t for t in names if t}
    return out


def _put_bar(hist, src, ticker, iso, o, h, l, c, v):
    if not ticker or not iso or not is_true_session(iso):
        return False
    if o is None or h is None or l is None or c is None or o <= 0:
        return False
    rec = {
        "o": float(o), "h": float(h), "l": float(l), "c": float(c),
        "v": float(v or 0), "src": src,
    }
    existing = hist[ticker]
    for i, (d, _) in enumerate(existing):
        if d == iso:
            # Prefer Yahoo/parquet over Finviz when both exist.
            if src in ("yahoo", "parquet") and existing[i][1].get("src") == "finviz":
                existing[i] = (iso, rec)
                return True
            return False
        if d > iso:
            existing.insert(i, (iso, rec))
            return True
    existing.append((iso, rec))
    return True


def ingest_parquet(hist):
    n = 0
    if not (_try_pyarrow() and os.path.isfile(OHLC_PATH)):
        return n
    raw = load_ohlc_bars()
    for t, bars in raw.items():
        t = str(t).strip().upper()
        for iso, o, h, l, c, v in bars:
            if _put_bar(hist, "parquet", t, iso, o, h, l, c, v):
                n += 1
    return n


def ingest_finviz(hist, fz):
    n = 0
    for iso, mp in fz.items():
        if not is_true_session(iso):
            continue
        for t, rec in mp.items():
            if _put_bar(
                hist, "finviz", t, iso,
                rec.get("open"), rec.get("high"), rec.get("low"),
                rec.get("close"), rec.get("vol"),
            ):
                n += 1
    return n


def _yahoo_symbol(ticker):
    t = (ticker or "").strip().upper()
    if not t:
        return t
    return t.replace(".", "-")


def _fetch_one(ticker, start, end):
    sym = _yahoo_symbol(ticker)
    rows = fetch_daily_fast(sym, start, end)
    return ticker, rows


def ingest_yahoo(hist, tickers, start, end, workers=YAHOO_WORKERS):
    """Fetch [start, end] Yahoo dailies for tickers missing gap sessions."""
    need = []
    gap = [d for d in YAHOO_GAP_DATES if start.isoformat() <= d <= end.isoformat()]
    for t in sorted(tickers):
        have = {d for d, _ in hist.get(t) or []}
        if any(d not in have for d in gap):
            need.append(t)
    ok, fail = 0, []
    if not need:
        return ok, fail, 0
    print(f"yahoo fetch n={len(need)} {start}..{end}", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(_fetch_one, t, start, end): t for t in need
        }
        for i, fut in enumerate(as_completed(futs), 1):
            t = futs[fut]
            try:
                _ticker, rows = fut.result()
                for r in rows:
                    iso = r["date"].isoformat() if hasattr(r["date"], "isoformat") else str(r["date"])
                    _put_bar(
                        hist, "yahoo", t, iso,
                        r.get("open"), r.get("high"), r.get("low"),
                        r.get("close"), r.get("volume"),
                    )
                ok += 1
            except Exception as e:  # noqa: BLE001
                fail.append((t, str(e)[:160]))
            if i % 250 == 0:
                print(f"  yahoo {i}/{len(need)} ok={ok} fail={len(fail)}", flush=True)
    return ok, fail, len(need)


def liquid_asof(hist, iso):
    """Tickers whose last true-session bar before iso has vol ≥ 1M."""
    out = set()
    for t, bars in hist.items():
        prior = [(d, r) for d, r in bars if d < iso]
        if not prior:
            continue
        if prior[-1][1].get("v", 0) >= PRIOR_VOL_LIQ:
            out.add(t)
    return out


def prior_feature_bars(hist, ticker, iso):
    """Completed true-session bars strictly before iso. No same-row peek."""
    out = []
    for d, r in hist.get(ticker) or []:
        if d >= iso:
            break
        out.append({
            "o": r["o"], "h": r["h"], "l": r["l"], "c": r["c"], "v": r["v"],
            "date": d,
        })
    return out


def row_from_prior(iso, ticker, prior):
    """One clock-clean panel row. prior must not include `iso`."""
    if any(b.get("date") == iso for b in prior):
        raise ValueError(f"LEAK abort: same-row bar in prior {ticker} {iso}")
    if len(prior) < MIN_PRIOR:
        return None
    today_open = None  # tallies + DF_lag1 do not use today's open
    xl = open_features(prior[-20:], today_open)
    if xl.get("same_row_df") or xl.get("same_row_bb") or xl.get("same_row_bq"):
        raise ValueError(f"LEAK abort: same-row DF/BB/BQ {ticker} {iso}")
    last = prior[-1]
    expect = df_pattern(last)
    if xl.get("df") != expect:
        raise ValueError(f"DF_lag1 drift {ticker} {iso}: {xl.get('df')} != {expect}")
    if last.get("date") >= iso:
        raise ValueError(f"LEAK abort: DF_lag1 bar date {last.get('date')} >= {iso}")
    if not prior_is_fresh(iso, last.get("date")):
        return None
    return {
        "date": iso,
        "ticker": ticker,
        "FQ": xl.get("FQ"),
        "ER": xl.get("ER"),
        "EP": xl.get("EP"),
        "AH": xl.get("AH"),
        "FR": xl.get("FR"),
        "DF_lag1": xl.get("df") or "None",
        "_prior_date": last.get("date"),
        "_n_prior": len(prior),
    }


def build_rows(hist, universe_by_date):
    rows = []
    skipped = defaultdict(int)
    for iso in PANEL_DATES:
        names = sorted(universe_by_date.get(iso) or [])
        n_ok = 0
        for t in names:
            prior = prior_feature_bars(hist, t, iso)
            rec = row_from_prior(iso, t, prior)
            if rec is None:
                skipped[iso] += 1
                continue
            rows.append(rec)
            n_ok += 1
        print(f"  {iso} universe={len(names)} rows={n_ok} skip={skipped[iso]}", flush=True)
    return rows, dict(skipped)


def write_csv(rows, path=CSV_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=HEADER, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({
                "date": r["date"],
                "ticker": r["ticker"],
                "FQ": _num_cell(r.get("FQ")),
                "ER": _num_cell(r.get("ER")),
                "EP": _num_cell(r.get("EP")),
                "AH": _num_cell(r.get("AH")),
                "FR": _num_cell(r.get("FR")),
                "DF_lag1": r.get("DF_lag1") or "None",
            })
    return path


def _count_by(rows, key):
    out = defaultdict(int)
    for r in rows:
        out[r[key]] += 1
    return dict(sorted(out.items()))


def summarize(rows, skipped, hist, universe_by_date, yahoo_stats, leak):
    by_date = _count_by(rows, "date")
    tickers = sorted({r["ticker"] for r in rows})
    hammer = sum(
        1 for r in rows
        if "Hammer" in (r.get("DF_lag1") or "") and "Inverted" not in (r.get("DF_lag1") or "")
    )
    src_counts = defaultdict(int)
    for bars in hist.values():
        for _d, r in bars:
            src_counts[r.get("src") or "?"] += 1
    gap_cov = {}
    for d in YAHOO_GAP_DATES:
        n = sum(1 for t, bars in hist.items() if any(x[0] == d for x in bars))
        gap_cov[d] = n
    return {
        "generated": str(date.today()),
        "header": list(HEADER),
        "leak": leak,
        "live_untouched": "flatten_robust",
        "clock": {
            "join": "Finviz after-close T × this panel on morning session T+1 "
                    "(values known 09:30 T+1)",
            "open_tallies": "FQ,ER,EP,AH,FR same-row open (prior-print)",
            "df": "DF_lag1 = DF[t−1] text; same-row DF is a leak (abort)",
            "never": ["same-row DF/BB/BQ", "same-row H/I", "M number",
                      "core_score", "H paint"],
            "holidays_skipped_as_sessions": sorted(HOLIDAYS),
            "weekend_mornings_emitted": ["2026-08-30", "2026-09-06"],
            "gate": gate_payload(),
        },
        "universe": {
            "rule": "liquid (prior-session volume ≥ 1,000,000) ∪ book-overlap "
                    "(that morning’s join ranked ∪ PIT prior 1d-buy ∪ PIT prior green pile)",
            "n_tickers": len(tickers),
            "prior_vol_liq": PRIOR_VOL_LIQ,
            "min_prior_bars": MIN_PRIOR,
            "n_universe_by_date": {d: len(universe_by_date.get(d) or []) for d in PANEL_DATES},
        },
        "n_rows": len(rows),
        "n_rows_by_date": by_date,
        "n_skipped_thin_history": skipped,
        "n_lag_hammer": hammer,
        "bar_sources": dict(src_counts),
        "yahoo": {
            "gap_dates": list(YAHOO_GAP_DATES),
            "ok": yahoo_stats.get("ok"),
            "fail_n": len(yahoo_stats.get("fail") or []),
            "tried": yahoo_stats.get("tried"),
            "fail_head": (yahoo_stats.get("fail") or [])[:12],
            "bars_on_gap_dates": gap_cov,
        },
        "tickers": tickers,
        "csv": os.path.relpath(CSV_PATH, REPO),
        "readme": os.path.relpath(MD_PATH, REPO),
    }


def write_readme(payload):
    gp = payload["clock"]["gate"]
    by = payload["n_rows_by_date"]
    lines = [
        "# Excel CLEAR letter panel — Theme Radar join mine",
        "",
        f"_Generated {payload['generated']} · **research only** · live `flatten_robust` frozen._",
        "",
        "## What this is",
        "",
        "A clock-clean CSV Theme Radar can join on "
        "`date,ticker,FQ,ER,EP,AH,FR,DF_lag1`.",
        "",
        "Values are **raw open-knowable letters** from `excel_open_features.open_features` "
        "/ `OPEN_SAME_ROW_LABELS` / `CLOCK_MAP`. Not book P&L. Not a live wire. "
        "`flatten_robust` and the cash book are not imported or changed.",
        "",
        "## Clock (hard)",
        "",
        "- **Join clock Theme Radar uses:** Finviz after-close **T** × this panel on "
        "morning session **T+1** (values known 09:30 T+1).",
        "- **FQ, ER, EP, AH, FR** — same-row **open** `value_mine_open` tallies "
        "(prior-print). Knowable at 09:30 on that `date`. In the locked 44.",
        "- **DF_lag1** — yesterday’s DF pattern text (`open_features` `df` on the last "
        "*completed* prior bar). DF is **close same-row**; only **lag t−1+** is "
        "open-legal. Same-row DF/BB/BQ at 09:30 is a **leak** — abort.",
        "- Never peek same-row H/I, M number, `core_score`, or H paint.",
        f"- Leak check: **{payload['leak']}**.",
        f"- Fill OPEN same-row: `{', '.join(gp['fill_open'])}`",
        f"- Value OPEN same-row (44): `{', '.join(gp['value_mine_open'])}`",
        f"- Same-row leak abort: `{', '.join(gp['same_row_leak_abort'])}`",
        f"- Open-44 tally subs: `{', '.join(gp['open_44_tally_subs'])}`",
        "",
        "## How to join",
        "",
        "1. Take Finviz after-close survivors dated **T** (Theme Radar’s tape).",
        "2. Left-join this panel on `ticker` where `panel.date = T+1` "
        "(next Theme Radar morning in `PANEL_DATES`, skipping weekends / Labor Day "
        "as *sessions*).",
        "3. Apply gates on the **raw columns** (do not treat this file as a book):",
        "   - avoid `FQ == 1`",
        "   - avoid `ER == 1` / `EP >= 0.03` / `AH >= 1`",
        "   - elevate lag-Hammer: `\"Hammer\" in DF_lag1` and `\"Inverted\" not in DF_lag1` "
        "(matches `feature_flags.prior_hammer`)",
        "   - elevate `FR >= 1`",
        "4. **EP units:** fraction from `open_features` (0.03 = 3%). The board recipe "
        "is `avoid_EP_ge03`. A brief that says `EP≥0.3` is **not** this CSV’s unit — "
        "do not scale EP to percent.",
        "",
        "Sunday Theme Radar mornings **2026-08-30** and **2026-09-06** are emitted. "
        "Prior bars for those rows are the last true weekday session (08-28 / 09-04). "
        "Labor Day **2026-09-07** is not a session and is not a panel date.",
        "",
        "## Universe",
        "",
        payload["universe"]["rule"] + ".",
        "",
        f"- Unique tickers in the CSV: **{payload['universe']['n_tickers']}** "
        f"(full list in `excel_clear_letter_panel.json`).",
        f"- Liquidity cut: prior-session volume ≥ {payload['universe']['prior_vol_liq']:,} "
        "(same as the open-gates liquid tape).",
        f"- A name-day needs ≥ {payload['universe']['min_prior_bars']} completed "
        "true-session bars before `date` (AH is a 6-session count).",
        "- Book-overlap uses **prior-day** `*_stock_book.json` 1d-buy and "
        "`*_green.json` (afternoon stamp → PIT). Same-day stock-book JSON is "
        "afternoon and is not an open feature.",
        "",
        "## Row counts",
        "",
        f"Total rows: **{payload['n_rows']}**. Lag-Hammer prints: "
        f"{payload['n_lag_hammer']}.",
        "",
        "| date | rows | universe | skipped (thin history) | weekday |",
        "|---|---:|---:|---:|---|",
    ]
    wd = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for iso in PANEL_DATES:
        d = date.fromisoformat(iso)
        lines.append(
            f"| {iso} | {by.get(iso, 0)} | "
            f"{payload['universe']['n_universe_by_date'].get(iso, 0)} | "
            f"{payload['n_skipped_thin_history'].get(iso, 0)} | {wd[d.weekday()]} |"
        )
    y = payload["yahoo"]
    lines += [
        "",
        "## Gaps / sources",
        "",
        f"- Bar sources in the stitch: `{payload['bar_sources']}`.",
        "- `data/prices/ohlc.parquet` last date **2026-08-21** (Yahoo/cache).",
        "- Finviz weekday dumps stitch 08-13…09-04 (no 08-26 export). "
        "Weekend / Labor Day Finviz dumps are **not** used as session bars.",
        f"- Yahoo fill for missing prior sessions {list(y['gap_dates'])}: "
        f"tried {y['tried']}, ok {y['ok']}, fail {y['fail_n']}. "
        f"Bars landed: `{y['bars_on_gap_dates']}`.",
        "- **2026-08-27:** if Yahoo 08-26 is missing, DF_lag1 may use **08-25** "
        "(documented fallback). Older priors are not emitted as lag-1.",
        "- **2026-09-08:** prior true session is 09-04 (weekend + Labor Day). "
        "No Yahoo 09-08 bar is required for that morning’s letters.",
        "- **2026-09-09…09-11:** need Yahoo 09-08 / 09-09 / 09-10. Yahoo fill "
        "is liquid ∪ book/green ∪ join top-80 (~3.8k names). Other join-only "
        "names are omitted here so DF_lag1 is not Friday 09-04 mislabeled as "
        "yesterday. ~3.5k rows remain — enough for an n≥30 fire bar.",
        "",
        "## Rebuild",
        "",
        "```",
        "python3 excel_bot/engine/excel_clear_letter_panel.py",
        "```",
        "",
        "Reuse only: `excel_clock_gate`, `excel_open_features`, parquet / Finviz / "
        "`fastfetch` Yahoo. No new letter formulas.",
        "",
        "## Explicitly not live",
        "",
        "Research artifact for Theme Radar’s join mine. Do not wire "
        "`flatten_robust`, `LIVE_POLICY`, `join_rules.json`, or the cash book.",
        "",
    ]
    with open(MD_PATH, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return MD_PATH


def run(yahoo=True):
    leak = leak_check()
    print("leak", leak, flush=True)
    fz = load_finviz()
    joins = load_join_days()
    books = load_book_1d()
    greens = load_green_tickers()
    overlap = book_overlap_by_date(joins, books, greens)
    hist = defaultdict(list)
    print("ingest parquet …", flush=True)
    n_pq = ingest_parquet(hist)
    print(f"  parquet bars={n_pq} tickers={len(hist)}", flush=True)
    print("ingest finviz sessions …", flush=True)
    n_fz = ingest_finviz(hist, fz)
    print(f"  finviz bars added={n_fz}", flush=True)

    # Universe: liquid-asof each morning ∪ book-overlap that morning.
    universe = {}
    all_names = set()
    for iso in PANEL_DATES:
        liq = liquid_asof(hist, iso)
        names = set(overlap.get(iso) or []) | liq
        universe[iso] = names
        all_names |= names
    print(f"universe unique={len(all_names)}", flush=True)

    # Yahoo only the names that need gap sessions: liquid + book/green +
    # join top-80 (full join tape is too heavy for a one-shot v8 fetch).
    yahoo_want = set()
    for iso in PANEL_DATES:
        yahoo_want |= liquid_asof(hist, iso)
        if iso in joins:
            yahoo_want.update(r["ticker"] for r in joins[iso][:80])
    for names in books.values():
        yahoo_want.update(r["ticker"] for r in names)
    for names in greens.values():
        yahoo_want.update(names)
    yahoo_want &= all_names

    yahoo_stats = {"ok": 0, "fail": [], "tried": 0}
    if yahoo:
        start = date(2026, 8, 24)
        end = date(2026, 9, 11)
        ok, fail, tried = ingest_yahoo(hist, yahoo_want, start, end)
        yahoo_stats = {"ok": ok, "fail": fail, "tried": tried}
        print(f"yahoo ok={ok} fail={len(fail)} tried={tried}", flush=True)
        # Recompute liquid after Yahoo (08-26 vol now known).
        for iso in PANEL_DATES:
            universe[iso] = set(overlap.get(iso) or []) | liquid_asof(hist, iso)
            all_names |= universe[iso]

    print("emit rows …", flush=True)
    rows, skipped = build_rows(hist, universe)
    write_csv(rows)
    payload = summarize(rows, skipped, hist, universe, yahoo_stats, leak)
    write_readme(payload)
    slim = {k: v for k, v in payload.items() if k != "tickers"}
    slim["n_tickers"] = payload["universe"]["n_tickers"]
    slim["tickers_head"] = payload["tickers"][:40]
    json.dump(payload, open(JSON_PATH, "w"), indent=2)
    print("wrote", CSV_PATH, MD_PATH, JSON_PATH, flush=True)
    print("rows", payload["n_rows"], "tickers", payload["universe"]["n_tickers"],
          "leak", leak, flush=True)
    for iso in PANEL_DATES:
        print(f"  {iso} {payload['n_rows_by_date'].get(iso, 0)}", flush=True)
    return payload


if __name__ == "__main__":
    yahoo = "--no-yahoo" not in sys.argv
    run(yahoo=yahoo)
