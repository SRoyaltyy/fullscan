"""Out-of-sample strategy mine OOS-0914.

Train uses only sessions 2026-08-13..2026-09-11 and a price store whose
last bar is on or before 2026-09-11. A file named on or after 2026-09-14
is not opened. Chosen rules are frozen as ``oos0914_*`` and then walked
one locked session at a time by ``factor_mine_sequential``.

Research only. This module does not send orders and does not rewrite
HOT4, holdup, flatten_robust, or the live factor-mine books.
"""
from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_freeze as fmf
from . import factor_mine_rules as fmr
from . import factor_mine_retro as retro
from . import factor_mine_sequential as seq

ROOT = fm.ROOT
PREREG_PATH = ROOT / "data" / "factor_mine" / "oos0914_preregister.json"
OUT_DIR = ROOT / "data" / "factor_mine" / "oos0914"
TRAIN_REPORT = OUT_DIR / "train_report.json"
FROZEN_PATH = OUT_DIR / "frozen_rules.json"
TEST_REPORT = OUT_DIR / "test_report.json"
STATE_ROOT = OUT_DIR / "state"
LEDGER_DIR = OUT_DIR / "ledgers"
FROZEN_LIST = OUT_DIR / "frozen_list.json"
SCOREBOARD = ROOT / "03_scoreboard" / "FACTOR_MINE_OOS0914.md"
LIVE_OHLC = ROOT / "data" / "prices" / "ohlc.parquet"
LIVE_META = ROOT / "data" / "prices" / "meta.json"
RETRO_OHLC = retro.RETRO_STORE
RETRO_META = retro.RETRO_META
SNAP_DIR = fmf.SNAP_DIR

CUTOFF = "2026-09-14"
TRAIN_START = "2026-08-13"
TRAIN_END = "2026-09-11"
TEST_START = "2026-09-14"
DESIGNED_AFTER = "2026-09-14"
FLAT_RT = 0.0015
KEEP_MIN_FIRES = 30
KEEP_MIN_WIN_RATE = 0.55
REAL_MONEY_MIN_SESSIONS = 20
RULES_LINK = "[IRONCLAD_RULES.md](../IRONCLAD_RULES.md)"


class FutureLeak(RuntimeError):
    """A train read tried to touch a bar or file on or after the cutoff."""


def file_date(path: Path) -> str | None:
    stem = path.stem[:10]
    if len(stem) == 10 and stem[4] == "-" and stem[7] == "-":
        return stem
    return None


def load_preregister(path: Path | None = None) -> dict:
    src = Path(path or PREREG_PATH)
    return json.loads(src.read_text(encoding="utf-8"))


def expand_candidates(doc: dict | None = None) -> list[dict]:
    """The preregistered grid. Own gates first, then Excel, then Theme Radar.

    The short Theme Radar books are research and are not in this list.
    """
    doc = doc if doc is not None else load_preregister()
    grid = doc.get("grid") or {}
    book = doc.get("book") or {}
    holds = [int(h) for h in (grid.get("holds") or [])]
    stops = list(grid.get("stop_pcts") or [None])
    cap = int(doc.get("max_candidates") or 50)
    provenance = {
        row["id"]: row
        for row in ((doc.get("own_provenance") or {}).get("bases") or [])
        if row.get("id")
    }
    out = []
    for base in doc.get("bases") or []:
        bid = str(base.get("id") or "")
        meta = provenance.get(bid) or {}
        late = bool(meta.get("designed_after"))
        for hold in holds:
            for stop in stops:
                tag = "sx" if stop is None else f"s{int(round(100 * float(stop)))}"
                item = {
                    "id": f"{bid}_h{hold}_{tag}",
                    "author": "oos",
                    "family": "own",
                    "created_on": "2026-09-28" if late else DESIGNED_AFTER,
                    "provenance": meta,
                    "universe": book.get("universe") or "union",
                    "hold": hold,
                    "side": book.get("side") or "long",
                    "top_n": int(book.get("top_n") or 4),
                    "require": dict(base.get("require") or {}),
                    "forbid": dict(base.get("forbid") or {}),
                    "rank": base.get("rank"),
                    "exit_when": {},
                    "size": book.get("size") or "leftover",
                    "sell": book.get("sell") or "time",
                    "s_boost": "none",
                    "day_cap": 1.0,
                    "take_pct": None,
                    "stop_pct": None if stop is None else float(stop),
                }
                if late:
                    item["clean_from"] = "2026-09-28"
                    item["designed_after_through"] = "2026-09-25"
                out.append(item)
    excel = doc.get("excel") or {}
    for model in excel.get("models") or []:
        out.append({
            "id": model["id"],
            "author": "excel",
            "family": "excel",
            "created_on": excel.get("created_on") or "2026-09-28",
            "clean_from": excel.get("clean_from") or "2026-09-28",
            "designed_after_through": excel.get("designed_after_through") or "2026-09-25",
            "universe": "candidates",
            "hold": int(excel.get("hold") or 1),
            "side": "long",
            "top_n": int(excel.get("top_n") or 4),
            "require": {"family": "excel", **model},
            "forbid": {},
            "rank": "model",
            "exit_when": {},
            "size": excel.get("size") or "equal",
            "sell": excel.get("exit") or "close",
            "s_boost": "none",
            "day_cap": 1.0,
            "take_pct": None,
            "stop_pct": None,
        })
    theme = doc.get("theme_radar_skip") or {}
    parent = theme.get("parent") or {}
    for rule in theme.get("rules") or []:
        out.append({
            "id": rule["id"],
            "author": "theme_radar",
            "family": "theme_radar_skip",
            "created_on": theme.get("created_on") or "2026-09-28",
            "clean_from": theme.get("clean_from") or "2026-09-28",
            "designed_after_through": theme.get("designed_after_through") or "2026-09-25",
            "universe": parent.get("universe") or "union",
            "hold": int(rule["hold"]),
            "side": "long",
            "top_n": int(parent.get("top_n") or 4),
            "require": {
                "family": "theme_radar_skip",
                "field": rule["field"],
                "lag": rule["lag"],
                "op": rule["op"],
                "earn": bool(rule.get("earn")),
                "er": bool(rule.get("er")),
                "hammer": bool(rule.get("hammer")),
                "since_first": theme.get("since_first") or "2026-08-06",
                "no_backfill": bool(theme.get("no_backfill", True)),
            },
            "forbid": {},
            "rank": parent.get("rank") or "hot_score",
            "exit_when": {},
            "size": parent.get("size") or "leftover",
            "sell": parent.get("sell") or "time",
            "s_boost": "none",
            "day_cap": 1.0,
            "take_pct": None,
            "stop_pct": None,
        })
    if len(out) > cap:
        raise RuntimeError(f"preregister expands to {len(out)} candidates; cap is {cap}")
    return out


def study_recipes(doc: dict | None = None) -> list[dict]:
    recipes = []
    for cand in expand_candidates(doc):
        if cand.get("family") not in (None, "own"):
            continue
        recipes.append(fm.make_recipe(
            cand["id"],
            universe=cand["universe"],
            hold=cand["hold"],
            side=cand["side"],
            top_n=cand["top_n"],
            require=cand["require"],
            forbid=cand["forbid"],
            rank=cand["rank"],
            exit_when=cand["exit_when"],
            size=cand["size"],
            sell=cand["sell"],
            take_pct=cand["take_pct"],
            stop_pct=cand["stop_pct"],
            note="OOS-0914 study id; not a frozen rule",
        ))
    return recipes


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_snapshot_dir(folder: Path, *, start: str, end: str,
                      cutoff: str = CUTOFF, read_text=None) -> dict[str, dict]:
    """Open snapshot files inside ``[start, end]`` only.

    A filename on or after ``cutoff``, or outside the window, is not opened.
    """
    reader = read_text or _read_text
    folder = Path(folder)
    found: dict[str, dict] = {}
    if not folder.is_dir():
        return found
    for path in sorted(folder.glob("*.json")):
        date = file_date(path)
        if not date:
            continue
        if date >= cutoff or date < start or date > end:
            continue
        doc = json.loads(reader(path))
        if str(doc.get("date") or date)[:10] >= cutoff:
            raise FutureLeak(f"{path.name} body is dated on or after {cutoff}")
        found[date] = doc
    return found


def snapshot_rows(doc: dict, date: str) -> list[dict]:
    """09:30 rows for one day. ``e_pol`` is stamped so the walk does not rescan."""
    rows = []
    for row in doc.get("rows") or []:
        if str(row.get("date") or date)[:10] != date:
            continue
        item = dict(row)
        item["date"] = date
        item["e_pol"] = False
        rows.append(item)
    return rows


def _meta_end(meta_path: Path) -> str:
    if not meta_path.is_file():
        return ""
    try:
        doc = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    return str(doc.get("last_date") or doc.get("end") or "")[:10]


def load_session_bars(path: Path, dates: list[str], tickers: set[str], *,
                      max_date: str, allow_test: bool = False) -> dict:
    """OHLC for ``dates`` only.

    Train (``allow_test`` false) refuses a store whose meta ends on or
    after the cutoff, and refuses any loaded bar on or after the cutoff.
    Sibling files whose names are dated on or after the cutoff are not opened.
    """
    path = Path(path)
    want = [str(d)[:10] for d in dates]
    if not allow_test:
        if max_date >= CUTOFF or any(d >= CUTOFF for d in want):
            raise FutureLeak(
                f"train bars cannot include {max_date}; cutoff is {CUTOFF}"
            )
        ended = _meta_end(path.parent / "meta.json")
        if ended and ended >= CUTOFF:
            raise FutureLeak(
                f"{path} meta ends {ended}; train will not open it"
            )
    for sib in path.parent.iterdir() if path.parent.is_dir() else []:
        sib_date = file_date(sib)
        if sib_date and sib_date >= CUTOFF and sib != path:
            continue
    if not path.is_file():
        return {}
    import pandas as pd

    frame = pd.read_parquet(
        path, columns=["date", "ticker", "open", "high", "low", "close"],
    )
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    if not allow_test:
        leaked = frame["date"] >= CUTOFF
        if bool(leaked.any()):
            raise FutureLeak(f"{path.name} contains bars on or after {CUTOFF}")
    keep_dates = {d for d in want if d <= max_date and (allow_test or d < CUTOFF)}
    names = {str(t).upper() for t in tickers if t}
    day = frame[frame["date"].isin(keep_dates) & frame["ticker"].isin(names)]
    bars: dict = {}
    for rec in day.itertuples(index=False):
        ticker = str(getattr(rec, "ticker", "") or "").upper()
        stamp = str(getattr(rec, "date", ""))[:10]
        if not ticker or not stamp or stamp > max_date:
            continue
        if not allow_test and stamp >= CUTOFF:
            raise FutureLeak(stamp)
        bars[(ticker, stamp)] = {
            "open": getattr(rec, "open", None),
            "high": getattr(rec, "high", None),
            "low": getattr(rec, "low", None),
            "close": getattr(rec, "close", None),
        }
    return bars


def _pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+.2f}%"


def _rate(value) -> str:
    if value is None:
        return "n/a"
    return f"{100.0 * float(value):.1f}%"


def compound_return(records: list[dict]) -> float | None:
    return seq.compound(records)


def trade_fires(records: list[dict] | None) -> int:
    """Buys. A recipe with zero fires never traded."""
    total = 0
    for row in records or []:
        total += len(row.get("buys") or [])
    return total


def is_untestable(records: list[dict] | None) -> bool:
    return trade_fires(records) == 0


def closed_win_rate(records: list[dict] | None) -> float | None:
    """Share of closing fills whose after-fee P&L is strictly positive."""
    pnls = []
    for row in records or []:
        for fill in row.get("fills") or []:
            if str(fill.get("side") or "") not in ("SELL", "COVER"):
                continue
            if fill.get("pnl") is None:
                continue
            pnls.append(float(fill["pnl"]))
    if not pnls:
        return None
    return round(sum(1 for pnl in pnls if pnl > 0) / len(pnls), 4)


def asymmetric_payoff(records: list[dict] | None) -> float | None:
    """Average winning close divided by the average losing close."""
    wins: list[float] = []
    losses: list[float] = []
    for row in records or []:
        for fill in row.get("fills") or []:
            if str(fill.get("side") or "") not in ("SELL", "COVER"):
                continue
            if fill.get("pnl") is None:
                continue
            pnl = float(fill["pnl"])
            if pnl > 0:
                wins.append(pnl)
            elif pnl < 0:
                losses.append(pnl)
    if not wins or not losses:
        return None
    return round(abs((sum(wins) / len(wins)) / (sum(losses) / len(losses))), 3)


def keep_bar_met(records: list[dict] | None) -> bool:
    """≥30 fires and a win rate above 55% after fees. Not the train filter."""
    rate = closed_win_rate(records)
    if rate is None:
        return False
    return trade_fires(records) >= KEEP_MIN_FIRES and rate > KEEP_MIN_WIN_RATE


def real_money_allowed(*, locked_sessions: int, beats_random4: bool,
                       beats_iwm: bool, without_best_beats: bool) -> bool:
    """Research stays research until about 20 locked sessions clear both baselines."""
    return (
        int(locked_sessions) >= REAL_MONEY_MIN_SESSIONS
        and bool(beats_random4)
        and bool(beats_iwm)
        and bool(without_best_beats)
    )


def ranked_pass_ids(rows: list[dict]) -> list[str]:
    """Passers only. A recipe that never traded stays out of the ranking."""
    passed = [
        row for row in rows
        if row.get("pass") and not row.get("untestable")
    ]
    passed.sort(key=lambda row: (
        -(row.get("after_fees_return") or -1e9),
        -(row.get("start_day_win_rate") or -1e9),
        row.get("id") or "",
    ))
    return [str(row["id"]) for row in passed]


def snapshot_input_sha(doc: dict) -> str:
    """Hash of the frozen morning rows. A later arrival is a different hash."""
    rows = (doc or {}).get("rows") or []
    raw = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def start_day_win_rate(records: list[dict]) -> float | None:
    """Fraction of start sessions whose remaining after-fee path is up."""
    means = []
    for row in records:
        mean = row.get("mean")
        if mean is None:
            means.append(0.0)
        else:
            means.append(float(mean))
    if not means:
        return None
    wins = 0
    for i in range(len(means)):
        acc = 1.0
        for mean in means[i:]:
            acc *= 1.0 + mean / 100.0
        if acc > 1.0:
            wins += 1
    return round(wins / len(means), 4)


def attributed_pnl(records: list[dict]) -> dict[str, float]:
    pnl: dict[str, float] = {}
    for row in records:
        for fill in row.get("fills") or []:
            if fill.get("pnl") is None or not fill.get("ticker"):
                continue
            ticker = str(fill["ticker"]).upper()
            pnl[ticker] = pnl.get(ticker, 0.0) + float(fill["pnl"])
    if records:
        pos = ((records[-1].get("state") or {}).get("pos") or {})
        for ticker, lot in pos.items():
            if not isinstance(lot, dict):
                continue
            try:
                shares = float(lot.get("shares") or 0)
                mark = lot.get("close_px")
                if mark is None:
                    mark = lot.get("last_px")
                cost = float(lot.get("cost") or 0)
                if mark is None:
                    continue
                extra = shares * float(mark) - cost
            except (TypeError, ValueError):
                continue
            name = str(ticker).upper()
            pnl[name] = pnl.get(name, 0.0) + extra
    return pnl


def best_ticker(records: list[dict]) -> str | None:
    pnl = attributed_pnl(records)
    if not pnl:
        return None
    ranked = sorted(pnl.items(), key=lambda item: (-item[1], item[0]))
    return ranked[0][0]


def _walk(dates, recipes, rows_for, bars, *, root: Path | None,
          persist: bool, exclude: str | None = None,
          fees=None) -> dict[str, list]:
    history: dict[str, list] = {}
    store = dict(bars)

    def bars_for(date: str):
        return {
            key: bar for key, bar in store.items()
            if str(key[1])[:10] == date
        }

    seq.walk(
        list(dates), recipes, rows_for=rows_for, bars_for=bars_for,
        root=root, persist=persist, fees=fees if fees is not None else fm.pt_fees(),
        regime={}, exclude=exclude, history=history,
    )
    return history


def _passes(row: dict, random_mean: float, iwm_ret: float) -> bool:
    ret = row.get("after_fees_return")
    start = row.get("start_day_win_rate")
    dropped = row.get("without_best_stock_return")
    if ret is None or start is None or dropped is None:
        return False
    if float(ret) <= 0 or float(start) <= 0 or float(dropped) <= 0:
        return False
    if float(ret) <= float(random_mean) or float(ret) <= float(iwm_ret):
        return False
    return True


def _score_named(dates, recipes, snaps, bars) -> dict[str, list]:
    by_date = snaps

    def rows_for(date: str):
        return snapshot_rows(by_date.get(date) or {}, date)

    return _walk(dates, recipes, rows_for, bars, root=None, persist=False)


def score_random4(dates, snaps, bars, *, flat_15bp: bool = False) -> dict:
    """1000 draws, seed 20260813, four names from that morning's rows."""
    pools = {}
    for date in dates:
        pools[date] = sorted({
            str(r.get("ticker") or "").upper()
            for r in snapshot_rows(snaps.get(date) or {}, date)
            if r.get("ticker")
        })
    rec = retro.random4_recipe()
    fees = fm.pt_fees()
    rets = []
    for i in range(retro.RANDOM4_DRAWS):
        picks = retro.random4_draw(pools, i)
        panel = retro.panel_from_picks(list(dates), picks)
        scored = retro.score_panel(
            panel, rec, bars, flat_15bp=flat_15bp, fees=fees, regime={},
        )
        rets.append(float(scored.get("total_ret_pct") or 0.0))
    rets_sorted = sorted(rets)
    n = len(expand_candidates())
    # Expected best of N looks at the N/(N+1) quantile of the null.
    null = retro.linear_percentile(rets, 100.0 * n / (n + 1))
    return {
        "draws": retro.RANDOM4_DRAWS,
        "seed": retro.RANDOM4_SEED,
        "n": retro.RANDOM4_N,
        "mean": retro._mean3(rets),
        "p5": retro.linear_percentile(rets, 5),
        "p50": retro.linear_percentile(rets, 50),
        "p95": retro.linear_percentile(rets, 95),
        "best_of_n_null": null,
        "n_candidates": n,
        "min": round(rets_sorted[0], 3) if rets_sorted else None,
        "max": round(rets_sorted[-1], 3) if rets_sorted else None,
        "fee": "flat_15bp" if flat_15bp else "futubull",
    }


def score_iwm(dates, bars, *, flat_15bp: bool = False) -> dict:
    rec = retro.iwm_recipe()
    picks = {d: ["IWM"] for d in dates}
    panel = retro.panel_from_picks(list(dates), picks)
    scored = retro.score_panel(
        panel, rec, bars, flat_15bp=flat_15bp, fees=fm.pt_fees(), regime={},
    )
    return {
        "after_fees_return": scored.get("total_ret_pct"),
        "final_equity": scored.get("final_equity"),
        "fee": "flat_15bp" if flat_15bp else "futubull",
    }


KEEP_BAR_NOTE = (
    "12 train fires against the 30-fire bar. Excel's luck test p=0.87."
)


def _orders_for_fee_view(row: dict) -> list[dict]:
    """Recorded orders, used only to reprice fees. Shares and prices stay."""
    for key in ("fills", "trades"):
        orders = [item for item in (row.get(key) or []) if isinstance(item, dict)]
        if orders and any("fees" in item for item in orders):
            return orders
    orders = []
    for key in ("buys", "sells"):
        for item in row.get(key) or []:
            if isinstance(item, dict):
                orders.append(item)
    return orders


def _flat_order_fee(shares, price) -> float:
    try:
        shares_n = float(shares)
        price_n = float(price)
    except (TypeError, ValueError):
        return 0.0
    if shares_n <= 0 or price_n <= 0:
        return 0.0
    return round(shares_n * price_n * (FLAT_RT / 2.0), 4)


def flat_15bp_means(rows: list[dict], *, capital: float | None = None) -> list:
    """Daily percent return of the same fills under a flat 15 bp fee.

    Futubull fees are already in ``equity``. This view charges 7.5 bp per
    side instead and does not change picks, shares, or prices.
    """
    prev = float(fm.CAPITAL if capital is None else capital)
    cum = 0.0
    out = []
    for row in rows or []:
        delta = 0.0
        for order in _orders_for_fee_view(row):
            futu = float(order.get("fees") or 0.0)
            delta += _flat_order_fee(order.get("shares"), order.get("price")) - futu
        cum += delta
        equity = row.get("equity")
        if equity is None or prev <= 0:
            out.append(None)
            if equity is not None:
                prev = float(equity) - cum
            continue
        adj = float(equity) - cum
        out.append(round(100.0 * (adj / prev - 1.0), 4))
        prev = adj
    return out


def _with_extra_fee(fn):
    """Run ``fn`` while each order also pays 7.5 bp (15 bp round trip)."""
    from . import paper_trade as pt
    orig = pt.order_fees

    def wrapped(shares, price, side, fees):
        base = orig(shares, price, side, fees)
        extra = 0.0
        if shares and price and shares > 0 and price > 0:
            extra = round(float(shares) * float(price) * (FLAT_RT / 2.0), 4)
        return round(float(base) + extra, 4)

    pt.order_fees = wrapped
    try:
        return fn()
    finally:
        pt.order_fees = orig


def mine() -> dict:
    """Score the preregistered family on the train window only."""
    doc = load_preregister()
    if str(doc.get("cutoff") or "") != CUTOFF:
        raise FutureLeak("preregister cutoff is not 2026-09-14")
    recipes = study_recipes(doc)
    dates = [
        d for d in sorted(load_snapshot_dir(
            SNAP_DIR, start=TRAIN_START, end=TRAIN_END,
        ))
    ]
    snaps = load_snapshot_dir(SNAP_DIR, start=TRAIN_START, end=TRAIN_END)
    tickers = {"IWM"}
    for date, snap in snaps.items():
        for row in snapshot_rows(snap, date):
            if row.get("ticker"):
                tickers.add(str(row["ticker"]).upper())
    bars = load_session_bars(
        LIVE_OHLC, dates, tickers, max_date=TRAIN_END, allow_test=False,
    )
    print(f"[oos0914] train days={len(dates)} names={len(tickers)} "
          f"own={len(recipes)} candidates={len(expand_candidates(doc))}", flush=True)
    history = _score_named(dates, recipes, snaps, bars)
    cands = expand_candidates(doc)
    excel_ids = [c["id"] for c in cands if c.get("family") == "excel"]
    theme_ids = [c for c in cands if c.get("family") == "theme_radar_skip"]
    if excel_ids:
        from .factor_mine_oos0914_excel import score_dates as excel_score
        history.update(excel_score(dates, allow_test=False))
    if theme_ids:
        history.update(_score_theme(dates, theme_ids, snaps, bars, allow_test=False))
    random4 = score_random4(dates, snaps, bars, flat_15bp=False)
    iwm = score_iwm(dates, bars, flat_15bp=False)
    rows = []
    own_by_name = {rec["name"]: rec for rec in recipes}
    for cand in cands:
        name = cand["id"]
        series = history.get(name) or []
        ret = compound_return(series)
        start = start_day_win_rate(series)
        winner = best_ticker(series)
        without = ret
        if winner and cand.get("family") == "excel":
            from .factor_mine_oos0914_excel import load_fit, score_dates as excel_score
            again = excel_score(dates, allow_test=False, fit=load_fit(), exclude=winner)
            without = compound_return(again.get(name) or [])
        elif winner and cand.get("family") == "theme_radar_skip":
            again = _score_theme(
                dates, [cand], snaps, bars, allow_test=False, exclude=winner,
            )
            without = compound_return(again.get(name) or [])
        elif winner and name in own_by_name:
            again = _score_named(
                dates, [own_by_name[name]], _without(snaps, winner), bars,
            )
            without = compound_return(again.get(name) or [])
        row = {
            "id": name,
            "author": cand.get("author"),
            "family": cand.get("family"),
            "created_on": cand.get("created_on"),
            "daily": [
                {
                    "date": item.get("date"),
                    "ret_pct": item.get("mean"),
                    "ret_pct_flat_15bp": flat,
                }
                for item, flat in zip(series, flat_15bp_means(series))
            ],
            "after_fees_return": ret,
            "start_day_win_rate": start,
            "best_stock": winner,
            "without_best_stock_return": without,
            "hold": cand.get("hold"),
            "stop_pct": cand.get("stop_pct"),
            "rank": cand.get("rank"),
            "require": cand.get("require") or {},
            "forbid": cand.get("forbid") or {},
            "n_days": len(series),
            "final_equity": None if not series else series[-1].get("equity"),
        }
        row["fires"] = trade_fires(series)
        row["untestable"] = is_untestable(series)
        row["win_rate"] = closed_win_rate(series)
        row["asymmetric_payoff"] = asymmetric_payoff(series)
        row["keep_bar"] = keep_bar_met(series)
        row["pass"] = _passes(row, random4["mean"], iwm["after_fees_return"])
        rows.append(row)
        print(
            f"[oos0914] {name} ret={ret} start={start} "
            f"without={without} pass={row['pass']}",
            flush=True,
        )
    passed_ids = ranked_pass_ids(rows)
    report = {
        "track": "OOS-0914",
        "cutoff": CUTOFF,
        "train": {"start": TRAIN_START, "end": TRAIN_END, "sessions": dates},
        "n_candidates": len(rows),
        "random4": random4,
        "iwm": iwm,
        "rows": rows,
        "passed": passed_ids,
        "freeze": passed_ids[:5],
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    TRAIN_REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[oos0914] passed={report['passed']} freeze={report['freeze']}", flush=True)
    return report


def _without(snaps: dict, ticker: str) -> dict:
    ban = str(ticker).upper()
    out = {}
    for date, doc in snaps.items():
        nxt = dict(doc)
        nxt["rows"] = [
            r for r in (doc.get("rows") or [])
            if str(r.get("ticker") or "").upper() != ban
        ]
        out[date] = nxt
    return out


def _score_theme(dates, studies, snaps, bars, *, allow_test: bool,
                 exclude: str | None = None) -> dict[str, list]:
    from .factor_mine_oos0914_theme import kept_rows, skip_calendar

    rules = []
    for study in studies:
        req = study["require"]
        rules.append({
            "id": study["id"],
            "field": req["field"],
            "lag": req["lag"],
            "op": req["op"],
            "earn": req.get("earn"),
            "er": req.get("er"),
            "hammer": req.get("hammer"),
            "hold": study["hold"],
            "since_first": req.get("since_first"),
        })
    flags = skip_calendar(dates, rules, allow_test=allow_test)
    ban = str(exclude or "").upper()
    history = {}
    for study in studies:
        rec = fm.make_recipe(
            study["id"], universe=study.get("universe") or "union",
            hold=int(study["hold"]), top_n=int(study.get("top_n") or 4),
            rank=study.get("rank") or "hot_score", sell=study.get("sell") or "time",
            size=study.get("size") or "leftover",
        )

        def rows_for(date, study=study, rec=rec):
            rows = snapshot_rows(snaps.get(date) or {}, date)
            if ban:
                rows = [
                    row for row in rows
                    if str(row.get("ticker") or "").upper() != ban
                ]
            return kept_rows(rows, rec, flags[study["id"]].get(date))

        history.update(_walk(dates, [rec], rows_for, bars, root=None, persist=False))
    return history


def _frozen_recipe(study: dict) -> dict:
    created = str(study.get("created_on") or DESIGNED_AFTER)[:10]
    note = "OOS-0914 frozen rule."
    if study.get("clean_from"):
        note += (
            f" Clean record starts {study['clean_from']}."
            f" {DESIGNED_AFTER} through {study.get('designed_after_through')} is designed_after."
        )
    else:
        note += " designed_after 2026-09-14."
    rec = fm.make_recipe(
        f"oos0914_{study['id']}",
        universe=study.get("universe") or "union",
        hold=int(study["hold"]),
        side="long",
        top_n=int(study.get("top_n") or 4),
        require=study.get("require") or {},
        forbid=study.get("forbid") or {},
        rank=study.get("rank"),
        size=study.get("size") or "leftover",
        sell=study.get("sell") or "time",
        take_pct=study.get("take_pct"),
        stop_pct=study.get("stop_pct"),
        note=note,
        created_on=created,
    )
    rec["created_on"] = created
    rec["author"] = study.get("author")
    rec["family"] = study.get("family") or "own"
    if study.get("clean_from"):
        rec["clean_from"] = study["clean_from"]
        rec["designed_after_through"] = study.get("designed_after_through")
    return rec


def freeze() -> dict:
    """Fingerprint the top passers. Does not score the test window."""
    if not TRAIN_REPORT.is_file():
        raise SystemExit("train report missing; run mine before freeze")
    report = json.loads(TRAIN_REPORT.read_text(encoding="utf-8"))
    chosen = list(report.get("freeze") or [])
    if not chosen:
        print("[oos0914] no candidate passed; freezing nothing", flush=True)
        payload = {"designed_after": DESIGNED_AFTER, "recipes": []}
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        FROZEN_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        write_frozen_list(report, [])
        return payload
    by_id = {r["id"]: r for r in expand_candidates()}
    rows_by_id = {row["id"]: row for row in (report.get("rows") or [])}
    recipes = []
    for name in chosen:
        study = by_id.get(name)
        if study is None:
            raise SystemExit(f"frozen id {name} is not in the preregister")
        rec = _frozen_recipe(study)
        row = rows_by_id.get(name) or {}
        if "keep_bar" in row or "keep_bar_met" in row:
            rec["keep_bar_met"] = bool(row.get("keep_bar_met", row.get("keep_bar")))
        if row.get("keep_bar_note"):
            rec["keep_bar_note"] = row["keep_bar_note"]
        recipes.append(rec)
    fmr.lock_recipe_rules(recipes, write=True, locked_on=DESIGNED_AFTER)
    payload = {
        "designed_after": DESIGNED_AFTER,
        "recipes": recipes,
        "fingerprints": {
            rec["name"]: fmr.recipe_fingerprint(rec) for rec in recipes
        },
    }
    FROZEN_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_frozen_list(report, recipes)
    print(f"[oos0914] froze {[r['name'] for r in recipes]}", flush=True)
    return payload


def write_frozen_list(report: dict, selected: list[dict]) -> dict:
    """Every preregistered candidate, fingerprinted, with its train daily path.

    Excel's luck test reads this file. It has no test-window results.
    """
    chosen = {rec.get("name") for rec in selected}
    by_id = {row["id"]: row for row in (report.get("rows") or [])}
    candidates = []
    for study in expand_candidates():
        rec = _frozen_recipe(study)
        train = by_id.get(study["id"]) or {}
        row = {
            "id": study["id"],
            "name": rec["name"],
            "author": study.get("author"),
            "family": study.get("family"),
            "created_on": rec.get("created_on"),
            "sha256": fmr.recipe_fingerprint(rec),
            "selected": rec["name"] in chosen,
            "after_fees_return": train.get("after_fees_return"),
            "daily": train.get("daily") or [],
        }
        if study.get("provenance"):
            row["provenance"] = study["provenance"]
        if study.get("clean_from"):
            row["clean_from"] = study["clean_from"]
        if rec["name"] in chosen and (
            train.get("keep_bar_met") is not None or train.get("keep_bar") is not None
        ):
            row["keep_bar_met"] = bool(train.get("keep_bar_met", train.get("keep_bar")))
        if rec["name"] in chosen and train.get("keep_bar_note"):
            row["keep_bar_note"] = train["keep_bar_note"]
        candidates.append(row)
    prereg = load_preregister()
    payload = {
        "track": "OOS-0914",
        "path": "data/factor_mine/oos0914/frozen_list.json",
        "luck_test_n": len(candidates),
        "train": (report.get("train") or {}),
        "selected": [rec.get("name") for rec in selected],
        "own_provenance": prereg.get("own_provenance") or {},
        "theme_radar_search": prereg.get("theme_radar_search") or {},
        "candidates": candidates,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dest = OUT_DIR / "frozen_list.json"
    dest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"[oos0914] frozen list {dest} n={len(candidates)}", flush=True)
    return payload


def frozen_recipes() -> list[dict]:
    if not FROZEN_PATH.is_file():
        return []
    doc = json.loads(FROZEN_PATH.read_text(encoding="utf-8"))
    return list(doc.get("recipes") or [])


def test_dates(through: str | None = None) -> list[str]:
    """Locked sessions from the cutoff through the last closed day."""
    closed = through or fm.last_closed_session(TEST_START) or TRAIN_END
    dates = []
    if not SNAP_DIR.is_dir():
        return dates
    for path in sorted(SNAP_DIR.glob("*.json")):
        date = file_date(path)
        if not date:
            continue
        if date < TEST_START or date > closed:
            continue
        dates.append(date)
    return dates


def _has_print(bar: dict | None) -> bool:
    if not bar:
        return False
    return bar.get("open") is not None or bar.get("close") is not None


def _pin_bars(date: str, tickers: set[str]) -> dict:
    """Read the frozen price pin. Do not call yfinance."""
    path = fmf.price_path(date)
    if not path.is_file():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    names = doc.get("names") or {}
    out = {}
    for ticker in tickers:
        info = names.get(ticker)
        if not isinstance(info, dict) or not _has_print(info):
            continue
        out[(ticker, date)] = {
            "open": info.get("open"),
            "high": info.get("high"),
            "low": info.get("low"),
            "close": info.get("close"),
        }
    return out


def _fill_test_bars(dates: list[str], tickers: set[str], bars: dict) -> dict:
    """Fill a test session the locked retro store does not cover.

    Retro bars stay as they are. A missing open and close comes from that
    day's frozen price pin, then from the live Yahoo store for a carried
    name the pin does not list. Neither store is written.
    """
    filled = dict(bars)
    names = {str(t).upper() for t in tickers if t and str(t).upper() != "IWM"}
    for date in dates:
        stamp = str(date)[:10]
        missing = {
            ticker for ticker in names
            if not _has_print(filled.get((ticker, stamp)))
        }
        if not missing:
            continue
        for key, bar in _pin_bars(stamp, missing).items():
            if not _has_print(filled.get(key)):
                filled[key] = bar
            missing.discard(key[0])
        if not missing:
            continue
        live = load_session_bars(
            LIVE_OHLC, [stamp], missing, max_date=stamp, allow_test=True,
        )
        for key, bar in live.items():
            if _has_print(bar) and not _has_print(filled.get(key)):
                filled[key] = bar
    return filled


def _bars_for_window(dates: list[str], tickers: set[str], *,
                     allow_test: bool) -> dict:
    if not dates:
        return {}
    path = RETRO_OHLC if allow_test else LIVE_OHLC
    bars = load_session_bars(
        path, dates, tickers, max_date=dates[-1], allow_test=allow_test,
    )
    if allow_test:
        bars = _fill_test_bars(dates, tickers, bars)
    return bars


def assert_new_session_has_bars(dates: list[str], recipes: list[dict],
                                snaps: dict, bars: dict, root: Path) -> None:
    """Refuse to lock a new day whose session tape is empty.

    One missing name stays ``unpriced_held``. An empty tape is a different
    failure: every lot carries at yesterday's price and nothing is bought,
    then that flat day is locked. A date that already has a state file is
    left alone so a rewalk of locked history does not fail.
    """
    root = Path(root)
    for date in dates:
        stamp = str(date)[:10]
        pending = [
            rec for rec in recipes
            if rec.get("name") and seq.read_state(rec["name"], stamp, root) is None
        ]
        if not pending:
            continue
        row_names = {
            str(row.get("ticker") or "").upper()
            for row in snapshot_rows(snaps.get(stamp) or snaps.get(date) or {}, stamp)
            if row.get("ticker")
        }
        if row_names:
            priced = sum(1 for ticker in row_names if _has_print(bars.get((ticker, stamp))))
            if priced == 0:
                raise SystemExit(
                    f"[oos0914] {stamp} has {len(row_names)} snapshot rows "
                    "and no session bars; refusing to lock a flat carry"
                )
            continue
        carried: set[str] = set()
        for rec in pending:
            carried |= fmf._positions_before(root / str(rec["name"]), stamp)
        if carried and not any(
            _has_print(bars.get((ticker, stamp))) for ticker in carried
        ):
            raise SystemExit(
                f"[oos0914] {stamp} has {len(carried)} carried names "
                "and no session bars; refusing to lock a flat carry"
            )


def _ensure_iwm(dates: list[str], bars: dict, *, allow_test: bool) -> dict:
    missing = [d for d in dates if ("IWM", d) not in bars]
    if not missing:
        return bars
    if not allow_test:
        raise FutureLeak(f"IWM missing on train sessions {missing[:6]}")
    import pandas as pd
    import yfinance as yf

    end = pd.Timestamp(dates[-1]) + pd.Timedelta(days=1)
    raw = yf.download(
        "IWM", start=dates[0], end=str(end.date()),
        auto_adjust=False, actions=False, progress=False, threads=False,
    )
    parsed = retro._iwm_frame(raw)
    for date in dates:
        bar = parsed.get(("IWM", date))
        if bar:
            bars[("IWM", date)] = bar
    return bars


def write_oos_ledger(date: str, doc: dict, path: Path | None = None) -> Path:
    """One locked test day. #338 refuses a changed fill; bytes never change.

    The first write stores a sha256 beside the file. A later run re-checks
    that fingerprint before it will accept the day.
    """
    dest = Path(path or (LEDGER_DIR / f"{date}.json"))
    payload = dict(doc)
    payload["date"] = str(date)[:10]
    raw = fmr._canonical(payload)
    digest = hashlib.sha256(raw).hexdigest()
    side = dest.with_name(dest.name + ".sha256")
    if dest.is_file():
        have = dest.read_bytes()
        if side.is_file():
            stamped = side.read_text(encoding="utf-8").strip()
            if hashlib.sha256(have).hexdigest() != stamped:
                raise fmr.AppendDrift(
                    f"ledger fingerprint mismatch {date}"
                )
        prior = json.loads(have.decode("utf-8"))
        fmr.assert_ledger_append(prior, payload)
        if have != raw or (side.is_file() and side.read_text(encoding="utf-8").strip() != digest):
            raise fmr.AppendDrift(
                f"ledger rewrite {date}: locked day bytes would change"
            )
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(raw)
    side.write_text(digest + "\n", encoding="utf-8")
    return dest


def _flat_mean_through(name: str, date: str, root: Path) -> float | None:
    """Flat-15bp day return through ``date``, from saved state fills."""
    folder = Path(root) / name
    stamps = []
    if folder.is_dir():
        stamps = sorted(
            path.stem for path in folder.glob("*.json")
            if len(path.stem) == 10 and path.stem <= str(date)[:10]
        )
    series = []
    for stamp in stamps:
        doc = seq.read_state(name, stamp, root)
        if doc:
            series.append(doc)
    if not series:
        doc = seq.read_state(name, date, root)
        series = [doc] if doc else []
    means = flat_15bp_means(series)
    return None if not means else means[-1]


def _ledger_doc(date: str, recipes: list[dict], root: Path,
                snap: dict) -> dict:
    slots = {}
    for rec in recipes:
        name = rec["name"]
        state = seq.read_state(name, date, root) or {}
        slots[name] = {
            "buys": state.get("buys") or [],
            "sells": state.get("sells") or [],
            "trades": state.get("fills") or [],
            "equity": state.get("equity"),
            "mean": state.get("mean"),
            "cash": state.get("cash"),
            "fees": state.get("fees"),
            "holdings": state.get("holdings") or [],
            "mean_flat_15bp": _flat_mean_through(name, date, root),
        }
    return {
        "date": date,
        "asof": "16:00_et_lock",
        "dropped": list(snap.get("dropped") or []),
        "input_sha256": snapshot_input_sha(snap),
        "recipes": slots,
    }


def _rows_for_factory(snaps: dict):
    def rows_for(date: str):
        if date >= CUTOFF and date not in snaps:
            # Test days are loaded one file at a time by the caller.
            doc = snaps.get(date)
            if doc is None:
                return []
        return snapshot_rows(snaps.get(date) or {}, date)
    return rows_for


def _held_into(root: Path, dates: list[str]) -> set[str]:
    """Names already held when any of ``dates`` opens.

    The bar load has to include them. A position that is not in that
    morning's snapshot rows otherwise has no print, and the book carries
    it flat. Existing state files are not opened for rewrite.
    """
    out: set[str] = set()
    root = Path(root)
    if not dates or not root.is_dir():
        return out
    for folder in root.iterdir():
        if not folder.is_dir():
            continue
        for date in dates:
            out |= fmf._positions_before(folder, date)
    return out


def walk_test(dates: list[str], recipes: list[dict], *,
              root: Path | None = None, ledger: bool = True) -> None:
    """Sequential lock. Existing state files are not recomputed."""
    root = Path(root or STATE_ROOT)
    if not recipes or not dates:
        return
    # Load each snapshot by its own name. Later filenames are not opened
    # until their own turn; this call opens only ``dates``.
    snaps = {}
    tickers = set()
    for date in dates:
        doc = (load_snapshot_dir(
            SNAP_DIR, start=date, end=date, cutoff="9999-99-99",
        ).get(date) or {})
        snaps[date] = doc
        for row in snapshot_rows(doc, date):
            if row.get("ticker"):
                tickers.add(str(row["ticker"]).upper())
    tickers |= _held_into(root, dates)
    bars = _bars_for_window(dates, tickers, allow_test=True)
    assert_new_session_has_bars(dates, recipes, snaps, bars, root)
    fees = fm.pt_fees()

    def rows_for(date: str):
        return snapshot_rows(snaps.get(date) or {}, date)

    _walk(dates, recipes, rows_for, bars, root=root, persist=True, fees=fees)
    if ledger:
        for date in dates:
            write_oos_ledger(date, _ledger_doc(date, recipes, root, snaps.get(date) or {}))


def _chain(name: str, dates: list[str], root: Path) -> list[dict]:
    return seq.chain_records(name, dates, root)


def _drop_best_test(name: str, dates: list[str], rec: dict, root: Path) -> dict:
    series = _chain(name, dates, root)
    winner = best_ticker(series)
    if not winner:
        return {"best_stock": None, "after_fees_return": compound_return(series)}
    snaps = load_snapshot_dir(
        SNAP_DIR, start=dates[0], end=dates[-1], cutoff="9999-99-99",
    )
    tickers = {winner}
    for date, doc in snaps.items():
        for row in snapshot_rows(doc, date):
            if row.get("ticker"):
                tickers.add(str(row["ticker"]).upper())
    bars = _bars_for_window(dates, tickers, allow_test=True)
    alt = _score_named(dates, [rec], _without(snaps, winner), bars)
    return {
        "best_stock": winner,
        "after_fees_return": compound_return(alt.get(name) or []),
    }


def _baselines(dates: list[str]) -> dict:
    snaps = load_snapshot_dir(
        SNAP_DIR, start=dates[0], end=dates[-1], cutoff="9999-99-99",
    )
    tickers = {"IWM"}
    for date, doc in snaps.items():
        for row in snapshot_rows(doc, date):
            if row.get("ticker"):
                tickers.add(str(row["ticker"]).upper())
    bars = _bars_for_window(dates, tickers, allow_test=True)
    bars = _ensure_iwm(dates, bars, allow_test=True)

    def both(kind: str):
        if kind == "random4":
            futu = score_random4(dates, snaps, bars, flat_15bp=False)
            plus = _with_extra_fee(lambda: score_random4(dates, snaps, bars, flat_15bp=False))
            # The extra-fee monkeypatch wraps order_fees, so score_random4's
            # flat_15bp flag stays off and Futubull is charged underneath.
            return {"futubull": futu, "futubull_plus_15bp": plus}
        futu = score_iwm(dates, bars, flat_15bp=False)
        plus = _with_extra_fee(lambda: score_iwm(dates, bars, flat_15bp=False))
        return {"futubull": futu, "futubull_plus_15bp": plus}

    return {"random4": both("random4"), "iwm": both("iwm")}


def render_hold_scoreboard() -> str:
    """The board while the candidate list is still open."""
    return "\n".join([
        "# Factor Mine OOS-0914",
        "",
        f"Rules for this mine: {RULES_LINK}.",
        "",
        "The candidate freeze is held. The draft list is in "
        "`data/factor_mine/oos0914_preregister.json`. "
        "No rule is frozen, and the test window is not scored.",
        "",
    ])


def render_scoreboard(train: dict, test: dict | None) -> str:
    """Plain language first, then the tables."""
    lines = [
        "# Factor Mine OOS-0914",
        "",
        f"Rules for this mine: {RULES_LINK}.",
        "",
    ]
    frozen = list((test or {}).get("rules") or [])
    if not frozen:
        lines.append(
            "No candidate passed the train filter. "
            f"The family was {train.get('n_candidates')} rules, written down "
            "before the mine. A rule had to make money after Futubull fees, "
            "have a positive start-day win rate, stay positive after its best "
            "stock was removed, and beat both the average of 1,000 random "
            "four-name books and an IWM buy-and-hold on the train window "
            f"({TRAIN_START} through {TRAIN_END}). None did. Nothing was frozen, "
            "and the test window was not scored."
        )
    else:
        base = (test or {}).get("baselines") or {}
        r4 = ((base.get("random4") or {}).get("futubull_plus_15bp") or {}).get("mean")
        iwm = ((base.get("iwm") or {}).get("futubull_plus_15bp") or {}).get("after_fees_return")
        n_days = len((test or {}).get("sessions") or [])
        span = ""
        sessions = (test or {}).get("sessions") or []
        if sessions:
            span = f"{sessions[0]} through {sessions[-1]}"
        logged = [rule for rule in frozen if rule.get("keep_bar_met") is False]
        if logged:
            note = next(
                (rule.get("keep_bar_note") for rule in logged if rule.get("keep_bar_note")),
                "",
            )
            lines.append(
                "The frozen rules are logged experiments, not keepers. "
                "`keep_bar_met` is false."
                + (f" {note}" if note else "")
            )
            lines.append("")
        bits = []
        for rule in frozen:
            if rule.get("clean_from") and rule.get("after_fees_return") is None:
                bits.append(
                    f"`{rule['name']}` has no clean test session yet. "
                    f"Its clean record starts {rule.get('clean_from')}. "
                    f"Sessions from {DESIGNED_AFTER} through "
                    f"{rule.get('designed_after_through')} were already seen, "
                    f"so they are designed_after and are not the result. "
                    f"The hindsight figure on those days is "
                    f"{_pct(rule.get('designed_after_return'))}, "
                    f"versus random picks {_pct(r4)} and IWM {_pct(iwm)}. "
                    f"Without its best stock ({rule.get('best_stock') or 'none'}) "
                    f"that hindsight window was "
                    f"{_pct(rule.get('designed_after_without_best_stock_return'))}."
                )
            else:
                bits.append(
                    f"`{rule['name']}` made {_pct(rule.get('after_fees_return'))} "
                    f"on the {n_days} locked test sessions ({span}) after fees, "
                    f"versus random picks {_pct(r4)} and IWM {_pct(iwm)}. "
                    f"Without its best stock ({rule.get('best_stock') or 'none'}) "
                    f"that test window was {_pct(rule.get('without_best_stock_return'))}."
                )
        lines.append(" ".join(bits))
    lines += ["", "## Train", ""]
    r4t = train.get("random4") or {}
    iwmt = train.get("iwm") or {}
    lines.append(
        f"Train sessions: {TRAIN_START} through {TRAIN_END} "
        f"({len((train.get('train') or {}).get('sessions') or [])} days). "
        f"RANDOM4 mean {_pct(r4t.get('mean'))} "
        f"(seed {r4t.get('seed')}, {r4t.get('draws')} draws). "
        f"IWM buy-and-hold {_pct(iwmt.get('after_fees_return'))}. "
        f"Best-of-{r4t.get('n_candidates')} null {_pct(r4t.get('best_of_n_null'))} "
        "(the random-book percentile a search of this size should expect to win)."
    )
    if any(row.get("family") in ("excel", "theme_radar_skip") for row in (train.get("rows") or [])):
        lines.append(
            "War room rules were seen on 2026-09-14 through 2026-09-25. "
            "Their clean record starts 2026-09-28. The train numbers are the "
            "pre-registered selection window."
        )
    if any(
        day.get("ret_pct_flat_15bp") is not None
        for row in (train.get("rows") or [])
        for day in (row.get("daily") or [])
    ):
        lines.append(
            "Each train row's daily path keeps the Futubull return in "
            "`ret_pct`. `ret_pct_flat_15bp` is those same picks and fills "
            "priced with a flat 15 bp fee (7.5 bp per side) instead."
        )
    lines += [
        "",
        "| rule | after fees | start-day win rate | fires | win rate | asymmetric | best stock | without best stock | pass |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |",
    ]
    for row in train.get("rows") or []:
        if row.get("untestable"):
            continue
        lines.append(
            f"| `{row['id']}` | {_pct(row.get('after_fees_return'))} | "
            f"{_rate(row.get('start_day_win_rate'))} | "
            f"{row.get('fires') if row.get('fires') is not None else ''} | "
            f"{_rate(row.get('win_rate'))} | "
            f"{row.get('asymmetric_payoff') if row.get('asymmetric_payoff') is not None else ''} | "
            f"{row.get('best_stock') or ''} | "
            f"{_pct(row.get('without_best_stock_return'))} | "
            f"{'yes' if row.get('pass') else 'no'} |"
        )
    if not frozen:
        lines += [
            "",
            "## Test",
            "",
            "Not scored. No rule was frozen.",
            "",
        ]
        return "\n".join(lines)
    lines += ["", "## Test days", ""]
    sessions = (test or {}).get("sessions") or []
    lines.append(
        "Each day uses that morning's frozen 09:30 snapshot and the prior "
        "close (cash, holdings, fees). A missing print is left on the snapshot's "
        "dropped list. The day still locks. Fills: buy at the open; a stop "
        "fills at the level, or at the open if the open gaps through it; "
        "if the same bar also hits a take-profit, the stop fills first. "
        "The flat 15bp column prices those same fills at 7.5 bp per side."
    )
    lines.append("")
    for rule in frozen:
        lines += [f"### `{rule['name']}`", ""]
        if rule.get("keep_bar_met") is False:
            lines.append("Logged experiment, not a keeper. `keep_bar_met` is false.")
            lines.append("")
        lines += [
            "| date | buys | sells | fees | cash | equity | day | flat 15bp |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for day in rule.get("days") or []:
            buys = ",".join(_tick(x) for x in (day.get("buys") or [])) or "—"
            sells = ",".join(_tick(x) for x in (day.get("sells") or [])) or "—"
            lines.append(
                f"| {day.get('date')} | {buys} | {sells} | "
                f"{day.get('fees')} | {day.get('cash')} | {day.get('equity')} | "
                f"{_pct(day.get('mean'))} | {_pct(day.get('mean_flat_15bp'))} |"
            )
        lines.append("")
        if rule.get("clean_from"):
            lines.append(
                f"Clean record starts {rule.get('clean_from')}. "
                f"Sessions through {rule.get('designed_after_through')} "
                "are designed_after and are not the result."
            )
            lines.append("")
    base = (test or {}).get("baselines") or {}
    lines += [
        "## Baselines",
        "",
        "Same test sessions. RANDOM4 is 1,000 draws of four names from that "
        "morning's snapshot, seed 20260813. IWM is buy-and-hold. "
        "The plus-15bp column is the Futubull schedule plus 7.5 bp per side.",
        "",
        "| baseline | Futubull | Futubull + 15 bp |",
        "| --- | ---: | ---: |",
        f"| RANDOM4 mean | {_pct(((base.get('random4') or {}).get('futubull') or {}).get('mean'))} | "
        f"{_pct(((base.get('random4') or {}).get('futubull_plus_15bp') or {}).get('mean'))} |",
        f"| IWM buy-and-hold | {_pct(((base.get('iwm') or {}).get('futubull') or {}).get('after_fees_return'))} | "
        f"{_pct(((base.get('iwm') or {}).get('futubull_plus_15bp') or {}).get('after_fees_return'))} |",
        "",
        "## Luck check",
        "",
        f"On the train window the best-of-{r4t.get('n_candidates')} null "
        f"(RANDOM4) is {_pct(r4t.get('best_of_n_null'))}. "
        f"RANDOM4 itself averaged {_pct(r4t.get('mean'))} "
        f"(5th–95th {_pct(r4t.get('p5'))} to {_pct(r4t.get('p95'))}). "
        "A frozen rule had to clear the RANDOM4 mean and IWM, and stay "
        "positive with its best stock removed, before it was named.",
        "",
    ]
    shorts = list((test or {}).get("research_shorts") or [])
    if shorts:
        lines += [
            "## Research shorts",
            "",
            "Theme Radar's short books are not in the 37 and were not frozen. "
            "Each enters at that session's close and covers h sessions later, "
            "or stays open and is marked at the last close when the cover "
            "falls past this window. Cost is 15 bp round trip plus 0.3 percent "
            "borrow. Days before 2026-09-28 are designed_after.",
            "",
            "| rule | after fees | fires |",
            "| --- | ---: | ---: |",
        ]
        for row in shorts:
            lines.append(
                f"| `{row.get('id')}` | {_pct(row.get('after_fees_return'))} | "
                f"{row.get('fires') if row.get('fires') is not None else ''} |"
            )
        lines.append("")
    return "\n".join(lines)


def _tick(item) -> str:
    if isinstance(item, dict):
        return str(item.get("ticker") or "")
    return str(item or "")


def _family(rec: dict) -> str:
    fam = rec.get("family")
    if fam:
        return str(fam)
    return str((rec.get("require") or {}).get("family") or "own")


def _bare_id(name: str) -> str:
    prefix = "oos0914_"
    text = str(name or "")
    return text[len(prefix):] if text.startswith(prefix) else text


def _lock_theme(dates: list[str], recipes: list[dict], root: Path) -> None:
    """Skip-gate books. State is stored under the frozen ``oos0914_`` name."""
    from .factor_mine_oos0914_theme import kept_rows, skip_calendar

    snaps = {}
    tickers = set()
    for date in dates:
        doc = (load_snapshot_dir(
            SNAP_DIR, start=date, end=date, cutoff="9999-99-99",
        ).get(date) or {})
        snaps[date] = doc
        for row in snapshot_rows(doc, date):
            if row.get("ticker"):
                tickers.add(str(row["ticker"]).upper())
    tickers |= _held_into(root, dates)
    bars = _bars_for_window(dates, tickers, allow_test=True)
    rules = []
    for rec in recipes:
        req = rec.get("require") or {}
        rules.append({
            "id": rec["name"],
            "field": req["field"],
            "lag": req["lag"],
            "op": req["op"],
            "earn": req.get("earn"),
            "er": req.get("er"),
            "hammer": req.get("hammer"),
            "hold": rec.get("hold"),
            "since_first": req.get("since_first"),
        })
    flags = skip_calendar(dates, rules, allow_test=True)
    for rec in recipes:
        plain = fm.make_recipe(
            rec["name"], universe=rec.get("universe") or "union",
            hold=int(rec["hold"]), top_n=int(rec.get("top_n") or 4),
            rank=rec.get("rank") or "hot_score", sell=rec.get("sell") or "time",
            size=rec.get("size") or "leftover",
            created_on=rec.get("created_on"),
        )

        def rows_for(date, rec=rec, plain=plain):
            rows = snapshot_rows(snaps.get(date) or {}, date)
            return kept_rows(rows, plain, flags[rec["name"]].get(date))

        _walk(dates, [plain], rows_for, bars, root=root, persist=True)


def _lock_excel(dates: list[str], recipes: list[dict], root: Path) -> None:
    """Load the frozen fit. Do not train again."""
    from .factor_mine_oos0914_excel import load_fit, score_dates

    history = score_dates(dates, allow_test=True, fit=load_fit())
    for rec in recipes:
        series = history.get(_bare_id(rec["name"])) or []
        by_date = {str(row.get("date"))[:10]: row for row in series}
        for date in dates:
            doc = by_date.get(date)
            if doc is None:
                continue
            seq.write_state(rec["name"], date, doc, root)


def lock_books(dates: list[str], recipes: list[dict], *,
               root: Path | None = None, ledger: bool = True) -> None:
    """Lock every frozen family, then write each day ledger once."""
    root = Path(root or STATE_ROOT)
    if not recipes or not dates:
        return
    own = [rec for rec in recipes if _family(rec) == "own"]
    theme = [rec for rec in recipes if _family(rec) == "theme_radar_skip"]
    excel = [rec for rec in recipes if _family(rec) == "excel"]
    if own:
        walk_test(dates, own, root=root, ledger=False)
    if theme:
        _lock_theme(dates, theme, root)
    if excel:
        _lock_excel(dates, excel, root)
    if not ledger:
        return
    snaps = {}
    for date in dates:
        snaps[date] = (load_snapshot_dir(
            SNAP_DIR, start=date, end=date, cutoff="9999-99-99",
        ).get(date) or {})
    for date in dates:
        write_oos_ledger(
            date, _ledger_doc(date, recipes, root, snaps.get(date) or {}),
        )


def _without_best(rec: dict, dates: list[str], root: Path) -> dict:
    """Rerun ``dates`` from $10k with the best ticker removed."""
    if not dates:
        return {"best_stock": None, "after_fees_return": None}
    series = _chain(rec["name"], dates, root)
    winner = best_ticker(series)
    if not winner:
        return {"best_stock": None, "after_fees_return": compound_return(series)}
    fam = _family(rec)
    if fam == "excel":
        from .factor_mine_oos0914_excel import load_fit, score_dates
        again = score_dates(dates, allow_test=True, fit=load_fit(), exclude=winner)
        return {
            "best_stock": winner,
            "after_fees_return": compound_return(again.get(_bare_id(rec["name"])) or []),
        }
    if fam == "theme_radar_skip":
        snaps = load_snapshot_dir(
            SNAP_DIR, start=dates[0], end=dates[-1], cutoff="9999-99-99",
        )
        tickers = {winner}
        for doc in snaps.values():
            for row in doc.get("rows") or []:
                if row.get("ticker"):
                    tickers.add(str(row["ticker"]).upper())
        bars = _bars_for_window(dates, tickers, allow_test=True)
        study = {
            "id": rec["name"],
            "hold": rec.get("hold"),
            "universe": rec.get("universe"),
            "top_n": rec.get("top_n"),
            "rank": rec.get("rank"),
            "sell": rec.get("sell"),
            "size": rec.get("size"),
            "require": rec.get("require") or {},
        }
        again = _score_theme(dates, [study], snaps, bars, allow_test=True, exclude=winner)
        return {
            "best_stock": winner,
            "after_fees_return": compound_return(again.get(rec["name"]) or []),
        }
    return _drop_best_test(rec["name"], dates, rec, root)


def _rule_report(rec: dict, dates: list[str], root: Path) -> dict:
    series = _chain(rec["name"], dates, root)
    clean = str(rec.get("clean_from") or "")[:10]
    peeked = [row for row in series if clean and str(row.get("date"))[:10] < clean]
    real = [row for row in series if not clean or str(row.get("date"))[:10] >= clean]
    flat_means = flat_15bp_means(series)
    report = {
        "name": rec["name"],
        "family": _family(rec),
        "days": [
            {
                "date": row.get("date"),
                "buys": row.get("buys") or [],
                "sells": row.get("sells") or [],
                "fees": row.get("fees"),
                "cash": row.get("cash"),
                "equity": row.get("equity"),
                "mean": row.get("mean"),
                "mean_flat_15bp": flat,
            }
            for row, flat in zip(series, flat_means)
        ],
    }
    if "keep_bar_met" in rec:
        report["keep_bar_met"] = bool(rec["keep_bar_met"])
    if rec.get("keep_bar_note"):
        report["keep_bar_note"] = rec["keep_bar_note"]
    if not clean:
        dropped = _without_best(rec, dates, root)
        report["after_fees_return"] = compound_return(series)
        report["start_day_win_rate"] = start_day_win_rate(series)
        report["best_stock"] = dropped.get("best_stock")
        report["without_best_stock_return"] = dropped.get("after_fees_return")
        return report
    report["clean_from"] = clean
    report["designed_after_through"] = rec.get("designed_after_through")
    report["designed_after_return"] = compound_return(peeked)
    report["after_fees_return"] = compound_return(real) if real else None
    report["start_day_win_rate"] = start_day_win_rate(real) if real else None
    if real:
        dropped = _without_best(rec, [d for d in dates if d >= clean], root)
        report["best_stock"] = dropped.get("best_stock")
        report["without_best_stock_return"] = dropped.get("after_fees_return")
    else:
        dropped = _without_best(rec, [d for d in dates if d < clean], root)
        report["best_stock"] = dropped.get("best_stock")
        report["without_best_stock_return"] = None
        report["designed_after_without_best_stock_return"] = dropped.get("after_fees_return")
    return report


def _short_rules() -> list[dict]:
    theme = (load_preregister().get("theme_radar_skip") or {})
    since = theme.get("since_first") or "2026-08-06"
    rules = []
    for rule in theme.get("rules") or []:
        rules.append({
            "id": rule["id"],
            "field": rule["field"],
            "lag": rule["lag"],
            "op": rule["op"],
            "earn": bool(rule.get("earn")),
            "er": bool(rule.get("er")),
            "hammer": bool(rule.get("hammer")),
            "hold": int(rule["hold"]),
            "since_first": since,
        })
    return rules


def _closes(dates: list[str], tickers: set[str], *, allow_test: bool) -> dict:
    import pandas as pd

    if not dates or not tickers:
        return {}
    cols = ["date", "ticker", "close"]
    frame = pd.read_parquet(LIVE_OHLC, columns=cols)
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    cap = dates[-1] if allow_test else TRAIN_END
    frame = frame[frame["date"] <= cap]
    if (not allow_test) and bool((frame["date"] >= CUTOFF).any()):
        raise FutureLeak("short research prices reach the cutoff")
    if allow_test and cap >= CUTOFF:
        retro_px = pd.read_parquet(RETRO_OHLC, columns=cols)
        retro_px["date"] = pd.to_datetime(retro_px["date"]).dt.strftime("%Y-%m-%d")
        retro_px["ticker"] = retro_px["ticker"].astype(str).str.upper()
        retro_px = retro_px[(retro_px["date"] >= CUTOFF) & (retro_px["date"] <= cap)]
        frame = pd.concat([frame, retro_px], ignore_index=True)
    want = set(dates)
    frame = frame[frame["ticker"].isin(tickers) & frame["date"].isin(want)]
    out = {}
    for row in frame.itertuples(index=False):
        out[(row.ticker, row.date)] = {"close": float(row.close)}
    return out


def _research_shorts(dates: list[str], *, allow_test: bool) -> list[dict]:
    """Same 12 gates, short at the close. Not a freeze candidate."""
    from .factor_mine_oos0914_theme import SNAP_DIR, score_shorts, skip_calendar

    if not dates or not SNAP_DIR.is_dir():
        return []
    rules = _short_rules()
    flags = skip_calendar(dates, rules, allow_test=allow_test)
    tickers = set()
    for rule in rules:
        for names in (flags.get(rule["id"]) or {}).values():
            if names:
                tickers.update(names)
    closes = _closes(dates, tickers, allow_test=allow_test)
    reports = score_shorts(dates, rules, closes, list(dates), allow_test=allow_test)
    rows = []
    for rule in rules:
        row = dict(reports.get(rule["id"]) or {})
        row["designed_after"] = bool(allow_test)
        rows.append(row)
    return rows


def _write_test(dates: list[str], recipes: list[dict]) -> dict:
    train = json.loads(TRAIN_REPORT.read_text(encoding="utf-8"))
    rules = [_rule_report(rec, dates, STATE_ROOT) for rec in recipes]
    test = {
        "sessions": dates,
        "rules": rules,
        "baselines": _baselines(dates) if dates else {},
        "research_shorts": _research_shorts(dates, allow_test=True),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    TEST_REPORT.write_text(json.dumps(test, indent=2), encoding="utf-8")
    SCOREBOARD.write_text(render_scoreboard(train, test), encoding="utf-8")
    return test


def score() -> dict:
    """Walk frozen rules on locked test days. No rules means no test walk."""
    if not TRAIN_REPORT.is_file():
        raise SystemExit("train report missing; run mine first")
    train = json.loads(TRAIN_REPORT.read_text(encoding="utf-8"))
    recipes = frozen_recipes()
    if not recipes:
        text = render_scoreboard(train, None)
        SCOREBOARD.write_text(text, encoding="utf-8")
        print("[oos0914] nothing frozen; test not scored", flush=True)
        return {"rules": []}
    dates = test_dates()
    lock_books(dates, recipes)
    test = _write_test(dates, recipes)
    for rule in test["rules"]:
        shown = rule.get("after_fees_return")
        if shown is None and rule.get("clean_from"):
            shown = rule.get("designed_after_return")
            print(
                f"[oos0914] test {rule['name']} clean=n/a "
                f"designed_after={shown}",
                flush=True,
            )
        else:
            print(f"[oos0914] test {rule['name']} {shown}", flush=True)
    return test


def append_commit_paths(date: str) -> list[str]:
    """Files an OOS append must land: the ledger, its sha256, and state."""
    day = str(date or "")[:10]
    paths = [
        f"data/factor_mine/oos0914/ledgers/{day}.json",
        f"data/factor_mine/oos0914/ledgers/{day}.json.sha256",
    ]
    for rec in frozen_recipes():
        name = str(rec.get("name") or "")
        if name:
            paths.append(f"data/factor_mine/oos0914/state/{name}/{day}.json")
    return paths


def assert_logged_append_committed(log_text: str, committed) -> list[str]:
    """Fail when the log says a day was appended and that day is not committed.

    ``committed`` is the path list on the branch the publish just pushed.
    A run that did not append returns an empty list.
    """
    import re
    dates = re.findall(
        r"\[oos0914\] nightly: appended (\d{4}-\d{2}-\d{2})",
        log_text or "",
    )
    if not dates:
        return []
    have = {str(path) for path in committed}
    missing = []
    for day in dates:
        for rel in append_commit_paths(day):
            if rel not in have:
                missing.append(rel)
    if missing:
        raise SystemExit(
            "OOS append was logged but nothing was committed:\n"
            + "\n".join(missing)
        )
    return dates


def append_nightly(*, through: str = "", write: bool = False) -> dict:
    """After the factor-mine land, append the next OOS day if it is locked.

    A day already on disk is not rewritten. No frozen rule means nothing
    to append. Live HOT4 / holdup state is a different directory.
    """
    recipes = frozen_recipes()
    if not recipes:
        print("[oos0914] nightly: nothing frozen", flush=True)
        return {"appended": None}
    closed = str(through or "")[:10]
    if len(closed) != 10:
        closed = fm.last_closed_session(TEST_START) or ""
    dates = test_dates(closed)
    if not dates:
        print("[oos0914] nightly: no locked test session", flush=True)
        return {"appended": None}
    have = [
        d for d in dates
        if seq.read_state(recipes[0]["name"], d, STATE_ROOT) is not None
    ]
    missing = [d for d in dates if d not in have]
    if not missing:
        print(f"[oos0914] nightly: through {dates[-1]} already locked", flush=True)
        return {"appended": None}
    # One new day. Earlier missing days are a gap; fill only the next one.
    nxt = missing[0]
    if not write:
        print(f"[oos0914] nightly: would append {nxt}", flush=True)
        return {"appended": None, "pending": nxt}
    # The calendar includes earlier locked days so day N reads day N-1.
    # Those state files already match and are not rewritten.
    lock_books([d for d in dates if d <= nxt], recipes)
    if TRAIN_REPORT.is_file():
        dates = test_dates(closed)
        _write_test(dates, recipes)
    print(f"[oos0914] nightly: appended {nxt}", flush=True)
    return {"appended": nxt}


def _same_number(left, right) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return abs(float(left) - float(right)) <= 1e-9


def load_train_history() -> dict[str, list]:
    """Replay train books in memory. Does not write a report or state."""
    doc = load_preregister()
    recipes = study_recipes(doc)
    dates = sorted(load_snapshot_dir(SNAP_DIR, start=TRAIN_START, end=TRAIN_END))
    snaps = load_snapshot_dir(SNAP_DIR, start=TRAIN_START, end=TRAIN_END)
    tickers = {"IWM"}
    for _date, snap in snaps.items():
        for row in snapshot_rows(snap, _date):
            if row.get("ticker"):
                tickers.add(str(row["ticker"]).upper())
    bars = load_session_bars(
        LIVE_OHLC, dates, tickers, max_date=TRAIN_END, allow_test=False,
    )
    print(f"[oos0914] fee view train days={len(dates)} names={len(tickers)}", flush=True)
    history = _score_named(dates, recipes, snaps, bars)
    cands = expand_candidates(doc)
    excel_ids = [c["id"] for c in cands if c.get("family") == "excel"]
    theme = [c for c in cands if c.get("family") == "theme_radar_skip"]
    if excel_ids:
        from .factor_mine_oos0914_excel import load_fit, score_dates as excel_score
        history.update(excel_score(dates, allow_test=False, fit=load_fit()))
    if theme:
        history.update(_score_theme(dates, theme, snaps, bars, allow_test=False))
    return history


def apply_report_view() -> None:
    """Add the flat-15bp daily view and stamp the four frozen rules.

    Picks, fills, Futubull returns, and fingerprints stay as they are.
    """
    manifest = fmf.MANIFEST_PATH.read_bytes()
    fit_bytes = (OUT_DIR / "excel_fit.pkl").read_bytes()
    state_bytes = {
        str(path.relative_to(STATE_ROOT)): path.read_bytes()
        for path in sorted(STATE_ROOT.glob("*/*.json"))
    }
    frozen = json.loads(FROZEN_PATH.read_text(encoding="utf-8"))
    fingerprints = {
        rec["name"]: fmr.recipe_fingerprint(rec) for rec in frozen["recipes"]
    }
    for name, digest in fingerprints.items():
        if digest != (frozen.get("fingerprints") or {}).get(name):
            raise SystemExit(f"fingerprint drift before the view {name}")
    ledger_paths = sorted(LEDGER_DIR.glob("*.json"))
    prior_sigs = {}
    prior_ledger = {}
    for path in ledger_paths:
        doc = json.loads(path.read_text(encoding="utf-8"))
        prior_ledger[path.name] = json.loads(json.dumps(doc))
        prior_sigs[path.name] = {
            name: fmr._ledger_signature(slot)
            for name, slot in (doc.get("recipes") or {}).items()
        }
    history = load_train_history()
    train = json.loads(TRAIN_REPORT.read_text(encoding="utf-8"))
    if len(train.get("rows") or []) != 37:
        raise SystemExit(f"expected 37 train rows, have {len(train.get('rows') or [])}")
    bare = {_bare_id(name) for name in fingerprints}
    for row in train["rows"]:
        series = history.get(row["id"]) or []
        flats = flat_15bp_means(series)
        daily = row.get("daily") or []
        if len(flats) != len(daily):
            raise SystemExit(f"{row['id']} daily length {len(daily)} != {len(flats)}")
        for point, flat, item in zip(daily, flats, series):
            if not _same_number(point.get("ret_pct"), item.get("mean")):
                raise SystemExit(
                    f"{row['id']} {point.get('date')} futubull "
                    f"{point.get('ret_pct')} != replay {item.get('mean')}"
                )
            point["ret_pct_flat_15bp"] = flat
        if row["id"] in bare:
            if int(row.get("fires") or 0) != 12:
                raise SystemExit(f"{row['id']} fires {row.get('fires')} != 12")
            row["keep_bar_met"] = False
            row["keep_bar_note"] = KEEP_BAR_NOTE
    TRAIN_REPORT.write_text(json.dumps(train, indent=2) + "\n", encoding="utf-8")
    by_train = {row["id"]: row for row in train["rows"]}
    frozen_list = json.loads(FROZEN_LIST.read_text(encoding="utf-8"))
    if len(frozen_list.get("candidates") or []) != 37:
        raise SystemExit("frozen list is not 37")
    for cand in frozen_list["candidates"]:
        src = by_train.get(cand["id"]) or {}
        old_daily = cand.get("daily") or []
        new_daily = src.get("daily") or []
        if len(old_daily) != len(new_daily):
            raise SystemExit(f"frozen list daily length {cand['id']}")
        for old, new in zip(old_daily, new_daily):
            if old.get("date") != new.get("date") or not _same_number(old.get("ret_pct"), new.get("ret_pct")):
                raise SystemExit(f"frozen list futubull daily changed {cand['id']}")
        cand["daily"] = new_daily
        if cand.get("sha256") != fingerprints.get(cand.get("name")) and cand["name"] in fingerprints:
            raise SystemExit(f"frozen list fingerprint {cand['name']}")
        if cand["id"] in bare:
            cand["keep_bar_met"] = False
            cand["keep_bar_note"] = KEEP_BAR_NOTE
    FROZEN_LIST.write_text(json.dumps(frozen_list, indent=2) + "\n", encoding="utf-8")
    for rec in frozen["recipes"]:
        if fmr.recipe_fingerprint(rec) != fingerprints[rec["name"]]:
            raise SystemExit(f"fingerprint moved {rec['name']}")
        rec["keep_bar_met"] = False
        rec["keep_bar_note"] = KEEP_BAR_NOTE
        if fmr.recipe_fingerprint(rec) != fingerprints[rec["name"]]:
            raise SystemExit(f"keep stamp changed fingerprint {rec['name']}")
    FROZEN_PATH.write_text(json.dumps(frozen, indent=2) + "\n", encoding="utf-8")
    recipes = frozen_recipes()
    for path in ledger_paths:
        doc = prior_ledger[path.name]
        date = str(doc.get("date") or path.stem)[:10]
        for name, slot in (doc.get("recipes") or {}).items():
            view = _flat_mean_through(name, date, STATE_ROOT)
            chain = []
            for earlier in ledger_paths:
                if earlier.name > path.name:
                    break
                earlier_doc = prior_ledger[earlier.name]
                earlier_slot = (earlier_doc.get("recipes") or {}).get(name) or {}
                chain.append({
                    "equity": earlier_slot.get("equity"),
                    "trades": earlier_slot.get("trades") or [],
                    "buys": earlier_slot.get("buys") or [],
                    "sells": earlier_slot.get("sells") or [],
                })
            from_ledger = flat_15bp_means(chain)[-1]
            if not _same_number(view, from_ledger):
                raise SystemExit(
                    f"flat view {name} {date} state {view} != ledger {from_ledger}"
                )
            slot["mean_flat_15bp"] = view
        raw = fmr._canonical(doc)
        path.write_bytes(raw)
        path.with_name(path.name + ".sha256").write_text(
            hashlib.sha256(raw).hexdigest() + "\n", encoding="utf-8",
        )
        got = {
            name: fmr._ledger_signature(slot)
            for name, slot in (doc.get("recipes") or {}).items()
        }
        if got != prior_sigs[path.name]:
            raise SystemExit(f"ledger signature changed {path.name}")
        snap = (load_snapshot_dir(
            SNAP_DIR, start=date, end=date, cutoff="9999-99-99",
        ).get(date) or {})
        proposed = _ledger_doc(date, recipes, STATE_ROOT, snap)
        if fmr._canonical(proposed) != raw:
            raise SystemExit(f"ledger rebuild drifted {date}")
    test = json.loads(TEST_REPORT.read_text(encoding="utf-8"))
    by_rec = {rec["name"]: rec for rec in recipes}
    for rule in test.get("rules") or []:
        series = _chain(rule["name"], test.get("sessions") or [], STATE_ROOT)
        flats = flat_15bp_means(series)
        days = rule.get("days") or []
        if len(flats) != len(days):
            raise SystemExit(f"test days {rule['name']}")
        for day, flat, row in zip(days, flats, series):
            if str(day.get("date"))[:10] != str(row.get("date"))[:10]:
                raise SystemExit(f"test day order {rule['name']}")
            if not _same_number(day.get("mean"), row.get("mean")):
                raise SystemExit(f"test futubull mean changed {rule['name']}")
            day["mean_flat_15bp"] = flat
        rec = by_rec[rule["name"]]
        rule["keep_bar_met"] = False
        rule["keep_bar_note"] = rec.get("keep_bar_note")
    TEST_REPORT.write_text(json.dumps(test, indent=2) + "\n", encoding="utf-8")
    SCOREBOARD.write_text(render_scoreboard(train, test), encoding="utf-8")
    if fmf.MANIFEST_PATH.read_bytes() != manifest:
        raise SystemExit("freeze manifest changed")
    if (OUT_DIR / "excel_fit.pkl").read_bytes() != fit_bytes:
        raise SystemExit("excel fit changed")
    now_state = {
        str(path.relative_to(STATE_ROOT)): path.read_bytes()
        for path in sorted(STATE_ROOT.glob("*/*.json"))
    }
    if now_state != state_bytes:
        raise SystemExit("state files changed")
    print("[oos0914] report view written", flush=True)


def main(argv=None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="OOS-0914 strategy mine")
    parser.add_argument("cmd", choices=("mine", "freeze", "score", "append"))
    parser.add_argument("--through", default="")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    if args.cmd == "mine":
        mine()
    elif args.cmd == "freeze":
        freeze()
    elif args.cmd == "score":
        score()
    else:
        append_nightly(through=args.through, write=args.write or True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
