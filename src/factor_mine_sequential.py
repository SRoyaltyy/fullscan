"""One session at a time, from yesterday's state file.

Day N reads that day's frozen snapshot, bars dated on or before N, and
``data/factor_mine/state/<recipe>/<prior>.json``. It writes
``data/factor_mine/state/<recipe>/<N>.json`` and does not open a later
snapshot or a later bar. A file that is already there is left as it is.

The #336 ledger loop resumed cash from the previous ledger, then
``score_recipe`` walked the whole panel in one ``simulate_book`` call.
This module is the book those headlines come from.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_freeze as fmf

ROOT = fm.ROOT
STATE_DIR = ROOT / "data" / "factor_mine" / "state"
FILL_SIDES = ("BUY", "SHORT", "SELL", "COVER")


def state_path(recipe: str, date: str, root: Path | None = None) -> Path:
    return Path(root or STATE_DIR) / recipe / f"{date}.json"


def dumps(doc: dict) -> str:
    return json.dumps(doc, sort_keys=True, indent=2) + "\n"


def write_state(recipe: str, date: str, doc: dict, root: Path | None = None) -> Path:
    """Write once. Identical bytes are a no-op. A different body is refused."""
    path = state_path(recipe, date, root)
    raw = dumps(doc).encode("utf-8")
    if path.is_file():
        have = path.read_bytes()
        if have == raw:
            return path
        raise fmf.FrozenHistory(
            f"{path} already frozen ({len(have)} bytes); refusing to rewrite"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return path


def read_state(recipe: str, date: str, root: Path | None = None) -> dict | None:
    path = state_path(recipe, date, root)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def prior_session(dates: list[str], date: str) -> str | None:
    prev = [d for d in dates if d < date]
    return prev[-1] if prev else None


def ledger_fingerprint(directory: Path | None = None) -> dict[str, tuple[int, int]]:
    """Size and mtime of each ledger file. The sequential walk must not move these."""
    folder = Path(directory or fmf.LEDGER_DIR)
    if not folder.is_dir():
        return {}
    out: dict[str, tuple[int, int]] = {}
    for path in sorted(folder.iterdir()):
        if not path.is_file():
            continue
        st = path.stat()
        out[path.name] = (st.st_size, st.st_mtime_ns)
    return out


def rows_for_day(date: str, snap_dir: Path | None = None) -> list[dict]:
    """The frozen snapshot named ``date``. Other files in the directory are not opened."""
    folder = Path(snap_dir or fmf.SNAP_DIR)
    path = folder / f"{date}.json"
    if not path.is_file():
        return []
    doc = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for row in doc.get("rows") or []:
        if str(row.get("date") or "")[:10] == date:
            rows.append(row)
    return rows


def bars_through(store: dict, date: str) -> dict:
    """Bars whose date is on or before ``date``. A later print is not passed in."""
    out = {}
    for key, bar in (store or {}).items():
        if not isinstance(key, tuple) or len(key) < 2:
            continue
        stamp = str(key[1])[:10]
        if stamp and stamp <= date:
            out[key] = bar
    return out


def load_bar_store(dates: list[str]) -> dict:
    """Retro OHLC for these sessions only. Later parquet rows are not copied in."""
    import pandas as pd

    from .factor_mine_retro import RETRO_STORE

    want = {str(d)[:10] for d in dates}
    bars: dict = {}
    if not RETRO_STORE.is_file() or not want:
        return bars
    df = pd.read_parquet(
        RETRO_STORE, columns=["date", "ticker", "open", "high", "low", "close"],
    )
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    day = df[df["date"].isin(want)]
    for rec in day.itertuples(index=False):
        ticker = str(getattr(rec, "ticker", "") or "").upper()
        stamp = str(getattr(rec, "date", ""))[:10]
        if not ticker or not stamp:
            continue
        bars[(ticker, stamp)] = {
            "open": getattr(rec, "open", None),
            "high": getattr(rec, "high", None),
            "low": getattr(rec, "low", None),
            "close": getattr(rec, "close", None),
        }
    return bars


def one_day_panel(date: str, rows: list[dict], calendar: list[str]) -> dict:
    """Today's candidate rows, on the session clock through today.

    Earlier dates stay on the clock so a lot's hold counts. Their rows
    are not loaded. A later date is not on the clock.
    """
    cal = [d for d in calendar if d <= date]
    if date not in cal:
        cal.append(date)
    copied = [dict(r) for r in rows if str(r.get("date") or date)[:10] == date]
    for row in copied:
        row["date"] = date
    by_date = {d: [] for d in cal}
    by_date[date] = copied
    return {
        "from_date": cal[0],
        "to_date": date,
        "session_dates": cal,
        "rows": copied,
        "by_date": by_date,
        "n_rows": len(copied),
        "n_sessions": len(cal),
        "asof": "09:30_et",
        # Snapshot rows already carry tape, clock, and oppset. Do not
        # rescan those files (a later file must not enter this session).
        "_ohlc_filled": True,
        "_tape_filled": True,
        "_clock_b": True,
        "_oppset": True,
    }


def _fills(trades: list[dict]) -> list[dict]:
    out = []
    for trade in trades:
        if trade.get("side") not in FILL_SIDES:
            continue
        out.append({
            "side": trade.get("side"),
            "ticker": trade.get("ticker"),
            "shares": trade.get("shares"),
            "price": trade.get("price"),
            "fees": trade.get("fees"),
            "pnl": trade.get("pnl"),
        })
    return out


def record_from_book(name: str, date: str, book: dict) -> dict:
    decision = fmf.decision_from_book(book, date)
    fills = _fills(decision.get("trades") or [])
    fees = round(sum(float(t.get("fees") or 0) for t in fills), 4)
    state = decision.get("state") or {}
    daily = decision.get("daily") or {}
    pos = state.get("pos") or {}
    return {
        "recipe": name,
        "date": date,
        "buys": [t.get("ticker") for t in (decision.get("buys") or []) if t.get("ticker")],
        "sells": [t.get("ticker") for t in (decision.get("sells") or []) if t.get("ticker")],
        "fills": fills,
        "fees": fees,
        "cash": state.get("cash"),
        "holdings": sorted(str(t) for t in pos.keys()),
        "equity": daily.get("equity"),
        "mean": daily.get("mean"),
        "n_trades_today": len(fills),
        "state": state,
    }


def session_mean(equity, prior: dict | None) -> float | None:
    """Session percent versus yesterday's close. The first day uses $10k.

    A one-day ``simulate_book`` labels that row as day 0 and divides by
    the original capital. The prior state's equity is the baseline.
    """
    base = float(fm.CAPITAL)
    state = (prior or {}).get("state") or {}
    saved = state.get("yday_equity")
    if saved not in (None, ""):
        try:
            base = float(saved)
        except (TypeError, ValueError):
            base = float(fm.CAPITAL)
    if equity is None or base <= 0:
        return None
    return round(100.0 * (float(equity) / base - 1.0), 4)


def step_recipe(date: str, rec: dict, rows: list[dict], bars: dict, *,
                calendar: list[str],
                prior: dict | None = None,
                members: list[dict] | None = None,
                fees=None, regime=None, rules=None) -> dict:
    """Simulate this session from ``prior`` state. ``bars`` must already be clipped."""
    name = rec.get("name") or ""
    panel = one_day_panel(date, rows, calendar)
    saved = {"state": prior["state"]} if prior and prior.get("state") else None
    if rec.get("members") or rec.get("universe") == "combo":
        spec = {
            "name": name,
            "members": list(rec.get("members") or []),
            "weights": list(rec.get("weights") or []),
            "net": rec.get("net") or "priority",
            "pool": rec.get("pool") or "shared",
        }
        book = fmf._simulate_combo(
            panel, spec, list(members or []), start=None, bars=bars,
            fees=fees, regime=regime, saved=saved,
        )
    else:
        from . import factor_mine_book as fmb
        book = fmb.simulate_book(
            panel, rec, bars=bars, fees=fees, regime=regime,
            resume=None if saved is None else saved.get("state"),
            rules=rules,
        )
    doc = record_from_book(name, date, book)
    doc["mean"] = session_mean(doc.get("equity"), prior)
    return doc


def walk(dates: list[str], recipes: list[dict], *,
         rows_for, bars_for, root: Path | None = None,
         persist: bool = True, fees=None, regime=None,
         exclude: str | None = None,
         history: dict[str, list] | None = None,
         rules=None) -> dict[str, dict]:
    """Step every recipe across ``dates``. Returns the last record per recipe.

    ``rows_for(date)`` and ``bars_for(date)`` supply that morning only.
    When ``persist`` is set, each close is written under ``root`` and an
    existing file is not recomputed. ``history`` collects every session
    in order, including days that were already on disk.
    """
    by_name = {r.get("name"): r for r in recipes if r.get("name")}
    last: dict[str, dict] = {}
    for date in dates:
        rows = list(rows_for(date) or [])
        if exclude:
            rows = [r for r in rows if str(r.get("ticker") or "").upper() != exclude.upper()]
        if rows and "e_pol" not in rows[0]:
            from . import factor_mine_probe as fmp
            fmp.attach_erd_polarity({
                "rows": rows,
                "session_dates": [d for d in dates if d <= date],
            })
        bars = bars_through(bars_for(date) or {}, date)
        prev = prior_session(dates, date)
        for rec in recipes:
            name = rec.get("name")
            if not name:
                continue
            if persist:
                have = read_state(name, date, root)
                if have is not None:
                    last[name] = have
                    if history is not None:
                        history.setdefault(name, []).append(have)
                    continue
            prior = read_state(name, prev, root) if (persist and prev) else None
            if not persist:
                prior = last.get(name) if prev else None
                if prior and prior.get("date") != prev:
                    prior = None
            members = [by_name[m] for m in (rec.get("members") or []) if m in by_name]
            if rec.get("members") and len(members) != len(rec.get("members") or []):
                continue
            doc = step_recipe(
                date, rec, rows, bars, calendar=dates, prior=prior,
                members=members, fees=fees, regime=regime, rules=rules,
            )
            if persist:
                write_state(name, date, doc, root)
            last[name] = doc
            if history is not None:
                history.setdefault(name, []).append(doc)
        print(f"[sequential] {date} recipes={len(last)}", flush=True)
    return last


def compound(records: list[dict]) -> float | None:
    """Last equity versus the $10k start, in percent."""
    if not records:
        return None
    equity = records[-1].get("equity")
    if equity is None:
        return None
    return round(100.0 * (float(equity) / float(fm.CAPITAL) - 1.0), 3)


def chain_records(recipe: str, dates: list[str], root: Path | None = None) -> list[dict]:
    out = []
    for date in dates:
        doc = read_state(recipe, date, root)
        if doc is not None:
            out.append(doc)
    return out


def run_books(dates: list[str] | None = None, *,
              root: Path | None = None) -> dict[str, dict]:
    """Write the Futubull 08-13 chain. Does not rewrite ledgers."""
    from .factor_mine_book import load_regime
    from .factor_mine_retro import SESSIONS, retro_recipes

    dates = list(dates or SESSIONS)
    before = ledger_fingerprint()
    recipes = retro_recipes()
    store = load_bar_store(dates)
    regime = load_regime()
    fees = fm.pt_fees()

    def rows_for(date: str):
        return rows_for_day(date)

    def bars_for(date: str):
        return {k: v for k, v in store.items() if str(k[1])[:10] == date}

    last = walk(
        dates, recipes, rows_for=rows_for, bars_for=bars_for,
        root=root, persist=True, fees=fees, regime=regime,
    )
    after = ledger_fingerprint()
    if after != before:
        raise fmf.FrozenHistory("sequential walk changed a ledger file")
    return last


def _main() -> None:
    from .factor_mine_retro import SESSIONS

    run_books(list(SESSIONS))
    for name in ("union_hot_n4_h1", "union_hot_n4_holdup"):
        rows = chain_records(name, list(SESSIONS))
        print(
            f"[sequential] {name} days={len(rows)} "
            f"return={compound(rows)} equity={None if not rows else rows[-1].get('equity')}",
            flush=True,
        )


if __name__ == "__main__":
    _main()
