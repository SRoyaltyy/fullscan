"""Retroactive point-in-time rebuild of Factor Mine sessions.

Each session D in 2026-08-13..2026-09-24 is rebuilt from the git blob of
every dated packet as of D 09:30 ET (``git log --before`` on that path).
A named input that was committed only after the open is withheld. That
day is ``incomplete_pit`` and the book carries. It is not filled from
the later file.

The retro price tape is a separate split-adjusted store. Indicators
keep using bars dated before D. The live unadjusted ``data/prices``
print tape is not rewritten.

  python -m src.factor_mine_retro
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from . import config
from . import factor_mine as fm
from . import factor_mine_freeze as fmf
from . import gainer_asof as ga
from . import ticker_lookback as tl

ROOT = fm.ROOT
RETRO_DIR = ROOT / "data" / "factor_mine" / "retro_prices"
RETRO_STORE = RETRO_DIR / "ohlc.parquet"
RETRO_META = RETRO_DIR / "meta.json"
REPORT_MD = ROOT / "03_scoreboard" / "FACTOR_MINE_RETRO_PIT.md"
REPORT_JSON = ROOT / "data" / "factor_mine" / "retro_report.json"
FLAT_15BP = 0.0015
PRICE_START = "2026-05-01"
GLND = "GLND"

# Sessions already on the published panel. Earlier days stay out.
SESSIONS = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
    "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
)

# D-dated files the 09:30 picks read. A later-only commit withholds the
# file and marks the day incomplete. A path that was never committed is
# absent (the product did not exist yet), not a look-ahead fill.
NAMED_INPUTS = {
    "digest": "01_daily/news/{d}_finviz_digest.json",
    "map_heat": "01_daily/map_heat/{d}_map_heat.json",
    "export": "data/exports/finviz_{d}.csv",
    "join": "data/join/{d}_ranked.csv",
    "catalyst": "01_daily/catalyst/{d}_dossiers.json",
    "baseline": "01_daily/map_heat/{d}_research_baseline.json",
    "weather": "01_daily/weather/{d}_weather.json",
    "predict": "01_daily/general/{d}_predict.md",
    "actions": "01_daily/news/{d}_actions.json",
    "judge": "01_daily/news/{d}_judge.json",
    "events": "01_daily/events/{d}_events.json",
    "research": "01_daily/map_heat/{d}_research.json",
}

INPUT_GLOBS = (
    "01_daily/news/2026-0[7-9]*",
    "01_daily/map_heat/2026-0[7-9]*",
    "01_daily/catalyst/2026-0[7-9]*",
    "01_daily/weather/2026-0[7-9]*",
    "01_daily/general/2026-0[7-9]*",
    "01_daily/events/2026-0[7-9]*",
    "01_daily/sectors/2026-0[7-9]*",
    "data/exports/finviz_2026-0[7-9]*.csv",
    "data/join/2026-0[7-9]*_ranked.csv",
    "data/stock_book/2026-0[7-9]*",
    "data/ab_checklist/2026-0[7-9]*",
    "data/peers/2026-0[7-9]*",
    "data/universe/2026-0[7-9]*",
    "data/quote_colors/2026-0[7-9]*",
    "03_scoreboard/scoreboard.json",
    "03_scoreboard/mover_lookback_action.json",
)

_HISTORY: dict[str, list[tuple[datetime, str]]] | None = None


def cutoff_for(date: str) -> datetime:
    return datetime.strptime(date, "%Y-%m-%d").replace(
        hour=9, minute=30, tzinfo=tl.ET)


def named_paths(date: str) -> dict[str, str]:
    return {key: tmpl.format(d=date) for key, tmpl in NAMED_INPUTS.items()}


def _git_commits(path: str) -> list[tuple[datetime, str]]:
    out = subprocess.run(
        ["git", "log", "--pretty=format:%cI %H", "--", path],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    rows = []
    for line in (out.stdout or "").splitlines():
        line = line.strip()
        if not line or " " not in line:
            continue
        stamp, sha = line.split(" ", 1)
        try:
            ts = datetime.fromisoformat(stamp)
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=tl.ET)
        rows.append((ts, sha.strip()))
    rows.sort(key=lambda item: item[0])
    return rows


def load_histories(paths: list[str] | None = None) -> dict[str, list[tuple[datetime, str]]]:
    """Commit list per path, oldest first. Cached for the process."""
    global _HISTORY
    if paths is None and _HISTORY is not None:
        return _HISTORY
    if paths is None:
        listing = subprocess.run(
            ["git", "ls-files", *INPUT_GLOBS],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        paths = [p for p in (listing.stdout or "").splitlines() if p]
        for date in SESSIONS:
            for rel in named_paths(date).values():
                if rel not in paths:
                    paths.append(rel)
    unique = sorted(set(paths))

    def one(rel: str):
        return rel, _git_commits(rel)

    found: dict[str, list[tuple[datetime, str]]] = {}
    with ThreadPoolExecutor(max_workers=16) as pool:
        for rel, rows in pool.map(one, unique):
            found[rel] = rows
    if paths is None or _HISTORY is None:
        _HISTORY = found
    return found


def commit_asof(path: str, cutoff: datetime,
                history: dict | None = None) -> str | None:
    """Latest commit of ``path`` at or before ``cutoff``. None if none."""
    rows = (history or load_histories()).get(path)
    if rows is None:
        rows = _git_commits(path)
    sha = None
    for ts, item in rows:
        if ts <= cutoff:
            sha = item
        else:
            break
    return sha


def classify_day(date: str, history: dict | None = None) -> dict:
    """``pit_rebuilt`` or ``incomplete_pit`` plus the source commit of each input."""
    history = history if history is not None else {}
    cutoff = cutoff_for(date)
    sources = {}
    missing_late = []
    absent = []
    for key, rel in named_paths(date).items():
        rows = history.get(rel)
        if rows is None:
            rows = _git_commits(rel)
            history[rel] = rows
        if not rows:
            absent.append(key)
            sources[rel] = None
            continue
        sha = commit_asof(rel, cutoff, {rel: rows})
        sources[rel] = sha
        if sha is None:
            missing_late.append(key)
    label = "incomplete_pit" if missing_late else "pit_rebuilt"
    return {
        "date": date,
        "label": label,
        "cutoff": cutoff.isoformat(),
        "sources": sources,
        "missing_late": missing_late,
        "absent": absent,
    }


def input_files() -> list[str]:
    return sorted(load_histories())


_BLOB_CACHE = Path("/tmp/fm_retro_blobs")


def _extract(sha: str, rel: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(f"{sha}:{rel}".encode()).hexdigest()
    cached = _BLOB_CACHE / key
    if not cached.is_file():
        raw = subprocess.run(
            ["git", "show", f"{sha}:{rel}"],
            cwd=ROOT, capture_output=True, check=False,
        )
        if raw.returncode != 0:
            return
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(raw.stdout)
    dest.write_bytes(cached.read_bytes())


def materialize(date: str, dest: Path, history: dict | None = None) -> dict:
    """Write the pre-09:30 tree. Post-cutoff blobs are not copied.

    A tree walked forward from an earlier cutoff drops files that this
    morning did not have, and does not keep a later commit of a path.
    """
    history = history or load_histories()
    info = classify_day(date, history)
    cutoff = cutoff_for(date)
    dest = Path(dest)
    stamp_path = dest / ".pit_shas.json"
    previous = {}
    if stamp_path.is_file():
        try:
            previous = json.loads(stamp_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            previous = {}
    chosen: dict[str, str] = {}
    for rel, rows in history.items():
        sha = None
        for ts, item in rows:
            if ts <= cutoff:
                sha = item
            else:
                break
        if sha:
            chosen[rel] = sha
    for rel in list(previous):
        if rel not in chosen:
            path = dest / rel
            if path.is_file():
                path.unlink()
    for rel, sha in chosen.items():
        if previous.get(rel) == sha and (dest / rel).is_file():
            continue
        _extract(sha, rel, dest / rel)
    dest.mkdir(parents=True, exist_ok=True)
    stamp_path.write_text(json.dumps(chosen), encoding="utf-8")
    return info


@contextmanager
def overlay_inputs(dest: Path):
    """Point packet readers at ``dest`` for one session build."""
    from . import finviz_events as fe
    from . import judge_apply
    from . import map_heat_research as mhr
    from . import sleeve_merge as sm
    from . import stock_book as sb
    from . import weather

    dest = Path(dest)
    saved = {
        "tl": (
            tl.ROOT, tl.BOOK_DIR, tl.JOIN_DIR, tl.EXPORT_DIR, tl.AB_DIR,
            tl.PEER_DIR, tl.UNIVERSE_DIR, tl.QUOTE_DIR, tl.CATALYST_DIR,
            tl.NEWS_DIR, tl.GENERAL_DIR, tl.MAP_HEAT_DIR, tl.WEATHER_DIR,
            tl.PRICE_STORE,
        ),
        "ga": ga.EXPORT_DIR,
        "fe": fe.EXPORT_DIR,
        "sb": (sb.NEWS_DIR, sb.JOIN_DIR),
        "mhr": (mhr.OUT_DIR, mhr.HEAT_DIR),
        "judge": judge_apply.NEWS_DIR,
        "weather": weather.DAILY,
        "sm": (sm.BOOK_DIR, sm.PAYLOAD, sm.ROOT),
        "score": config.SCOREBOARD_JSON,
        "index": tl._INDEX,
    }
    tl.ROOT = dest
    tl.BOOK_DIR = dest / "data" / "stock_book"
    tl.JOIN_DIR = dest / "data" / "join"
    tl.EXPORT_DIR = dest / "data" / "exports"
    tl.AB_DIR = dest / "data" / "ab_checklist"
    tl.PEER_DIR = dest / "data" / "peers"
    tl.UNIVERSE_DIR = dest / "data" / "universe"
    tl.QUOTE_DIR = dest / "data" / "quote_colors"
    tl.CATALYST_DIR = dest / "01_daily" / "catalyst"
    tl.NEWS_DIR = dest / "01_daily" / "news"
    tl.GENERAL_DIR = dest / "01_daily" / "general"
    tl.MAP_HEAT_DIR = dest / "01_daily" / "map_heat"
    tl.WEATHER_DIR = dest / "01_daily" / "weather"
    tl.PRICE_STORE = RETRO_STORE if RETRO_STORE.is_file() else tl.PRICE_STORE
    ga.EXPORT_DIR = tl.EXPORT_DIR
    fe.EXPORT_DIR = tl.EXPORT_DIR
    fe._EXPORT_IDX.clear()
    sb.NEWS_DIR = tl.NEWS_DIR
    sb.JOIN_DIR = tl.JOIN_DIR
    mhr.OUT_DIR = tl.MAP_HEAT_DIR
    mhr.HEAT_DIR = tl.MAP_HEAT_DIR
    judge_apply.NEWS_DIR = tl.NEWS_DIR
    weather.DAILY = dest / "01_daily"
    sm.BOOK_DIR = tl.BOOK_DIR
    sm.ROOT = dest
    payload = dest / "03_scoreboard" / "mover_lookback_action.json"
    if not payload.is_file():
        payload.parent.mkdir(parents=True, exist_ok=True)
        payload.write_text(json.dumps({
            "session_dates": list(SESSIONS),
            "regime": {},
            "called_rows": [],
        }), encoding="utf-8")
    sm.PAYLOAD = payload
    score = dest / "03_scoreboard" / "scoreboard.json"
    config.SCOREBOARD_JSON = str(score if score.is_file() else Path(saved["score"]))
    tl._INDEX = None
    fmf.reset_price_memory()
    try:
        yield
    finally:
        (tl.ROOT, tl.BOOK_DIR, tl.JOIN_DIR, tl.EXPORT_DIR, tl.AB_DIR,
         tl.PEER_DIR, tl.UNIVERSE_DIR, tl.QUOTE_DIR, tl.CATALYST_DIR,
         tl.NEWS_DIR, tl.GENERAL_DIR, tl.MAP_HEAT_DIR, tl.WEATHER_DIR,
         tl.PRICE_STORE) = saved["tl"]
        ga.EXPORT_DIR = saved["ga"]
        fe.EXPORT_DIR = saved["fe"]
        fe._EXPORT_IDX.clear()
        sb.NEWS_DIR, sb.JOIN_DIR = saved["sb"]
        mhr.OUT_DIR, mhr.HEAT_DIR = saved["mhr"]
        judge_apply.NEWS_DIR = saved["judge"]
        weather.DAILY = saved["weather"]
        sm.BOOK_DIR, sm.PAYLOAD, sm.ROOT = saved["sm"]
        config.SCOREBOARD_JSON = saved["score"]
        tl._INDEX = saved["index"]
        fmf.reset_price_memory()


def _stamp_manifest(date: str, info: dict) -> None:
    man = fmf.load_manifest()
    slot = man.setdefault("snapshots", {}).setdefault(date, {})
    slot["label"] = info["label"]
    slot["missing_late"] = list(info.get("missing_late") or [])
    slot["absent"] = list(info.get("absent") or [])
    slot["source_commits"] = {
        k: v for k, v in (info.get("sources") or {}).items() if v
    }
    man["retro"] = {
        "kind": "pit_rebuilt",
        "window": [SESSIONS[0], SESSIONS[-1]],
        "price_store": str(RETRO_STORE.relative_to(ROOT)),
        "adjusted": True,
    }
    fmf.save_manifest(man)


def _price_ok(ticker: str, date: str) -> bool:
    return fmf.row_price_problem(ticker, date) is None


def build_day_rows(date: str, dest: Path) -> tuple[list[dict], list[str]]:
    """Panel rows for one pit_rebuilt session. Names without bars are dropped."""
    with overlay_inputs(dest):
        extra = fm.build_panel(date, date, fail_closed=False)
    rows = []
    dropped = []
    for row in extra.get("rows") or []:
        if row.get("date") != date:
            continue
        ticker = row.get("ticker")
        if not _price_ok(ticker, date):
            dropped.append(ticker)
            continue
        rows.append(row)
    return rows, dropped


def snapshot_for(date: str, info: dict, rows: list[dict],
                 prices_sha: str | None) -> dict:
    """Frozen rows plus the git commit of each input.

    Does not copy the live map-heat file. The heat vintage is the
    pre-09:30 commit recorded in ``info``.
    """
    prior = None
    earlier = [d for d in SESSIONS if d < date]
    if earlier:
        prior = earlier[-1]
    heat_sha = (info.get("sources") or {}).get(
        f"01_daily/map_heat/{date}_map_heat.json")
    frozen_rows = []
    if info["label"] != "incomplete_pit":
        for row in rows:
            item = dict(row)
            item["heat_vintage"] = heat_sha
            item["open_0930"] = row.get("open")
            frozen_rows.append(item)
        frozen_rows.sort(key=lambda r: (
            r.get("date") or "", int(r.get("src_rank") or 0), r.get("ticker") or "",
        ))
    return {
        "date": date,
        "asof": "09:30_et",
        "open_clock": "09:30 ET",
        "label": info["label"],
        "origin": "retro_pit",
        "cutoff": info["cutoff"],
        "carry": info["label"] == "incomplete_pit",
        "prior_session": prior,
        "heat": {"vintage": heat_sha, "source_commit": heat_sha},
        "prices_sha256": prices_sha,
        "sources": info.get("sources") or {},
        "missing_late": list(info.get("missing_late") or []),
        "absent": list(info.get("absent") or []),
        "n_rows": len(frozen_rows),
        "rows": frozen_rows,
    }


def retro_recipes() -> list[dict]:
    from . import factor_mine_combo as fmc
    recs = list(fm.build_recipes())
    for spec in fmc.combo_specs():
        recs.append(fmc.combo_recipe(spec))
    return recs


def _empty_panel(dates: list[str]) -> dict:
    return {
        "from_date": dates[0] if dates else SESSIONS[0],
        "to_date": dates[-1] if dates else SESSIONS[-1],
        "session_dates": list(dates),
        "rows": [],
        "by_date": {d: [] for d in dates},
        "n_rows": 0,
        "n_sessions": len(dates),
        "asof": "09:30_et",
    }


def assemble_panel(dates: list[str]) -> dict:
    """Frozen rows only. Incomplete days contribute an empty book."""
    panel = _empty_panel(dates)
    by_date = dict(panel["by_date"])
    for date in dates:
        snap = fmf.read_json(fmf.snapshot_path(date)) or {}
        by_date[date] = list(snap.get("rows") or [])
    rows = []
    for date in dates:
        rows.extend(by_date[date])
    panel["by_date"] = by_date
    panel["rows"] = rows
    panel["n_rows"] = len(rows)
    return panel


def _bars_for(panel: dict, date: str) -> dict:
    """Official retro bars for every name the store has on ``date``."""
    import pandas as pd
    bars = fmf.bars_from_panel(panel)
    if not RETRO_STORE.is_file():
        return bars
    df = pd.read_parquet(RETRO_STORE)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    day = df[df["date"] == date]
    for rec in day.itertuples(index=False):
        ticker = str(getattr(rec, "ticker", "") or "").upper()
        if not ticker:
            continue
        bars[(ticker, date)] = {
            "open": getattr(rec, "open", None),
            "high": getattr(rec, "high", None),
            "low": getattr(rec, "low", None),
            "close": getattr(rec, "close", None),
        }
    return bars


def lock_price_meta(df) -> str:
    """Write the adjusted store once. A second lock with the same bytes is a no-op."""
    import pandas as pd
    RETRO_DIR.mkdir(parents=True, exist_ok=True)
    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame = frame.drop_duplicates(subset=["date", "ticker"], keep="first")
    frame = frame.sort_values(["ticker", "date"]).reset_index(drop=True)
    if RETRO_STORE.is_file() and RETRO_META.is_file():
        try:
            meta = json.loads(RETRO_META.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            meta = {}
        if meta.get("locked"):
            have = hashlib.sha256(RETRO_STORE.read_bytes()).hexdigest()
            if have == meta.get("sha256"):
                print(f"[retro] price store already locked sha={have[:12]}", flush=True)
                return have
    frame.to_parquet(RETRO_STORE, index=False)
    raw = RETRO_STORE.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    meta = {
        "locked": True,
        "adjusted": True,
        "auto_adjust": True,
        "sha256": digest,
        "start": str(frame["date"].min())[:10],
        "end": str(frame["date"].max())[:10],
        "n_rows": int(len(frame)),
        "n_tickers": int(frame["ticker"].nunique()),
        "bytes": len(raw),
    }
    RETRO_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[retro] locked adjusted bars sha={digest[:12]} "
          f"rows={meta['n_rows']} tickers={meta['n_tickers']}", flush=True)
    return digest


def candidate_tickers() -> list[str]:
    """Liquid names across the window, plus names already on the panel."""
    names: set[str] = set()
    if fm.PANEL_PATH.is_file():
        try:
            panel = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            panel = {}
        for row in panel.get("rows") or []:
            tick = fm._tick(row.get("ticker"))
            if tick:
                names.add(tick)
    for date in SESSIONS:
        path = ga.EXPORT_DIR / f"finviz_{date}.csv"
        if not path.is_file():
            continue
        df = ga.load_finviz(date)
        for raw in ga._liquid_tape(
            df, top_n=0, min_change=0.0, liquid=True,
            min_mcap_m=None, side="up", skip_change=True,
        ):
            tick = fm._tick((raw or {}).get("ticker"))
            if tick:
                names.add(tick)
    names.add(GLND)
    names.discard("")
    return sorted(names)


def fetch_adjusted_bars(tickers: list[str] | None = None) -> str:
    """One split-adjusted download. Stored bars are not replaced."""
    import pandas as pd
    import yfinance as yf

    if RETRO_META.is_file():
        try:
            meta = json.loads(RETRO_META.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            meta = {}
        if meta.get("locked") and RETRO_STORE.is_file():
            return str(meta.get("sha256") or "")
    names = list(tickers or candidate_tickers())
    end = "2026-09-25"
    frames = []
    if RETRO_STORE.is_file():
        frames.append(pd.read_parquet(RETRO_STORE))
    chunk = 80
    for i in range(0, len(names), chunk):
        batch = names[i:i + chunk]
        print(f"[retro] adjusted fetch {i // chunk + 1}/"
              f"{(len(names) - 1) // chunk + 1} {batch[0]}…{batch[-1]}",
              flush=True)
        raw = yf.download(
            tickers=batch, start=PRICE_START, end=end, group_by="ticker",
            auto_adjust=True, actions=False, threads=True, progress=False,
        )
        part = _flatten_adjusted(raw, batch)
        if not len(part):
            time.sleep(6)
            raw = yf.download(
                tickers=batch, start=PRICE_START, end=end, group_by="ticker",
                auto_adjust=True, actions=False, threads=True, progress=False,
            )
            part = _flatten_adjusted(raw, batch)
        if len(part):
            frames.append(part)
        time.sleep(1.0)
    if not frames:
        raise RuntimeError("adjusted price fetch returned no bars")
    return lock_price_meta(pd.concat(frames, ignore_index=True))


def _flatten_adjusted(raw, tickers: list[str]):
    """Reuse the store flattener. Download already applied split adjustment."""
    from . import price_store as ps
    return ps._flatten_yf(raw, tickers)


def flat_15bp_order_fees(shares: int, price: float, side: str, fees) -> float:
    """7.5 bp per side so a round trip costs 15 bp of notional."""
    if shares <= 0 or price <= 0:
        return 0.0
    return round(float(shares) * float(price) * (FLAT_15BP / 2.0), 4)


def drop_ticker(panel: dict, ticker: str) -> dict:
    ticker = str(ticker or "").upper()
    rows = [r for r in (panel.get("rows") or []) if r.get("ticker") != ticker]
    by_date = {}
    for row in rows:
        by_date.setdefault(row.get("date"), []).append(row)
    out = dict(panel)
    out["rows"] = rows
    out["by_date"] = by_date
    out["n_rows"] = len(rows)
    return out


def _full_bars(panel: dict) -> dict:
    import pandas as pd
    bars = fmf.bars_from_panel(panel)
    if not RETRO_STORE.is_file():
        return bars
    df = pd.read_parquet(RETRO_STORE)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    want = set(panel.get("session_dates") or [])
    tickers = {r.get("ticker") for r in (panel.get("rows") or [])}
    tickers.add(GLND)
    day = df[df["date"].isin(want) & df["ticker"].isin(tickers)]
    for rec in day.itertuples(index=False):
        ticker = str(getattr(rec, "ticker", "") or "").upper()
        date = str(getattr(rec, "date", ""))[:10]
        if not ticker or not date:
            continue
        bars[(ticker, date)] = {
            "open": getattr(rec, "open", None),
            "high": getattr(rec, "high", None),
            "low": getattr(rec, "low", None),
            "close": getattr(rec, "close", None),
        }
    return bars


def score_recipe(panel: dict, name: str, *, flat_15bp: bool = False,
                 exclude: str | None = None, start: str | None = None) -> dict:
    from . import factor_mine_book as fmb
    from . import paper_trade as pt

    rec = next((r for r in retro_recipes() if r.get("name") == name), None)
    if rec is None:
        raise SystemExit(f"missing recipe {name}")
    view = drop_ticker(panel, exclude) if exclude else panel
    if start:
        view = fm.slice_panel(view, start, view.get("to_date"))
    fees = fm.pt_fees()
    orig = pt.order_fees
    if flat_15bp:
        pt.order_fees = flat_15bp_order_fees
    try:
        book = fmb.simulate_book(view, rec, bars=_full_bars(view), fees=fees)
    finally:
        pt.order_fees = orig
    created = fm.recipe_created_on(name, rec)
    return {
        "name": name,
        "created_on": created,
        "in_sample_before": created,
        "start": start or view.get("from_date"),
        "exclude": exclude,
        "fee": "flat_15bp" if flat_15bp else "futubull",
        "total_ret_pct": book.get("total_ret_pct"),
        "final_equity": book.get("final_equity"),
        "n_trades": book.get("n_trades"),
    }


def write_report(classed: list[dict], scores: list[dict]) -> None:
    rebuilt = [c["date"] for c in classed if c["label"] == "pit_rebuilt"]
    incomplete = [c for c in classed if c["label"] == "incomplete_pit"]
    lines = [
        "# Factor Mine retroactive point-in-time rebuild",
        "",
        f"Window `{SESSIONS[0]}` → `{SESSIONS[-1]}`. "
        "Each D-dated packet is the last git commit at or before D 09:30 ET. "
        "A named input that exists only in a later commit is withheld.",
        "",
        f"- pit_rebuilt: {len(rebuilt)}",
        f"- incomplete_pit: {len(incomplete)}",
        "",
        "## Incomplete days",
        "",
        "These days are carried (no new ranking). The later file is not used.",
        "",
    ]
    for info in incomplete:
        late = ", ".join(info.get("missing_late") or []) or "(none)"
        lines.append(f"- `{info['date']}` withheld: {late}")
    lines += ["", "## pit_rebuilt", ""]
    lines.append(", ".join(f"`{d}`" for d in rebuilt) or "(none)")
    lines += [
        "",
        "## HOT4 and holdup",
        "",
        "Total return on the frozen history. "
        "`union_hot_n4_holdup` days before `2026-09-21` are in-sample. "
        "Without GLND drops that ticker from the candidate rows. "
        "Flat 15bp is 7.5 bp per side (15 bp round trip), not the Futubull schedule.",
        "",
        "| recipe | fee | universe | window | return % | trades |",
        "| --- | --- | --- | --- | ---: | ---: |",
    ]
    for row in scores:
        universe = "without GLND" if row.get("exclude") else "with GLND"
        lines.append(
            f"| `{row['name']}` | {row['fee']} | {universe} | "
            f"{row.get('start')} | {row.get('total_ret_pct')} | {row.get('n_trades')} |"
        )
    lines += [
        "",
        "Prices: `data/factor_mine/retro_prices/ohlc.parquet` "
        "(Yahoo `auto_adjust=True`, locked, first bar wins). "
        "Indicators use bars dated before D. "
        "The live `data/prices/ohlc.parquet` print tape was not rewritten.",
        "",
    ]
    REPORT_MD.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    REPORT_JSON.write_text(json.dumps({
        "classed": [
            {k: v for k, v in info.items() if k != "sources"}
            | {"n_sources": sum(1 for s in (info.get("sources") or {}).values() if s)}
            for info in classed
        ],
        "scores": scores,
    }, indent=2), encoding="utf-8")
    print(f"[retro] wrote {REPORT_MD}", flush=True)


def rebuild(*, fetch: bool = True) -> dict:
    """Classify, lock bars, write snapshots and ledgers, score HOT4."""
    history = load_histories()
    classed = [classify_day(d, history) for d in SESSIONS]
    for info in classed:
        late = ",".join(info["missing_late"]) or "-"
        print(f"[retro] {info['date']} {info['label']} late={late}", flush=True)
    prices_sha = None
    if fetch:
        prices_sha = fetch_adjusted_bars()
    elif RETRO_META.is_file():
        prices_sha = json.loads(RETRO_META.read_text(encoding="utf-8")).get("sha256")
    tl.PRICE_STORE = RETRO_STORE
    fmf.reset_price_memory()
    import tempfile
    work = Path(tempfile.mkdtemp(prefix="fm-retro-"))
    panel = _empty_panel(list(SESSIONS))
    recipes = retro_recipes()
    print(f"[retro] recipes={len(recipes)}", flush=True)
    for info in classed:
        date = info["date"]
        if info["label"] == "pit_rebuilt":
            materialize(date, work, history)
            rows, dropped = build_day_rows(date, work)
            info["dropped_prices"] = dropped
            print(f"[retro] built {date} rows={len(rows)} dropped={len(dropped)}",
                  flush=True)
        else:
            rows = []
            print(f"[retro] carry {date}", flush=True)
        snap = snapshot_for(date, info, rows, prices_sha)
        fmf.write_snapshot(date, snap, restate=False)
        _stamp_manifest(date, info)
        panel["by_date"][date] = list(snap.get("rows") or [])
        panel["rows"] = [r for r in panel["rows"] if r.get("date") != date]
        panel["rows"].extend(panel["by_date"][date])
        panel["n_rows"] = len(panel["rows"])
        # Ledger sees the sessions landed so far so start-books exist.
        landed = [d for d in SESSIONS if d <= date]
        day_panel = assemble_panel(landed)
        bars = _bars_for(day_panel, date)
        try:
            ledger = fmf.build_ledger(
                day_panel, {}, recipes, date, bars,
            )
        except fmf.HoldDay as e:
            print(f"[retro] ledger hold {date}: {e}", flush=True)
            ledger = {
                "date": date,
                "origin": "incomplete_pit",
                "carry": True,
                "error": str(e),
                "recipes": {},
            }
        ledger["label"] = info["label"]
        fmf.write_ledger(date, ledger, restate=False)
    full = assemble_panel(list(SESSIONS))
    scores = []
    for name in ("union_hot_n4_h1", "union_hot_n4_holdup"):
        created = fm.recipe_created_on(name, {"name": name})
        for flat in (False, True):
            for exclude in (None, GLND):
                scores.append(score_recipe(
                    full, name, flat_15bp=flat, exclude=exclude))
                if created and created > SESSIONS[0]:
                    scores.append(score_recipe(
                        full, name, flat_15bp=flat, exclude=exclude,
                        start=created))
    write_report(classed, scores)
    return {"classed": classed, "scores": scores, "prices_sha": prices_sha}


def main() -> None:
    rebuild(fetch=os.environ.get("FM_RETRO_NO_FETCH") != "1")


if __name__ == "__main__":
    main()
