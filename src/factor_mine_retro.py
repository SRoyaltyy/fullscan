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
import random
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
        "code_sha": fmf.code_sha(),
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


RANDOM4_N = 4
RANDOM4_DRAWS = 1000
RANDOM4_SEED = 20260813
BASELINE_WINDOWS = (SESSIONS[0], "2026-09-21")


def linear_percentile(values, p: float) -> float:
    """Linear percentile. Index is ``(n - 1) * (p / 100)``."""
    xs = sorted(float(v) for v in values)
    n = len(xs)
    if n == 0:
        raise ValueError("empty")
    if n == 1:
        return round(xs[0], 3)
    k = (n - 1) * (float(p) / 100.0)
    lo = int(k)
    hi = min(lo + 1, n - 1)
    weight = k - lo
    return round(xs[lo] * (1.0 - weight) + xs[hi] * weight, 3)


def _mean3(values) -> float:
    return round(sum(float(v) for v in values) / len(values), 3)


def median_count(values) -> int | float:
    """Median of a count. A .5 split stays one decimal."""
    xs = sorted(float(v) for v in values)
    n = len(xs)
    mid = n // 2
    med = xs[mid] if n % 2 else (xs[mid - 1] + xs[mid]) / 2.0
    if abs(med - round(med)) < 1e-9:
        return int(round(med))
    return round(med, 1)


def random4_recipe() -> dict:
    """Inline book. Not registered and not written to the creation catalog."""
    return {
        "name": "random4",
        "universe": "union",
        "hold": 1,
        "side": "long",
        "top_n": RANDOM4_N,
        "require": {},
        "forbid": {},
        "rank": "list",
        "exit_when": {},
        "size": "leftover",
        "sell": "list",
        "s_boost": "none",
        "day_cap": 1.0,
        "take_pct": None,
        "stop_pct": None,
        "note": "4 random names from that morning's frozen candidate list",
        "created_on": SESSIONS[0],
    }


def iwm_recipe() -> dict:
    return {
        "name": "iwm",
        "universe": "union",
        "hold": 1,
        "side": "long",
        "top_n": 1,
        "require": {},
        "forbid": {},
        "rank": "list",
        "exit_when": {},
        "size": "leftover",
        "sell": "list",
        "s_boost": "none",
        "day_cap": 1.0,
        "take_pct": None,
        "stop_pct": None,
        "note": "IWM buy-and-hold over the same sessions",
        "created_on": SESSIONS[0],
    }


def snapshot_pools(dates: list[str] | None = None) -> dict[str, list[str]]:
    """Sorted unique tickers on each frozen snapshot.

    That list is the morning candidate set the HOT4 book was scored on.
    An incomplete day has no rows, so the pool is empty.
    """
    pools: dict[str, list[str]] = {}
    for date in dates or SESSIONS:
        snap = fmf.read_json(fmf.snapshot_path(date)) or {}
        seen: set[str] = set()
        for row in snap.get("rows") or []:
            tick = fm._tick(row.get("ticker"))
            if tick:
                seen.add(tick)
        pools[date] = sorted(seen)
    return pools


def random4_draw(pools: dict[str, list[str]], draw_i: int, *,
                 n: int = RANDOM4_N, seed: int = RANDOM4_SEED,
                 exclude: str | None = None) -> dict[str, list[str]]:
    """One draw. ``exclude`` is removed before the sample."""
    rng = random.Random(int(seed) + int(draw_i))
    ban = str(exclude or "").upper()
    out: dict[str, list[str]] = {}
    for date in sorted(pools):
        pool = [t for t in pools[date] if t != ban]
        k = min(int(n), len(pool))
        out[date] = rng.sample(pool, k) if k else []
    return out


def _sim_ready(panel: dict) -> dict:
    """Skip tape, catalyst, and clock fills. These rows are already picks."""
    panel["_ohlc_filled"] = True
    panel["_tape_filled"] = True
    panel["_clock_b"] = True
    panel["_oppset"] = True
    return panel


def panel_from_picks(dates: list[str], picks: dict[str, list[str]]) -> dict:
    """Rows only on the day they are bought. A later day without the name sells it."""
    rows = []
    by_date: dict[str, list] = {}
    for date in dates:
        day_rows = []
        for i, tick in enumerate(picks.get(date) or []):
            row = {
                "date": date,
                "ticker": tick,
                "sources": ["union"],
                "src_rank": i,
                "ohlc_ret_1": 0.0,
                "rsi": 50.0,
            }
            day_rows.append(row)
            rows.append(row)
        by_date[date] = day_rows
    panel = {
        "from_date": dates[0] if dates else None,
        "to_date": dates[-1] if dates else None,
        "session_dates": list(dates),
        "rows": rows,
        "by_date": by_date,
        "n_rows": len(rows),
        "n_sessions": len(dates),
        "asof": "09:30_et",
    }
    return _sim_ready(panel)


def score_panel(panel: dict, rec: dict, bars: dict, *,
                flat_15bp: bool = False, fees=None,
                regime=None, rules=None) -> dict:
    """One cash book. ``regime`` is optional; None reads the morning S files."""
    from . import factor_mine_book as fmb
    from . import paper_trade as pt

    fees = fm.pt_fees() if fees is None else fees
    orig = pt.order_fees
    if flat_15bp:
        pt.order_fees = flat_15bp_order_fees
    try:
        book = fmb.simulate_book(
            panel, rec, bars=bars, fees=fees, regime=regime, rules=rules,
        )
    finally:
        pt.order_fees = orig
    return {
        "total_ret_pct": book.get("total_ret_pct"),
        "n_trades": book.get("n_trades"),
        "final_equity": book.get("final_equity"),
        "n_open": book.get("n_open"),
        "trades": book.get("trades"),
    }


def morning_regime(dates: list[str]) -> dict:
    """Same morning S ``score_recipe`` would read, cached for the draws."""
    from .factor_mine_book import morning_s

    regime = {}
    for date in dates:
        score = morning_s(None, date)
        if score is not None:
            regime[date] = {"predict_score": float(score)}
    return regime


def bars_for_tickers(tickers: set[str], dates: list[str]) -> dict:
    """Adjusted retro bars for these names. Does not write the store."""
    import pandas as pd

    bars: dict = {}
    if not RETRO_STORE.is_file() or not tickers:
        return bars
    df = pd.read_parquet(
        RETRO_STORE, columns=["date", "ticker", "open", "high", "low", "close"],
    )
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    want = set(dates)
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


def _summarize(rets: list[float], trades: list) -> dict:
    return {
        "mean": _mean3(rets),
        "p5": linear_percentile(rets, 5),
        "p50": linear_percentile(rets, 50),
        "p95": linear_percentile(rets, 95),
        "trades": median_count(trades),
        "n_draws": len(rets),
    }


def score_random4(pools: dict[str, list[str]], bars: dict, *,
                  draws: int = RANDOM4_DRAWS, seed: int = RANDOM4_SEED,
                  windows: tuple[str, ...] = BASELINE_WINDOWS,
                  regime=None, fees=None) -> list[dict]:
    """1000 draws × Futubull and 15bp × with and without GLND × both windows."""
    rec = random4_recipe()
    fees = fm.pt_fees() if fees is None else fees
    rows = []
    for flat in (False, True):
        for exclude in (None, GLND):
            for start in windows:
                dates = [d for d in SESSIONS if d >= start and d in pools]
                window_pools = {d: pools[d] for d in dates}
                rets = []
                trades = []
                label = "without GLND" if exclude else "with GLND"
                fee = "flat_15bp" if flat else "futubull"
                print(f"[retro] random4 {fee} {label} from {start} draws={draws}",
                      flush=True)
                for i in range(int(draws)):
                    picks = random4_draw(
                        window_pools, i, n=RANDOM4_N, seed=seed, exclude=exclude,
                    )
                    panel = panel_from_picks(dates, picks)
                    scored = score_panel(
                        panel, rec, bars, flat_15bp=flat, fees=fees, regime=regime,
                    )
                    rets.append(scored["total_ret_pct"])
                    trades.append(scored["n_trades"])
                    if draws >= 100 and (i + 1) % 250 == 0:
                        print(f"[retro] random4 {fee} {label} {start} {i + 1}/{draws}",
                              flush=True)
                summary = _summarize(rets, trades)
                rows.append({
                    "name": "random4",
                    "fee": fee,
                    "exclude": exclude,
                    "universe": label,
                    "start": start,
                    "draws": int(draws),
                    "seed": int(seed),
                    **summary,
                })
    return rows


def _bar_ok(bar: dict | None) -> bool:
    if not bar:
        return False
    return bar.get("open") is not None and bar.get("close") is not None


def _iwm_frame(raw) -> dict:
    import pandas as pd

    if raw is None or len(raw) == 0:
        return {}
    frame = raw.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        level0 = [str(c) for c in frame.columns.get_level_values(0)]
        if "Open" in level0:
            frame.columns = frame.columns.get_level_values(0)
        else:
            frame.columns = frame.columns.get_level_values(-1)
    frame = frame.reset_index()
    cols = {str(c).lower(): c for c in frame.columns}
    date_col = cols.get("date") or cols.get("datetime") or frame.columns[0]
    out = {}
    for _, row in frame.iterrows():
        day = str(pd.Timestamp(row[date_col]).date())

        def num(key, row=row):
            col = cols.get(key)
            if col is None:
                return None
            try:
                val = float(row[col])
            except (TypeError, ValueError):
                return None
            if val != val:
                return None
            return val

        opened, closed = num("open"), num("close")
        if opened is None and closed is None:
            continue
        out[("IWM", day)] = {
            "open": opened,
            "high": num("high"),
            "low": num("low"),
            "close": closed,
        }
    return out


def _iwm_yahoo(dates: list[str]) -> dict:
    import yfinance as yf

    raw = yf.download(
        "IWM", start=min(dates), end="2026-09-25",
        auto_adjust=True, actions=False, progress=False, threads=False,
    )
    return _iwm_frame(raw)


def _iwm_stooq(dates: list[str]) -> dict:
    text = fmf._fetch_stooq("IWM")
    out = {}
    for day in dates:
        bar = fmf.parse_stooq_bar(text, day)
        if bar.get("open") is None and bar.get("close") is None:
            continue
        out[("IWM", day)] = bar
    return out


def fetch_iwm_bars(dates: list[str]) -> tuple[dict, str]:
    """Adjusted IWM in memory. Neither price store is written."""
    try:
        bars = _iwm_yahoo(dates)
        yahoo_days = {d for d in dates if _bar_ok(bars.get(("IWM", d)))}
    except Exception as exc:
        print(f"[retro] IWM yahoo failed: {exc}", flush=True)
        bars = {}
        yahoo_days = set()
    missing = [d for d in dates if d not in yahoo_days]
    stooq_days: set[str] = set()
    if missing:
        alt = _iwm_stooq(dates)
        for day in missing:
            bar = alt.get(("IWM", day))
            if _bar_ok(bar):
                bars[("IWM", day)] = bar
                stooq_days.add(day)
    still = [d for d in dates if not _bar_ok(bars.get(("IWM", d)))]
    if still:
        raise RuntimeError(f"IWM tape missing {still[:8]}")
    if stooq_days and yahoo_days:
        source = (
            "Yahoo auto_adjust=True with Stooq iwm.us filling "
            f"{len(stooq_days)} session(s), in memory. "
            "Not written to data/factor_mine/retro_prices or data/prices."
        )
    elif stooq_days:
        source = (
            "Stooq iwm.us daily, in memory (Yahoo did not cover the window). "
            "Not written to data/factor_mine/retro_prices or data/prices."
        )
    else:
        source = (
            "Yahoo auto_adjust=True, fetched in memory. "
            "Not written to data/factor_mine/retro_prices or data/prices."
        )
    return bars, source


def score_iwm(bars: dict, *, windows: tuple[str, ...] = BASELINE_WINDOWS,
              regime=None, fees=None, tape: str = "") -> list[dict]:
    """One buy, held every session. Hard-red does not skip the entry."""
    rec = iwm_recipe()
    fees = fm.pt_fees() if fees is None else fees
    rules = {"hard_red_no_new": False}
    rows = []
    for flat in (False, True):
        for start in windows:
            dates = [d for d in SESSIONS if d >= start]
            picks = {d: ["IWM"] for d in dates}
            panel = panel_from_picks(dates, picks)
            scored = score_panel(
                panel, rec, bars, flat_15bp=flat, fees=fees,
                regime=regime, rules=rules,
            )
            rows.append({
                "name": "iwm",
                "fee": "flat_15bp" if flat else "futubull",
                "universe": "buy-and-hold",
                "start": start,
                "mean": scored["total_ret_pct"],
                "trades": scored["n_trades"],
                "final_equity": scored["final_equity"],
                "n_open": scored["n_open"],
                "tape": tape,
            })
    return rows


def _baseline_md(payload: dict) -> str:
    note = payload.get("open_check") or ""
    lines = [
        "## Baselines",
        "",
        "Scored on the same frozen sessions as HOT4 and holdup. "
        "Same $10k leftover book and the same fees (Futubull, and flat 15bp "
        "as 7.5 bp per side). RANDOM4 draws 4 names from that day's frozen "
        "snapshot list, the morning candidate rows the HOT4 book saw. "
        f"Seed `{RANDOM4_SEED}`, {RANDOM4_DRAWS} draws, "
        "`random.Random(seed + draw)`. "
        "An empty snapshot buys nothing. With GLND keeps that name in the "
        "pool. Without GLND drops it before the draw. Mean and 5/50/95 are "
        "total return percent, linear percentile. Trades are the median count. "
        "The morning S is the same one the HOT4 book reads, so a hard-red "
        "morning still sits.",
        "",
        "IWM is buy-and-hold over those sessions. The name stays on the list "
        "every day, so the lot is not sold, and the book is marked at the last "
        "close. Hard-red does not skip the IWM entry. IWM is one series, not "
        "a with-GLND and without-GLND pair.",
        "",
        f"IWM tape: {payload.get('iwm', {}).get('tape', '')}",
        "",
        "| baseline | fee | universe | window | mean % | p5 | p50 | p95 | trades |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload.get("random4", {}).get("rows") or []:
        lines.append(
            f"| `random4` | {row['fee']} | {row['universe']} | {row['start']} | "
            f"{row['mean']} | {row['p5']} | {row['p50']} | {row['p95']} | "
            f"{row['trades']} |"
        )
    for row in payload.get("iwm", {}).get("rows") or []:
        lines.append(
            f"| `iwm` | {row['fee']} | {row['universe']} | {row['start']} | "
            f"{row['mean']} |  |  |  | {row['trades']} |"
        )
    lines += ["", note, ""]
    return "\n".join(lines)


def _splice_baselines_md(text: str, section: str) -> str:
    marker = "## Baselines"
    if marker in text:
        text = text[:text.index(marker)].rstrip() + "\n"
    else:
        text = text.rstrip() + "\n"
    return text + "\n" + section


def _splice_baselines_json(original: str, payload: dict) -> str:
    """Attach ``baselines`` without reformatting classed or scores."""
    marker = '\n  "baselines": '
    if marker in original:
        head = original.split(marker, 1)[0].rstrip()
        if head.endswith(","):
            head = head[:-1].rstrip()
    else:
        head = original.rstrip()
        if not head.endswith("}"):
            raise RuntimeError("retro report json is not an object")
        head = head[:-1].rstrip()
    blob = json.dumps(payload, indent=2)
    indented = blob.replace("\n", "\n  ")
    return head + ",\n  \"baselines\": " + indented + "\n}\n"


DAILY_RETURNS_CSV = ROOT / "data" / "factor_mine" / "daily_returns.csv"


def daily_return_pct(daily: dict | None) -> float | None:
    """Session percent versus yesterday's equity.

    A resumed ledger row stores ``mean`` against the original $10k when
    that simulation only walked the new day. The shuffle series uses
    ``equity / yday_equity - 1`` instead.
    """
    row = daily or {}
    equity = row.get("equity")
    if equity is None:
        return None
    prev = row.get("yday_equity")
    try:
        equity = float(equity)
        base = float(prev) if prev not in (None, "") else float(fm.CAPITAL)
    except (TypeError, ValueError):
        return None
    if base == 0:
        return None
    return round(100.0 * (equity / base - 1.0), 4)


def write_daily_returns(path: Path | None = None,
                        dates: list[str] | None = None) -> int:
    """CSV columns: recipe, start_date, D, ret. One ledger file at a time."""
    import csv

    dest = Path(path or DAILY_RETURNS_CSV)
    dest.parent.mkdir(parents=True, exist_ok=True)
    days = list(dates) if dates is not None else fmf._ledger_dates()
    n = 0
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["recipe", "start_date", "D", "ret"])
        for date in days:
            ledger = fmf.read_ledger(date)
            if not ledger:
                continue
            recipes = ledger.get("recipes") or {}
            for name in sorted(recipes):
                starts = (recipes[name] or {}).get("starts") or {}
                for start in sorted(starts):
                    ret = daily_return_pct((starts[start] or {}).get("daily"))
                    if ret is None:
                        continue
                    writer.writerow([name, start, date, f"{ret:.4f}"])
                    n += 1
            del ledger
    print(f"[retro] daily returns {n} rows {dest}", flush=True)
    return n


def write_baselines(payload: dict) -> None:
    """Append or replace the baselines section. HOT4 rows stay as they are."""
    text = REPORT_MD.read_text(encoding="utf-8") if REPORT_MD.is_file() else ""
    REPORT_MD.write_text(_splice_baselines_md(text, _baseline_md(payload)), encoding="utf-8")
    raw = REPORT_JSON.read_text(encoding="utf-8") if REPORT_JSON.is_file() else "{}\n"
    REPORT_JSON.write_text(_splice_baselines_json(raw, payload), encoding="utf-8")
    print(f"[retro] baselines appended to {REPORT_MD.name}", flush=True)


def publish_baselines(*, draws: int = RANDOM4_DRAWS) -> dict:
    """Score RANDOM4 and IWM from frozen snapshots. Does not rebuild them."""
    pools = snapshot_pools()
    tickers = {t for names in pools.values() for t in names}
    print(f"[retro] baseline pools days={len(pools)} names={len(tickers)}", flush=True)
    bars = bars_for_tickers(tickers, list(SESSIONS))
    regime = morning_regime(list(SESSIONS))
    fees = fm.pt_fees()
    random_rows = score_random4(
        pools, bars, draws=draws, regime=regime, fees=fees,
    )
    iwm_bars, tape = fetch_iwm_bars(list(SESSIONS))
    iwm_rows = score_iwm(iwm_bars, regime=regime, fees=fees, tape=tape)
    payload = {
        "random4": {
            "n": RANDOM4_N,
            "draws": int(draws),
            "seed": RANDOM4_SEED,
            "rows": random_rows,
        },
        "iwm": {"tape": tape, "rows": iwm_rows},
        "open_check": (
            "Open cross-check uses theme-radar `{D}.raw.csv` Finviz Open when "
            "that fetch's scrape_ts is after D 09:30 ET and before the next "
            "session's 09:30 ET, and the sha256 matches HASHES.json. From "
            "2026-09-25 the slim `{D}.csv` Open column (appended last) is the "
            "next file under the same guard. A missing or late scrape uses "
            "Stooq for that day. `current.csv` is not a source. Webull paper "
            "fills stay a third check. The source used each day is "
            "`data/factor_mine/open_source_log.csv`. Per-recipe session "
            "returns for the shuffle test are "
            "`data/factor_mine/daily_returns.csv` "
            "(recipe, start_date, D, ret as percent versus the prior close equity)."
        ),
    }
    write_baselines(payload)
    return payload


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
