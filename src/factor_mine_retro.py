"""Retroactive point-in-time rebuild of Factor Mine sessions.

Each session D in 2026-08-13..2026-09-24 is rebuilt from the git blob of
every dated packet as of D 09:30 ET (``git log --before`` on that path).
A named input that was committed only after the open is withheld. That
day is ``incomplete_pit`` and the book carries. It is not filled from
the later file.

The retro price tape is a separate raw store (``auto_adjust=False``).
Split and dividend factors live in ``retro_prices/actions.parquet``.
Indicators apply only events with an ex-date before D. The live
``data/prices`` print tape is not rewritten.

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
RETRO_ACTIONS = RETRO_DIR / "actions.parquet"
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
    from . import price_store as ps

    saved = {
        "tl": (
            tl.ROOT, tl.BOOK_DIR, tl.JOIN_DIR, tl.EXPORT_DIR, tl.AB_DIR,
            tl.PEER_DIR, tl.UNIVERSE_DIR, tl.QUOTE_DIR, tl.CATALYST_DIR,
            tl.NEWS_DIR, tl.GENERAL_DIR, tl.MAP_HEAT_DIR, tl.WEATHER_DIR,
            tl.PRICE_STORE,
        ),
        "actions": ps.ACTIONS_PATH,
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
    ps.ACTIONS_PATH = RETRO_ACTIONS
    ps.reset_action_cache()
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
        ps.ACTIONS_PATH = saved["actions"]
        ps.reset_action_cache()
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
        "actions": str(RETRO_ACTIONS.relative_to(ROOT)),
        "adjusted": False,
        "auto_adjust": False,
    }
    fmf.save_manifest(man)


def _price_ok(ticker: str, date: str) -> bool:
    return fmf.row_price_problem(ticker, date) is None


def build_day_rows(date: str, dest: Path) -> tuple[list[dict], list[str]]:
    """Panel rows for one pit_rebuilt session.

    Names that still fail the price gate stay on the hole list. They
    are not removed from the row list here; the day is held instead.
    """
    with overlay_inputs(dest):
        rows = _panel_rows(date)
        holes = _row_holes(date, rows)
    return rows, holes


def _panel_rows(date: str) -> list[dict]:
    extra = fm.build_panel(date, date, fail_closed=False)
    return [row for row in (extra.get("rows") or []) if row.get("date") == date]


def _row_holes(date: str, rows: list[dict]) -> list[str]:
    holes = []
    for row in rows:
        ticker = row.get("ticker")
        if not _price_ok(ticker, date):
            holes.append(ticker)
    return holes


def snapshot_for(date: str, info: dict, rows: list[dict],
                 prices_sha: str | None, candidates: dict | None = None) -> dict:
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
    if info["label"] not in ("incomplete_pit", "held", "skipped"):
        for row in rows:
            item = dict(row)
            item["heat_vintage"] = heat_sha
            item["open_0930"] = row.get("open")
            frozen_rows.append(item)
        frozen_rows.sort(key=lambda r: (
            r.get("date") or "", int(r.get("src_rank") or 0), r.get("ticker") or "",
        ))
    snap = {
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
        "tape": "raw",
        "auto_adjust": False,
        "n_rows": len(frozen_rows),
        "n_dropped": len(info.get("dropped") or []),
        "dropped": list(info.get("dropped") or []),
        "rows": frozen_rows,
    }
    if candidates is not None:
        snap["candidates"] = {
            "n": candidates.get("n"),
            "prior_export": candidates.get("prior_export"),
            "excluded": list(candidates.get("excluded") or []),
            "names": list(candidates.get("names") or []),
        }
    return snap


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


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _read_meta() -> dict:
    if not RETRO_META.is_file():
        return {}
    try:
        meta = json.loads(RETRO_META.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return meta if isinstance(meta, dict) else {}


def _is_raw_lock() -> str | None:
    """Sha of a locked raw store. An adjusted lock is not raw."""
    meta = _read_meta()
    if not meta.get("locked") or not RETRO_STORE.is_file():
        return None
    if meta.get("auto_adjust") is not False or meta.get("adjusted") is True:
        return None
    have = hashlib.sha256(RETRO_STORE.read_bytes()).hexdigest()
    if have != meta.get("sha256"):
        return None
    return have


def _normalize_ohlc(df):
    import pandas as pd
    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    for col in ("open", "high", "low", "close", "volume"):
        if col not in frame.columns:
            frame[col] = None
    frame = frame[["date", "ticker", "open", "high", "low", "close", "volume"]]
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["close"])
    frame = frame.drop_duplicates(subset=["date", "ticker"], keep="first")
    return frame.sort_values(["ticker", "date"]).reset_index(drop=True)


def write_actions(df) -> None:
    """Keep-first split/dividend rows in the retro factor table.

    Does not write ``data/prices/actions.parquet``.
    """
    import pandas as pd
    from . import price_store as ps

    cols = ["date", "ticker", "dividend", "split", "close"]
    RETRO_DIR.mkdir(parents=True, exist_ok=True)
    frames = []
    if RETRO_ACTIONS.is_file():
        frames.append(pd.read_parquet(RETRO_ACTIONS))
    if df is not None and len(df):
        frames.append(df)
    if not frames:
        pd.DataFrame(columns=cols).to_parquet(RETRO_ACTIONS, index=False)
        return
    out = pd.concat(frames, ignore_index=True)
    for col in cols:
        if col not in out.columns:
            out[col] = None
    out = out[cols]
    out["date"] = pd.to_datetime(out["date"]).dt.normalize()
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out["dividend"] = pd.to_numeric(out["dividend"], errors="coerce").fillna(0.0)
    out["split"] = pd.to_numeric(out["split"], errors="coerce").fillna(0.0)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.drop_duplicates(subset=["date", "ticker"], keep="first")
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    out.to_parquet(RETRO_ACTIONS, index=False)
    ps.reset_action_cache()
    print(f"[retro] actions {len(out)} events", flush=True)


def lock_price_meta(df, actions=None) -> str:
    """Write raw bars. A matching raw lock is a no-op. Adjusted bytes are replaced."""
    import pandas as pd
    RETRO_DIR.mkdir(parents=True, exist_ok=True)
    frame = _normalize_ohlc(df)
    if not len(frame):
        raise RuntimeError("raw price store has no bars")
    frame.to_parquet(RETRO_STORE, index=False)
    raw = RETRO_STORE.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if actions is not None:
        write_actions(actions)
    elif not RETRO_ACTIONS.is_file():
        write_actions(pd.DataFrame())
    meta = {
        "locked": True,
        "adjusted": False,
        "auto_adjust": False,
        "sha256": digest,
        "actions": _rel(RETRO_ACTIONS) if RETRO_ACTIONS.is_file() else None,
        "start": str(frame["date"].min())[:10],
        "end": str(frame["date"].max())[:10],
        "n_rows": int(len(frame)),
        "n_tickers": int(frame["ticker"].nunique()),
        "bytes": len(raw),
    }
    RETRO_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[retro] locked raw bars sha={digest[:12]} "
          f"rows={meta['n_rows']} tickers={meta['n_tickers']}", flush=True)
    return digest


def merge_raw_bars(ohlc, actions=None) -> str:
    """Append raw bars. A stored (date, ticker) print is not replaced.

    An adjusted lock is discarded rather than concatenated.
    """
    import pandas as pd
    frames = []
    if RETRO_STORE.is_file() and _is_raw_lock():
        frames.append(pd.read_parquet(RETRO_STORE))
    if ohlc is not None and len(ohlc):
        frames.append(ohlc)
    if not frames:
        raise RuntimeError("raw price merge has no bars")
    return lock_price_meta(pd.concat(frames, ignore_index=True), actions)


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


def _store_tickers() -> set[str]:
    import pandas as pd
    if not RETRO_STORE.is_file():
        return set()
    df = pd.read_parquet(RETRO_STORE, columns=["ticker"])
    return set(df["ticker"].astype(str).str.upper())


def _download_raw(names: list[str], start: str, end: str):
    """Yahoo raw prints plus the dated factor rows. The live store is not written."""
    import pandas as pd
    import yfinance as yf
    from . import price_store as ps

    ohlc_frames = []
    action_frames = []
    chunk = 80
    batches = max(1, (len(names) - 1) // chunk + 1) if names else 1
    for i in range(0, len(names), chunk):
        batch = names[i:i + chunk]
        print(f"[retro] raw fetch {i // chunk + 1}/{batches} "
              f"{batch[0]}…{batch[-1]}", flush=True)
        raw = yf.download(
            tickers=batch, start=start, end=end, group_by="ticker",
            auto_adjust=False, actions=True, threads=True, progress=False,
        )
        part = ps._flatten_yf(raw, batch)
        acts = ps._flatten_actions(raw, batch)
        if not len(part):
            time.sleep(6)
            raw = yf.download(
                tickers=batch, start=start, end=end, group_by="ticker",
                auto_adjust=False, actions=True, threads=True, progress=False,
            )
            part = ps._flatten_yf(raw, batch)
            acts = ps._flatten_actions(raw, batch)
        if len(part):
            ohlc_frames.append(part)
        if acts is not None and len(acts):
            action_frames.append(acts)
        time.sleep(1.0)
    ohlc = pd.concat(ohlc_frames, ignore_index=True) if ohlc_frames else pd.DataFrame()
    actions = (
        pd.concat(action_frames, ignore_index=True) if action_frames else pd.DataFrame()
    )
    return ohlc, actions


def fetch_raw_bars(tickers: list[str] | None = None) -> str:
    """One raw download. A locked raw store is kept. Adjusted bars are replaced."""
    import pandas as pd

    locked = _is_raw_lock()
    if locked:
        print(f"[retro] raw price store already locked sha={locked[:12]}", flush=True)
        return locked
    if _read_meta().get("auto_adjust") is True:
        print("[retro] replacing adjusted price lock with raw bars", flush=True)
    names = set(tickers or candidate_tickers())
    names.update(_store_tickers())
    names.discard("")
    ordered = sorted(names)
    end = "2026-09-25"
    ohlc, actions = _download_raw(ordered, PRICE_START, end)
    if not len(ohlc):
        raise RuntimeError("raw price fetch returned no bars")
    return lock_price_meta(ohlc, actions)


def fetch_adjusted_bars(tickers: list[str] | None = None) -> str:
    """Raw bars only. Adjusted downloads are not stored."""
    return fetch_raw_bars(tickers)


def _missing_session(date: str, tickers: list[str]) -> list[str]:
    """Tickers with no raw open and close on ``date``."""
    import pandas as pd

    names = sorted({str(t).strip().upper() for t in tickers if t})
    if not names:
        return []
    if not RETRO_STORE.is_file():
        return names
    df = pd.read_parquet(RETRO_STORE, columns=["date", "ticker", "open", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["ticker"] = df["ticker"].astype(str).str.upper()
    day = df[df["date"] == str(date)[:10]]
    ok: set[str] = set()
    for rec in day.itertuples(index=False):
        opened, closed = rec.open, rec.close
        if opened != opened or closed != closed:
            continue
        if opened is None or closed is None:
            continue
        ok.add(str(rec.ticker))
    return [t for t in names if t not in ok]


def _bar_fields(date: str, ticker: str, opened, high, low, closed, volume=None):
    try:
        o = float(opened)
        c = float(closed)
    except (TypeError, ValueError):
        return None
    if o != o or c != c:
        return None
    try:
        h = float(high) if high is not None and high == high else max(o, c)
    except (TypeError, ValueError):
        h = max(o, c)
    try:
        lo = float(low) if low is not None and low == low else min(o, c)
    except (TypeError, ValueError):
        lo = min(o, c)
    return {
        "date": str(date)[:10],
        "ticker": str(ticker).upper(),
        "open": o,
        "high": h,
        "low": lo,
        "close": c,
        "volume": volume,
    }


_RAW_QUOTE_CACHE: dict[str, dict] = {}
# date → tickers whose bar was inserted from raw.csv (Yahoo had no print).
SINGLE_SOURCE: dict[str, set[str]] = {}
RAW_FILL_START = "2026-08-06"
RAW_FILL_END = "2026-09-24"
RAW_FILL_SKIP = frozenset({"2026-08-27"})


def _radar_raw_quotes(date: str) -> dict:
    """OHLC from ``{D}.raw.csv`` when the commit guard accepts it.

    ``Price`` is the close. ``Prev Close`` is the prior session's close.
    A late commit, a hash miss, or a missing file returns nothing.
    """
    day = str(date or "")[:10]
    if day in _RAW_QUOTE_CACHE:
        return _RAW_QUOTE_CACHE[day]
    tape = fmf.day_open_tape(day)
    if tape.get("source") != "finviz_raw":
        _RAW_QUOTE_CACHE[day] = {}
        return {}
    raw = fmf._theme_radar_raw_bytes(day)
    if not raw:
        _RAW_QUOTE_CACHE[day] = {}
        return {}
    parsed = fmf.parse_theme_radar_prices(raw.decode("utf-8", errors="replace"))
    _RAW_QUOTE_CACHE[day] = parsed
    return parsed


def raw_fill_dates() -> list[str]:
    """Weekdays from 2026-08-06 through 2026-09-24, except 2026-08-27."""
    import datetime as dt

    start = dt.date.fromisoformat(RAW_FILL_START)
    end = dt.date.fromisoformat(RAW_FILL_END)
    out = []
    day = start
    while day <= end:
        iso = day.isoformat()
        if day.weekday() < 5 and iso not in RAW_FILL_SKIP:
            out.append(iso)
        day += dt.timedelta(days=1)
    return out


def _prev_weekday(day: str) -> str:
    import datetime as dt

    cursor = dt.date.fromisoformat(str(day)[:10]) - dt.timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor -= dt.timedelta(days=1)
    return cursor.isoformat()


def _known_prints() -> set[tuple[str, str]]:
    """(date, ticker) pairs that already have an open and a close."""
    import pandas as pd

    if not RETRO_STORE.is_file():
        return set()
    df = pd.read_parquet(RETRO_STORE, columns=["date", "ticker", "open", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["ticker"] = df["ticker"].astype(str).str.upper()
    ok = df["open"].notna() & df["close"].notna()
    return {
        (str(rec.date), str(rec.ticker))
        for rec in df.loc[ok, ["date", "ticker"]].itertuples(index=False)
    }


def fill_raw_history(tickers: list[str] | None = None) -> int:
    """Insert raw.csv bars only for names Yahoo has no print for.

    A ticker that already has any stored print is left alone, including
    its missing sessions, highs, lows, and prior closes. ``Prev Close``
    is not turned into a bar. Inserted names are tagged ``single_source``.
    """
    import pandas as pd

    allowed = None
    if tickers is not None:
        allowed = {str(t).strip().upper() for t in tickers if t}
    known = _known_prints()
    have_yahoo = {ticker for _date, ticker in known}
    rows = []
    for date in raw_fill_dates():
        quotes = _radar_raw_quotes(date)
        if not quotes:
            continue
        for ticker, quote in quotes.items():
            if allowed is not None and ticker not in allowed:
                continue
            if ticker in have_yahoo:
                continue
            key = (date, ticker)
            if key in known:
                continue
            bar = _bar_fields(
                date, ticker,
                quote.get("open"), quote.get("high"), quote.get("low"),
                quote.get("close"), quote.get("volume"),
            )
            if not bar:
                continue
            rows.append(bar)
            SINGLE_SOURCE.setdefault(date, set()).add(ticker)
            known.add(key)
    if not rows:
        return 0
    print(f"[retro] raw.csv history rows {len(rows)}", flush=True)
    merge_raw_bars(pd.DataFrame(rows))
    return len(rows)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return float(ordered[mid - 1] + ordered[mid]) / 2.0


def summarize_price_diffs(pairs: list[tuple[float, float]]) -> dict:
    """Median and max |yahoo − raw| and |yahoo − raw| / |yahoo|."""
    abs_diff = []
    pct_diff = []
    for yahoo, raw in pairs:
        try:
            a = float(yahoo)
            b = float(raw)
        except (TypeError, ValueError):
            continue
        if a != a or b != b:
            continue
        gap = abs(a - b)
        abs_diff.append(gap)
        base = abs(a) if a else abs(b)
        pct_diff.append(gap / base if base else 0.0)
    return {
        "n": len(abs_diff),
        "median_abs": _median(abs_diff),
        "max_abs": max(abs_diff) if abs_diff else None,
        "median_pct": _median(pct_diff),
        "max_pct": max(pct_diff) if pct_diff else None,
    }


def compare_yahoo_frame(yahoo, quotes_by_date: dict) -> dict:
    """Diff stats for names present in both a Yahoo frame and raw.csv."""
    import pandas as pd

    empty = {
        "open": summarize_price_diffs([]),
        "close": summarize_price_diffs([]),
    }
    if yahoo is None or not len(yahoo):
        return empty
    frame = yahoo.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    opens: list[tuple[float, float]] = []
    closes: list[tuple[float, float]] = []
    for rec in frame.itertuples(index=False):
        quote = (quotes_by_date.get(str(rec.date)[:10]) or {}).get(str(rec.ticker))
        if not quote:
            continue
        if quote.get("open") is not None and rec.open == rec.open:
            opens.append((float(rec.open), float(quote["open"])))
        if quote.get("close") is not None and rec.close == rec.close:
            closes.append((float(rec.close), float(quote["close"])))
    return {
        "open": summarize_price_diffs(opens),
        "close": summarize_price_diffs(closes),
    }


def validate_raw_vs_yahoo(tickers: list[str] | None = None) -> dict:
    """Download Yahoo raw in memory and compare it to guarded raw.csv prints.

    The download is not merged into either price store.
    """
    dates = raw_fill_dates()
    quotes = {day: _radar_raw_quotes(day) for day in dates}
    names: set[str] = set()
    for doc in quotes.values():
        names.update(doc)
    if tickers is not None:
        allow = {str(t).strip().upper() for t in tickers if t}
        names &= allow
    names.discard("")
    if not names:
        stats = compare_yahoo_frame(None, quotes)
        stats["tickers"] = 0
        return stats
    end = "2026-09-25"
    yahoo, _actions = _download_raw(sorted(names), RAW_FILL_START, end)
    stats = compare_yahoo_frame(yahoo, quotes)
    stats["tickers"] = len(names)
    print(
        f"[retro] raw vs yahoo close n={stats['close']['n']} "
        f"median={stats['close']['median_abs']} max={stats['close']['max_abs']}",
        flush=True,
    )
    return stats


def drop_record(ticker: str, date: str) -> dict | None:
    """Why this name is dropped, or None when it can be ranked."""
    from . import ohlc_ripper as ohlc

    gaps = list(fmf.completeness_gaps(ticker, date))
    need = int(ohlc.INDICATOR_LOOKBACK)
    priors = [
        b for b in fmf._raw_bars(ticker)
        if str(b.get("date") or "") < str(date)[:10] and b.get("close") is not None
    ]
    if len(priors) < need:
        gaps.append(f"indicator bars {len(priors)}/{need}")
    if not gaps:
        return None
    # One reason string. Duplicate lookback lines stay as completeness wrote them.
    reason = "; ".join(dict.fromkeys(gaps))
    return {"ticker": str(ticker).upper(), "missing": gaps, "reason": reason}


def is_unrankable(ticker: str, date: str) -> bool:
    """True when the hot score would be computed on a short history.

    The lookback is the window ``ohlc.features`` requests
    (``INDICATOR_LOOKBACK`` prior closes). A missing session print is
    the same hole: the name is excluded, not scored as zero.
    """
    from . import ohlc_ripper as ohlc

    if fmf.completeness_gaps(ticker, date):
        return True
    need = int(ohlc.INDICATOR_LOOKBACK)
    priors = [
        b for b in fmf._raw_bars(ticker)
        if str(b.get("date") or "") < str(date)[:10] and b.get("close") is not None
    ]
    return len(priors) < need


def unrankable_holds(n_unrankable: int, n_candidates: int) -> bool:
    """True when unrankable candidates are strictly above the 10% line."""
    if n_candidates <= 0:
        return True
    return (float(n_unrankable) / float(n_candidates)) > float(fmf.UNRANKABLE_MAX_SHARE)


def parse_stooq_history(text: str) -> list[dict]:
    """Every Stooq daily bar that has an open and a close."""
    import csv
    import io

    reader = csv.DictReader(io.StringIO(text or ""))
    if not reader.fieldnames:
        return []
    fields = {name.strip().lower(): name for name in reader.fieldnames if name}
    rows = []
    for row in reader:
        stamp = str(row.get(fields.get("date") or "Date") or "").strip()
        if len(stamp) == 8 and stamp.isdigit():
            stamp = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:]}"
        if len(stamp) < 10:
            continue
        bar = _bar_fields(
            stamp[:10],
            "",
            row.get(fields.get("open") or "Open"),
            row.get(fields.get("high") or "High"),
            row.get(fields.get("low") or "Low"),
            row.get(fields.get("close") or "Close"),
            row.get(fields.get("volume") or "Volume"),
        )
        if bar:
            bar.pop("ticker", None)
            rows.append(bar)
    return rows


def recover_session_bars(date: str, tickers: list[str]) -> list[str]:
    """Fill a missing session from Yahoo, then raw.csv when Yahoo has no print.

    Stooq is not a price source. Returns tickers that still have no open
    and close on ``date``. A stored bar is not overwritten.
    """
    names = sorted({str(t).strip().upper() for t in tickers if t})
    missing = _missing_session(date, names)
    if not missing:
        return []
    print(f"[retro] {date} yahoo retry {len(missing)}", flush=True)
    ohlc, actions = _download_raw(missing, PRICE_START, "2026-09-25")
    if len(ohlc):
        merge_raw_bars(ohlc, actions if len(actions) else None)
    # Earlier raw.csv sessions too, under each file's own commit guard.
    # Yahoo bars already stored are left alone. A name filled only from
    # raw.csv is marked single_source.
    fill_raw_history(names)
    missing = _missing_session(date, names)
    if missing:
        print(f"[retro] {date} still missing {len(missing)}: {missing[:12]}", flush=True)
    return missing


def price_gate_fails(missing, holes, gaps) -> bool:
    """True when a session print is still absent or the cross-check failed."""
    return bool(missing or holes or gaps)


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
        auto_adjust=False, actions=False, progress=False, threads=False,
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
    """Raw IWM in memory. Neither price store is written."""
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
            "Yahoo auto_adjust=False with Stooq iwm.us filling "
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
            "Yahoo auto_adjust=False, fetched in memory. "
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
DAILY_RETURN_FIELDS = (
    "recipe", "recipe_created_date", "start_date", "D",
    "net_ret_futubull", "net_ret_15bp", "day_status", "source_shas",
    "timing_clean", "news_clean", "reads_news", "fires", "untestable",
)
# Fills that count as the recipe firing. OPEN/CLOSE marks are not trades.
FILL_SIDES = frozenset({"BUY", "SHORT", "SELL", "COVER"})
DAY_STATUSES = ("locked", "pit_rebuilt", "incomplete_pit", "held", "skipped")
# #331 data/quarantine_sessions.json. Not on main; the file is copied here
# so news_clean uses that list (18 sessions), not the shorter sketch.
QUARANTINE_PATH = ROOT / "data" / "quarantine_sessions.json"
# Gates that read the news packet, the catalyst camera, the judge camera,
# or the map-heat camera. Alarm / cond / zero-red tallies mix every camera
# and do not by themselves mark a recipe as a news reader.
_NEWS_INPUT_KEYS = frozenset({
    "news", "news_present", "news_box", "headline", "digest",
    "news_or_headline", "news_and_headline", "news_or_red",
    "catal", "catal_present", "major_catalyst",
    "yday_or_catalyst", "yday_and_catalyst",
    "judge", "heat",
    "clk_fresh_cat_coil", "clk_earn_guide_react", "clk_neg_weak_fail",
})


def format_bool(flag: bool) -> str:
    return "true" if flag else "false"


def news_quarantine_dates(path: Path | None = None) -> set[str]:
    """Session dates whose news packet #331 marks stale or undated."""
    src = path or QUARANTINE_PATH
    try:
        data = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    rows = data.get("sessions") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return set()
    out = set()
    for row in rows:
        if isinstance(row, dict) and row.get("date"):
            out.add(str(row["date"])[:10])
        elif isinstance(row, str) and len(row) >= 10:
            out.add(row[:10])
    return out


def timing_clean_day(status: str) -> bool:
    """True only when every named input was committed before 09:30 ET."""
    return str(status or "") == "pit_rebuilt"


def news_clean_day(day: str, quarantined: set[str] | None = None) -> bool:
    """False on a #331 quarantine date. Every other session is true."""
    bad = news_quarantine_dates() if quarantined is None else quarantined
    return str(day or "")[:10] not in bad


def _recipe_gate_keys(rec: dict | None) -> set[str]:
    rec = rec or {}
    keys: set[str] = set()
    for slot in ("require", "forbid", "exit_when"):
        block = rec.get(slot) or {}
        if isinstance(block, dict):
            keys.update(str(k) for k in block)
    return keys


def recipe_reads_news(name: str, by_name: dict | None = None,
                      seen: set[str] | None = None) -> bool:
    """True when this recipe, or a combo member, reads news inputs."""
    if by_name is None:
        by_name = {r.get("name"): r for r in retro_recipes() if r.get("name")}
    seen = set() if seen is None else seen
    if name in seen:
        return False
    seen.add(name)
    rec = by_name.get(name) or {}
    if _recipe_gate_keys(rec) & _NEWS_INPUT_KEYS:
        return True
    for member in rec.get("members") or []:
        if recipe_reads_news(str(member), by_name, seen):
            return True
    return False


def _fill_count(block: dict | None) -> int:
    """BUY / SHORT / SELL / COVER fills. OPEN and CLOSE marks are not fires."""
    return sum(
        1 for trade in ((block or {}).get("trades") or [])
        if trade.get("side") in FILL_SIDES
    )


def session_return_from_equity(daily: dict | None,
                               prior: float | None) -> float | None:
    """Session percent versus the previous session's equity.

    A resumed split ledger stores ``yday_equity`` as the original $10k.
    ``prior`` is that start's equity on the previous session, and it
    wins when it is present. The first session still uses the stored base.
    """
    row = daily or {}
    equity = row.get("equity")
    if equity is None:
        return None
    try:
        equity = float(equity)
    except (TypeError, ValueError):
        return None
    if prior is not None:
        base = float(prior)
    else:
        prev = row.get("yday_equity")
        try:
            base = float(prev) if prev not in (None, "") else float(fm.CAPITAL)
        except (TypeError, ValueError):
            return None
    if base == 0:
        return None
    return round(100.0 * (equity / base - 1.0), 4)


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


def format_net(value) -> str:
    """Four-decimal percent. A missing return is empty, never 0."""
    if value is None or value == "":
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if number != number:  # NaN
        return ""
    return f"{number:.4f}"


def ledger_day_status(ledger: dict | None) -> str:
    """``locked``, ``pit_rebuilt``, ``incomplete_pit``, ``skipped``, or ``held``."""
    label = str((ledger or {}).get("label") or "")
    if label == "skipped":
        return "skipped"
    if not ledger or not (ledger.get("recipes") or {}):
        return "held"
    if label in ("pit_rebuilt", "incomplete_pit", "locked"):
        return label
    if label in ("held", "held_incomplete", "held_review"):
        return "held"
    if ledger.get("origin") == "frozen":
        return "locked"
    return "held"


def load_recipe_catalog() -> dict[str, str]:
    """All 339 recipes → creation date. Missing file is an empty map."""
    path = fmf.CREATED_PATH
    if not path.is_file():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(doc, dict):
        return {}
    return {str(name): str(created)[:10] for name, created in doc.items()}


def format_source_shas(day: str, commits: dict | None) -> str:
    """Named-input commits for D, 12-char sha, sorted by input key.

    A null commit is ``key=``. An empty map is an empty cell.
    """
    if not commits:
        return ""
    day = str(day or "")[:10]

    def key_for(path: str) -> str:
        for key, rel in NAMED_INPUTS.items():
            if path == rel.format(d=day):
                return key
        return Path(path).name

    parts = []
    for path in sorted(commits, key=lambda item: (key_for(item), item)):
        sha = str(commits.get(path) or "")
        parts.append(f"{key_for(path)}={sha[:12]}")
    return ",".join(parts)


def _source_commits(day: str, manifest: dict | None = None) -> dict:
    man = manifest if manifest is not None else fmf.load_manifest()
    meta = (man.get("snapshots") or {}).get(str(day)[:10]) or {}
    commits = meta.get("source_commits") or {}
    return commits if isinstance(commits, dict) else {}


def net_returns_15bp(dates: list[str], names: list[str] | None = None) -> dict:
    """Session percent under flat 15bp for each recipe, start, and day.

    Same books as the frozen futubull ledgers, with the per-side fee
    swapped. Nothing is written back to a ledger or snapshot. A failed
    recipe is absent from the map so its cells stay empty.
    """
    from . import factor_mine_book as fmb
    from . import factor_mine_combo as fmc
    from . import paper_trade as pt

    days = [str(d)[:10] for d in dates]
    panel = assemble_panel(days)
    bars = _full_bars(panel)
    regime = fmb.load_regime()
    by_name = {r.get("name"): r for r in retro_recipes() if r.get("name")}
    wanted = list(names) if names is not None else sorted(by_name)
    fees = fm.pt_fees()
    out: dict[tuple[str, str, str], float | None] = {}
    orig = pt.order_fees
    pt.order_fees = flat_15bp_order_fees
    try:
        for i, name in enumerate(wanted):
            rec = by_name.get(name)
            if rec is None:
                continue
            members = [by_name[m] for m in (rec.get("members") or []) if m in by_name]
            if rec.get("members") and len(members) != len(rec.get("members") or []):
                print(f"[retro] 15bp skip {name}: missing member", flush=True)
                continue
            try:
                for start in days:
                    if rec.get("members"):
                        pool = rec.get("pool") or "shared"
                        weights = list(rec.get("weights") or [1] * len(members))
                        if pool == "split":
                            book = fmc.simulate_split(
                                panel, members, weights, start=start,
                                bars=bars, fees=fees, regime=regime, name=name,
                            )
                        else:
                            book = fmc.simulate_shared(
                                panel, members, weights, start=start,
                                bars=bars, fees=fees, regime=regime,
                                net=rec.get("net") or "priority", name=name,
                            )
                    else:
                        book = fmb.simulate_book(
                            panel, rec, start=start, bars=bars, fees=fees,
                            regime=regime,
                        )
                    for row in book.get("daily") or []:
                        stamp = str(row.get("date") or "")[:10]
                        if stamp < start:
                            continue
                        out[(name, start, stamp)] = daily_return_pct(row)
            except Exception as e:  # noqa: BLE001
                print(f"[retro] 15bp failed {name}: {e}", flush=True)
            if (i + 1) % 20 == 0 or i + 1 == len(wanted):
                print(f"[retro] 15bp {i + 1}/{len(wanted)}", flush=True)
    finally:
        pt.order_fees = orig
    return out


def write_daily_returns(path: Path | None = None,
                        dates: list[str] | None = None,
                        *,
                        catalog: dict | None = None,
                        flat_returns: dict | None = None,
                        manifest: dict | None = None) -> int:
    """One row per recipe × start_date × day, including held days.

    Columns: recipe, recipe_created_date, start_date, D,
    net_ret_futubull, net_ret_15bp, day_status, source_shas,
    timing_clean, news_clean, reads_news, fires, untestable.
    A held or missing day has empty returns. A real flat session is 0.
    A start whose ledgers contain zero fills is untestable: its returns
    are blank, not 0. timing_clean is the day's label (pit_rebuilt only).
    news_clean is false on a #331 quarantine date. reads_news is the
    recipe, repeated on every row. The Futubull session percent uses the
    prior session's equity for that start, so a resumed split row that
    stored yesterday as $10k is restated. Ledgers are reduced one file
    at a time.
    """
    import csv

    dest = Path(path or DAILY_RETURNS_CSV)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dates is not None:
        days = [str(d)[:10] for d in dates]
    else:
        days = sorted(set(SESSIONS) | set(fmf._ledger_dates()))
    book = catalog if catalog is not None else load_recipe_catalog()
    names = sorted(book)
    flat = flat_returns if flat_returns is not None else net_returns_15bp(days, names)
    by_name = {r.get("name"): r for r in retro_recipes() if r.get("name")}
    reads = {name: recipe_reads_news(name, by_name) for name in names}
    quarantined = news_quarantine_dates()
    fut: dict[tuple[str, str, str], float | None] = {}
    fires: dict[tuple[str, str, str], int] = {}
    fire_total: dict[tuple[str, str], int] = {}
    prior_equity: dict[tuple[str, str], float] = {}
    present: set[tuple[str, str, str]] = set()
    status_of: dict[str, str] = {}
    shas_of: dict[str, str] = {}
    for day in days:
        ledger = fmf.read_ledger(day)
        status_of[day] = ledger_day_status(ledger)
        shas_of[day] = format_source_shas(day, _source_commits(day, manifest))
        recipes = (ledger or {}).get("recipes") or {}
        for name in names:
            starts = (recipes.get(name) or {}).get("starts") or {}
            for start, block in starts.items():
                daily = (block or {}).get("daily") if isinstance(block, dict) else None
                if not isinstance(daily, dict) or daily.get("equity") is None:
                    continue
                start = str(start)[:10]
                key = (name, start, day)
                chain = (name, start)
                present.add(key)
                fut[key] = session_return_from_equity(daily, prior_equity.get(chain))
                prior_equity[chain] = float(daily["equity"])
                n_fills = _fill_count(block if isinstance(block, dict) else None)
                fires[key] = n_fills
                fire_total[chain] = fire_total.get(chain, 0) + n_fills
        del ledger
    n = 0
    held = 0
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(list(DAILY_RETURN_FIELDS))
        for name in names:
            created = str(book.get(name) or "")[:10]
            for start in days:
                quiet = fire_total.get((name, start), None) == 0
                for day in days:
                    if day < start:
                        continue
                    key = (name, start, day)
                    status = status_of.get(day) or "held"
                    flags = [
                        format_bool(timing_clean_day(status)),
                        format_bool(news_clean_day(day, quarantined)),
                        format_bool(reads.get(name, False)),
                    ]
                    missing = status in ("held", "skipped") or key not in present
                    if missing:
                        cell = status if status in ("held", "skipped") else "held"
                        writer.writerow([
                            name, created, start, day, "", "", cell,
                            shas_of.get(day) or "", *flags, "", "",
                        ])
                        held += 1
                    else:
                        day_fires = fires.get(key, 0)
                        if quiet:
                            fut_cell, flat_cell, mark = "", "", "true"
                        else:
                            fut_cell = format_net(fut.get(key))
                            flat_cell = format_net(flat.get(key))
                            mark = "false"
                        writer.writerow([
                            name, created, start, day,
                            fut_cell, flat_cell,
                            status_of.get(day) or "held",
                            shas_of.get(day) or "", *flags,
                            str(day_fires), mark,
                        ])
                    n += 1
    print(f"[retro] daily returns {n} rows held={held} {dest}", flush=True)
    return n


def _compound_pct(values: list[float]) -> float | None:
    """Chain session percents. An empty window has no return."""
    if not values:
        return None
    equity = 1.0
    for value in values:
        equity *= 1.0 + float(value) / 100.0
    return round(100.0 * (equity - 1.0), 3)


def _parse_net(cell: str) -> float | None:
    text = str(cell or "").strip()
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if number != number:
        return None
    return number


def _parse_fires(row: dict) -> int | None:
    """None when the row has no fires cell. Zero is a real count."""
    if "fires" not in row:
        return None
    text = str(row.get("fires") if row.get("fires") is not None else "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def recipe_clean_windows(rows: list[dict]) -> list[dict]:
    """One row per recipe: all days, timing_clean days, and both-clean days.

    The book is the earliest ``start_date`` for that recipe. A blank
    return is left out of the chain and out of ``n``. Both-clean is
    filled only when ``reads_news`` is true. A window whose fires sum
    to zero is untestable: the return is blank even if the cells say 0.
    """
    by_recipe: dict[str, list[dict]] = {}
    for row in rows:
        by_recipe.setdefault(str(row.get("recipe") or ""), []).append(row)
    out = []
    for name in sorted(by_recipe):
        series = by_recipe[name]
        starts = [str(r.get("start_date") or "")[:10] for r in series if r.get("start_date")]
        if not starts:
            continue
        start = min(starts)
        book = [r for r in series if str(r.get("start_date") or "")[:10] == start]
        book.sort(key=lambda r: str(r.get("D") or ""))
        reads = str((book[0].get("reads_news") if book else "") or "").lower() == "true"

        def chain(pred) -> tuple[int | None, float | None, float | None, int | None, bool]:
            fut_vals = []
            flat_vals = []
            fires_sum = 0
            saw_fires = False
            n_book = 0
            for row in book:
                if not pred(row):
                    continue
                counted = _parse_fires(row)
                if counted is not None:
                    fires_sum += counted
                    saw_fires = True
                    n_book += 1
                fut = _parse_net(row.get("net_ret_futubull") or "")
                flat = _parse_net(row.get("net_ret_15bp") or "")
                if fut is None and flat is None:
                    continue
                if fut is not None:
                    fut_vals.append(fut)
                if flat is not None:
                    flat_vals.append(flat)
            if saw_fires and fires_sum == 0:
                return n_book, None, None, 0, True
            n = max(len(fut_vals), len(flat_vals))
            fires_out = fires_sum if saw_fires else None
            return n, _compound_pct(fut_vals), _compound_pct(flat_vals), fires_out, False

        all_n, all_fut, all_flat, all_fires, untestable = chain(lambda _r: True)
        timing_n, timing_fut, timing_flat, timing_fires, _timing_quiet = chain(
            lambda r: str(r.get("timing_clean") or "").lower() == "true")
        if reads:
            clean_n, clean_fut, clean_flat, clean_fires, _clean_quiet = chain(
                lambda r: str(r.get("timing_clean") or "").lower() == "true"
                and str(r.get("news_clean") or "").lower() == "true")
        else:
            clean_n = clean_fut = clean_flat = clean_fires = None
        out.append({
            "recipe": name,
            "reads_news": reads,
            "start": start,
            "untestable": untestable,
            "all_n": all_n,
            "all_fires": all_fires,
            "all_futubull": all_fut,
            "all_15bp": all_flat,
            "timing_n": timing_n,
            "timing_fires": timing_fires,
            "timing_futubull": timing_fut,
            "timing_15bp": timing_flat,
            "clean_n": clean_n,
            "clean_fires": clean_fires,
            "clean_futubull": clean_fut,
            "clean_15bp": clean_flat,
        })
    return out


def format_window_pct(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.3f}"


def format_fires(value: int | None) -> str:
    if value is None:
        return ""
    return str(int(value))


def clean_window_markdown(rows: list[dict]) -> str:
    """Markdown table of each recipe on the three windows."""
    windows = recipe_clean_windows(rows)
    lines = [
        "## Clean windows",
        "",
        "`timing_clean` is true only on `pit_rebuilt` days. "
        "`news_clean` is false on the 18 sessions in "
        "`data/quarantine_sessions.json` from #331 "
        "(stale dated news and undated Finviz headlines). "
        "`reads_news` is true when the recipe, or a combo member, "
        "gates on news, a headline, the digest, a catalyst, the judge, "
        "or map-heat. The both-clean window is filled only for those recipes. "
        "Each book is the earliest start date. A blank return is omitted. "
        "Returns are the compounded session percents already in the CSV. "
        "Days that fail the flag are left out of the chain. The calendar "
        "is not rebuilt. `fires` counts BUY, SHORT, SELL, and COVER. "
        "A window with zero fires is untestable: the return is blank, "
        "and that recipe is left out of rankings and the luck test.",
        "",
        "| recipe | reads_news | untestable | all n | all fires | all futubull % | all 15bp % | timing n | timing fires | timing futubull % | timing 15bp % | both n | both fires | both futubull % | both 15bp % |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in windows:
        both_n = "" if row["clean_n"] is None else str(row["clean_n"])
        lines.append(
            f"| `{row['recipe']}` | {format_bool(row['reads_news'])} | "
            f"{format_bool(row['untestable'])} | "
            f"{row['all_n']} | {format_fires(row['all_fires'])} | "
            f"{format_window_pct(row['all_futubull'])} | "
            f"{format_window_pct(row['all_15bp'])} | "
            f"{row['timing_n']} | {format_fires(row['timing_fires'])} | "
            f"{format_window_pct(row['timing_futubull'])} | "
            f"{format_window_pct(row['timing_15bp'])} | "
            f"{both_n} | {format_fires(row['clean_fires'])} | "
            f"{format_window_pct(row['clean_futubull'])} | "
            f"{format_window_pct(row['clean_15bp'])} |"
        )
    return "\n".join(lines) + "\n"


def splice_clean_windows(text: str, section: str) -> str:
    """Replace an existing clean-window section, or append it."""
    marker = "## Clean windows"
    if marker in text:
        head = text.split(marker, 1)[0].rstrip() + "\n\n"
        return head + section
    body = text.rstrip() + "\n\n" if text.strip() else ""
    return body + section


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
            "Open cross-check uses theme-radar `{D}.raw.csv` Finviz Open "
            "(`finviz_raw`) when the latest git commit of that file is before "
            "the next session's 09:30 ET. The time is the GitHub commits API "
            "committer timestamp (`commits?path=data/snapshots/{D}.raw.csv`). "
            "There is no lower bound; the snapshot workflow starts after the "
            "close. From 2026-09-24 a present scrape_ts must also be before "
            "that next open. The stamp is the slim `{D}.csv` scrape_ts column, "
            "or `scrape_ts_utc` in theme-radar `manifest.json`. It is not read "
            "from raw.csv. A missing raw export, a commit at or after the next "
            "open, a late scrape_ts, or a hash mismatch uses Stooq. From "
            "2026-09-25 the slim `{D}.csv` Open column is the next file "
            "(`finviz_snapshot`) under the same upper bound. `current.csv` is "
            "not a source. Webull paper fills stay a third check. On this "
            "window every session except 2026-08-27 uses `finviz_raw`. "
            "2026-08-27 has no raw export, so it uses Stooq. The log is "
            "`data/factor_mine/open_source_log.csv` (source, commit sha, "
            "commit time). Per-recipe session returns for the shuffle test "
            "are `data/factor_mine/daily_returns.csv` (recipe, "
            "recipe_created_date, start_date, D, net_ret_futubull, "
            "net_ret_15bp, day_status, source_shas, timing_clean, "
            "news_clean, reads_news, fires, untestable). One row per recipe, "
            "start date, and day. A held or missing day is `held` with empty "
            "returns, never 0. timing_clean is true only on pit_rebuilt days. "
            "news_clean is false on the #331 quarantine dates. fires counts "
            "BUY, SHORT, SELL, and COVER. A start with zero fires is "
            "untestable and its returns are blank."
        ),
    }
    write_baselines(payload)
    return payload


def write_report(classed: list[dict], scores: list[dict],
                 raw_diff: dict | None = None) -> None:
    rebuilt = [c["date"] for c in classed if c["label"] == "pit_rebuilt"]
    incomplete = [c for c in classed if c["label"] == "incomplete_pit"]
    held = [c for c in classed if c["label"] == "held"]
    skipped = [c for c in classed if c["label"] == "skipped"]
    lines = [
        "# Factor Mine retroactive point-in-time rebuild",
        "",
        f"Window `{SESSIONS[0]}` → `{SESSIONS[-1]}`. "
        "Each D-dated packet is the last git commit at or before D 09:30 ET. "
        "A named input that exists only in a later commit is withheld.",
        "",
        "Yahoo `auto_adjust=False` daily bars are the only price source and "
        "are used as stored (split-adjusted, dividends not applied). A "
        "Finviz or Stooq disagreement is a warning and does not hold the day. "
        "raw.csv fills a name only when Yahoo has no print at all, and that "
        "name is tagged single_source. A name with no print or too few "
        "earlier bars is dropped. The day locks unless nobody is rankable.",
        "",
        f"- pit_rebuilt: {len(rebuilt)}",
        f"- incomplete_pit: {len(incomplete)}",
        f"- held: {len(held)}",
        f"- skipped: {len(skipped)}",
        "",
        "## Held and skipped days",
        "",
        "A day is not held because a print is missing or because Yahoo "
        "disagrees with Finviz or Stooq. The only skip is zero rankable "
        "names. A held row here is a ledger failure, not a price gap.",
        "",
    ]
    for info in held:
        reason = info.get("hold_reason") or ""
        lines.append(f"- `{info['date']}` held {reason}")
    for info in skipped:
        n_drop = info.get("unrankable_n")
        lines.append(f"- `{info['date']}` skipped no rankable names dropped={n_drop}")
    if not held and not skipped:
        lines.append("- (none)")
    lines += ["", "## Dropped names", ""]
    for info in classed:
        lines.append(
            f"- `{info['date']}` dropped {info.get('unrankable_n') or 0}"
            + (f" / {info.get('unrankable_share')}" if info.get("unrankable_share") is not None else "")
        )
    if raw_diff:
        lines += ["", "## raw.csv versus Yahoo raw", ""]
        lines.append(
            "Compared on names that have both a Yahoo raw print and a "
            "commit-guarded raw.csv print. The Yahoo download is not stored."
        )
        for field in ("open", "close"):
            stats = (raw_diff.get(field) or {})
            lines.append(
                f"- {field}: n={stats.get('n')} "
                f"median abs={stats.get('median_abs')} "
                f"max abs={stats.get('max_abs')} "
                f"median pct={stats.get('median_pct')} "
                f"max pct={stats.get('max_pct')}"
            )
    lines += [
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
        "(Yahoo `auto_adjust=False` split-adjusted bars, used as stored, "
        "locked, first bar wins). "
        "Split and dividend factors: `data/factor_mine/retro_prices/actions.parquet` "
        "(dated, keep-first). Indicators apply only events with ex-date before D. "
        "The live `data/prices` tape was not rewritten.",
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
        "raw_vs_yahoo": raw_diff or {},
    }, indent=2), encoding="utf-8")
    print(f"[retro] wrote {REPORT_MD}", flush=True)


def _held_ledger(date: str, reason: str) -> dict:
    return {
        "date": date,
        "origin": "retro_pit",
        "label": "held",
        "reason": reason,
        "recipes": {},
        "code_sha": fmf.code_sha(),
    }


def _mark_candidates(date: str, provenance: dict, names: list[str]) -> tuple[list[str], list[str]]:
    """Tag unrankable and single_source names. Return (unrankable, rankable)."""
    single = SINGLE_SOURCE.get(str(date)[:10], set())
    bad = []
    good = []
    for ticker in names:
        if is_unrankable(ticker, date):
            bad.append(ticker)
        else:
            good.append(ticker)
    bad_set = set(bad)
    for row in provenance.get("names") or []:
        ticker = row.get("ticker")
        if ticker in bad_set:
            row["unrankable"] = True
        if ticker in single:
            row["single_source"] = True
    provenance["unrankable_n"] = len(bad)
    provenance["unrankable_share"] = (
        round(len(bad) / len(names), 6) if names else None
    )
    provenance["single_source_n"] = sum(
        1 for row in (provenance.get("names") or []) if row.get("single_source")
    )
    return bad, good


def review_day(date: str, info: dict, dest: Path,
               history: dict | None = None) -> tuple[list[dict], dict]:
    """Drop names Yahoo cannot rank. A price disagreement is a warning.

    The remaining names are ranked and the day locks. Zero rankable
    names skip the day. Finviz and Stooq never hold it. A raw.csv fill
    is only a name Yahoo has no print for, and that name is single_source.
    """
    materialize(date, dest, history)
    with overlay_inputs(dest):
        provenance = fmf.candidate_provenance(date, list(SESSIONS))
        names = [row["ticker"] for row in provenance.get("names") or []]
    missing = recover_session_bars(date, names)
    with overlay_inputs(dest):
        fmf.reset_price_memory()
        bad, good = _mark_candidates(date, provenance, names)
        fmf.write_candidates(date, provenance, restate=True)
        info["unrankable_n"] = len(bad)
        info["unrankable_share"] = provenance.get("unrankable_share")
        info["missing_prices"] = list(missing)
        info["dropped"] = []
        for ticker in bad:
            info["dropped"].append(drop_record(ticker, date) or {
                "ticker": ticker,
                "missing": ["unrankable"],
                "reason": "unrankable",
            })
        rows: list[dict] = []
        if info["label"] == "pit_rebuilt":
            dropped = set(bad)
            rows = [
                row for row in _panel_rows(date)
                if row.get("ticker") not in dropped
                and not is_unrankable(row.get("ticker"), date)
            ]
        traded = list(fmf.paper_fills(date))
        single = SINGLE_SOURCE.get(str(date)[:10], set())
        check = sorted({
            str(t).strip().upper()
            for t in list(good) + traded + [r.get("ticker") for r in rows]
            if t and str(t).strip().upper() not in single
        })
        # Comparison only. A missing reference or a disagreement does not
        # hold the day, and Stooq is not fetched to fill a bar.
        gaps = fmf.session_cross_check(date, check, fetch_stooq=False)
        decision = fmf.LAST_OPEN_SOURCE.get(str(date)[:10])
        if decision:
            fmf.write_open_source_row(date, decision)
    disagreements = [gap for gap in gaps if not gap.get("missing")]
    info["cross_check_n"] = len(disagreements)
    info["cross_check"] = list(disagreements[:12])
    if disagreements:
        print(
            f"[retro] WARNING {date} price cross-check "
            f"gaps={len(disagreements)} "
            f"sample={disagreements[:4]}",
            flush=True,
        )
    if info["label"] != "pit_rebuilt":
        print(f"[retro] carry {date}", flush=True)
        return [], provenance
    if not good:
        info["label"] = "skipped"
        info["skip_reason"] = "no rankable names"
        print(
            f"[retro] skip {date} no rankable names "
            f"dropped={len(bad)}/{len(names)}",
            flush=True,
        )
        return [], provenance
    print(
        f"[retro] built {date} rows={len(rows)} "
        f"dropped={len(bad)}/{len(names)}",
        flush=True,
    )
    return rows, provenance


def rebuild(*, fetch: bool = True) -> dict:
    """Classify, lock raw bars, write snapshots and ledgers, score HOT4."""
    history = load_histories()
    classed = [classify_day(d, history) for d in SESSIONS]
    for info in classed:
        late = ",".join(info["missing_late"]) or "-"
        print(f"[retro] {info['date']} {info['label']} late={late}", flush=True)
    if _is_raw_lock() and not fetch:
        prices_sha = _read_meta().get("sha256")
    else:
        prices_sha = fetch_raw_bars()
    tl.PRICE_STORE = RETRO_STORE
    fmf.reset_price_memory()
    raw_diff: dict = {}
    if os.environ.get("FM_RETRO_SKIP_VALIDATE") != "1":
        try:
            raw_diff = validate_raw_vs_yahoo(candidate_tickers())
        except Exception as exc:  # noqa: BLE001
            print(f"[retro] raw vs yahoo skipped: {exc}", flush=True)
            raw_diff = {"error": str(exc)}
    import tempfile
    work = Path(tempfile.mkdtemp(prefix="fm-retro-"))
    panel = _empty_panel(list(SESSIONS))
    recipes = retro_recipes()
    print(f"[retro] recipes={len(recipes)}", flush=True)
    for info in classed:
        date = info["date"]
        rows, provenance = review_day(date, info, work, history)
        prices_sha = _read_meta().get("sha256") or prices_sha
        snap = snapshot_for(date, info, rows, prices_sha, provenance)
        fmf.write_snapshot(date, snap, restate=True)
        _stamp_manifest(date, info)
        panel["by_date"][date] = list(snap.get("rows") or [])
        panel["rows"] = [r for r in panel["rows"] if r.get("date") != date]
        panel["rows"].extend(panel["by_date"][date])
        panel["n_rows"] = len(panel["rows"])
        # Ledger sees the sessions landed so far so start-books exist.
        landed = [d for d in SESSIONS if d <= date]
        day_panel = assemble_panel(landed)
        bars = _bars_for(day_panel, date)
        if info["label"] in ("held", "skipped"):
            ledger = _held_ledger(date, info.get("skip_reason") or info.get("hold_reason") or info["label"])
            ledger["label"] = info["label"]
        else:
            try:
                ledger = fmf.build_ledger(
                    day_panel, {}, recipes, date, bars,
                )
            except fmf.HoldDay as e:
                print(f"[retro] ledger hold {date}: {e}", flush=True)
                info["label"] = "held"
                ledger = _held_ledger(date, str(e))
                snap = snapshot_for(date, info, [], prices_sha, provenance)
                fmf.write_snapshot(date, snap, restate=True)
                _stamp_manifest(date, info)
        ledger["label"] = info["label"]
        ledger["code_sha"] = ledger.get("code_sha") or fmf.code_sha()
        fmf.write_ledger(date, ledger, restate=True)
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
    write_report(classed, scores, raw_diff=raw_diff)
    return {"classed": classed, "scores": scores, "prices_sha": prices_sha}


def main() -> None:
    rebuild(fetch=os.environ.get("FM_RETRO_NO_FETCH") != "1")


if __name__ == "__main__":
    main()
